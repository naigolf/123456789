from flask import Flask, render_template, request, redirect, url_for, send_file
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile
import threading
import time

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

UPLOAD_FOLDER = 'uploads'
SORTED_FOLDER = 'sorted'
CONSOLIDATED_FOLDER = 'consolidated_by_sku'
ZIPPED_FOLDER = 'zipped_archives'

# Ensure necessary folders exist
for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
    os.makedirs(folder, exist_ok=True)

def cleanup_old_files():
    """ลบไฟล์เก่าที่เกิน 1 ชั่วโมง"""
    try:
        current_time = time.time()
        for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path):
                    # ลบไฟล์ที่เก่ากว่า 1 ชั่วโมง
                    if current_time - os.path.getctime(file_path) > 3600:
                        os.remove(file_path)
    except Exception as e:
        print(f"Cleanup error: {e}")

# --- PDF Sorting Logic (Optimized) ---
def sort_pdf_by_order_and_sku(input_pdf_path, output_dir):
    """Process PDF with progress tracking and error handling"""
    try:
        reader = PdfReader(input_pdf_path)
        writers = {}
        last_order_id = None
        last_sku = None

        def extract_order_id(text):
            match = re.search(r"Order ID[: ]+(\\d+)", text)
            return match.group(1) if match else None

        def extract_barcode(text):
            match = re.search(r"\\b\\d{10,18}\\b", text)
            return match.group(0) if match else None

        print(f"Processing PDF with {len(reader.pages)} pages...")
        
        with pdfplumber.open(input_pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                # แสดง progress ทุก 50 หน้า
                if i % 50 == 0:
                    print(f"Processing page {i+1}/{len(reader.pages)}")
                
                text = page.extract_text() or ""
                lines = text.splitlines()

                # Extract Order ID
                order_id = extract_order_id(text)
                if order_id:
                    last_order_id = order_id
                else:
                    order_id = last_order_id

                # Extract barcode
                barcode = extract_barcode(text)

                sku = None

                # If no barcode, use previous SKU
                if barcode is None and last_sku is not None:
                    sku = last_sku
                else:
                    # Find SKU from product table
                    for idx, line in enumerate(lines):
                        if "Product Name" in line and "Seller SKU" in line:
                            if idx + 1 < len(lines):
                                product_line = lines[idx + 1].strip()
                                parts = product_line.split()
                                if len(parts) >= 2:
                                    sku = parts[-2]
                            break

                if not sku:
                    sku = last_sku if last_sku else f"UNKNOWN_{i}"

                sku = sku.replace("/", "_").replace("\\", "_").strip()
                last_sku = sku

                if order_id and sku:
                    group_key = f"{order_id}_{sku}"
                    if group_key not in writers:
                        writers[group_key] = PdfWriter()

                    try:
                        writers[group_key].add_page(reader.pages[i])
                    except Exception as e:
                        print(f"Error adding page {i}: {e}")

        # Save files
        sorted_files_count = 0
        for group_key, writer in writers.items():
            if len(writer.pages) > 0:
                output_file_path = os.path.join(output_dir, f"{group_key}.pdf")
                with open(output_file_path, "wb") as f:
                    writer.write(f)
                sorted_files_count += 1
        
        print(f"Sorted {sorted_files_count} files successfully")
        return sorted_files_count
        
    except Exception as e:
        print(f"Error in sort_pdf_by_order_and_sku: {e}")
        return 0

# --- PDF Consolidation Logic ---
def consolidate_pdfs_by_sku(sorted_dir, consolidated_output_dir):
    """Consolidate PDFs with error handling"""
    try:
        order_id_to_primary_sku_map = {}

        # Map Order IDs to Primary SKUs
        pdf_files = [f for f in os.listdir(sorted_dir) if f.endswith('.pdf')]
        print(f"Found {len(pdf_files)} PDF files to consolidate")
        
        for filename in pdf_files:
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                sku_with_ext = parts[1]
                sku = sku_with_ext.replace('.pdf', '')

                if order_id not in order_id_to_primary_sku_map:
                    order_id_to_primary_sku_map[order_id] = sku

        # Group files by primary SKU and order ID
        grouped_files_by_primary_sku = {}
        for filename in pdf_files:
            file_path = os.path.join(sorted_dir, filename)
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                primary_sku = order_id_to_primary_sku_map.get(order_id)

                if primary_sku:
                    if primary_sku not in grouped_files_by_primary_sku:
                        grouped_files_by_primary_sku[primary_sku] = []
                    grouped_files_by_primary_sku[primary_sku].append((order_id, file_path))

        # Consolidate files by primary SKU
        consolidated_files_count = 0
        for primary_sku, files_list in grouped_files_by_primary_sku.items():
            files_list.sort(key=lambda x: x[0])

            writer = PdfWriter()
            total_pages = 0

            for order_id, file_path in files_list:
                try:
                    reader = PdfReader(file_path)
                    for page in reader.pages:
                        writer.add_page(page)
                        total_pages += 1
                except Exception as e:
                    print(f"Error processing {os.path.basename(file_path)}: {e}")

            if len(writer.pages) > 0:
                output_filename = f"{primary_sku}.pdf"
                output_file_path = os.path.join(consolidated_output_dir, output_filename)
                with open(output_file_path, "wb") as f:
                    writer.write(f)
                consolidated_files_count += 1
                print(f"Consolidated {primary_sku}.pdf with {total_pages} pages")
                
        return consolidated_files_count
        
    except Exception as e:
        print(f"Error in consolidate_pdfs_by_sku: {e}")
        return 0

# --- Zip Archive Creation Logic ---
def create_zip_archive(source_dir, output_zip_path):
    """Create zip archive with progress"""
    try:
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            files = [f for f in os.listdir(source_dir) if f.endswith('.pdf')]
            for i, file in enumerate(files):
                file_path = os.path.join(source_dir, file)
                zipf.write(file_path, file)
                if i % 5 == 0:  # แสดง progress ทุก 5 ไฟล์
                    print(f"Zipping {i+1}/{len(files)} files")
        return True
    except Exception as e:
        print(f"Error creating zip: {e}")
        return False

# --- Flask Routes ---
@app.route('/')
def index():
    cleanup_old_files()  # ทำความสะอาดไฟล์เก่าเมื่อมี request ใหม่
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file upload with progress feedback"""
    try:
        if 'pdf_files' not in request.files:
            return redirect(request.url)

        files = request.files.getlist('pdf_files')
        uploaded_count = 0
        total_sorted_pdfs_count = 0

        # Clear previous files for fresh processing
        for folder in [SORTED_FOLDER, CONSOLIDATED_FOLDER]:
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                if os.path.isfile(item_path):
                    os.remove(item_path)

        # Process each uploaded file
        for file in files:
            if file and file.filename and file.filename.endswith('.pdf'):
                filename = file.filename
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                file.save(file_path)
                uploaded_count += 1
                print(f"Uploaded: {filename}")

                # Sort PDF with timeout protection
                num_sorted = sort_pdf_by_order_and_sku(file_path, SORTED_FOLDER)
                total_sorted_pdfs_count += num_sorted

        # Consolidate PDFs
        if total_sorted_pdfs_count > 0:
            total_consolidated_pdfs_count = consolidate_pdfs_by_sku(SORTED_FOLDER, CONSOLIDATED_FOLDER)
            
            # Create zip archive
            zip_filename = "consolidated_pdfs.zip"
            output_zip_path = os.path.join(ZIPPED_FOLDER, zip_filename)
            zip_success = create_zip_archive(CONSOLIDATED_FOLDER, output_zip_path)
            
            if uploaded_count > 0 and zip_success:
                download_url = url_for('download_zip', filename=zip_filename)
                return f'''
                <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <h1 style="color: #28a745;">✅ Processing Complete!</h1>
                    <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                        <p><strong>Uploaded:</strong> {uploaded_count} PDF files</p>
                        <p><strong>Sorted:</strong> {total_sorted_pdfs_count} order-SKU combinations</p>
                        <p><strong>Consolidated:</strong> {total_consolidated_pdfs_count} primary SKU files</p>
                    </div>
                    <a href="{download_url}" style="display: inline-block; background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">
                        📥 Download Consolidated PDFs
                    </a>
                    <br><br>
                    <a href="/" style="color: #007bff;">🔄 Process more files</a>
                </div>
                '''
            else:
                return '''
                <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <h1 style="color: #dc3545;">❌ Processing Error</h1>
                    <p>There was an error processing your files. Please try again.</p>
                    <a href="/" style="color: #007bff;">🔄 Try again</a>
                </div>
                '''
        else:
            return '''
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <h1 style="color: #ffc107;">⚠️ No PDFs Processed</h1>
                <p>No valid PDF files were found or processed.</p>
                <a href="/" style="color: #007bff;">🔄 Try again</a>
            </div>
            '''
            
    except Exception as e:
        print(f"Upload error: {e}")
        return f'''
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #dc3545;">❌ Server Error</h1>
            <p>An error occurred: {str(e)}</p>
            <a href="/" style="color: #007bff;">🔄 Try again</a>
        </div>
        '''

@app.route('/download/<filename>')
def download_zip(filename):
    """Serve zip file for download"""
    try:
        return send_file(
            os.path.join(ZIPPED_FOLDER, filename), 
            as_attachment=True,
            download_name=f"consolidated_pdfs_{time.strftime('%Y%m%d_%H%M%S')}.zip"
        )
    except Exception as e:
        return f"Error downloading file: {str(e)}"

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    return {'status': 'healthy', 'timestamp': time.time()}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
