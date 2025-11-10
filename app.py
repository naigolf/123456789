from flask import Flask, render_template, request, redirect, url_for, send_file
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
SORTED_FOLDER = 'sorted'
CONSOLIDATED_FOLDER = 'consolidated_by_sku'
ZIPPED_FOLDER = 'zipped_archives'

# Ensure necessary folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(SORTED_FOLDER, exist_ok=True)
os.makedirs(CONSOLIDATED_FOLDER, exist_ok=True)
os.makedirs(ZIPPED_FOLDER, exist_ok=True)

# --- PDF Sorting Logic ---
def sort_pdf_by_order_and_sku(input_pdf_path, output_dir):
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

    with pdfplumber.open(input_pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
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
                                sku = parts[-2]  # Use first Seller SKU only
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
    return sorted_files_count

# --- PDF Consolidation Logic ---
def consolidate_pdfs_by_sku(sorted_dir, consolidated_output_dir):
    order_id_to_primary_sku_map = {}

    # Map Order IDs to Primary SKUs
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                sku_with_ext = parts[1]
                sku = sku_with_ext.replace('.pdf', '')

                if order_id not in order_id_to_primary_sku_map:
                    order_id_to_primary_sku_map[order_id] = sku

    # Group files by primary SKU and order ID
    grouped_files_by_primary_sku = {}
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
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
        files_list.sort(key=lambda x: x[0])  # Sort by order_id

        writer = PdfWriter()

        for order_id, file_path in files_list:
            try:
                reader = PdfReader(file_path)
                for page in reader.pages:
                    writer.add_page(page)
            except Exception as e:
                print(f"Error processing {os.path.basename(file_path)}: {e}")

        if len(writer.pages) > 0:
            output_filename = f"{primary_sku}.pdf"
            output_file_path = os.path.join(consolidated_output_dir, output_filename)
            with open(output_file_path, "wb") as f:
                writer.write(f)
            consolidated_files_count += 1
            
    return consolidated_files_count

# --- Zip Archive Creation Logic ---
def create_zip_archive(source_dir, output_zip_path):
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.basename(file_path))
    return output_zip_path

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
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

    for file in files:
        if file and file.filename and file.filename.endswith('.pdf'):
            filename = file.filename
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(file_path)
            uploaded_count += 1

            # Sort PDF
            num_sorted = sort_pdf_by_order_and_sku(file_path, SORTED_FOLDER)
            total_sorted_pdfs_count += num_sorted

    # Consolidate PDFs
    total_consolidated_pdfs_count = consolidate_pdfs_by_sku(SORTED_FOLDER, CONSOLIDATED_FOLDER)

    # Create zip archive
    zip_filename = f"consolidated_pdfs.zip"
    output_zip_path = os.path.join(ZIPPED_FOLDER, zip_filename)
    create_zip_archive(CONSOLIDATED_FOLDER, output_zip_path)

    if uploaded_count > 0:
        download_url = url_for('download_zip', filename=zip_filename)
        return f'''
        <h1>✅ Processing Complete!</h1>
        <p>Uploaded: {uploaded_count} files</p>
        <p>Sorted: {total_sorted_pdfs_count} PDF files by Order ID and SKU</p>
        <p>Consolidated: {total_consolidated_pdfs_count} PDF files by primary SKU</p>
        <p><a href="{download_url}" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Download Consolidated PDFs</a></p>
        <p><a href="/">Upload more files</a></p>
        '''
    else:
        return '<h1>❌ No PDF files uploaded</h1><p><a href="/">Try again</a></p>'

@app.route('/download/<filename>')
def download_zip(filename):
    return send_file(os.path.join(ZIPPED_FOLDER, filename), as_attachment=True)

# ... โค้ดเดิมทั้งหมด ...

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
