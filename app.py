from flask import Flask, render_template, request, send_file
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile
import time

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10MB max

# Create directories
for folder in ['uploads', 'sorted', 'consolidated', 'zips']:
    os.makedirs(folder, exist_ok=True)

def safe_process_pdf(input_path, output_dir):
    """Process PDF with memory optimization"""
    try:
        print(f"Starting to process: {input_path}")
        
        # ใช้ PdfReader ด้วย stream mode เพื่อประหยัด memory
        reader = PdfReader(input_path)
        total_pages = len(reader.pages)
        
        writers = {}
        current_order = None
        current_sku = None
        
        print(f"Total pages to process: {total_pages}")
        
        for page_num in range(total_pages):
            if page_num % 20 == 0:
                print(f"Processing page {page_num + 1}/{total_pages}")
                # Force garbage collection ทุก 20 หน้า
                import gc
                gc.collect()
            
            try:
                # ใช้ pdfplumber เฉพาะเมื่อจำเป็น
                with pdfplumber.open(input_path) as pdf:
                    pdf_page = pdf.pages[page_num]
                    text = pdf_page.extract_text() or ""
            except:
                text = ""
            
            # Extract order ID
            order_match = re.search(r"Order ID[: ]+(\\d+)", text)
            if order_match:
                current_order = order_match.group(1)
            
            # Extract SKU - simplified logic
            sku_match = re.search(r"Seller SKU[:\\s]+(\\S+)", text)
            if sku_match:
                current_sku = sku_match.group(1)
            
            if not current_sku:
                # Fallback SKU extraction
                for line in text.split('\\n'):
                    if 'SKU' in line or 'สินค้า' in line:
                        parts = line.split()
                        if len(parts) > 1:
                            current_sku = parts[-1]
                            break
            
            if not current_sku:
                current_sku = f"page_{page_num + 1}"
            
            # Clean SKU
            current_sku = re.sub(r'[^\\w\\d-]', '_', str(current_sku))
            
            if current_order and current_sku:
                key = f"{current_order}_{current_sku}"
                if key not in writers:
                    writers[key] = PdfWriter()
                
                # Add page to writer
                writers[key].add_page(reader.pages[page_num])
        
        # Save all writers
        saved_count = 0
        for key, writer in writers.items():
            if len(writer.pages) > 0:
                output_path = os.path.join(output_dir, f"{key}.pdf")
                with open(output_path, "wb") as f:
                    writer.write(f)
                saved_count += 1
        
        print(f"Successfully created {saved_count} sorted PDFs")
        return saved_count
        
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return 0

def simple_consolidate(input_dir, output_dir):
    """Consolidate by first SKU found for each order"""
    try:
        order_sku_map = {}
        files_by_sku = {}
        
        # First pass: map orders to SKUs
        for filename in os.listdir(input_dir):
            if filename.endswith('.pdf'):
                parts = filename.split('_', 1)
                if len(parts) == 2:
                    order_id, sku_with_ext = parts
                    sku = sku_with_ext.replace('.pdf', '')
                    
                    if order_id not in order_sku_map:
                        order_sku_map[order_id] = sku
        
        # Second pass: group by primary SKU
        for filename in os.listdir(input_dir):
            if filename.endswith('.pdf'):
                parts = filename.split('_', 1)
                if len(parts) == 2:
                    order_id = parts[0]
                    primary_sku = order_sku_map.get(order_id)
                    
                    if primary_sku:
                        if primary_sku not in files_by_sku:
                            files_by_sku[primary_sku] = []
                        files_by_sku[primary_sku].append(filename)
        
        # Consolidate files
        consolidated_count = 0
        for sku, filenames in files_by_sku.items():
            writer = PdfWriter()
            
            for filename in filenames:
                try:
                    reader = PdfReader(os.path.join(input_dir, filename))
                    for page in reader.pages:
                        writer.add_page(page)
                except Exception as e:
                    print(f"Error adding {filename}: {e}")
            
            if len(writer.pages) > 0:
                output_path = os.path.join(output_dir, f"{sku}.pdf")
                with open(output_path, "wb") as f:
                    writer.write(f)
                consolidated_count += 1
        
        return consolidated_count
        
    except Exception as e:
        print(f"Consolidation error: {e}")
        return 0

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>PDF Processor</title>
        <style>
            body { font-family: Arial; max-width: 600px; margin: 50px auto; padding: 20px; }
            .upload-area { border: 2px dashed #007bff; padding: 40px; text-align: center; margin: 20px 0; }
            .btn { background: #007bff; color: white; padding: 12px 24px; border: none; cursor: pointer; }
            .info { background: #f8f9fa; padding: 15px; margin: 20px 0; }
        </style>
    </head>
    <body>
        <h1>📄 PDF Order Processor</h1>
        
        <div class="info">
            <strong>Optimized for large PDFs (200+ pages)</strong><br>
            Upload PDF order documents to sort by Order ID and SKU
        </div>

        <form action="/process" method="post" enctype="multipart/form-data">
            <div class="upload-area">
                <h3>Select PDF Files</h3>
                <input type="file" name="pdf_files" accept=".pdf" multiple required>
                <br><br>
                <button type="submit" class="btn">Process PDFs</button>
            </div>
        </form>

        <div class="info">
            <strong>Process:</strong>
            <ol>
                <li>Upload PDF order documents</li>
                <li>System extracts Order IDs and SKUs</li>
                <li>Groups pages by Order ID + SKU</li>
                <li>Consolidates by primary SKU</li>
                <li>Download consolidated files</li>
            </ol>
        </div>
    </body>
    </html>
    '''

@app.route('/process', methods=['POST'])
def process_pdfs():
    try:
        if 'pdf_files' not in request.files:
            return "No files selected", 400
        
        files = request.files.getlist('pdf_files')
        uploaded_files = []
        
        # Clear previous files
        for folder in ['sorted', 'consolidated', 'zips']:
            for file in os.listdir(folder):
                os.remove(os.path.join(folder, file))
        
        # Upload files
        for file in files:
            if file and file.filename.endswith('.pdf'):
                filename = f"upload_{int(time.time())}_{len(uploaded_files)}.pdf"
                filepath = os.path.join('uploads', filename)
                file.save(filepath)
                uploaded_files.append(filepath)
        
        if not uploaded_files:
            return "No valid PDF files uploaded", 400
        
        # Process each PDF
        total_sorted = 0
        for filepath in uploaded_files:
            sorted_count = safe_process_pdf(filepath, 'sorted')
            total_sorted += sorted_count
        
        # Consolidate
        consolidated_count = simple_consolidate('sorted', 'consolidated')
        
        # Create zip
        zip_path = os.path.join('zips', 'results.zip')
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file in os.listdir('consolidated'):
                zipf.write(os.path.join('consolidated', file), file)
        
        return f'''
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: green;">✅ Processing Complete</h1>
            <div style="background: #f0f8ff; padding: 20px; border-radius: 10px;">
                <p><strong>Files Processed:</strong> {len(uploaded_files)}</p>
                <p><strong>Sorted Groups:</strong> {total_sorted}</p>
                <p><strong>Consolidated Files:</strong> {consolidated_count}</p>
            </div>
            <br>
            <a href="/download/results.zip" style="
                background: #28a745; 
                color: white; 
                padding: 12px 24px; 
                text-decoration: none; 
                border-radius: 5px;
                display: inline-block;
            ">📥 Download Results</a>
            <br><br>
            <a href="/">🔄 Process More Files</a>
        </div>
        '''
        
    except Exception as e:
        return f'''
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: red;">❌ Processing Error</h1>
            <p>Error: {str(e)}</p>
            <a href="/">🔄 Try Again</a>
        </div>
        '''

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_file(
            os.path.join('zips', filename),
            as_attachment=True,
            download_name=f"processed_pdfs_{time.strftime('%Y%m%d')}.zip"
        )
    except Exception as e:
        return f"Download error: {str(e)}"

@app.route('/health')
def health():
    return {'status': 'ok', 'time': time.time()}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
