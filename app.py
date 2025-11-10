from flask import Flask, render_template, request, send_file, jsonify
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile
import time
import gc

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10MB max

# Create directories
for folder in ['uploads', 'sorted', 'consolidated', 'zips']:
    os.makedirs(folder, exist_ok=True)

def process_pdf_in_chunks(input_path, output_dir, chunk_size=50):
    """Process PDF in chunks to reduce memory usage"""
    try:
        print(f"Processing PDF in chunks of {chunk_size} pages...")
        
        # Get total pages first
        with open(input_path, 'rb') as f:
            reader = PdfReader(f)
            total_pages = len(reader.pages)
        
        print(f"Total pages: {total_pages}")
        
        writers = {}
        current_order = None
        current_sku = None
        
        # Process in chunks
        for chunk_start in range(0, total_pages, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_pages)
            print(f"Processing chunk {chunk_start + 1}-{chunk_end}")
            
            # Process each page in current chunk
            for page_num in range(chunk_start, chunk_end):
                try:
                    # Extract text using pdfplumber for current page only
                    with pdfplumber.open(input_path) as pdf:
                        pdf_page = pdf.pages[page_num]
                        text = pdf_page.extract_text() or ""
                except Exception as e:
                    print(f"Error extracting text from page {page_num + 1}: {e}")
                    text = ""
                
                # Extract order ID (simple regex)
                order_match = re.search(r"Order\s*ID[:\\s]*(\\d+)", text, re.IGNORECASE)
                if order_match:
                    current_order = order_match.group(1)
                    print(f"Found Order ID: {current_order}")
                
                # Extract SKU (simple regex)
                sku_match = re.search(r"SKU[:\\s]*(\\S+)", text, re.IGNORECASE)
                if not sku_match:
                    sku_match = re.search(r"Seller\\s*SKU[:\\s]*(\\S+)", text, re.IGNORECASE)
                
                if sku_match:
                    current_sku = sku_match.group(1)
                    print(f"Found SKU: {current_sku}")
                
                # Fallback: use first word after "Product"
                if not current_sku:
                    product_match = re.search(r"Product[:\\s]*(\\S+)", text, re.IGNORECASE)
                    if product_match:
                        current_sku = product_match.group(1)
                
                if not current_sku:
                    current_sku = f"page_{page_num + 1}"
                
                # Clean SKU
                current_sku = re.sub(r'[^\\w\\d-]', '_', str(current_sku))[:50]  # Limit length
                
                if current_order and current_sku:
                    key = f"{current_order}_{current_sku}"
                    if key not in writers:
                        writers[key] = PdfWriter()
                    
                    # Add page to writer
                    with open(input_path, 'rb') as f:
                        page_reader = PdfReader(f)
                        writers[key].add_page(page_reader.pages[page_num])
                
                # Force garbage collection every 10 pages
                if page_num % 10 == 0:
                    gc.collect()
            
            # Save current chunk's writers to free memory
            for key, writer in list(writers.items()):
                if len(writer.pages) > 0:
                    output_path = os.path.join(output_dir, f"{key}.pdf")
                    with open(output_path, "wb") as f:
                        writer.write(f)
                    # Clear writer to free memory
                    writers[key] = PdfWriter()
            
            gc.collect()
        
        # Count result files
        result_files = [f for f in os.listdir(output_dir) if f.endswith('.pdf')]
        print(f"Created {len(result_files)} sorted PDFs")
        return len(result_files)
        
    except Exception as e:
        print(f"Error in chunk processing: {str(e)}")
        return 0

def fast_consolidate(input_dir, output_dir):
    """Fast consolidation with minimal memory usage"""
    try:
        # Group by first part of filename (order_id)
        sku_groups = {}
        
        for filename in os.listdir(input_dir):
            if filename.endswith('.pdf'):
                # Extract order_id (first part before underscore)
                order_id = filename.split('_')[0]
                if order_id not in sku_groups:
                    sku_groups[order_id] = []
                sku_groups[order_id].append(filename)
        
        # Consolidate each group
        consolidated_count = 0
        for order_id, filenames in sku_groups.items():
            if filenames:
                # Use first file's SKU as primary SKU
                primary_sku = filenames[0].split('_')[1].replace('.pdf', '')
                writer = PdfWriter()
                
                for filename in filenames:
                    try:
                        file_path = os.path.join(input_dir, filename)
                        reader = PdfReader(file_path)
                        for page in reader.pages:
                            writer.add_page(page)
                    except Exception as e:
                        print(f"Error adding {filename}: {e}")
                
                if len(writer.pages) > 0:
                    output_path = os.path.join(output_dir, f"{primary_sku}.pdf")
                    with open(output_path, "wb") as f:
                        writer.write(f)
                    consolidated_count += 1
                    print(f"Consolidated {primary_sku}.pdf with {len(writer.pages)} pages")
        
        return consolidated_count
        
    except Exception as e:
        print(f"Consolidation error: {e}")
        return 0

def cleanup_directories():
    """Clean up all working directories"""
    for folder in ['uploads', 'sorted', 'consolidated', 'zips']:
        for filename in os.listdir(folder):
            try:
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Error cleaning {file_path}: {e}")

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>PDF Processor - Optimized</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                max-width: 600px; 
                margin: 0 auto; 
                padding: 20px;
                background: #f5f5f5;
            }
            .container {
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .upload-area { 
                border: 2px dashed #007bff; 
                padding: 40px; 
                text-align: center; 
                margin: 20px 0;
                border-radius: 10px;
            }
            .btn { 
                background: #007bff; 
                color: white; 
                padding: 12px 24px; 
                border: none; 
                border-radius: 5px;
                cursor: pointer; 
                font-size: 16px;
            }
            .btn:hover {
                background: #0056b3;
            }
            .btn:disabled {
                background: #6c757d;
                cursor: not-allowed;
            }
            .info { 
                background: #e7f3ff; 
                padding: 15px; 
                margin: 20px 0;
                border-radius: 5px;
            }
            .progress {
                display: none;
                background: #28a745;
                color: white;
                padding: 15px;
                text-align: center;
                border-radius: 5px;
                margin: 20px 0;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📄 PDF Processor (Optimized)</h1>
            
            <div class="info">
                <strong>Optimized for large PDFs on Render Free Plan</strong><br>
                • Processes PDFs in chunks to avoid memory issues<br>
                • Supports 200+ page documents<br>
                • Automatic memory management
            </div>

            <form id="uploadForm" action="/process" method="post" enctype="multipart/form-data">
                <div class="upload-area">
                    <h3>Select PDF Files</h3>
                    <input type="file" name="pdf_files" accept=".pdf" multiple required>
                    <br><br>
                    <button type="submit" class="btn" id="submitBtn">Process PDFs</button>
                </div>
            </form>

            <div class="progress" id="progress">
                ⏳ Processing... This may take a few minutes for large PDFs
            </div>

            <div class="info">
                <strong>How it works:</strong>
                <ol>
                    <li>Upload PDF order documents</li>
                    <li>System processes in small chunks (50 pages at a time)</li>
                    <li>Extracts Order IDs and SKUs automatically</li>
                    <li>Groups and consolidates by primary SKU</li>
                    <li>Download final results as ZIP</li>
                </ol>
            </div>
        </div>

        <script>
            document.getElementById('uploadForm').addEventListener('submit', function() {
                document.getElementById('progress').style.display = 'block';
                document.getElementById('submitBtn').disabled = true;
                document.getElementById('submitBtn').textContent = 'Processing...';
            });
        </script>
    </body>
    </html>
    '''

@app.route('/process', methods=['POST'])
def process_pdfs():
    try:
        cleanup_directories()  # Clean up before starting
        
        if 'pdf_files' not in request.files:
            return "No files selected", 400
        
        files = request.files.getlist('pdf_files')
        uploaded_files = []
        
        # Upload files
        for file in files:
            if file and file.filename.endswith('.pdf'):
                filename = f"upload_{int(time.time())}_{len(uploaded_files)}.pdf"
                filepath = os.path.join('uploads', filename)
                file.save(filepath)
                uploaded_files.append(filepath)
                print(f"Uploaded: {filename}")
        
        if not uploaded_files:
            return "No valid PDF files uploaded", 400
        
        # Process each PDF with small chunk size
        total_sorted = 0
        for filepath in uploaded_files:
            print(f"Starting processing: {filepath}")
            sorted_count = process_pdf_in_chunks(filepath, 'sorted', chunk_size=30)  # Smaller chunks
            total_sorted += sorted_count
            print(f"Finished processing: {filepath}, created {sorted_count} files")
        
        # Consolidate results
        print("Starting consolidation...")
        consolidated_count = fast_consolidate('sorted', 'consolidated')
        print(f"Consolidated {consolidated_count} files")
        
        # Create zip
        print("Creating ZIP file...")
        zip_path = os.path.join('zips', 'results.zip')
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file in os.listdir('consolidated'):
                if file.endswith('.pdf'):
                    zipf.write(os.path.join('consolidated', file), file)
        
        print("Processing complete!")
        
        return f'''
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #28a745;">✅ Processing Complete!</h1>
            <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                <p><strong>Files Uploaded:</strong> {len(uploaded_files)}</p>
                <p><strong>Order-SKU Groups Created:</strong> {total_sorted}</p>
                <p><strong>Consolidated PDFs:</strong> {consolidated_count}</p>
            </div>
            <a href="/download/results.zip" style="
                background: #28a745; 
                color: white; 
                padding: 12px 24px; 
                text-decoration: none; 
                border-radius: 5px;
                display: inline-block;
                font-weight: bold;
            ">📥 Download Consolidated PDFs</a>
            <br><br>
            <a href="/" style="color: #007bff;">🔄 Process More Files</a>
        </div>
        '''
        
    except Exception as e:
        print(f"Processing error: {str(e)}")
        return f'''
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #dc3545;">❌ Processing Error</h1>
            <p>Error: {str(e)}</p>
            <p>This might be due to large file size. Try with smaller PDFs or contact support.</p>
            <a href="/" style="color: #007bff;">🔄 Try Again</a>
        </div>
        '''

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_file(
            os.path.join('zips', filename),
            as_attachment=True,
            download_name=f"processed_pdfs_{time.strftime('%Y%m%d_%H%M')}.zip"
        )
    except Exception as e:
        return f"Download error: {str(e)}"

@app.route('/health')
def health():
    return {'status': 'healthy', 'timestamp': time.time(), 'memory_optimized': True}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # Disable debug mode for production
    app.run(host='0.0.0.0', port=port, debug=False)
