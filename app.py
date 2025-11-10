from flask import Flask, render_template, request, send_file
import os
import PyPDF2
import re
import zipfile
import time

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024  # 5MB max

# Create directories
for folder in ['uploads', 'output']:
    os.makedirs(folder, exist_ok=True)

def extract_text_from_page(pdf_path, page_num):
    """Extract text from single page without loading entire PDF"""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            if page_num < len(reader.pages):
                text = reader.pages[page_num].extract_text() or ""
                return text
    except Exception as e:
        print(f"Error reading page {page_num}: {e}")
    return ""

def simple_pdf_processor(input_path, output_dir):
    """Ultra-light PDF processor for free plan"""
    try:
        print("Starting ultra-light processing...")
        
        # Get basic PDF info first
        with open(input_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            total_pages = len(reader.pages)
        
        print(f"PDF has {total_pages} pages")
        
        # Process maximum 100 pages to avoid memory issues
        max_pages = min(total_pages, 100)
        results = []
        
        for page_num in range(max_pages):
            if page_num % 20 == 0:
                print(f"Processing page {page_num + 1}/{max_pages}")
            
            text = extract_text_from_page(input_path, page_num)
            
            # Simple order ID extraction
            order_id = None
            order_match = re.search(r'Order\s*ID[:\s]*(\d+)', text, re.IGNORECASE)
            if order_match:
                order_id = order_match.group(1)
            
            # Simple SKU extraction
            sku = None
            sku_match = re.search(r'SKU[:\s]*(\S+)', text, re.IGNORECASE)
            if sku_match:
                sku = sku_match.group(1)
            
            if not sku:
                # Try other patterns
                for pattern in [r'Product[:\s]*(\S+)', r'Item[:\s]*(\S+)']:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        sku = match.group(1)
                        break
            
            if not sku:
                sku = f"page_{page_num + 1}"
            
            # Clean values
            if order_id:
                order_id = re.sub(r'[^\w\d]', '', order_id)
            sku = re.sub(r'[^\w\d]', '_', sku)[:30]
            
            if order_id and sku:
                results.append({
                    'order_id': order_id,
                    'sku': sku,
                    'page_num': page_num
                })
        
        print(f"Found {len(results)} order-SKU combinations")
        return results
        
    except Exception as e:
        print(f"Processing error: {e}")
        return []

def create_simple_output(input_path, results, output_dir):
    """Create simple text output instead of PDF manipulation"""
    try:
        if not results:
            return 0
        
        # Group by order_id + sku
        groups = {}
        for result in results:
            key = f"{result['order_id']}_{result['sku']}"
            if key not in groups:
                groups[key] = []
            groups[key].append(result['page_num'])
        
        # Create summary files
        output_files = []
        
        for key, pages in groups.items():
            # Create text summary
            summary_file = os.path.join(output_dir, f"{key}_summary.txt")
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"Order ID: {key.split('_')[0]}\n")
                f.write(f"SKU: {key.split('_')[1]}\n")
                f.write(f"Pages: {len(pages)}\n")
                f.write(f"Page Numbers: {sorted(pages)}\n")
                f.write(f"Original PDF: {os.path.basename(input_path)}\n")
                f.write(f"Processed: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            output_files.append(summary_file)
        
        print(f"Created {len(output_files)} summary files")
        return len(output_files)
        
    except Exception as e:
        print(f"Output creation error: {e}")
        return 0

def cleanup_files():
    """Clean up all files"""
    for folder in ['uploads', 'output']:
        for filename in os.listdir(folder):
            try:
                filepath = os.path.join(folder, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            except Exception as e:
                print(f"Cleanup error: {e}")

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>PDF Order Extractor</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, sans-serif; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            .container {
                max-width: 500px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                padding: 30px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            }
            h1 {
                text-align: center;
                color: #333;
                margin-bottom: 10px;
            }
            .subtitle {
                text-align: center;
                color: #666;
                margin-bottom: 30px;
            }
            .upload-area {
                border: 3px dashed #667eea;
                border-radius: 10px;
                padding: 40px 20px;
                text-align: center;
                margin: 20px 0;
                background: #f8f9ff;
                transition: all 0.3s;
            }
            .upload-area:hover {
                border-color: #764ba2;
                background: #f0f2ff;
            }
            .file-input {
                width: 100%;
                margin: 15px 0;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 5px;
            }
            .btn {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                padding: 15px 30px;
                border-radius: 25px;
                cursor: pointer;
                font-size: 16px;
                font-weight: bold;
                width: 100%;
                transition: transform 0.2s;
            }
            .btn:hover {
                transform: translateY(-2px);
            }
            .btn:disabled {
                opacity: 0.6;
                cursor: not-allowed;
                transform: none;
            }
            .info {
                background: #e7f3ff;
                padding: 15px;
                border-radius: 10px;
                margin: 20px 0;
                font-size: 14px;
            }
            .features {
                list-style: none;
                margin: 15px 0;
            }
            .features li {
                padding: 5px 0;
                color: #555;
            }
            .features li:before {
                content: "✓ ";
                color: #28a745;
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📄 PDF Order Extractor</h1>
            <div class="subtitle">Lightweight • Fast • Free Plan Optimized</div>
            
            <div class="info">
                <strong>Perfect for Render Free Plan</strong>
                <ul class="features">
                    <li>Processes up to 100 pages per PDF</li>
                    <li>Extracts Order IDs & SKUs automatically</li>
                    <li>Creates organized text summaries</li>
                    <li>Ultra-low memory usage</li>
                </ul>
            </div>

            <form action="/process" method="post" enctype="multipart/form-data">
                <div class="upload-area">
                    <h3>📤 Upload PDF Files</h3>
                    <input type="file" name="pdf_files" accept=".pdf" multiple required class="file-input">
                    <button type="submit" class="btn">Extract Order Data</button>
                </div>
            </form>

            <div class="info">
                <strong>How it works:</strong>
                <ol style="margin-left: 20px; margin-top: 10px;">
                    <li>Upload your PDF order documents</li>
                    <li>System scans for Order IDs and SKUs</li>
                    <li>Generates organized text summaries</li>
                    <li>Download results as ZIP file</li>
                </ol>
            </div>
        </div>
    </body>
    </html>
    '''

@app.route('/process', methods=['POST'])
def process_pdfs():
    try:
        cleanup_files()  # Clean up before starting
        
        if 'pdf_files' not in request.files:
            return '''
            <div style="max-width: 500px; margin: 20px auto; padding: 30px; background: white; border-radius: 15px; text-align: center;">
                <h2 style="color: #dc3545;">❌ No files selected</h2>
                <a href="/" style="display: inline-block; margin-top: 20px; padding: 10px 20px; background: #667eea; color: white; text-decoration: none; border-radius: 20px;">Try Again</a>
            </div>
            ''', 400
        
        files = request.files.getlist('pdf_files')
        processed_files = []
        
        for file in files:
            if file and file.filename.lower().endswith('.pdf'):
                # Save uploaded file
                filename = f"doc_{int(time.time())}.pdf"
                filepath = os.path.join('uploads', filename)
                file.save(filepath)
                
                print(f"Processing: {filename}")
                
                # Process PDF
                results = simple_pdf_processor(filepath, 'output')
                
                # Create output files
                file_count = create_simple_output(filepath, results, 'output')
                processed_files.append({
                    'filename': filename,
                    'results': len(results),
                    'output_files': file_count
                })
        
        if not processed_files:
            return '''
            <div style="max-width: 500px; margin: 20px auto; padding: 30px; background: white; border-radius: 15px; text-align: center;">
                <h2 style="color: #dc3545;">❌ No valid PDF files</h2>
                <p>Please upload PDF files only.</p>
                <a href="/" style="display: inline-block; margin-top: 20px; padding: 10px 20px; background: #667eea; color: white; text-decoration: none; border-radius: 20px;">Try Again</a>
            </div>
            ''', 400
        
        # Create ZIP file
        zip_filename = f"results_{int(time.time())}.zip"
        zip_path = os.path.join('output', zip_filename)
        
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file in os.listdir('output'):
                if file.endswith('.txt'):
                    zipf.write(os.path.join('output', file), file)
        
        total_results = sum(f['results'] for f in processed_files)
        total_files = sum(f['output_files'] for f in processed_files)
        
        return f'''
        <div style="max-width: 500px; margin: 20px auto; padding: 30px; background: white; border-radius: 15px; text-align: center;">
            <h2 style="color: #28a745;">✅ Processing Complete!</h2>
            
            <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0; text-align: left;">
                <p><strong>Files Processed:</strong> {len(processed_files)}</p>
                <p><strong>Order-SKU Entries Found:</strong> {total_results}</p>
                <p><strong>Summary Files Created:</strong> {total_files}</p>
            </div>
            
            <a href="/download/{zip_filename}" style="
                display: inline-block; 
                background: linear-gradient(135deg, #28a745, #20c997);
                color: white; 
                padding: 15px 30px; 
                text-decoration: none; 
                border-radius: 25px;
                font-weight: bold;
                margin: 10px 0;
            ">📥 Download Results ZIP</a>
            
            <br>
            
            <a href="/" style="
                display: inline-block;
                color: #667eea; 
                text-decoration: none;
                margin-top: 15px;
            ">🔄 Process More Files</a>
        </div>
        '''
        
    except Exception as e:
        print(f"Server error: {e}")
        return f'''
        <div style="max-width: 500px; margin: 20px auto; padding: 30px; background: white; border-radius: 15px; text-align: center;">
            <h2 style="color: #dc3545;">❌ Processing Error</h2>
            <p>Error: {str(e)}</p>
            <p>Please try with a smaller PDF file.</p>
            <a href="/" style="display: inline-block; margin-top: 20px; padding: 10px 20px; background: #667eea; color: white; text-decoration: none; border-radius: 20px;">Try Again</a>
        </div>
        ''', 500

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_file(
            os.path.join('output', filename),
            as_attachment=True,
            download_name=f"order_extraction_{time.strftime('%Y%m%d')}.zip"
        )
    except Exception as e:
        return f'''
        <div style="max-width: 500px; margin: 20px auto; padding: 30px; background: white; border-radius: 15px; text-align: center;">
            <h2 style="color: #dc3545;">❌ Download Error</h2>
            <p>File not found: {filename}</p>
            <a href="/" style="display: inline-block; margin-top: 20px; padding: 10px 20px; background: #667eea; color: white; text-decoration: none; border-radius: 20px;">Back to Home</a>
        </div>
        '''

@app.route('/health')
def health_check():
    return {'status': 'healthy', 'memory_optimized': True, 'timestamp': time.time()}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
