from flask import Flask, render_template, request, send_file, jsonify, url_for
import os, re, zipfile, pdfplumber, threading, uuid, traceback, time, gc
from PyPDF2 import PdfWriter, PdfReader
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_UPLOAD = "uploads"
BASE_SORTED = "sorted"
BASE_CONSOLIDATED = "consolidated_by_sku"
BASE_ZIPPED = "zipped_archives"

for d in [BASE_UPLOAD, BASE_SORTED, BASE_CONSOLIDATED, BASE_ZIPPED]:
    os.makedirs(d, exist_ok=True)

jobs = {}

# ---------- helper functions ----------
def extract_order_id(text):
    m = re.search(r"Order ID[: ]+(\d+)", text)
    return m.group(1) if m else None

def extract_barcode(text):
    m = re.search(r"\b\d{10,18}\b", text)
    return m.group(0) if m else None

def extract_sku_from_product_table(lines):
    """Improved SKU extraction from product table"""
    for idx, line in enumerate(lines):
        if "Product Name" in line and "Seller SKU" in line:
            # Look for product data in next few lines
            for data_line_idx in range(idx + 1, min(idx + 5, len(lines))):
                data_line = lines[data_line_idx].strip()
                if data_line and not any(x in data_line for x in ["Product Name", "Seller SKU", "Quantity", "Price"]):
                    parts = data_line.split()
                    if len(parts) >= 2:
                        # Try to find SKU (usually second last or last)
                        for i in range(len(parts) - 1, max(len(parts) - 3, 0), -1):
                            if len(parts[i]) >= 2:  # SKU should have some length
                                return parts[i]
    return None

def cleanup_old_jobs():
    """Clean up jobs older than 1 hour to prevent memory leak"""
    try:
        current_time = time.time()
        jobs_to_delete = []
        
        for job_id, job_info in jobs.items():
            if current_time - job_info.get('created_time', current_time) > 3600:  # 1 hour
                jobs_to_delete.append(job_id)
        
        for job_id in jobs_to_delete:
            del jobs[job_id]
            print(f"Cleaned up old job: {job_id}")
            
    except Exception as e:
        print(f"Error cleaning old jobs: {e}")

def create_zip_background(job_id, consolidated_files, job_consolidated, job_zipped):
    """Runs in background to create zip and finalize job."""
    try:
        jobs[job_id]["message"] = "Creating ZIP archive..."
        zip_name = f"results_{job_id}.zip"
        zip_path = os.path.join(job_zipped, zip_name)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for f in consolidated_files:
                p = os.path.join(job_consolidated, f)
                if os.path.exists(p):
                    zipf.write(p, arcname=f)

        jobs[job_id]["zip"] = zip_path
        jobs[job_id]["progress"] = 100
        jobs[job_id]["status"] = "done"
        jobs[job_id]["message"] = f"Completed ✅ - {len(consolidated_files)} files created"
        print(f"[{job_id}] ✅ ZIP created successfully at {zip_path}")

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error creating zip: {e}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print(f"[{job_id}] ❌ ZIP creation failed:", e)
        print(traceback.format_exc())


def process_pdf_job(job_id, uploaded_path, original_filename):
    """Main processing job with improved SKU extraction and deduplication."""
    pdf_plumber = None
    reader = None
    
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["progress"] = 0
        jobs[job_id]["message"] = "Preparing..."
        jobs[job_id]["created_time"] = time.time()

        job_sorted = os.path.join(BASE_SORTED, job_id)
        job_consolidated = os.path.join(BASE_CONSOLIDATED, job_id)
        job_zipped = os.path.join(BASE_ZIPPED, job_id)
        for d in [job_sorted, job_consolidated, job_zipped]:
            os.makedirs(d, exist_ok=True)

        # Count pages
        jobs[job_id]["message"] = "Counting pages..."
        with pdfplumber.open(uploaded_path) as pdf_counter:
            total_pages = len(pdf_counter.pages) if pdf_counter.pages else 0
        
        print(f"[{job_id}] Total pages: {total_pages}")

        # Open PDF once for reading
        jobs[job_id]["message"] = "Loading PDF..."
        reader = PdfReader(uploaded_path)
        pdf_plumber = pdfplumber.open(uploaded_path)
        
        # Track page info without keeping writers in memory
        page_info = []
        last_order_id, last_sku = None, None
        
        jobs[job_id]["message"] = "Analyzing pages..."
        
        for i in range(total_pages):
            try:
                # Extract text using pdfplumber
                page = pdf_plumber.pages[i]
                text = page.extract_text() or ""
                lines = text.splitlines()
                
                # Extract order ID
                order_id = extract_order_id(text)
                if order_id:
                    last_order_id = order_id
                else:
                    order_id = last_order_id

                # Extract SKU with improved logic
                barcode = extract_barcode(text)
                sku = None
                
                # If no barcode and we have previous SKU, use it
                if barcode is None and last_sku is not None:
                    sku = last_sku
                else:
                    # Try multiple methods to extract SKU
                    sku = extract_sku_from_product_table(lines)
                    
                    # Fallback: look for SKU patterns in text
                    if not sku:
                        sku_match = re.search(r'SKU[:\\s]*([\\w\\d-]+)', text, re.IGNORECASE)
                        if sku_match:
                            sku = sku_match.group(1)
                    
                    # Final fallback
                    if not sku:
                        sku = last_sku if last_sku else f"UNKNOWN_{i}"

                # Clean and validate SKU
                sku = re.sub(r'[^\\w\\d-]', '_', str(sku)).strip()
                if not sku or sku == "UNKNOWN":
                    sku = f"UNKNOWN_{i}"
                
                last_sku = sku

                if order_id and sku:
                    group_key = f"{order_id}_{sku}"
                    page_info.append((i, group_key, order_id, sku))
                else:
                    print(f"Warning: missing order_id or sku on page {i}")

                # Update progress (0-50% for analysis)
                if i % 10 == 0 or i == total_pages - 1:
                    progress = int((i + 1) / total_pages * 50)
                    jobs[job_id]["progress"] = progress
                    jobs[job_id]["message"] = f"Analyzing pages... ({i+1}/{total_pages})"
                    
            except Exception as e:
                print(f"Error analyzing page {i}: {e}")
                continue

        # Close pdfplumber to free memory
        pdf_plumber.close()
        pdf_plumber = None
        
        # Group pages by key and remove duplicates
        jobs[job_id]["message"] = "Grouping pages..."
        grouped_pages = {}
        order_sku_combinations = set()
        
        for page_num, group_key, order_id, sku in page_info:
            if group_key not in grouped_pages:
                grouped_pages[group_key] = []
                order_sku_combinations.add((order_id, sku))
            grouped_pages[group_key].append(page_num)

        print(f"[{job_id}] Found {len(grouped_pages)} unique order-SKU combinations")

        # Write sorted PDFs in batches to avoid memory issues
        jobs[job_id]["message"] = "Saving sorted PDFs..."
        total_groups = len(grouped_pages)
        
        for idx, (group_key, page_nums) in enumerate(grouped_pages.items()):
            writer = PdfWriter()
            try:
                for page_num in page_nums:
                    writer.add_page(reader.pages[page_num])
                
                out_path = os.path.join(job_sorted, f"{group_key}.pdf")
                with open(out_path, "wb") as f:
                    writer.write(f)
                    
            except Exception as e:
                print(f"Error writing group {group_key}: {e}")
            finally:
                # Clear writer to free memory
                del writer
                
            # Update progress (50-70% for writing)
            if idx % 5 == 0 or idx == total_groups - 1:
                progress = 50 + int((idx + 1) / total_groups * 20)
                jobs[job_id]["progress"] = progress
                jobs[job_id]["message"] = f"Saving sorted PDFs... ({idx+1}/{total_groups})"

        # Close reader and force garbage collection
        reader = None
        gc.collect()

        # Map primary SKUs - use first SKU encountered for each order
        jobs[job_id]["message"] = "Mapping primary SKUs..."
        jobs[job_id]["progress"] = 70
        
        order_id_to_primary_sku = {}
        order_sku_pairs = []
        
        # Collect all order-SKU pairs
        for filename in os.listdir(job_sorted):
            if filename.endswith(".pdf"):
                parts = filename.rsplit("_", 1)
                if len(parts) == 2:
                    order_id = parts[0]
                    sku = parts[1].replace(".pdf", "")
                    order_sku_pairs.append((order_id, sku))
        
        # Use first SKU for each order as primary
        for order_id, sku in order_sku_pairs:
            if order_id not in order_id_to_primary_sku:
                order_id_to_primary_sku[order_id] = sku
                print(f"[{job_id}] Order {order_id} -> Primary SKU: {sku}")

        print(f"[{job_id}] Mapped {len(order_id_to_primary_sku)} orders to primary SKUs")

        # Consolidate by primary sku
        jobs[job_id]["message"] = "Consolidating by primary SKU..."
        jobs[job_id]["progress"] = 75
        
        grouped_files_by_primary_sku = {}
        for filename in os.listdir(job_sorted):
            if filename.endswith(".pdf"):
                file_path = os.path.join(job_sorted, filename)
                parts = filename.rsplit("_", 1)
                if len(parts) == 2:
                    order_id = parts[0]
                    primary_sku = order_id_to_primary_sku.get(order_id)
                    if primary_sku:
                        grouped_files_by_primary_sku.setdefault(primary_sku, []).append((order_id, file_path))

        print(f"[{job_id}] Grouped into {len(grouped_files_by_primary_sku)} primary SKUs")

        # Write consolidated PDFs one at a time
        jobs[job_id]["message"] = "Saving consolidated PDFs..."
        consolidated_files = []
        total_skus = len(grouped_files_by_primary_sku)
        
        for sku_idx, (primary_sku, files_list) in enumerate(grouped_files_by_primary_sku.items()):
            files_list.sort(key=lambda x: x[0])  # Sort by order_id for consistency
            writer = PdfWriter()
            total_pages_in_sku = 0
            
            try:
                for order_id, file_path in files_list:
                    try:
                        r = PdfReader(file_path)
                        for p in r.pages:
                            writer.add_page(p)
                            total_pages_in_sku += 1
                    except Exception as e:
                        print(f"Error merging {file_path}: {e}")
                
                if len(writer.pages) > 0:
                    out_file = os.path.join(job_consolidated, f"{primary_sku}.pdf")
                    with open(out_file, "wb") as f:
                        writer.write(f)
                    consolidated_files.append(f"{primary_sku}.pdf")
                    print(f"[{job_id}] Consolidated {primary_sku}.pdf with {total_pages_in_sku} pages")
                    
            except Exception as e:
                print(f"Error consolidating SKU {primary_sku}: {e}")
            finally:
                del writer
                gc.collect()
            
            # Update progress (75-90% for consolidation)
            if sku_idx % 2 == 0 or sku_idx == total_skus - 1:
                progress = 75 + int((sku_idx + 1) / total_skus * 15)
                jobs[job_id]["progress"] = progress
                jobs[job_id]["message"] = f"Saving consolidated PDFs... ({sku_idx+1}/{total_skus})"

        jobs[job_id]["files"] = consolidated_files
        jobs[job_id]["progress"] = 90
        jobs[job_id]["message"] = "Finalizing (zipping)..."

        # Run ZIP creation in background
        threading.Thread(
            target=create_zip_background,
            args=(job_id, consolidated_files, job_consolidated, job_zipped),
            daemon=True
        ).start()

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error: {str(e)}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        print(f"[{job_id}] Error in process_pdf_job:", e)
        print(traceback.format_exc())
    
    finally:
        # Clean up resources
        if pdf_plumber:
            try:
                pdf_plumber.close()
            except:
                pass
        reader = None
        gc.collect()


# ---------- Flask routes ----------
@app.route("/")
def index():
    cleanup_old_jobs()  # Clean up on home page visit
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    cleanup_old_jobs()  # Clean up before new upload
    
    if "file" in request.files:
        file = request.files["file"]
    else:
        return jsonify({"error": "No file uploaded"}), 400

    if file.filename == "":
        return jsonify({"error": "No filename"}), 400

    if not file.filename.lower().endswith('.pdf'):
        return jsonify({"error": "Only PDF files are supported"}), 400

    filename = secure_filename(file.filename)
    saved_path = os.path.join(BASE_UPLOAD, filename)
    file.save(saved_path)

    job_id = uuid.uuid4().hex
    jobs[job_id] = {
        "status": "pending", 
        "progress": 0, 
        "message": "Queued", 
        "files": [], 
        "zip": None,
        "created_time": time.time(),
        "original_filename": filename
    }

    threading.Thread(target=process_pdf_job, args=(job_id, saved_path, filename), daemon=True).start()

    return jsonify({
        "job_id": job_id, 
        "status_url": url_for("job_status", job_id=job_id),
        "download_url": url_for("download_job_zip", job_id=job_id)
    }), 202


@app.route("/status/<job_id>")
def job_status(job_id):
    info = jobs.get(job_id)
    if not info:
        return jsonify({"error": "Job not found"}), 404
    
    result = {
        "status": info["status"],
        "progress": info.get("progress", 0),
        "message": info.get("message", ""),
        "files": info.get("files", []),
        "zip": None,
        "original_filename": info.get("original_filename", "")
    }
    
    if info.get("zip"):
        result["zip"] = url_for("download_job_zip", job_id=job_id)
    
    if info.get("status") == "error":
        result["error"] = info.get("message", "Unknown error")
        result["traceback"] = info.get("traceback")
    
    return jsonify(result)


@app.route("/download/<job_id>")
def download_job_zip(job_id):
    info = jobs.get(job_id)
    if not info:
        return "Job not found", 404
    
    if not info.get("zip"):
        return "ZIP file not ready yet", 404
    
    return send_file(
        info["zip"], 
        as_attachment=True,
        download_name=f"consolidated_orders_{info.get('original_filename', 'results')}.zip"
    )


@app.route("/jobs")
def list_jobs():
    """Endpoint to see all current jobs (for debugging)"""
    job_list = []
    for job_id, info in jobs.items():
        job_list.append({
            "job_id": job_id,
            "status": info.get("status"),
            "progress": info.get("progress", 0),
            "message": info.get("message", ""),
            "original_filename": info.get("original_filename", "")
        })
    return jsonify({"jobs": job_list, "total": len(job_list)})


@app.route("/cleanup", methods=["POST"])
def manual_cleanup():
    """Manual cleanup endpoint"""
    try:
        before_count = len(jobs)
        cleanup_old_jobs()
        after_count = len(jobs)
        return jsonify({
            "message": f"Cleanup completed. Removed {before_count - after_count} old jobs.",
            "remaining_jobs": after_count
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
