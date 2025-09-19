"""
extract_en_text_and_ocr.py
- Processes Pakistan Code Civil Laws (English PDFs)
- Extracts text using PyMuPDF (fast) if possible
- Falls back to OCR (Tesseract) if PDF has no text
- Saves into Data/in-progress(interim)/pakistancode_text_en/
- Maintains Data/metadata/pakistancode_ocr_manifest_en.json
"""

import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io, os, json
from pathlib import Path

# Paths
IN_DIR = Path("Data/raw/pakistancode/civil_en")
OUT_DIR = Path("Data/in-progress(interim)/pakistancode_text_en")
META = Path("Data/metadata/pakistancode_ocr_manifest_en.json")
TESS_LANG = "eng"  # For Urdu later: "eng+urd"

OUT_DIR.mkdir(parents=True, exist_ok=True)

manifest = {}
if META.exists():
    manifest = json.loads(META.read_text())

def process_pdf(pdf_path: Path):
    key = str(pdf_path)
    if key in manifest and manifest[key].get("status") == "processed":
        print("⏩ Skipping:", pdf_path.name)
        return

    try:
        doc = fitz.open(str(pdf_path))
        all_text = []

        for page in doc:
            txt = page.get_text().strip()
            if txt:
                all_text.append(txt)

        extracted = "\n\n".join(all_text).strip()

        if len(extracted.split()) > 20:  # enough real text
            out_txt = OUT_DIR / (pdf_path.stem + ".txt")
            out_txt.write_text(extracted, encoding="utf-8")
            manifest[key] = {
                "pdf": str(pdf_path),
                "text_path": str(out_txt),
                "method": "pymupdf",
                "status": "processed"
            }
            print("✅ Extracted (digital):", pdf_path.name)
            return

        # If no text, fallback to OCR
        print("🔎 Performing OCR:", pdf_path.name)
        ocr_text_pages = []
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            text = pytesseract.image_to_string(img, lang=TESS_LANG)
            ocr_text_pages.append(text)

        full_text = "\n\n".join(ocr_text_pages).strip()
        out_txt = OUT_DIR / (pdf_path.stem + ".txt")
        out_txt.write_text(full_text, encoding="utf-8")
        manifest[key] = {
            "pdf": str(pdf_path),
            "text_path": str(out_txt),
            "method": "tesseract",
            "status": "processed"
        }

    except Exception as e:
        print("❌ Error processing:", pdf_path.name, "|", e)
        manifest[key] = {
            "pdf": str(pdf_path),
            "error": str(e),
            "status": "error"
        }

    META.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

for pdf_file in sorted(IN_DIR.glob("*.pdf")):
    process_pdf(pdf_file)

print("📂 Outputs saved in:", OUT_DIR)
print("📝 Manifest updated:", META)
