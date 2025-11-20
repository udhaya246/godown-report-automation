#!/usr/bin/env python3
"""
Automated Daily Loading Report
Dropbox → Compile next-day rows → Twilio WhatsApp

Expected Dropbox folder structure:

  /godowns/incoming/godown1/*.xlsx
  /godowns/incoming/godown2/*.xlsx
  /godowns/processed/<godown>/
  /godowns/reports/

Environment variables (GitHub Secrets):
DROPBOX_TOKEN
TWILIO_SID
TWILIO_AUTH
WHATSAPP_FROM
WHATSAPP_TO
"""

import os
import io
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
from dropbox import Dropbox
from dropbox.files import WriteMode
from twilio.rest import Client

# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")
WHATSAPP_FROM = os.getenv("WHATSAPP_FROM", "whatsapp:+14155238886")

INCOMING_ROOT = "/godowns/incoming"
PROCESSED_ROOT = "/godowns/processed"
REPORTS_ROOT = "/godowns/reports"

MAX_ROWS = 200

required = [
    ("DROPBOX_TOKEN", DROPBOX_TOKEN),
    ("TWILIO_SID", TWILIO_SID),
    ("TWILIO_AUTH", TWILIO_AUTH),
    ("WHATSAPP_TO", WHATSAPP_TO),
]

missing = [k for k, v in required if not v]
if missing:
    print(f"Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")

dbx = Dropbox(DROPBOX_TOKEN)
twilio = Client(TWILIO_SID, TWILIO_AUTH)

# ---------------------------------------------------------
# Helper functions
# ---------------------------------------------------------
def ensure_folder(path):
    try:
        dbx.files_get_metadata(path)
    except:
        try:
            dbx.files_create_folder_v2(path)
            logging.info(f"Created folder: {path}")
        except:
            pass

def list_godown_folders(root):
    try:
        res = dbx.files_list_folder(root)
        return [e.name for e in res.entries if hasattr(e, "name")]
    except Exception as e:
        logging.error(f"Failed to list folder {root}: {e}")
        return []

def list_files(path):
    try:
        res = dbx.files_list_folder(path)
        return [f for f in res.entries if f.name.lower().endswith((".xlsx", ".xls", ".csv"))]
    except:
        return []

def download(path):
    try:
        meta, resp = dbx.files_download(path)
        return resp.content, meta.name
    except Exception as e:
        logging.error(f"Download failed {path}: {e}")
        return None, None

def df_from_bytes(raw, fname):
    bio = io.BytesIO(raw)
    try:
        if fname.endswith(".csv"):
            return pd.read_csv(io.StringIO(raw.decode("utf-8", "ignore")))
        return pd.read_excel(bio)
    except Exception as e:
        logging.error(f"Failed reading {fname}: {e}")
        return None

def normalize(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def filter_tomorrow(df):
    tomorrow = (datetime.utcnow() + timedelta(days=1)).date()
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if not date_cols:
        return df.copy()
    for col in date_cols:
        try:
            parsed = pd.to_datetime(df[col], errors="ignore").dt.date
            mask = parsed == tomorrow
            if mask.any():
                return df[mask].copy()
        except:
            pass
    return pd.DataFrame(columns=df.columns)

def move_to_processed(src, godown):
    dest_folder = f"{PROCESSED_ROOT}/{godown}"
    ensure_folder(dest_folder)
    dest_path = f"{dest_folder}/{os.path.basename(src)}"
    try:
        dbx.files_move_v2(src, dest_path, autorename=True)
        logging.info(f"Moved → {dest_path}")
    except Exception as e:
        logging.error(f"Move failed {src}: {e}")

def upload_report(text):
    ensure_folder(REPORTS_ROOT)
    fname = f"report_{(datetime.utcnow()+timedelta(days=1)).date()}.txt"
    path = f"{REPORTS_ROOT}/{fname}"
    try:
        dbx.files_upload(text.encode(), path, mode=WriteMode.overwrite)
        logging.info(f"Uploaded report → {path}")
    except Exception as e:
        logging.error(f"Upload failed: {e}")

def send_whatsapp(text):
    try:
        twilio.messages.create(from_=WHATSAPP_FROM, to=WHATSAPP_TO, body=text)
        logging.info("WhatsApp message sent")
    except Exception as e:
        logging.error(f"Twilio failed: {e}")

# ---------------------------------------------------------
# Report builder
# ---------------------------------------------------------
def build_report(compiled):
    lines = []
    lines.append("NEXT-DAY LOADING REPORT")
    lines.append(f"Date: {(datetime.utcnow() + timedelta(days=1)).date()}")
    lines.append("-" * 40)

    total = 0

    for godown, df in compiled.items():
        lines.append(f"\nGODOWN: {godown.upper()}")
        if df.empty:
            lines.append("  No items")
            continue
        for _, row in df.head(MAX_ROWS).iterrows():
            p = str(row.get("PARTY", "")).strip()
            m = str(row.get("MATERIAL", "")).strip()
            q = str(row.get("QTY", row.get("QUANTITY", ""))).strip()
            v = str(row.get("VEHICLE NO", row.get("VEHICLE", ""))).strip()
            line = f"• {p} — {m} — {q}"
            if v:
                line += f" — {v}"
            lines.append(line)
            total += 1

    lines.append("\n" + "-"*40)
    lines.append(f"Total Items: {total}")

    return "\n".join(lines)

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    logging.info("=== START ===")
    ensure_folder(PROCESSED_ROOT)
    ensure_folder(REPORTS_ROOT)

    godowns = list_godown_folders(INCOMING_ROOT)
    compiled = {}
    any_rows = False

    for gd in godowns:
        folder = f"{INCOMING_ROOT}/{gd}"
        files = list_files(folder)
        all_rows = pd.DataFrame()

        for f in files:
            path = f.path_lower
            raw, fname = download(path)
            if not raw:
                move_to_processed(path, gd)
                continue
            df = df_from_bytes(raw, fname)
            if df is None:
                move_to_processed(path, gd)
                continue
            df = normalize(df)
            rows = filter_tomorrow(df)
            if not rows.empty:
                any_rows = True
                all_rows = pd.concat([all_rows, rows], ignore_index=True)
            move_to_processed(path, gd)

        compiled[gd] = all_rows

    report = build_report(compiled)
    upload_report(report)

    if any_rows:
        send_whatsapp(report)
    else:
        send_whatsapp("No items scheduled for tomorrow.")

    logging.info("=== COMPLETE ===")

if __name__ == "__main__":
    main()
