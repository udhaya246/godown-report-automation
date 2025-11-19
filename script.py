#!/usr/bin/env python3
"""
Dropbox -> compile next-day loading rows -> Twilio WhatsApp text message
Folder layout expected:
 /godowns/incoming/godown1/*.xlsx
 /godowns/incoming/godown2/*.xlsx
 /godowns/processed/
 /godowns/compiled_reports/

Environment variables (set as GitHub secrets):
DROPBOX_TOKEN
TWILIO_SID
TWILIO_AUTH
WHATSAPP_TO    (e.g. whatsapp:+91XXXXXXXXXX)
MAX_ROWS (optional, default 200)
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

# ------- Config from env -------
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")            # include 'whatsapp:+91...'
WHATSAPP_FROM = os.getenv("WHATSAPP_FROM", "whatsapp:+14155238886")  # Twilio Sandbox default
INCOMING_ROOT = os.getenv("INCOMING_ROOT", "/godowns/incoming")
PROCESSED_ROOT = os.getenv("PROCESSED_ROOT", "/godowns/processed")
COMPILED_ROOT = os.getenv("COMPILED_ROOT", "/godowns/compiled_reports")
MAX_ROWS = int(os.getenv("MAX_ROWS", "200"))

# minimal validation
missing = [n for n, v in [
    ("DROPBOX_TOKEN", DROPBOX_TOKEN),
    ("TWILIO_SID", TWILIO_SID),
    ("TWILIO_AUTH", TWILIO_AUTH),
    ("WHATSAPP_TO", WHATSAPP_TO),
] if not v]
if missing:
    print("Missing required env vars: " + ", ".join(missing), file=sys.stderr)
    sys.exit(2)

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

dbx = Dropbox(DROPBOX_TOKEN)
tw = Client(TWILIO_SID, TWILIO_AUTH)

def list_godown_folders(root):
    """Return list of subfolder names under incoming root (godown1, godown2...)"""
    try:
        res = dbx.files_list_folder(root)
    except Exception as e:
        logging.error("Failed to list incoming root %s: %s", root, e)
        return []
    names = []
    for entry in res.entries:
        if entry.is_folder:
            names.append(entry.name)
    # also handle pagination
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        for entry in res.entries:
            if entry.is_folder:
                names.append(entry.name)
    return names

def list_files_in_folder(folder_path):
    """Return file metadata entries for Excel files in a folder path"""
    try:
        res = dbx.files_list_folder(folder_path)
    except Exception as e:
        logging.error("Cannot list folder %s: %s", folder_path, e)
        return []
    files = [e for e in res.entries if hasattr(e, "name") and e.name.lower().endswith((".xlsx", ".xls", ".csv"))]
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        for e in res.entries:
            if hasattr(e, "name") and e.name.lower().endswith((".xlsx", ".xls", ".csv")):
                files.append(e)
    return files

def download_bytes(path):
    try:
        md, res = dbx.files_download(path)
        return res.content, md.name
    except Exception as e:
        logging.error("Download failed for %s: %s", path, e)
        return None, None

def df_from_bytes(content_bytes, filename):
    bio = io.BytesIO(content_bytes)
    name = filename.lower()
    try:
        if name.endswith(".csv"):
            # try decode utf-8, fallback to latin1
            txt = bio.getvalue().decode("utf-8", errors="replace")
            return pd.read_csv(io.StringIO(txt))
        else:
            # xls/xlsx
            return pd.read_excel(bio, engine="openpyxl")
    except Exception as e:
        logging.error("Failed to read %s: %s", filename, e)
        return None

def normalize_df(df):
    # strip column names
    df.columns = [str(c).strip() for c in df.columns]
    return df

def collect_tomorrow_rows(df):
    # look for any date-like column
    tomorrow = (datetime.utcnow() + timedelta(days=1)).date()
    # try common names
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if not date_cols:
        # if no date column, assume all rows are for next day (or user wants that)
        return df.copy()
    for c in date_cols:
        try:
            parsed = pd.to_datetime(df[c], errors="coerce").dt.date
            mask = parsed == tomorrow
            if mask.any():
                return df[mask].copy()
        except Exception:
            continue
    # none matched
    return pd.DataFrame(columns=df.columns)

def format_report(compiled_rows_by_godown):
    """compiled_rows_by_godown: dict {godown_name: DataFrame}"""
    lines = []
    lines.append("NEXT DAY LOADING SUMMARY")
    lines.append(f"Date: {(datetime.utcnow() + timedelta(days=1)).date().isoformat()}")
    lines.append("-"*40)
    total_items = 0
    for godown, df in compiled_rows_by_godown.items():
        lines.append(f"\nGODOWN: {godown.upper()}")
        if df.empty:
            lines.append("  No items")
            continue
        # pick columns to display, prefer these names if present
        prefer = ["PARTY","MATERIAL","APROX QTY","QTY","QUANTITY","VEHICLE NO","VEHICLE","RATE / KG","RATE","STATUS"]
        present = list(df.columns)
        cols = []
        for p in prefer:
            for col in present:
                if col.strip().upper() == p:
                    cols.append(col)
        # add others if needed
        for c in present:
            if c not in cols:
                cols.append(c)
        # produce lines
        for idx, row in df.head(MAX_ROWS).iterrows():
            total_items += 1
            # build compact line using available columns
            party = str(row.get("PARTY","")).strip()
            material = str(row.get("MATERIAL","")).strip()
            qty = str(row.get("APROX QTY", row.get("QTY", row.get("QUANTITY","")))).strip()
            vehicle = str(row.get("VEHICLE NO", row.get("VEHICLE",""))).strip()
            line = f"• {party} — {material} — {qty}"
            if vehicle:
                line += f" — {vehicle}"
            lines.append(line)
    lines.append("\n" + "-"*40)
    lines.append(f"Total items listed: {total_items}")
    return "\n".join(lines)

def move_processed(src_path, godown):
    # move to PROCESSED_ROOT/<godown>/
    name = os.path.basename(src_path)
    dest = f"{PROCESSED_ROOT.rstrip('/')}/{godown}/{name}"
    try:
        # create target folder implicitly by moving (Dropbox autogenerates)
        dbx.files_move_v2(src_path, dest, autorename=True)
        logging.info("Moved %s -> %s", src_path, dest)
    except Exception as e:
        logging.error("Failed to move %s to %s: %s", src_path, dest, e)

def upload_compiled_text(text):
    fname = f"report_{(datetime.utcnow()+timedelta(days=1)).date().isoformat()}.txt"
    path = f"{COMPILED_ROOT.rstrip('/')}/{fname}"
    try:
        dbx.files_upload(text.encode("utf-8"), path, mode=WriteMode.overwrite)
        logging.info("Uploaded compiled report to %s", path)
    except Exception as e:
        logging.error("Failed to upload compiled report: %s", e)

def send_whatsapp_text(body):
    try:
        tw.messages.create(
            from_=WHATSAPP_FROM,
            to=WHATSAPP_TO,
            body=body
        )
        logging.info("WhatsApp sent to %s", WHATSAPP_TO)
    except Exception as e:
        logging.error("Twilio send failed: %s", e)

def main():
    logging.info("Start processing")
    godowns = list_godown_folders(INCOMING_ROOT)
    if not godowns:
        logging.warning("No godown folders found under %s", INCOMING_ROOT)
        # optionally send a message that no files found
        # send_whatsapp_text("No godown files found today.")
        return
    compiled = {}
    any_row = False
    for gd in godowns:
        folder = f"{INCOMING_ROOT.rstrip('/')}/{gd}"
        files = list_files_in_folder(folder)
        logging.info("Found %d files in %s", len(files), folder)
        all_rows = pd.DataFrame()
        for f in files:
            path = f.path_lower if hasattr(f, "path_lower") else f.path_display
            content, name = download_bytes(path)
            if content is None:
                continue
            df = df_from_bytes(content, name)
            if df is None:
                move_processed(path, gd)  # move problematic file to processed to avoid repeat
                continue
            df = normalize_df(df)
            rows = collect_tomorrow_rows(df)
            if not rows.empty:
                any_row = True
                all_rows = pd.concat([all_rows, rows], ignore_index=True, sort=False)
            # move original to processed
            move_processed(path, gd)
        compiled[gd] = all_rows
    # build report
    report_text = format_report(compiled)
    upload_compiled_text(report_text)
    if any_row:
        send_whatsapp_text(report_text)
    else:
        # optional: still send a short "no items" message
        send_whatsapp_text("No items scheduled for loading tomorrow.")
    logging.info("Run complete.")

if __name__ == "__main__":
    main()
