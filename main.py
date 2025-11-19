#!/usr/bin/env python3
"""
Dropbox -> parse Excel/CSV -> WhatsApp Cloud API sender
Saves processed files to processed folder to avoid duplicates.

Configure via environment variables:
DROPBOX_TOKEN, DROPBOX_FOLDER, DROPBOX_PROCESSED_FOLDER,
WHATSAPP_TOKEN, WHATSAPP_PHONE_ID, RECIPIENT_NUMBER, MAX_ROWS (optional)
"""

import os
import io
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from dropbox import Dropbox
from dropbox.files import WriteMode

# --- Configuration from environment ---
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/godowns/incoming")
DROPBOX_PROCESSED = os.getenv("DROPBOX_PROCESSED_FOLDER", "/godowns/processed")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")
RECIPIENT_NUMBER = os.getenv("RECIPIENT_NUMBER")  # e.g. +91XXXXXXXXXX
MAX_ROWS = int(os.getenv("MAX_ROWS", "80"))

# Validation
missing = []
for name, val in [
    ("DROPBOX_TOKEN", DROPBOX_TOKEN),
    ("WHATSAPP_TOKEN", WHATSAPP_TOKEN),
    ("WHATSAPP_PHONE_ID", WHATSAPP_PHONE_ID),
    ("RECIPIENT_NUMBER", RECIPIENT_NUMBER),
]:
    if not val:
        missing.append(name)
if missing:
    print(f"Missing required env vars: {', '.join(missing)}", file=sys.stderr)
    sys.exit(2)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

dbx = Dropbox(DROPBOX_TOKEN)

def list_files(folder):
    res = []
    try:
        result = dbx.files_list_folder(folder)
    except Exception as e:
        logging.error("Failed to list folder %s: %s", folder, e)
        return res
    for entry in result.entries:
        # only files (not folders)
        if hasattr(entry, "name"):
            res.append(entry)
    # pagination (if any)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        for entry in result.entries:
            if hasattr(entry, "name"):
                res.append(entry)
    return res

def download_file(path):
    try:
        md, res = dbx.files_download(path)
        return res.content, md.name
    except Exception as e:
        logging.error("Failed to download %s: %s", path, e)
        return None, None

def move_file(src_path, dest_folder):
    try:
        name = os.path.basename(src_path)
        dest_path = f"{dest_folder.rstrip('/')}/{name}"
        dbx.files_move_v2(src_path, dest_path, allow_shared_folder=True, autorename=True)
        logging.info("Moved %s -> %s", src_path, dest_path)
        return True
    except Exception as e:
        logging.error("Failed to move %s to processed: %s", src_path, e)
        return False

def parse_to_dataframe(content_bytes, filename):
    buf = io.BytesIO(content_bytes)
    lower = filename.lower()
    try:
        if lower.endswith(".csv"):
            # decode as text and use pandas
            text = buf.getvalue().decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(text))
        elif lower.endswith((".xls", ".xlsx")):
            df = pd.read_excel(buf, engine="openpyxl")
        else:
            logging.warning("Unsupported file extension for %s. Try reading as CSV.", filename)
            text = buf.getvalue().decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(text))
        return df
    except Exception as e:
        logging.error("Error parsing %s: %s", filename, e)
        return None

def normalize_columns(df):
    # standardize column names to simple uppercase no-spaces keys
    new_cols = {c: c.strip() for c in df.columns}
    df.rename(columns=new_cols, inplace=True)
    return df

def filter_tomorrow_rows(df):
    # Find a date-like column: attempt common names
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if not date_cols:
        logging.warning("No date column detected. Returning entire dataframe.")
        return df
    # try each date column until one yields rows matching tomorrow
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    logging.info("Filtering rows for Loading Date == %s", tomorrow.isoformat())
    out = pd.DataFrame()
    for c in date_cols:
        try:
            parsed = pd.to_datetime(df[c], errors="coerce").dt.date
            mask = parsed == tomorrow
            if mask.any():
                out = df[mask].copy()
                logging.info("Found %d rows in column %s for tomorrow", out.shape[0], c)
                return out
        except Exception:
            continue
    # fallback: if none matched, return empty
    logging.info("No rows with tomorrow's date found in any detected date column.")
    return out

def build_table_text(df):
    # Choose columns to show if present
    # prefer these names, otherwise use all
    prefer = ["PARTY","MATERIAL","APROX QTY","APPROX QTY","QTY","QUANTITY","RATE / KG","RATE","VEHICLE NO","VEHICLE","ACTUAL QTY","STATUS","PAYMENT","DATE"]
    cols = []
    existing = [c for c in df.columns]
    for name in prefer:
        for col in existing:
            if col.strip().upper() == name:
                cols.append(col)
    # add any other columns not included
    for col in existing:
        if col not in cols:
            cols.append(col)
    if df.empty:
        return "No items scheduled for loading tomorrow."
    # limit rows
    df_display = df.head(MAX_ROWS)
    # prepare table lines (monospace)
    # compute column widths
    headers = [str(c) for c in cols]
    rows = []
    for _, r in df_display.iterrows():
        row = [str(r.get(c, "") if not pd.isna(r.get(c,"")) else "") for c in cols]
        rows.append(row)
    col_widths = [max(len(str(h)), max((len(row[i]) for row in rows), default=0)) for i,h in enumerate(headers)]
    # build header line
    header_line = " | ".join(h.ljust(col_widths[i]) for i,h in enumerate(headers))
    sep_line = "-+-".join("-"*col_widths[i] for i in range(len(headers)))
    data_lines = []
    for row in rows:
        data_lines.append(" | ".join(row[i].ljust(col_widths[i]) for i in range(len(headers))))
    # wrap in triple backticks for monospace in WhatsApp
    table_text = "```\n" + header_line + "\n" + sep_line + "\n" + "\n".join(data_lines) + "\n```"
    footer = ""
    if len(df) > MAX_ROWS:
        footer = f"\n(Showing first {MAX_ROWS} of {len(df)} rows)\n"
    return f"Loading schedule for { (datetime.now()+timedelta(days=1)).date().isoformat() }\n\n{table_text}{footer}"

def send_whatsapp_text(body_text):
    # WhatsApp Cloud API endpoint
    url = f"https://graph.facebook.com/v16.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": RECIPIENT_NUMBER,
        "type": "text",
        "text": {"preview_url": False, "body": body_text}
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        logging.info("WhatsApp message sent successfully.")
        return True
    else:
        logging.error("WhatsApp API error %s: %s", r.status_code, r.text)
        return False

def process_file(entry):
    path = entry.path_lower if hasattr(entry, "path_lower") else entry.path_display
    logging.info("Processing file: %s", path)
    content, name = download_file(path)
    if content is None:
        return False
    df = parse_to_dataframe(content, name)
    if df is None:
        logging.error("Parsing yielded no dataframe for %s", name)
        # still move to processed to avoid loops? we will move to processed/error
        move_file(path, DROPBOX_PROCESSED + "/error")
        return False
    df = normalize_columns(df)
    rows = filter_tomorrow_rows(df)
    text = build_table_text(rows)
    ok = send_whatsapp_text(text)
    # move file to processed if message sent or even if not (to avoid retries) - adjust if you want retries
    move_dest = DROPBOX_PROCESSED + ("" if DROPBOX_PROCESSED.endswith("") else "")
    # ensure processed folder exists? Dropbox will create during move with autorename
    moved = move_file(path, DROPBOX_PROCESSED)
    return ok

def main():
    logging.info("Starting run: listing files in %s", DROPBOX_FOLDER)
    files = list_files(DROPBOX_FOLDER)
    if not files:
        logging.info("No files found. Exiting.")
        return 0
    # sort by server modified to process oldest-first
    files_sorted = sorted(files, key=lambda e: getattr(e, "server_modified", datetime.now()))
    processed_any = False
    for f in files_sorted:
        # skip hidden files
        if f.name.startswith("."):
            continue
        try:
            ok = process_file(f)
            processed_any = processed_any or ok
        except Exception as e:
            logging.exception("Error processing file %s: %s", getattr(f, "name", "unknown"), e)
            try:
                move_file(f.path_lower, DROPBOX_PROCESSED + "/error")
            except:
                pass
    if not processed_any:
        logging.info("No messages sent in this run.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
