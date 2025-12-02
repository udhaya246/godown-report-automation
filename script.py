#!/usr/bin/env python3
"""
Automated Daily Loading Report
Dropbox → Compile next-day rows → Twilio WhatsApp

Expected Dropbox folder structure:

  /godowns/incoming/REDHILLS/*.xlsx
  /godowns/incoming/SR GLASS/*.xlsx
  /godowns/incoming/SRIPERUMBUDUR/*.xlsx
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
from datetime import datetime, timedelta, timezone
import pandas as pd
import dropbox
from dropbox import Dropbox
from dropbox.files import WriteMode
from twilio.rest import Client

MAX_ROWS = 500


# ---------------------------------------
# ✅ NEW: USE REFRESH TOKEN TO AUTHENTICATE
# ---------------------------------------
def get_dropbox_client():
    """Return Dropbox client using refresh token (never expires)."""
    app_key = os.environ["DROPBOX_APP_KEY"]
    app_secret = os.environ["DROPBOX_APP_SECRET"]
    refresh_token = os.environ["DROPBOX_REFRESH_TOKEN"]

    dbx = dropbox.Dropbox(
        oauth2_refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret
    )
    return dbx


def load_excel_from_dropbox(dbx, file_path):
    """Download Excel from Dropbox and return DataFrame."""
    _, res = dbx.files_download(file_path)
    return pd.read_excel(io.BytesIO(res.content))


def move_file(dbx, src, dst):
    """Move or rename a file inside Dropbox."""
    try:
        dbx.files_move_v2(src, dst, autorename=True)
    except Exception as e:
        print(f"Error moving file {src} → {dst}: {e}")


def fetch_files(dbx, folder):
    """List files inside a Dropbox folder."""
    try:
        res = dbx.files_list_folder(folder)
        return res.entries
    except Exception:
        return []


def compile_data(dbx, folder):
    """Load all Excel files from folder and combine into dict by godown."""
    compiled = {}
    files = fetch_files(dbx, folder)

    for file in files:
        if not file.name.lower().endswith((".xlsx", ".xls")):
            continue

        path = f"{folder}/{file.name}"
        print(f"Processing: {path}")

        df = load_excel_from_dropbox(dbx, path)

        godown = (
            df.get("GODOWN", ["UNKNOWN"])[0]
            if "GODOWN" in df.columns
            else "UNKNOWN"
        )

        if godown not in compiled:
            compiled[godown] = pd.DataFrame()

        compiled[godown] = pd.concat([compiled[godown], df], ignore_index=True)

    return compiled


# -----------------------------------------------------
# FORMATTED REPORT FUNCTION
# -----------------------------------------------------
def build_report(compiled):
    lines = []
    IST = timezone(timedelta(hours=5, minutes=30))
    tomorrow = (datetime.now(IST) + timedelta(days=1)).date()

    lines.append("NEXT-DAY LOADING REPORT")
    lines.append(f"Date: {tomorrow}")
    lines.append("-" * 40)

    total = 0

    # Column formatting widths
    COL_PARTY = 18
    COL_MATERIAL = 14
    COL_QTY = 10
    COL_RATE = 10

    # Header row
    header = (
        f"{'PARTY'.ljust(COL_PARTY)}"
        f"{'MATERIAL'.ljust(COL_MATERIAL)}"
        f"{'QTY'.ljust(COL_QTY)}"
        f"{'RATE'.ljust(COL_RATE)}"
    )

    separator = "-" * (COL_PARTY + COL_MATERIAL + COL_QTY + COL_RATE)

    for godown, df in compiled.items():

        lines.append(f"\nGODOWN: {godown.upper()}")

        if df.empty:
            lines.append("  No items")
            continue

        # Insert headers for the table
        lines.append(header)
        lines.append(separator)

        for _, row in df.head(MAX_ROWS).iterrows():

            p = str(row.get("PARTY", "")).strip()
            m = str(row.get("MATERIAL", "")).strip()
            q = str(row.get("APROX QTY", row.get("QUANTITY", ""))).strip()
            r = str(row.get("RATE / KG", "")).strip()

            # Format aligned row
            line = (
                f"{p.ljust(COL_PARTY)[:COL_PARTY]}"
                f"{m.ljust(COL_MATERIAL)[:COL_MATERIAL]}"
                f"{q.ljust(COL_QTY)[:COL_QTY]}"
                f"{r.ljust(COL_RATE)[:COL_RATE]}"
            )

            lines.append(line)
            total += 1

    lines.append("\n" + "-" * 40)
    lines.append(f"Total Items: {total}")

    return "\n".join(lines)


def save_report(dbx, folder, text):
    """Save TXT file into Dropbox report folder."""
    date = datetime.now().strftime("%Y-%m-%d")
    filename = f"report_{date}.txt"
    path = f"{folder}/{filename}"

    dbx.files_upload(
        text.encode("utf-8"),
        path,
        mode=dropbox.files.WriteMode("overwrite"),
    )

    print(f"Saved report: {path}")


def send_whatsapp(msg):
    """Send WhatsApp message through Twilio."""
    sid = os.getenv("TWILIO_SID")
    auth = os.getenv("TWILIO_AUTH")

    client = Client(sid, auth)

    whatsapp_from = os.getenv("WHATSAPP_FROM")
    whatsapp_to = os.getenv("WHATSAPP_TO") or os.getenv("CEO_WHATSAPP_TO")

    message = client.messages.create(
        body=msg,
        from_=whatsapp_from,
        to=whatsapp_to
    )

    print("WhatsApp sent:", message.sid)


def main():
    print("Starting script...")

    # ---------------------------------------
    # ✅ NEW Dropbox client using REFRESH TOKEN
    # ---------------------------------------
    dbx = get_dropbox_client()

    incoming = os.getenv("INCOMING_ROOT")
    processed = os.getenv("PROCESSED_ROOT")
    reports = os.getenv("REPORTS_ROOT")

    compiled = compile_data(dbx, incoming)
    report = build_report(compiled)

    save_report(dbx, reports, report)

    send_whatsapp(report)

    # Move processed files
    files = fetch_files(dbx, incoming)
    for f in files:
        src = f"{incoming}/{f.name}"
        dst = f"{processed}/{f.name}"
        move_file(dbx, src, dst)

    print("Done.")


if __name__ == "__main__":
    main()
