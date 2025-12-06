#!/usr/bin/env python3
"""
Automated Daily Loading Report
Dropbox → Compile next-day rows → Twilio WhatsApp

Environment variables (GitHub Secrets):
DROPBOX_APP_KEY
DROPBOX_APP_SECRET
DROPBOX_REFRESH_TOKEN
TWILIO_SID
TWILIO_AUTH
WHATSAPP_FROM
WHATSAPP_TO (or CEO_WHATSAPP_TO)
INCOMING_ROOT
PROCESSED_ROOT
REPORTS_ROOT
"""

import os
import io
from datetime import datetime, timedelta, timezone
import pandas as pd
import dropbox
from dropbox.files import WriteMode
from twilio.rest import Client

MAX_ROWS = 500

# ---------------------------------------
# Dropbox client using REFRESH TOKEN
# ---------------------------------------
def get_dropbox_client():
    app_key = os.environ["DROPBOX_APP_KEY"]
    app_secret = os.environ["DROPBOX_APP_SECRET"]
    refresh_token = os.environ["DROPBOX_REFRESH_TOKEN"]

    return dropbox.Dropbox(
        oauth2_refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret
    )


def load_excel_from_dropbox(dbx, file_path):
    _, res = dbx.files_download(file_path)
    return pd.read_excel(io.BytesIO(res.content))


def move_file(dbx, src, dst):
    try:
        dbx.files_move_v2(src, dst, autorename=True)
    except Exception as e:
        print(f"Error moving {src} → {dst}: {e}")


def fetch_files(dbx, folder):
    try:
        res = dbx.files_list_folder(folder)
        return res.entries
    except Exception:
        return []


def compile_all_godowns(dbx, incoming_root):
    compiled = {}

    # list all godown subfolders
    try:
        subentries = dbx.files_list_folder(incoming_root).entries
    except Exception as e:
        print("Error listing incoming root:", e)
        return compiled

    for entry in subentries:
        if isinstance(entry, dropbox.files.FolderMetadata):
            godown_folder = f"{incoming_root}/{entry.name}"
            godown_name = entry.name

            compiled[godown_name] = pd.DataFrame()

            files = fetch_files(dbx, godown_folder)

            for file in files:
                if not file.name.lower().endswith((".xlsx", ".xls")):
                    continue

                path = f"{godown_folder}/{file.name}"
                print(f"Processing: {path}")

                try:
                    df = load_excel_from_dropbox(dbx, path)
                    compiled[godown_name] = pd.concat(
                        [compiled[godown_name], df], ignore_index=True
                    )
                except Exception as e:
                    print("Error reading Excel:", path, e)

    return compiled

# -----------------------------------------------------
# FINAL UPDATED FORMATTED REPORT FUNCTION
# -----------------------------------------------------
def build_report(compiled):
    lines = []

    # ---------------------------------------------
    # 1️⃣ EXTRACT DATE FROM EXCEL FILE IF PRESENT
    # ---------------------------------------------
    report_date = None

    for godown, df in compiled.items():
        if report_date is None and not df.empty:

            # find DATE column
            date_col = None
            for c in df.columns:
                if "DATE" in c.upper():
                    date_col = c
                    break

            if date_col:
                try:
                    report_date = pd.to_datetime(df[date_col].iloc[0]).date()
                except:
                    report_date = None

    # Fallback → use today's IST date
    if report_date is None:
        IST = timezone(timedelta(hours=5, minutes=30))
        report_date = datetime.now(IST).date()

    # ---------------------------------------------
    # 2️⃣ HEADER
    # ---------------------------------------------
    lines.append("NEXT-DAY LOADING REPORT")
    lines.append(f"Date: {report_date}")
    lines.append("-" * 40)

    total = 0

    # column spacing
    COL_PARTY = 18
    COL_MATERIAL = 14
    COL_QTY = 12
    COL_RATE = 12

    header = (
        f"{'PARTY'.ljust(COL_PARTY)}"
        f"{'MATERIAL'.ljust(COL_MATERIAL)}"
        f"{'QTY'.ljust(COL_QTY)}"
        f"{'RATE'.ljust(COL_RATE)}"
    )

    separator = "-" * (COL_PARTY + COL_MATERIAL + COL_QTY + COL_RATE)

    # supported variations
    QTY_KEYS = ["APROX QTY", "APPROX QTY", "QUANTITY", "QTY"]
    RATE_KEYS = ["RATE / KG", "RATE", "RATE PER KG", "RATE/KG"]

    # ---------------------------------------------
    # 3️⃣ PROCESS EACH GODOWN
    # ---------------------------------------------
    for godown, df in compiled.items():
        lines.append(f"\nGODOWN: {godown.upper()}")

        if df.empty:
            lines.append("  No items")
            continue

        # normalize headers
        normalized = {c.upper().strip(): c for c in df.columns}

        lines.append(header)
        lines.append(separator)

        # iterate rows
        for _, row in df.head(MAX_ROWS).iterrows():

            # PARTY
            party = str(row.get(normalized.get("PARTY", ""), "")).strip()

            # MATERIAL
            material = str(row.get(normalized.get("MATERIAL", ""), "")).strip()

            # QTY
            qty = ""
            for k in QTY_KEYS:
                key = normalized.get(k)
                if key:
                    qty = str(row.get(key, "")).strip()
                    break

            # RATE
            rate = ""
            for k in RATE_KEYS:
                key = normalized.get(k)
                if key:
                    rate = str(row.get(key, "")).strip()
                    break

            # formatted line
            line = (
                f"{party.ljust(COL_PARTY)[:COL_PARTY]}"
                f"{material.ljust(COL_MATERIAL)[:COL_MATERIAL]}"
                f"{qty.ljust(COL_QTY)[:COL_QTY]}"
                f"{rate.ljust(COL_RATE)[:COL_RATE]}"
            )

            lines.append(line)
            total += 1

    # ---------------------------------------------
    # FOOTER
    # ---------------------------------------------
    lines.append("\n" + "-" * 40)
    lines.append(f"Total Items: {total}")

    return "\n".join(lines)


def save_report(dbx, folder, text):
    date = datetime.now().strftime("%Y-%m-%d")
    filename = f"report_{date}.txt"
    path = f"{folder}/{filename}"

    dbx.files_upload(
        text.encode("utf-8"),
        path,
        mode=WriteMode("overwrite"),
    )

    print(f"Saved report: {path}")


def send_whatsapp(msg):
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

    dbx = get_dropbox_client()

    incoming = os.getenv("INCOMING_ROOT")
    processed = os.getenv("PROCESSED_ROOT")
    reports = os.getenv("REPORTS_ROOT")

    compiled = compile_all_godowns(dbx, incoming)
    report = build_report(compiled)

    save_report(dbx, reports, report)
    send_whatsapp(report)

    # ---------------------------------------------------------
    # SAFE FILE MOVING (ONLY FILES, NOT FOLDERS)
    # ---------------------------------------------------------
    try:
        godown_folders = dbx.files_list_folder(incoming).entries
    except Exception:
        godown_folders = []

    for entry in godown_folders:

        if not isinstance(entry, dropbox.files.FolderMetadata):
            continue  # skip files in root

        godown_path = f"{incoming}/{entry.name}"
        godown_processed = f"{processed}/{entry.name}"

        # ensure processed/godown exists
        try:
            dbx.files_create_folder_v2(godown_processed)
        except:
            pass

        files = fetch_files(dbx, godown_path)

        for f in files:
            if isinstance(f, dropbox.files.FolderMetadata):
                continue  # skip subfolders

            src = f"{godown_path}/{f.name}"
            dst = f"{godown_processed}/{f.name}"
            move_file(dbx, src, dst)

    print("Done.")


if __name__ == "__main__":
    main()
