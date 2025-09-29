#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor ofert – Pracuj.pl
Frazy: Główna księgowa, Główny księgowy, Chief Accountant

Zmiany:
- W wiadomości e-mail oraz w załączniku wysyłamy WYŁĄCZNIE XLSX (bez CSV).
- „Nazwa firmy” pobieramy z employer name na podstronie ogłoszenia:
  <h2 data-test="text-employerName">...</h2> (fallback: stara heurystyka z listingu).
- Tabela w mailu: Nazwa firmy | Nazwa stanowiska | Link | Data dodania ogłoszenia.
- Sortowanie: najświeższe u góry (po „published” jeśli dostępne, inaczej po „first_seen”).
- Priorytet wysyłki: SendGrid; fallback: SMTP (Gmail/M365). Obsługa wielu odbiorców (TO_EMAIL po przecinku).

Wymagane paczki (requirements.txt):
requests
beautifulsoup4
lxml
pandas
python-dateutil
sendgrid
html5lib
openpyxl
"""

import os
import re
import time
import json
import argparse
import datetime as dt
import unicodedata
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import tz

# --- Ścieżki / pliki ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_CSV = os.path.join(DATA_DIR, "offers_master.csv")   # master trzymamy nadal jako CSV

# --- HTTP ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NatJobMonitor/1.2)",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}
SEARCH_URL = "https://www.pracuj.pl/praca/{kw}%3Bkw"
REQUEST_DELAY = 1.2  # s


# --------------- Pomocniki dat ---------------
def _strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

def parse_pl_date(text: str) -> Optional[datetime]:
    """Parsuje np. 'Opublikowana: 12 września 2025' / '12 wrzesnia 2025' → datetime."""
    if not text:
        return None
    s = _strip_accents(str(text).lower())
    s = re.sub(r"opublikowana:\s*", "", s)
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", s)
    if not m:
        try:
            return pd.to_datetime(text, dayfirst=True, errors="raise")
        except Exception:
            return None
    d, mon, y = int(m.group(1)), m.group(2), int(m.group(3))
    months = {
        "stycznia":1, "lutego":2, "marca":3, "kwietnia":4, "maja":5, "czerwca":6,
        "lipca":7, "sierpnia":8, "wrzesnia":9, "pazdziernika":10, "listopada":11, "grudnia":12
    }
    mm = months.get(mon)
    if not mm:
        return None
    try:
        return datetime(y, mm, d)
    except Exception:
        return None


# --------------- Scraping ---------------
def sanitize_keyword(kw: str) -> str:
    from urllib.parse import quote
    return quote(kw, safe="")

def fetch_page(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200:
            return r.text
        return None
    except requests.RequestException:
        return None

def parse_list_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    offers = []
    for a in soup.select("a[href*='/praca/'][href*='oferta']"):
        href = a.get("href", "").strip()
        title = a.get_text(strip=True)
        if not href or not title:
            continue
        m = re.search(r"oferta.*?(\d{7,})", href)
        job_id = m.group(1) if m else None

        # Minimalny kontekst; employer_name i tak dobierzemy z podstrony
        offers.append({
            "job_id": job_id,
            "title": title,
            "url": ("https://www.pracuj.pl" + href) if href.startswith("/") else href,
            "company": None,          # fallback z listingu, ale i tak wzbogacimy
            "location": None,
        })

    # deduplikacja po job_id/url
    unique = {}
    for o in offers:
        k = o.get("job_id") or o["url"]
        unique[k] = o
    return list(unique.values())

def iterate_pages_for_keyword(keyword: str, max_pages: int = 50) -> List[Dict]:
    out = []
    kw_enc = sanitize_keyword(keyword)
    page = 1
    while page <= max_pages:
        url = SEARCH_URL.format(kw=kw_enc)
        if page > 1:
            url = f"{url}?pn={page}"
        html = fetch_page(url)
        if not html:
            break
        batch = parse_list_page(html)
        if not batch:
            break

        # filtr – tytuł musi zawierać frazę
        pat = re.compile(keyword, re.IGNORECASE)
        batch = [b for b in batch if pat.search(b.get("title") or "")]

        # przerwij, jeśli nic nowego
        new_keys = { (b.get("job_id") or b["url"]) for b in batch }
        old_keys = { (b.get("job_id") or b["url"]) for b in out }
        if not (new_keys - old_keys) and page > 1:
            break

        out.extend(batch)
        page += 1
        time.sleep(REQUEST_DELAY)
    return out

def enrich_offer_details(offer: Dict) -> Dict:
    """Wzbogaca o employer_name, published, industry (jeśli dostępne)."""
    html = fetch_page(offer["url"])
    if not html:
        return offer
    soup = BeautifulSoup(html, "lxml")

    # employer_name z <h2 data-test="text-employerName">
    employer = None
    el = soup.select_one('[data-test="text-employerName"]')
    if el:
        employer = el.get_text(strip=True)

    # data publikacji (heurystyka)
    pub = None
    el_pub = soup.find(string=re.compile(r"Opublikowana:\s*\d{1,2}\s"))
    if el_pub:
        pub = el_pub.strip()

    # branża (opcjonalnie)
    industry = None
    for el2 in soup.select("li,div,span"):
        t = el2.get_text(" ", strip=True)
        if re.search(r"\bBranża\b", t, re.IGNORECASE):
            m = re.search(r"Branża[:\s]+(.+)$", t, re.IGNORECASE)
            if m:
                industry = m.group(1).strip()
                break

    offer["employer_name"] = employer
    offer["published"] = pub
    offer["industry"] = industry
    return offer


# --------------- Master CSV ---------------
def load_master() -> pd.DataFrame:
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(MASTER_CSV):
        return pd.read_csv(MASTER_CSV, dtype=str)
    return pd.DataFrame(columns=[
        "job_id","title","company","employer_name","location",
        "url","industry","published","first_seen","source"
    ])

def save_master(df: pd.DataFrame):
    df.to_csv(MASTER_CSV, index=False, encoding="utf-8")

def dedupe_concat(master: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    master = master.copy()
    new_df = new_df.copy()
    master["__key"] = master["job_id"].fillna("") + "|" + master["url"].fillna("")
    new_df["__key"] = new_df["job_id"].fillna("") + "|" + new_df["url"].fillna("")
    combined = pd.concat([master, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["__key"])
    combined = combined.drop(columns=["__key"])
    return combined


# --------------- HTML – pojedyncza tabela ---------------
def build_html_summary(df: pd.DataFrame) -> str:
    """
    Jedna tabela: Nazwa firmy | Nazwa stanowiska | Link | Data dodania ogłoszenia.
    Sort: najświeższe na górze (published → first_seen).
    """
    if df is None or df.empty:
        return "<p>(Brak nowych ofert)</p>"

    work = df.copy().fillna("")
    for col in ["title","url","published","first_seen","employer_name","company"]:
        if col not in work.columns:
            work[col] = ""

    # wybór daty do wyświetlenia i klucza sortowania
    pub_dt = work["published"].apply(lambda s: parse_pl_date(s) if isinstance(s, str) else None)
    fs_dt  = pd.to_datetime(work["first_seen"], errors="coerce")

    sort_key, date_out = [], []
    for i in range(len(work)):
        dt_pub = pub_dt.iloc[i]
        dt_fs  = fs_dt.iloc[i]
        chosen = dt_pub if pd.notnull(dt_pub) else dt_fs
        sort_key.append(chosen if pd.notnull(chosen) else pd.NaT)
        date_out.append(
            dt_pub.strftime("%Y-%m-%d") if pd.notnull(dt_pub)
            else (pd.to_datetime(dt_fs).strftime("%Y-%m-%d") if pd.notnull(dt_fs) else "")
        )
    work["_sort"] = sort_key
    work["_date_str"] = date_out

    work = work.sort_values(by="_sort", ascending=False, na_position="last")

    rows_html = []
    for _, r in work.iterrows():
        company = (r.get("employer_name") or r.get("company") or "—")
        title   = r["title"] or "—"
        url     = r["url"]
        date_s  = r["_date_str"] or ""
        link_html = f'<a href="{url}" target="_blank" rel="noopener noreferrer">otwórz ogłoszenie</a>'
        rows_html.append(
            f"<tr><td>{company}</td><td>{title}</td><td>{link_html}</td><td>{date_s}</td></tr>"
        )

    header = (
        "<p style='font-family:Arial,sans-serif'>"
        "Poniżej <strong>nowe oferty</strong> (Pracuj.pl):"
        "</p>"
    )
    table = (
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:13px'>"
        "<thead><tr>"
        "<th style='text-align:left'>Nazwa firmy</th>"
        "<th style='text-align:left'>Nazwa stanowiska</th>"
        "<th style='text-align:left'>Link</th>"
        "<th style='text-align:left'>Data dodania ogłoszenia</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
    )
    return header + table


# --------------- E-mail (tylko XLSX jako załącznik) ---------------
def email_new_offers_xlsx(new_offers_df: pd.DataFrame, xlsx_path: Optional[str]):
    """
    Wysyła maila z HTML-ową tabelą + JEDYNYM załącznikiem XLSX (jeśli istnieje).
    Wspiera wielu odbiorców (TO_EMAIL = 'a@a.com, b@b.com').
    """
    # odbiorcy
    to_value = os.environ.get("TO_EMAIL", "")
    to_emails = [e.strip() for e in to_value.split(",") if e.strip()]
    if not to_emails:
        print("No recipients in TO_EMAIL. Skipping email.")
        return
    from_email = os.environ.get("FROM_EMAIL", to_emails[0])

    subject = "NOWE oferty (Pracuj.pl) – Chief Accountant / Główna/y Księgowa/y"
    html_body = build_html_summary(new_offers_df)
    text_body = "Zobacz tabelę HTML w treści wiadomości. (Załącznik: XLSX)."

    # --- SendGrid (priorytet) ---
    api_key = os.environ.get("SENDGRID_API_KEY")
    if api_key:
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, Content
            import base64

            message = Mail(from_email=from_email, to_emails=to_emails, subject=subject)
            message.add_content(Content("text/plain", text_body))
            message.add_content(Content("text/html", html_body))

            attachments = []
            if xlsx_path and os.path.exists(xlsx_path):
                with open(xlsx_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                attachments.append(
                    Attachment(FileContent(encoded),
                               FileName(os.path.basename(xlsx_path)),
                               FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                               Disposition("attachment"))
                )
            if attachments:
                message.attachment = attachments

            sg = SendGridAPIClient(api_key)
            sg.send(message)
            print("Email (XLSX) sent via SendGrid.")
            return
        except Exception as e:
            print("SendGrid failed:", e)

    # --- SMTP fallback (Gmail/M365) ---
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    if smtp_host and smtp_user and smtp_pass:
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders

            msg = MIMEMultipart("mixed")
            msg["From"] = from_email
            msg["To"] = ", ".join(to_emails)
            msg["Subject"] = subject

            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(text_body, "plain", "utf-8"))
            alt.attach(MIMEText(html_body, "html", "utf-8"))
            msg.attach(alt)

            if xlsx_path and os.path.exists(xlsx_path):
                part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with open(xlsx_path, "rb") as f:
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(xlsx_path)}")
                msg.attach(part)

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.sendmail(from_email, to_emails, msg.as_string())
            print("Email (XLSX) sent via SMTP.")
            return
        except Exception as e:
            print("SMTP failed:", e)

    print("No email credentials provided or email sending failed. Skipping email.")


# --------------- Główny przebieg ---------------
def run_scrape(full: bool) -> int:
    # frazy z pliku
    with open(os.path.join(os.path.dirname(__file__), "keywords.json"), "r", encoding="utf-8") as f:
        keywords = json.load(f)

    all_rows = []
    for kw in keywords:
        print(f"[INFO] Szukam: {kw}")
        offers = iterate_pages_for_keyword(kw, max_pages=80 if full else 20)
        print(f"[INFO] Znaleziono {len(offers)} kart dla '{kw}' (przed enrich).")
        for i, off in enumerate(offers):
            off["source"] = "pracuj.pl"
            off["first_seen"] = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S%z")
            # teraz wzbogacamy KAŻDĄ ofertę, aby mieć employer_name
            try:
                off = enrich_offer_details(off)
            except Exception:
                pass
            all_rows.append(off)
            time.sleep(0.35)  # delikatniejsze tempo: więcej wizyt na podstronach

    df_new = pd.DataFrame(all_rows).drop_duplicates(subset=["job_id","url"])
    df_new = df_new[(df_new["title"].notna()) & (df_new["url"].notna())]

    master = load_master()
    before = len(master)

    if not master.empty:
        master["__key"] = master["job_id"].fillna("") + "|" + master["url"].fillna("")
    df_new["__key"] = df_new["job_id"].fillna("") + "|" + df_new["url"].fillna("")
    new_only = df_new.copy() if master.empty else df_new[~df_new["__key"].isin(set(master["__key"]))].copy()
    df_new = df_new.drop(columns=["__key"], errors="ignore")
    master = master.drop(columns=["__key"], errors="ignore")

    combined = dedupe_concat(master, df_new)
    added = len(combined) - before

    save_master(combined)

    # --- SORTOWANIE nowości (najświeższe na górze) ---
    def _parse_any_date(pub, fs):
        dt_pub = parse_pl_date(pub) if isinstance(pub, str) else None
        dt_fs  = pd.to_datetime(fs, errors="coerce")
        return dt_pub if pd.notnull(dt_pub) else dt_fs

    if not new_only.empty:
        new_only = new_only.copy()
        new_only["_sort"] = new_only.apply(lambda r: _parse_any_date(r.get("published"), r.get("first_seen")), axis=1)
        new_only["_date_str"] = new_only.apply(
            lambda r: (_parse_any_date(r.get("published"), r.get("first_seen")).strftime("%Y-%m-%d")
                       if _parse_any_date(r.get("published"), r.get("first_seen")) is not None else ""),
            axis=1
        )
        new_only = new_only.sort_values(by="_sort", ascending=False, na_position="last").drop(columns=["_sort"])
    else:
        new_only["_date_str"] = ""

    # --- XLSX „dzisiejsze nowości” ---
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    today = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y%m%d")
    daily_xlsx = os.path.join(DATA_DIR, f"offers_NEW_{today}.xlsx")

    # zbuduj tabelę z czytelnymi kolumnami
    export = new_only.copy()
    export["Nazwa firmy"] = export["employer_name"].fillna("").replace("", pd.NA)
    export["Nazwa firmy"] = export["Nazwa firmy"].fillna(export.get("company"))
    export["Nazwa stanowiska"] = export["title"]
    export["Link"] = export["url"]
    export["Data dodania ogłoszenia"] = export["_date_str"]

    final_cols = ["Nazwa firmy", "Nazwa stanowiska", "Link", "Data dodania ogłoszenia"]
    with pd.ExcelWriter(daily_xlsx, engine="openpyxl") as xw:
        (export[final_cols] if not export.empty else pd.DataFrame(columns=final_cols)).to_excel(
            xw, index=False, sheet_name="NEW"
        )

    # --- e-mail (Tylko XLSX w załączniku, HTML tabela w treści) ---
    email_new_offers_xlsx(new_only, daily_xlsx)

    print(f"[DONE] Dodano: {added} (nowe dziś: {len(new_only)}) | Razem w master: {len(combined)}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Wymuś pełny zaciąg (głęboka paginacja).")
    args = parser.parse_args()

    first_run = not os.path.exists(MASTER_CSV)
    full = args.full or first_run
    raise SystemExit(run_scrape(full=full))
