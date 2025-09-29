#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor ofert – Pracuj.pl
Frazy: Główna księgowa, Główny księgowy, Chief Accountant

Kluczowe funkcje:
- Pierwszy bieg: pełny zaciąg (głębsza paginacja), kolejne: przyrosty + deduplikacja.
- Wzbogacanie ogłoszeń z podstrony:
    * employer_name z JSON-LD (hiringOrganization.name) lub [data-test="text-employerName"],
    * datePosted (data publikacji) z JSON-LD → 'published_iso' (YYYY-MM-DD),
    * validThrough (opcjonalnie) z JSON-LD lub bloku „do 11 paź” (nie używamy do sortu).
- E-mail:
    * treść: jedna tabela HTML (Nazwa firmy | Nazwa stanowiska | Link | Data dodania ogłoszenia),
    * załącznik: WYŁĄCZNIE XLSX (bez CSV), posortowane najświeższe na górze,
    * obsługa wielu odbiorców (TO_EMAIL: "a@a.com, b@b.com"),
    * SendGrid (priorytet) lub SMTP (Gmail/M365) jako fallback.

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
import json
import time
import argparse
import unicodedata
import datetime as dt
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import tz

# === Ścieżki / pliki ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_CSV = os.path.join(DATA_DIR, "offers_master.csv")   # master trzymamy nadal w CSV

# === HTTP ===
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NatJobMonitor/1.3)",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}
SEARCH_URL = "https://www.pracuj.pl/praca/{kw}%3Bkw"
REQUEST_DELAY = 1.2  # s

# =========================================================
# Pomocniki – daty
# =========================================================
def _strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

def parse_pl_date(text: str) -> Optional[datetime]:
    """
    Parsuje polskie daty:
    - 'Opublikowana: 12 września 2025'
    - '12 wrzesnia 2025'
    - '(do 11 paź)'  → zwraca 11.X bieżącego (heurystycznie) roku
    Zwraca datetime albo None.
    """
    if not text:
        return None
    s = _strip_accents(str(text).lower()).strip()
    s = re.sub(r"^(opublikowana|opublikowano|dodano):\s*", "", s)

    months_full = {
        "stycznia":1,"lutego":2,"marca":3,"kwietnia":4,"maja":5,"czerwca":6,
        "lipca":7,"sierpnia":8,"wrzesnia":9,"pazdziernika":10,"listopada":11,"grudnia":12
    }
    months_abbr = {
        "sty":1,"lut":2,"mar":3,"kwi":4,"maj":5,"cze":6,"lip":7,"sie":8,"wrz":9,"paz":10,"lis":11,"gru":12
    }

    # 1) pełny zapis: 12 wrzesnia 2025
    m = re.search(r"(\d{1,2})\s+([a-ząćęłńóśźż]+)\s+(\d{4})", s)
    if m:
        d = int(m.group(1)); mon = _strip_accents(m.group(2)); y = int(m.group(3))
        mm = months_full.get(mon) or months_abbr.get(mon[:3])
        if mm:
            try: return datetime(y, mm, d)
            except: return None

    # 2) skrót „do 11 paz”
    m2 = re.search(r"(\d{1,2})\s+([a-z]{3})\b", s)
    if m2:
        d = int(m2.group(1)); mon3 = _strip_accents(m2.group(2)[:3])
        mm = months_abbr.get(mon3)
        if mm:
            today = datetime.now()
            y = today.year
            try: return datetime(y, mm, d)
            except: return None

    # 3) fallback: pandas
    try:
        return pd.to_datetime(text, dayfirst=True, errors="raise").to_pydatetime()
    except Exception:
        return None

# =========================================================
# Scraping – listing i podstrony ogłoszeń
# =========================================================
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

        offers.append({
            "job_id": job_id,
            "title": title,
            "url": ("https://www.pracuj.pl" + href) if href.startswith("/") else href,
            "company": None,      # fallback – i tak pobierzemy employer_name z podstrony
            "location": None,
        })

    # deduplikacja po job_id/url
    uniq = {}
    for o in offers:
        k = o.get("job_id") or o["url"]
        uniq[k] = o
    return list(uniq.values())

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

        # filtr: tytuł zawiera frazę
        pat = re.compile(keyword, re.IGNORECASE)
        batch = [b for b in batch if pat.search(b.get("title") or "")]

        # przerwij jeśli nic nowego
        new_keys = {(b.get("job_id") or b["url"]) for b in batch}
        old_keys = {(b.get("job_id") or b["url"]) for b in out}
        if not (new_keys - old_keys) and page > 1:
            break

        out.extend(batch)
        page += 1
        time.sleep(REQUEST_DELAY)
    return out

def enrich_offer_details(offer: Dict) -> Dict:
    """
    Wzbogaca:
    - employer_name: JSON-LD (hiringOrganization.name) lub [data-test="text-employerName"]
    - published_iso/published: JSON-LD (datePosted)
    - valid_through: JSON-LD (validThrough) lub z bloku 'do 11 paź' (opcjonalnie)
    - industry: heurystycznie
    """
    html = fetch_page(offer["url"])
    if not html:
        return offer
    soup = BeautifulSoup(html, "lxml")

    employer = None
    date_posted_iso = None
    valid_through_iso = None

    # JSON-LD – najpewniejsze
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue

        candidates = []
        if isinstance(data, dict):
            if data.get("@type") == "JobPosting":
                candidates = [data]
            elif "@graph" in data and isinstance(data["@graph"], list):
                candidates = [x for x in data["@graph"] if isinstance(x, dict) and x.get("@type") == "JobPosting"]
        elif isinstance(data, list):
            candidates = [x for x in data if isinstance(x, dict) and x.get("@type") == "JobPosting"]

        for jp in candidates:
            org = jp.get("hiringOrganization")
            if isinstance(org, dict) and org.get("name"):
                employer = employer or org.get("name")
            if jp.get("datePosted"):
                date_posted_iso = jp["datePosted"]
            if jp.get("validThrough"):
                valid_through_iso = jp["validThrough"]

    # Fallback employer_name – nagłówek
    if not employer:
        el = soup.select_one('[data-test="text-employerName"]')
        if el:
            employer = el.get_text(strip=True)

    # Fallback validThrough – blok „do DD mmm”
    if not valid_through_iso:
        dur = soup.select_one('[data-test="section-duration-info"]')
        if dur:
            txt = _strip_accents(" ".join(dur.stripped_strings).lower())
            m = re.search(r"do\s+(\d{1,2})\s+([a-z]{3})", txt)
            if m:
                d = int(m.group(1))
                mon3 = m.group(2)
                mm_map = {"sty":1,"lut":2,"mar":3,"kwi":4,"maj":5,"cze":6,"lip":7,"sie":8,"wrz":9,"paz":10,"lis":11,"gru":12}
                mm = mm_map.get(mon3)
                if mm:
                    today = datetime.now()
                    y = today.year
                    try:
                        vt = datetime(y, mm, d)
                        valid_through_iso = vt.strftime("%Y-%m-%d")
                    except Exception:
                        pass

    # industry – jak było (heurystyka)
    industry = None
    for el2 in soup.select("li,div,span"):
        t = el2.get_text(" ", strip=True)
        if re.search(r"\bBranża\b", t, re.IGNORECASE):
            m = re.search(r"Branża[:\s]+(.+)$", t, re.IGNORECASE)
            if m:
                industry = m.group(1).strip()
                break

    offer["employer_name"] = employer
    offer["industry"] = industry

    if date_posted_iso:
        try:
            dp = pd.to_datetime(date_posted_iso, utc=False, errors="coerce")
            if pd.notnull(dp):
                offer["published_iso"] = dp.strftime("%Y-%m-%d")  # do sortu i wyświetlania
                offer["published"] = dp.strftime("%d %m %Y")      # opcjonalne „czytelne”
        except Exception:
            pass

    if valid_through_iso:
        offer["valid_through"] = valid_through_iso

    return offer

# =========================================================
# Master CSV
# =========================================================
def load_master() -> pd.DataFrame:
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(MASTER_CSV):
        return pd.read_csv(MASTER_CSV, dtype=str)
    return pd.DataFrame(columns=[
        "job_id","title","company","employer_name","location",
        "url","industry","published","published_iso","valid_through","first_seen","source"
    ])

def save_master(df: pd.DataFrame):
    df.to_csv(MASTER_CSV, index=False, encoding="utf-8")

def dedupe_concat(master: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    master = master.copy()
    new_df = new_df.copy()
    master["__key"] = master["job_id"].fillna("") + "|" + master["url"].fillna("")
    new_df["__key"] = new_df["job_id"].fillna("") + "|" + new_df["url"].fillna("")
    combined = pd.concat([master, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["__key"]).drop(columns=["__key"])
    return combined

# =========================================================
# HTML do maila – jedna tabela
# =========================================================
def build_html_summary(df: pd.DataFrame) -> str:
    """
    Jedna tabela: Nazwa firmy | Nazwa stanowiska | Link | Data dodania ogłoszenia.
    Sort: najświeższe na górze (published_iso → published parsowane → first_seen).
    """
    if df is None or df.empty:
        return "<p>(Brak nowych ofert)</p>"

    work = df.copy().fillna("")
    for col in ["title","url","published","published_iso","first_seen","employer_name","company"]:
        if col not in work.columns:
            work[col] = ""

    # Klucz sortowania
    def _parse_any_date_row(r):
        if r.get("published_iso"):
            return pd.to_datetime(r.get("published_iso"), errors="coerce")
        dt_pub = parse_pl_date(r.get("published"))
        if dt_pub:
            return pd.to_datetime(dt_pub)
        return pd.to_datetime(r.get("first_seen"), errors="coerce")

    work["_sort"] = work.apply(_parse_any_date_row, axis=1)
    work = work.sort_values(by="_sort", ascending=False, na_position="last")

    rows = []
    for _, r in work.iterrows():
        company = (r.get("employer_name") or r.get("company") or "—")
        title   = r.get("title") or "—"
        url     = r.get("url") or ""
        date_s  = r.get("published_iso") or ""
        link_html = f'<a href="{url}" target="_blank" rel="noopener noreferrer">otwórz ogłoszenie</a>'
        rows.append(f"<tr><td>{company}</td><td>{title}</td><td>{link_html}</td><td>{date_s}</td></tr>")

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
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )
    return header + table

# =========================================================
# E-mail – tylko XLSX jako załącznik
# =========================================================
def email_new_offers_xlsx(new_offers_df: pd.DataFrame, xlsx_path: Optional[str]):
    """
    Wysyła maila z tabelą HTML + ZAŁĄCZNIK XLSX (bez CSV).
    Obsługuje wielu odbiorców w TO_EMAIL (po przecinku).
    """
    to_value = os.environ.get("TO_EMAIL", "")
    to_emails = [e.strip() for e in to_value.split(",") if e.strip()]
    if not to_emails:
        print("No recipients in TO_EMAIL. Skipping email.")
        return
    from_email = os.environ.get("FROM_EMAIL", to_emails[0])

    subject = "NOWE oferty (Pracuj.pl) – Chief Accountant / Główna/y Księgowa/y"
    html_body = build_html_summary(new_offers_df)
    text_body = "Zobacz tabelę HTML w treści wiadomości. (Załącznik: XLSX)."

    # SendGrid
    api_key = os.environ.get("SENDGRID_API_KEY")
    if api_key:
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, Content
            import base64

            message = Mail(from_email=from_email, to_emails=to_emails, subject=subject)
            message.add_content(Content("text/plain", text_body))
            message.add_content(Content("text/html", html_body))

            if xlsx_path and os.path.exists(xlsx_path):
                with open(xlsx_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                message.attachment = [
                    Attachment(
                        FileContent(encoded),
                        FileName(os.path.basename(xlsx_path)),
                        FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                        Disposition("attachment"),
                    )
                ]

            sg = SendGridAPIClient(api_key)
            sg.send(message)
            print("Email (XLSX) sent via SendGrid.")
            return
        except Exception as e:
            print("SendGrid failed:", e)

    # SMTP fallback
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

# =========================================================
# Główny przebieg
# =========================================================
def run_scrape(full: bool) -> int:
    # frazy z pliku
    with open(os.path.join(os.path.dirname(__file__), "keywords.json"), "r", encoding="utf-8") as f:
        keywords = json.load(f)

    all_rows = []
    for kw in keywords:
        print(f"[INFO] Szukam: {kw}")
        offers = iterate_pages_for_keyword(kw, max_pages=80 if full else 20)
        print(f"[INFO] Znaleziono {len(offers)} kart dla '{kw}' (przed enrich).")
        for off in offers:
            off["source"] = "pracuj.pl"
            off["first_seen"] = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S%z")
            # wzbogacamy KAŻDĄ ofertę, aby mieć employer_name + datePosted
            try:
                off = enrich_offer_details(off)
            except Exception:
                pass
            all_rows.append(off)
            time.sleep(0.35)  # delikatniejsze tempo, bo wchodzimy na podstrony

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

    # Sort nowości (najświeższe na górze): preferuj published_iso → potem published parsowane → first_seen
    def _parse_any_date_row(r):
        if r.get("published_iso"):
            return pd.to_datetime(r.get("published_iso"), errors="coerce")
        dt_pub = parse_pl_date(r.get("published"))
        if dt_pub:
            return pd.to_datetime(dt_pub)
        return pd.to_datetime(r.get("first_seen"), errors="coerce")

    if not new_only.empty:
        new_only = new_only.copy()
        new_only["_sort"] = new_only.apply(_parse_any_date_row, axis=1)
        new_only["_date_str"] = new_only.apply(
            lambda r: (r.get("published_iso") or
                       (parse_pl_date(r.get("published")).strftime("%Y-%m-%d")
                        if parse_pl_date(r.get("published")) else
                        (pd.to_datetime(r.get("first_seen"), errors="coerce").strftime("%Y-%m-%d")
                         if pd.notnull(pd.to_datetime(r.get("first_seen"), errors="coerce")) else ""))),
            axis=1
        )
        new_only = new_only.sort_values(by="_sort", ascending=False, na_position="last").drop(columns=["_sort"])
    else:
        new_only["_date_str"] = ""

    # XLSX „dzisiejsze nowości”
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    today = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y%m%d")
    daily_xlsx = os.path.join(DATA_DIR, f"offers_NEW_{today}.xlsx")

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

    # E-mail (HTML tabela + tylko XLSX jako załącznik)
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
