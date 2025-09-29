#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor ofert – Pracuj.pl
Frazy: Główna księgowa, Główny księgowy, Chief Accountant

Funkcje:
- pełny import przy 1. uruchomieniu (głębsza paginacja),
- dzienne przyrosty (dopisywanie nowych do master CSV),
- e-mail z podsumowaniem:
    * treść: TABELA HTML (grupowanie po title: Firma | Link),
    * załączniki: CSV (zawsze) + XLSX (czytelna tabela).

Wysyłka maila:
- priorytet: SendGrid (SECRET: SENDGRID_API_KEY, TO_EMAIL, opcj. FROM_EMAIL),
- fallback: SMTP (SMTP_HOST, SMTP_USER, SMTP_PASS, opcj. SMTP_PORT, FROM_EMAIL, TO_EMAIL).
  Gmail: smtp.gmail.com:587 + hasło aplikacji (App Password), FROM=SMTP_USER.

Zależności (requirements.txt):
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
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import tz


# --- Ścieżki / pliki ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_CSV = os.path.join(DATA_DIR, "offers_master.csv")

# --- HTTP ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NatJobMonitor/1.1)",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}
SEARCH_URL = "https://www.pracuj.pl/praca/{kw}%3Bkw"
REQUEST_DELAY = 1.2  # s


# --------------- Utils scraping ---------------
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

        # Kontekst (próba wyłuskania firmy/lokalizacji)
        card = a.find_parent()
        context_text = " ".join(card.parent.stripped_strings) if card and card.parent else ""

        company = None
        # heurystyka: pierwszy „rozsądny” fragment tekstu jako nazwa firmy
        comp_match = re.search(r"\b([A-ZĄĆĘŁŃÓŚŹŻ0-9][\w\.\-&’'() ]{2,})\b", context_text)
        if comp_match:
            company = comp_match.group(1).strip()

        location = None
        loc_match = re.search(r"####\s*([A-ZĄĆĘŁŃÓŚŹŻa-ząćęłńóśźż ,\-\(\)]+)", context_text)
        if loc_match:
            location = loc_match.group(1).strip()

        offers.append({
            "job_id": job_id,
            "title": title,
            "url": ("https://www.pracuj.pl" + href) if href.startswith("/") else href,
            "company": company,
            "location": location,
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

        # filtr bezpieczeństwa – tytuł musi zawierać frazę
        pat = re.compile(keyword, re.IGNORECASE)
        batch = [b for b in batch if pat.search(b.get("title") or "")]

        # stop gdy brak nowych rekordów na kolejnych stronach
        new_keys = { (b.get("job_id") or b["url"]) for b in batch }
        old_keys = { (b.get("job_id") or b["url"]) for b in out }
        if not (new_keys - old_keys) and page > 1:
            break

        out.extend(batch)
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def enrich_offer_details(offer: Dict) -> Dict:
    """Opcjonalne wzbogacenie (data publikacji, branża – jeśli występuje)."""
    html = fetch_page(offer["url"])
    if not html:
        return offer
    soup = BeautifulSoup(html, "lxml")

    # Data publikacji – luźna heurystyka (nie zawsze dostępna)
    pub = None
    for el in soup.find_all(string=re.compile(r"Opublikowana:\s*\d{1,2}\s")):
        pub = el.strip()
        break

    industry = None
    for el in soup.select("li,div,span"):
        t = el.get_text(" ", strip=True)
        if re.search(r"\bBranża\b", t, re.IGNORECASE):
            m = re.search(r"Branża[:\s]+(.+)$", t, re.IGNORECASE)
            if m:
                industry = m.group(1).strip()
                break

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
        "job_id","title","company","location","url","industry","published","first_seen","source"
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

import unicodedata
from datetime import datetime

def _strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

def parse_pl_date(text: str) -> Optional[datetime]:
    """
    Próbuje sparsować polską datę typu:
    'Opublikowana: 12 września 2025' / '12 wrzesnia 2025'
    Zwraca datetime (00:00) albo None.
    """
    if not text:
        return None
    s = _strip_accents(text.lower())
    # usuń prefiks 'opublikowana:'
    s = re.sub(r"opublikowana:\s*", "", s)
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", s)
    if not m:
        # fallback: spróbuj standardowych formatów
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

# --------------- HTML tabela do e-maila ---------------
def build_html_summary(df: pd.DataFrame) -> str:
    """
    Jedna tabela: Nazwa firmy | Nazwa stanowiska | Link | Data dodania ogłoszenia.
    Sortowanie: najświeższe na górze (po 'published' jeśli da się odczytać, inaczej po 'first_seen').
    """
    if df is None or df.empty:
        return "<p>(Brak nowych ofert)</p>"

    work = df.copy()
    for col in ["title", "company", "url", "published", "first_seen"]:
        if col not in work.columns:
            work[col] = ""
    work = work.fillna("")

    # Policz kolumny pomocnicze: data publikacji (parsowana) i fallback do first_seen
    pub_dt = work["published"].apply(lambda s: parse_pl_date(s) if isinstance(s, str) else None)
    fs_dt = pd.to_datetime(work["first_seen"], errors="coerce")

    # sort_key: preferuj publikację, jeśli znamy; w przeciwnym razie first_seen
    sort_key = []
    date_out = []
    for i in range(len(work)):
        dt_pub = pub_dt.iloc[i]
        dt_fs  = fs_dt.iloc[i]
        chosen = dt_pub if pd.notnull(dt_pub) else dt_fs
        sort_key.append(chosen if pd.notnull(chosen) else pd.NaT)
        # Data do wyświetlenia – YYYY-MM-DD
        if pd.notnull(dt_pub):
            date_out.append(dt_pub.strftime("%Y-%m-%d"))
        elif pd.notnull(dt_fs):
            date_out.append(pd.to_datetime(dt_fs).strftime("%Y-%m-%d"))
        else:
            date_out.append("")
    work["_sort"] = sort_key
    work["_date_str"] = date_out

    # Sort malejąco (najświeższe na górze); NaT na końcu
    work = work.sort_values(by="_sort", ascending=False, na_position="last")

    # Budowa HTML
    rows_html = []
    for _, r in work.iterrows():
        company = r["company"] or "—"
        title   = r["title"] or "—"
        url     = r["url"]
        date_s  = r["_date_str"] or ""
        link_html = f'<a href="{url}" target="_blank" rel="noopener noreferrer">otwórz ogłoszenie</a>'
        rows_html.append(
            f"<tr>"
            f"<td>{company}</td>"
            f"<td>{title}</td>"
            f"<td>{link_html}</td>"
            f"<td>{date_s}</td>"
            f"</tr>"
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


# --------------- E-mail (SendGrid + SMTP) ---------------
def email_new_offers(new_offers_csv_path: str,
                     new_offers_df: pd.DataFrame,
                     extra_attachment_path: Optional[str] = None):
    """
    Wysyła maila:
    - HTML (tabelki) + fallback text/plain,
    - Załączniki: CSV (zawsze) + opcjonalnie XLSX.
    Nie rzuca wyjątku na niepowodzeniu – tylko loguje.
    """
    to_email = os.environ.get("TO_EMAIL")
    from_email = os.environ.get("FROM_EMAIL", to_email)
    subject = "NOWE oferty (Pracuj.pl) – Chief Accountant / Główna/y Księgowa/y"

    html_body = build_html_summary(new_offers_df)

    if new_offers_df is None or new_offers_df.empty:
        text_body = "Brak nowych ofert."
    else:
        lines = []
        for _, r in new_offers_df.head(30).iterrows():
            lines.append(f"- {r.get('title','')} | {r.get('company','')} | {r.get('url','')}")
        if len(new_offers_df) > 30:
            lines.append(f"... i {len(new_offers_df)-30} więcej.")
        text_body = "NOWE oferty (Pracuj.pl):\n\n" + "\n".join(lines)

    # --- SendGrid (priorytet) ---
    api_key = os.environ.get("SENDGRID_API_KEY")
    if api_key and to_email:
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, Content
            import base64

            message = Mail(from_email=from_email, to_emails=to_email, subject=subject)
            message.add_content(Content("text/plain", text_body))
            message.add_content(Content("text/html", html_body))

            # CSV
            with open(new_offers_csv_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
            attachments = [Attachment(FileContent(encoded),
                                      FileName(os.path.basename(new_offers_csv_path)),
                                      FileType("text/csv"),
                                      Disposition("attachment"))]

            # XLSX (opcjonalnie)
            if extra_attachment_path and os.path.exists(extra_attachment_path):
                with open(extra_attachment_path, "rb") as f:
                    encoded2 = base64.b64encode(f.read()).decode()
                attachments.append(
                    Attachment(FileContent(encoded2),
                               FileName(os.path.basename(extra_attachment_path)),
                               FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                               Disposition("attachment"))
                )
            message.attachment = attachments

            sg = SendGridAPIClient(api_key)
            sg.send(message)
            print("Email sent via SendGrid.")
            return
        except Exception as e:
            print("SendGrid failed:", e)

    # --- SMTP fallback (Gmail/M365) ---
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    if smtp_host and smtp_user and smtp_pass and to_email:
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders

            msg = MIMEMultipart("mixed")
            msg["From"] = from_email
            msg["To"] = to_email
            msg["Subject"] = subject

            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(text_body, "plain", "utf-8"))
            alt.attach(MIMEText(html_body, "html", "utf-8"))
            msg.attach(alt)

            # CSV
            part = MIMEBase("application", "octet-stream")
            with open(new_offers_csv_path, "rb") as f:
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f"attachment; filename={os.path.basename(new_offers_csv_path)}")
            msg.attach(part)

            # XLSX (opcjonalnie)
            if extra_attachment_path and os.path.exists(extra_attachment_path):
                part2 = MIMEBase("application",
                                 "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with open(extra_attachment_path, "rb") as f:
                    part2.set_payload(f.read())
                encoders.encode_base64(part2)
                part2.add_header("Content-Disposition",
                                 f"attachment; filename={os.path.basename(extra_attachment_path)}")
                msg.attach(part2)

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.sendmail(from_email, [to_email], msg.as_string())
            print("Email sent via SMTP.")
            return
        except Exception as e:
            print("SMTP failed:", e)

    print("No email credentials provided or email sending failed. Skipping email.")


# --------------- Główny przebieg ---------------
def run_scrape(full: bool) -> int:
    # frazy z pliku
    with open(os.path.join(os.path.dirname(__file__), "keywords.json"),
              "r", encoding="utf-8") as f:
        keywords = json.load(f)

    all_rows = []
    for kw in keywords:
        print(f"[INFO] Szukam: {kw}")
        offers = iterate_pages_for_keyword(kw, max_pages=80 if full else 20)
        print(f"[INFO] Znaleziono {len(offers)} kart dla '{kw}' (przed enrich).")
        for i, off in enumerate(offers):
            off["source"] = "pracuj.pl"
            off["first_seen"] = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S%z")
            if (i % 3) == 0:
                try:
                    off = enrich_offer_details(off)
                except Exception:
                    pass
            all_rows.append(off)
            time.sleep(0.2)

    df_new = pd.DataFrame(all_rows).drop_duplicates(subset=["job_id","url"])
    df_new = df_new[(df_new["title"].notna()) & (df_new["url"].notna())]

    master = load_master()
    before = len(master)

    if not master.empty:
        master["__key"] = master["job_id"].fillna("") + "|" + master["url"].fillna("")
    df_new["__key"] = df_new["job_id"].fillna("") + "|" + df_new["url"].fillna("")
    if master.empty:
        new_only = df_new.copy()
    else:
        new_only = df_new[~df_new["__key"].isin(set(master["__key"]))].copy()
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
        new_only["_sort"] = new_only.apply(
            lambda r: _parse_any_date(r.get("published"), r.get("first_seen")),
            axis=1
        )
        new_only = new_only.sort_values(
            by="_sort", ascending=False, na_position="last"
        ).drop(columns=["_sort"])
   
    # pliki dnia
    today = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y%m%d")
    daily_csv = os.path.join(DATA_DIR, f"offers_NEW_{today}.csv")
    if not new_only.empty:
        new_only.to_csv(daily_csv, index=False, encoding="utf-8")
    else:
        pd.DataFrame(columns=combined.columns).to_csv(daily_csv, index=False, encoding="utf-8")

    # XLSX – ładna tabelka
    daily_xlsx = os.path.join(DATA_DIR, f"offers_NEW_{today}.xlsx")
    cols = ["title","company","location","industry","published","url","first_seen","source","job_id"]
    cols = [c for c in cols if c in (new_only.columns if not new_only.empty else combined.columns)]
    with pd.ExcelWriter(daily_xlsx, engine="openpyxl") as xw:
        (new_only[cols] if not new_only.empty else new_only).to_excel(xw, index=False, sheet_name="NEW")

    # e-mail (CSV + XLSX, HTML w treści)
    email_new_offers(daily_csv, new_only, extra_attachment_path=daily_xlsx)

    print(f"[DONE] Dodano: {added} (nowe dziś: {len(new_only)}) | Razem w master: {len(combined)}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Wymuś pełny zaciąg (głęboka paginacja).")
    args = parser.parse_args()

    first_run = not os.path.exists(MASTER_CSV)
    full = args.full or first_run
    raise SystemExit(run_scrape(full=full))
