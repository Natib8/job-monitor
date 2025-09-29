#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Pracuj.pl dla fraz:
- Główna księgowa
- Główny księgowy
- Chief Accountant

Funkcje:
- pełny import (pierwsze uruchomienie, gdy nie ma data/offers_master.csv)
- przyrosty dzienne (dopisywanie nowych ogłoszeń bez duplikatów)
- wysyłka e-mail z NOWYMI ofertami (lista w treści + CSV w załączniku)

Uwaga techniczna:
- Pracuj.pl generuje listingi w klasycznych URL-ach z parametrem ;kw (keyword).
- Paginacja bywa ukryta JS-em. Skrypt próbuje pn=2,3,... aż braknie wyników.
- Identyfikator ogłoszenia bierzemy z końcówki URL (ciąg cyfr).
"""

import os
import re
import csv
import time
import json
import argparse
import datetime as dt
from typing import List, Dict, Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import tz

# --- Konfiguracja bazowa ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_CSV = os.path.join(DATA_DIR, "offers_master.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NatJobMonitor/1.0; +https://example.com)",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}

SEARCH_URL = "https://www.pracuj.pl/praca/{kw}%3Bkw"

# Delikatne opóźnienie, aby nie „młotkować” serwisu
REQUEST_DELAY = 1.2  # sekundy


def sanitize_keyword(kw: str) -> str:
    # URL-escape dla spacji i polskich znaków zrobi przeglądarka; tu wystarczy prosty replace
    # ale użyjemy requests do prawidłowego kodowania
    from urllib.parse import quote
    return quote(kw, safe="")


def parse_list_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    offers = []
    # Każda karta oferty ma nagłówek z <a> prowadzącym do /praca/...oferta,100xxxxxxx
    for a in soup.select("a[href*='/praca/'][href*='oferta']"):
        href = a.get("href", "").strip()
        title = a.get_text(strip=True)
        if not href or not title:
            continue
        # Upewnijmy się, że to link do oferty (z numerem id)
        m = re.search(r"oferta.*?(\d{7,})", href)
        job_id = m.group(1) if m else None
        # Nazwa firmy i lokalizacja zwykle są w sąsiedztwie:
        card = a.find_parent()
        company = None
        location = None
        # Spróbuj znaleźć najbliższe elementy z nazwą firmy i lokalizacją
        # (layout bywa różny – bierzemy kilka heurystyk)
        for sib in (card, card.parent):
            if not sib:
                continue
            # Firma
            comp = sib.find(["a","span"], string=re.compile(r".+"), attrs={})
            # Lokalizacja bywa po "#### " w wylistowanym układzie – fallback: tekst z elementów z "####"
            # Tu weźmiemy pierwsze "#### " z tekstem miasta dzielnicy – przy parsywaniu HTML-owego dumpu i w runtime.
        # Zrobimy prostsze: pobierzmy firmę i lokację z całej karty tekstowo:
        context_text = " ".join(card.parent.stripped_strings) if card and card.parent else ""
        # Firma: czasem występuje jako link do pracodawcy.* – wyciągniemy pierwszy taki fragment
        comp_match = re.search(r"\b([A-ZĄĆĘŁŃÓŚŹŻ0-9][\w\.\-&’'() ]{2,})\b.*?(?:pracodawcy\.pracuj\.pl|Siedziba firmy|####)", context_text)
        if comp_match:
            company = comp_match.group(1).strip()
        # Lokalizacja: po "#### " bywa np. "#### Kraków" w dumpach
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
    # Usunięcie duplikatów po job_id/url
    unique = {}
    for o in offers:
        k = o.get("job_id") or o["url"]
        unique[k] = o
    return list(unique.values())


def fetch_page(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200 and "Oferty pracy" in r.text:
            return r.text
        # Jeśli przeglądarka „niewspierana” – i tak zwykle dostajemy treść listingu.
        if r.status_code == 200:
            return r.text
        return None
    except requests.RequestException:
        return None


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
            # brak nowych kart – zatrzymaj
            break
        # filtr bezpieczeństwa: zostaw tylko oferty zawierające frazę w tytule (case-insensitive, PL znaki)
        pat = re.compile(keyword, re.IGNORECASE)
        batch = [b for b in batch if pat.search(b.get("title") or "")]
        # dopisz
        # Jeśli na kolejnych stronach zaczniemy powtarzać wyniki, to przerwijmy
        new_keys = { (b.get("job_id") or b["url"]) for b in batch }
        old_keys = { (b.get("job_id") or b["url"]) for b in out }
        if not new_keys - old_keys and page > 1:
            break
        out.extend(batch)
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def enrich_offer_details(offer: Dict) -> Dict:
    """Pobierz dodatkowe info z podstrony ogłoszenia (data publikacji, branża, itd. – jeśli dostępne)."""
    url = offer["url"]
    html = fetch_page(url)
    if not html:
        return offer
    soup = BeautifulSoup(html, "lxml")
    # Data publikacji – heurystyka (bywa „Opublikowana: DD mmmm YYYY”)
    pub = None
    for el in soup.find_all(text=re.compile(r"Opublikowana:\s*\d{1,2}\s")):
        pub = el.strip()
        break
    # Branża (jeśli widoczna)
    industry = None
    for el in soup.select("li,div,span"):
        t = el.get_text(" ", strip=True)
        if re.search(r"\bBranża\b", t, re.IGNORECASE):
            # np. "Branża: Produkcja"
            m = re.search(r"Branża[:\s]+(.+)$", t, re.IGNORECASE)
            if m:
                industry = m.group(1).strip()
                break
    offer["published"] = pub
    offer["industry"] = industry
    return offer


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
    # Klucz: job_id jeśli jest, w przeciwnym razie URL
    master = master.copy()
    new_df = new_df.copy()
    master["key"] = master["job_id"].fillna("") + "|" + master["url"].fillna("")
    new_df["key"] = new_df["job_id"].fillna("") + "|" + new_df["url"].fillna("")
    combined = pd.concat([master, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["key"])
    combined = combined.drop(columns=["key"])
    return combined


def email_new_offers(new_offers_csv_path: str, new_offers_df: pd.DataFrame):
    """
    Wysyłka maila:
    - preferowany: SendGrid (sekret SENDGRID_API_KEY)
    - alternatywnie: SMTP (SMTP_HOST, SMTP_USER, SMTP_PASS, SMTP_PORT)
    """
    to_email = os.environ.get("TO_EMAIL")
    from_email = os.environ.get("FROM_EMAIL", to_email)
    subject = "NOWE oferty (Pracuj.pl) – Chief Accountant / Główna/y Księgowa/y"
    # Zbuduj treść
    lines = []
    for _, r in new_offers_df.head(30).iterrows():
        lines.append(f"- {r['title']} | {r.get('company') or ''} | {r.get('location') or ''} | {r['url']}")
    if len(new_offers_df) > 30:
        lines.append(f"... i {len(new_offers_df)-30} więcej.")
    body = (
        "Cześć,\n\n"
        "Poniżej nowe oferty z ostatniego uruchomienia monitora (Pracuj.pl):\n\n" +
        "\n".join(lines if lines else ["(Brak nowych ofert)"]) +
        "\n\nPozdrawiam,\nJob Monitor"
    )

    # 1) SendGrid
    api_key = os.environ.get("SENDGRID_API_KEY")
    if api_key and to_email:
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
            message = Mail(
                from_email=from_email,
                to_emails=to_email,
                subject=subject,
                plain_text_content=body
            )
            with open(new_offers_csv_path, "rb") as f:
                data = f.read()
            import base64
            encoded = base64.b64encode(data).decode()
            attachment = Attachment(
                FileContent(encoded),
                FileName(os.path.basename(new_offers_csv_path)),
                FileType("text/csv"),
                Disposition("attachment"),
            )
            message.attachment = attachment
            sg = SendGridAPIClient(api_key)
            sg.send(message)
            print("Email sent via SendGrid.")
            return
        except Exception as e:
            print("SendGrid failed:", e)

    # 2) SMTP fallback
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    if smtp_host and smtp_user and smtp_pass and to_email:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        part = MIMEBase("application", "octet-stream")
        with open(new_offers_csv_path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(new_offers_csv_path)}")
        msg.attach(part)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, [to_email], msg.as_string())
        print("Email sent via SMTP.")
        return

    print("No email credentials provided. Skipping email sending.")


def run_scrape(full: bool) -> int:
    # Wczytaj frazy:
    with open(os.path.join(os.path.dirname(__file__), "keywords.json"), "r", encoding="utf-8") as f:
        keywords = json.load(f)

    # Pobierz z listingu (paginacja pn=2,3,... aż braknie wyników)
    all_rows = []
    for kw in keywords:
        print(f"[INFO] Szukam: {kw}")
        offers = iterate_pages_for_keyword(kw, max_pages=80 if full else 20)
        print(f"[INFO] Znaleziono {len(offers)} kart dla '{kw}' (przed enrich).")
        # enrich (delikatnie, by nie spamować)
        for i, off in enumerate(offers):
            off["source"] = "pracuj.pl"
            off["first_seen"] = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S%z")
            # Wzbogacenie co 1 na 3, żeby nie przesadzić (możesz zmienić na każdy)
            if (i % 3) == 0:
                try:
                    off = enrich_offer_details(off)
                except Exception:
                    pass
            all_rows.append(off)
            time.sleep(0.2)

    df_new = pd.DataFrame(all_rows).drop_duplicates(subset=["job_id","url"])
    # Filtr bezpieczeństwa – niech zostaną tylko realne oferty (z tytułem i URL)
    df_new = df_new[(df_new["title"].notna()) & (df_new["url"].notna())]

    master = load_master()
    before = len(master)
    # Zbuduj „nowe” względem mastera, żeby móc wysłać maila
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

    # Zapisz master
    save_master(combined)

    # Zapisz CSV z dzisiejszymi nowościami (do maila)
    today = dt.datetime.now(tz=tz.gettz("Europe/Warsaw")).strftime("%Y%m%d")
    daily_csv = os.path.join(DATA_DIR, f"offers_NEW_{today}.csv")
    if not new_only.empty:
        new_only.to_csv(daily_csv, index=False, encoding="utf-8")
    else:
        # nawet jak pusto – wyślij „puste” podsumowanie
        pd.DataFrame(columns=combined.columns).to_csv(daily_csv, index=False, encoding="utf-8")

    # Mail
    email_new_offers(daily_csv, new_only)

    print(f"[DONE] Dodano: {added} (nowe dziś: {len(new_only)})  |  Razem w master: {len(combined)}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Wymuś duży, pełny zaciąg (wiele stron).")
    args = parser.parse_args()

    # Jeśli to pierwszy bieg (brak master.csv) – wymuś full
    first_run = not os.path.exists(MASTER_CSV)
    full = args.full or first_run

    raise SystemExit(run_scrape(full=full))
