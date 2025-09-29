# Monitor ofert – Pracuj.pl (Chief Accountant / Główna/y księgowa/y)

## Jak to działa
- **Pierwszy bieg**: jeśli nie ma `data/offers_master.csv`, skrypt robi **pełny zaciąg** (dużo stron) i tworzy master.
- **Kolejne biegi (codziennie)**: skrypt dokłada **tylko nowości** + wysyła e-mail z listą nowych ofert i CSV dnia.

## Frazesy
Edytuj `scraper/keywords.json` (domyślnie 3 frazy).

## E-mail (2 opcje)
**SendGrid (prościej w Actions):**
- Ustaw w repo **Secrets**:
  - `SENDGRID_API_KEY`
  - `TO_EMAIL` (np. `nbartosiewicz@altoadvisory.pl`)
  - (opcjonalnie) `FROM_EMAIL`

**SMTP (np. Microsoft 365 / inny):**
- Ustaw:
  - `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`, (opcjonalnie) `SMTP_PORT`
  - `TO_EMAIL` i (opcjonalnie) `FROM_EMAIL`

## Ręczne uruchomienie
