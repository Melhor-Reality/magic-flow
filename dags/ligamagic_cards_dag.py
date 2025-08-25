from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


BASE_DIR = Path(__file__).resolve().parents[1]
TEMP_DIR = BASE_DIR / "temp"
URL = "https://www.ligamagic.com.br/ajax/cards/main.php"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}


def extract_pages(**_context) -> list[str]:
    pages: list[str] = []
    page = 1
    max_pages = 5  # safety limit
    while page <= max_pages:
        data = {"opc": "nextPage", "page": str(page), "search": "0"}
        resp = requests.post(URL, data=data, headers=HEADERS)
        if not resp.ok or not resp.text.strip():
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("div.tabela-cards table > tbody > tr")
        if not rows:
            break
        pages.append(resp.text)
        page += 1
        time.sleep(1)
    return pages


def _to_float(value: str) -> float | None:
    cleaned = re.sub(r"[^0-9.,-]", "", value).replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def transform_cards(ti, **_context) -> list[dict]:
    pages: list[str] = ti.xcom_pull(task_ids="extract_pages")
    cards: list[dict] = []
    for html in pages:
        soup = BeautifulSoup(html, "html.parser")
        for row in soup.select("div.tabela-cards table > tbody > tr"):
            cols = row.find_all("td")
            if len(cols) >= 5:
                card_name = cols[1].get_text(strip=True)
                min_val = _to_float(cols[2].get_text(strip=True))
                avg_val = _to_float(cols[3].get_text(strip=True))
                max_val = _to_float(cols[4].get_text(strip=True))
                cards.append(
                    {
                        "card_name": card_name,
                        "min": min_val,
                        "avg": avg_val,
                        "max": max_val,
                    }
                )
    return cards


def load_cards(ti, **_context) -> None:
    cards = ti.xcom_pull(task_ids="transform_cards")
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    output_file = TEMP_DIR / "cards.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(cards, f, ensure_ascii=False, indent=2)


with DAG(
    dag_id="ligamagic_cards",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    extract_op = PythonOperator(task_id="extract_pages", python_callable=extract_pages)
    transform_op = PythonOperator(task_id="transform_cards", python_callable=transform_cards)
    load_op = PythonOperator(task_id="load_cards", python_callable=load_cards)

    extract_op >> transform_op >> load_op
