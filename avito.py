import re
import csv
import json
import time
import uuid
import random

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ============================================
# CONFIG
# ============================================

BASE_URL    = "https://www.avito.ma"
LISTING_URL = "https://www.avito.ma/fr/maroc/immobilier"
OUTPUT_CSV  = "avito_dataset.csv"
MAX_PAGES   = 10   # number of listing pages to scrape
MAX_LINKS   = 300  # safety cap (~30 ads/page x 10 pages)

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


# ============================================
# SELENIUM DRIVER (shared instance)
# ============================================

def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=fr-FR")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    # Remove webdriver fingerprint via CDP
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['fr-FR', 'fr'] });
        """
    })

    return driver


# ============================================
# CLEAN TEXT
# ============================================

def clean_text(value):
    if not value:
        return ""
    value = str(value)
    value = value.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


# ============================================
# PARSE PUBLICATION DATE
# ============================================

def parse_publication_date(soup):
    time_tag = soup.find("time")
    if not time_tag:
        return ""

    # Try datetime attribute first (most reliable)
    datetime_attr = time_tag.get("datetime")
    if datetime_attr:
        try:
            dt = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
            dt = dt.replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    # Fallback: parse "il y a X minutes/heures/jours/mois/ans"
    text = clean_text(time_tag.get_text()).lower()
    now  = datetime.now()

    try:
        match = re.search(r"\d+", text)
        n = int(match.group()) if match else 1

        if "minute" in text:
            dt = now - timedelta(minutes=n)
        elif "heure" in text:
            dt = now - timedelta(hours=n)
        elif "jour" in text:
            dt = now - timedelta(days=n)
        elif "mois" in text:
            dt = now - timedelta(days=n * 30)
        elif "an" in text:
            dt = now - timedelta(days=n * 365)
        else:
            return text

        return dt.strftime("%Y-%m-%d %H:%M:%S")

    except Exception:
        return text


# ============================================
# GET LINKS — scrape MAX_PAGES listing pages
# ============================================

def get_links():
    import requests
    print(f"Scraping {MAX_PAGES} listing pages...")

    links = []

    for page in range(1, MAX_PAGES + 1):
        url = f"{LISTING_URL}?o={page}"
        print(f"  Page {page}/{MAX_PAGES} ({len(links)} links so far)...")

        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            html = response.text
        except Exception as e:
            print(f"  [ERROR] Page {page} -> {e}")
            break

        soup      = BeautifulSoup(html, "html.parser")
        new_found = 0

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"):
                href = urljoin(BASE_URL, href)
            if re.search(r"_\d+\.htm$", href):
                href = href.split("?")[0]
                if href not in links:
                    links.append(href)
                    new_found += 1
            if len(links) >= MAX_LINKS:
                break

        if new_found == 0:
            print(f"  No new links on page {page}, stopping early.")
            break

        # Polite delay between listing pages
        time.sleep(random.uniform(1, 2))

    print(f"Found {len(links)} total links\n")
    return links


# ============================================
# FETCH PAGE + EQUIPMENTS via SELENIUM
# ============================================

def fetch_page_with_selenium(driver, url):
    driver.get(url)

    # Wait for h1 (max 10s)
    for _ in range(20):
        if driver.find_elements(By.TAG_NAME, "h1"):
            break
        time.sleep(0.5)

    # Scroll to trigger lazy loading
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    time.sleep(1)

    # Click ALL "Voir plus" buttons
    driver.execute_script("""
        const buttons = document.querySelectorAll('button');
        for (const btn of buttons) {
            if (btn.getAttribute('aria-label') === 'Voir plus') {
                btn.click();
            }
        }
    """)

    # Wait for equipment count to stabilize
    prev_count = 0
    for _ in range(10):
        equip_imgs = driver.find_elements(
            By.XPATH,
            "//h2[text()='Équipements']/following-sibling::div//img"
        )
        curr_count = len(equip_imgs)
        if curr_count > 0 and curr_count == prev_count:
            break
        prev_count = curr_count
        time.sleep(0.2)

    # Extract equipments from live DOM
    equipments = []
    try:
        equip_imgs = driver.find_elements(
            By.XPATH,
            "//h2[text()='Équipements']/following-sibling::div//img"
        )
        for img in equip_imgs:
            alt = img.get_attribute("alt")
            if alt and alt not in equipments:
                equipments.append(alt)
    except Exception as e:
        print(f"[WARN] Equipment extraction: {e}")

    html = driver.page_source
    return html, equipments


# ============================================
# PARSE PUBLICATION
# ============================================

def parse_publication(driver, url):
    html, equipments = fetch_page_with_selenium(driver, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    data = {}

    data["id"]  = str(uuid.uuid4())
    data["url"] = url

    # ---- TITLE ----
    h1 = soup.find("h1")
    data["title"] = clean_text(h1.get_text()) if h1 else ""

    # ---- PRICE ----
    price_el = soup.find("p", class_=re.compile("sc-16573058-12"))
    data["price"] = clean_text(price_el.get_text()) if price_el else ""

    # ---- LOCATION ----
    loc = soup.find("span", class_=re.compile("sc-16573058-17"))
    data["location"] = clean_text(loc.get_text()) if loc else ""

    # ---- PUBLICATION TIME ----
    data["publication_time"] = parse_publication_date(soup)

    # ---- SELLER ----
    seller = soup.find(attrs={"data-testid": "SellerName"})
    data["seller"] = clean_text(seller.get_text()) if seller else ""

    # ---- CATEGORY ----
    cat_label = soup.find("span", string=re.compile(r"Cat[eé]gorie", re.I))
    if cat_label:
        cat_val = cat_label.find_next("span")
        data["category"] = clean_text(cat_val.get_text()) if cat_val else ""
    else:
        data["category"] = ""

    # ---- DESCRIPTION ----
    desc = soup.find("div", class_=re.compile("sc-9bb253d7-0"))
    data["description"] = clean_text(desc.get_text()) if desc else ""

    # ---- BREADCRUMB ----
    breadcrumb = []
    ol = soup.find("ol")
    if ol:
        for li in ol.find_all("li"):
            text = clean_text(li.get_text())
            text = re.sub(r"Home Icon", "", text).strip()
            if text:
                breadcrumb.append(text)
    data["breadcrumb"] = json.dumps(breadcrumb, ensure_ascii=False)

    # ---- PROPERTIES ----
    properties = {}
    for param in soup.find_all("div", class_=re.compile("sc-cd1c365e-1")):
        value = param.find("span", class_=re.compile("fjZBup"))
        label = param.find("span", class_=re.compile("bXFCIH"))
        if value and label:
            properties[clean_text(label.get_text())] = clean_text(value.get_text())
    data["properties"] = json.dumps(properties, ensure_ascii=False)

    # ---- EQUIPMENTS (from Selenium live DOM) ----
    data["equipments"] = json.dumps(equipments, ensure_ascii=False)

    # ---- IMAGES (exclude card thumbnails from other listings) ----
    images = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if src and "content.avito.ma" in src and "?t=card" not in src and src not in images:
            images.append(src)
    data["images"] = json.dumps(images, ensure_ascii=False)

    print(f"[OK] {data['title']} | {data['price']} | {data['publication_time']} -- {len(equipments)} equipments")
    return data


# ============================================
# SAVE CSV
# ============================================

def save_csv(data_list):
    keys = [
        "id", "url", "title", "price", "location",
        "publication_time", "breadcrumb", "category",
        "seller", "description", "properties", "equipments", "images"
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in data_list:
            writer.writerow(row)
    print(f"\nSaved {len(data_list)} rows -> {OUTPUT_CSV}")


# ============================================
# MAIN
# ============================================

def main():
    links = get_links()
    if not links:
        print("No links found")
        return

    results = []
    print(f"Scraping {len(links)} ads...\n")

    driver = create_driver()

    try:
        for i, link in enumerate(links, 1):
            print(f"[{i}/{len(links)}]", end=" ")
            try:
                data = parse_publication(driver, link)
                if data:
                    results.append(data)
            except Exception as e:
                print(f"[ERROR] {link} -> {e}")
            time.sleep(random.uniform(1, 2))
    finally:
        driver.quit()

    save_csv(results)


if __name__ == "__main__":
    main()