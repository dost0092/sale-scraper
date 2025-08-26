import os
import re
import json
import asyncio
import aiohttp
import gspread
import pandas as pd
import csv
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import xlsxwriter
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Configuration
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
credentials = service_account.Credentials.from_service_account_info(
    json.loads(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)
SPREADSHEET_ID = '1BHQygLkM1fLw6VKjYuvDbC_qpMfQ7MzTqzOTMgQt1t8'  # From your error
LOCAL_CSV_PATH = "foreclosure_sales.csv"
OUTPUT_FILE = "foreclosure_sales.xlsx"

TARGET_COUNTIES = [
    {"county_id": "25", "county_name": "Atlantic County, NJ"},
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"}
]

class ForeclosureScraper:
    def __init__(self):
        self.credentials = None
        self.service = None
        self.scheduler = AsyncIOScheduler()
        self.setup_google_credentials()
        
    def setup_google_credentials(self):
        """Setup Google credentials with proper error handling"""
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            print("Google Sheets API client initialized successfully")
        except Exception as e:
            print(f"Error initializing Google Sheets client: {e}")
            print("Please ensure:")
            print("1. Google Sheets API is enabled at: https://console.developers.google.com/apis/api/sheets.googleapis.com")
            print("2. Service account key file exists and is valid")
            print("3. Service account email has edit access to the spreadsheet")
        
    def norm_text(self, s: str) -> str:
        if not s:
            return ""
        return re.sub(r"\s+", " ", s).strip()

    def extract_property_id_from_href(self, href: str) -> str:
        try:
            q = parse_qs(urlparse(href).query)
            return q.get("PropertyId", [""])[0]
        except Exception:
            return ""

    async def goto_with_retry(self, page, url: str, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = await page.goto(url, wait_until="networkidle", timeout=60000)
                if response and response.status == 200:
                    return response
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                await asyncio.sleep(2 ** attempt)
        return None

    async def dismiss_banners(self, page):
        selectors = [
            "button:has-text('Accept')", "button:has-text('I Agree')",
            "button:has-text('Close')", "button.cookie-accept",
            "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception:
                pass

    async def scrape_county_sales(self, page, county):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']} -> {url}")
        
        try:
            await self.goto_with_retry(page, url)
            await self.dismiss_banners(page)

            try:
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except PlaywrightTimeoutError:
                print(f"[WARN] No sales found for {county['county_name']}")
                return []

            rows = page.locator("table.table.table-striped tbody tr")
            n = await rows.count()
            results = []

            for i in range(n):
                row = rows.nth(i)
                tds = row.locator("td")
                
                # Get the case number from the second column (index 1)
                case_number = self.norm_text(await tds.nth(1).inner_text()) if await tds.count() > 1 else ""
                
                # Get the sale date from the third column (index 2)
                sale_date = self.norm_text(await tds.nth(2).inner_text()) if await tds.count() > 2 else ""
                
                defendant = self.norm_text(await tds.nth(3).inner_text()) if await tds.count() > 3 else ""
                address = self.norm_text(await tds.nth(4).inner_text()) if await tds.count() > 4 else ""

                details_a = row.locator("td.hidden-print a")
                details_href = await details_a.get_attribute("href") or ""
                details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                property_id = self.extract_property_id_from_href(details_href)

                prop_address, approx_judgment = "", ""
                if details_url:
                    try:
                        await self.goto_with_retry(page, details_url)
                        await self.dismiss_banners(page)
                        await page.wait_for_selector(".sale-details-list", timeout=10000)
                        
                        items = page.locator(".sale-details-list .sale-detail-item")
                        for j in range(await items.count()):
                            label = self.norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
                            val = self.norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())
                            if "Property Address" in label:
                                prop_address = val
                            elif "Approx" in label:
                                approx_judgment = val
                    except Exception as e:
                        print(f"Error scraping details for {county['county_name']}: {str(e)}")
                    finally:
                        # Navigate back to the list page
                        await self.goto_with_retry(page, url)
                        await self.dismiss_banners(page)
                        await page.wait_for_selector("table.table.table-striped tbody tr", timeout=30000)
                        rows = page.locator("table.table.table-striped tbody tr")

                results.append({
                    "County": county['county_name'],
                    "Case Number": case_number,
                    "Defendant": defendant,
                    "Address": address,
                    "Sale Date": sale_date,
                    "Property Address": prop_address,
                    "Approx Judgment": approx_judgment,
                    "Property ID": property_id,
                    "Scrape Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            return results
        except Exception as e:
            print(f"Error scraping {county['county_name']}: {str(e)}")
            return []

    def parse_date(self, value):
        if pd.isna(value) or str(value).strip() == "":
            return None

        # Try normal parsing
        try:
            return pd.to_datetime(value, errors="raise")
        except Exception:
            pass

        # Handle custom formats like "L-24000380" or "F-25000134"
        m = re.match(r"[A-Z\-]*([0-9]{2})([0-9]{2})([0-9]{3,})", str(value))
        if m:
            yy, mm, _ = m.groups()
            year = 2000 + int(yy) if int(yy) < 50 else 1900 + int(yy)
            month = int(mm)

            # Only return if month is valid
            if 1 <= month <= 12:
                return datetime(year, month, 1)
            else:
                return None  # Invalid → skip

        return None

    def clean_dates_and_create_excel(self, data):
        """Clean dates and create Excel file with one sheet for all data + county-wise sheets"""
        df = pd.DataFrame(data)
        invalid_rows = []

        # Parse and clean Sale Date
        if "Sale Date" in df.columns:
            parsed_dates = []
            for i, val in enumerate(df["Sale Date"]):
                parsed = self.parse_date(val)
                if parsed is None and not pd.isna(val) and str(val).strip() != "":
                    invalid_rows.append(df.iloc[i].to_dict())
                parsed_dates.append(parsed)
            df["Sale Date"] = parsed_dates

        if invalid_rows:
            pd.DataFrame(invalid_rows).to_csv("invalid_dates.csv", index=False)
            print("⚠️ Invalid dates saved to invalid_dates.csv")

        try:
            with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
                workbook = writer.book

                header_format = workbook.add_format({
                    "bold": True, "bg_color": "#4257a7", "font_color": "white",
                    "border": 1, "align": "center", "valign": "vcenter"
                })
                cell_format = workbook.add_format({"border": 1})
                date_format = workbook.add_format({"num_format": "yyyy-mm-dd", "border": 1})

                # --- First sheet: All Data ---
                sheet_name = "All Data"
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]

                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df.columns):
                    max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                    worksheet.set_column(i, i, min(max_len, 40), cell_format)
                    if "Date" in col:
                        worksheet.set_column(i, i, 15, date_format)

                # --- County-wise sheets ---
                for county, county_df in df.groupby("County"):
                    sheet_name = county[:30]
                    county_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    worksheet = writer.sheets[sheet_name]

                    for col_num, value in enumerate(county_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)

                    for i, col in enumerate(county_df.columns):
                        max_len = max(county_df[col].astype(str).map(len).max(), len(col)) + 2
                        worksheet.set_column(i, i, min(max_len, 40), cell_format)
                        if "Date" in col:
                            worksheet.set_column(i, i, 15, date_format)

            print(f"✅ Excel file saved: {OUTPUT_FILE} (All Data + county-wise sheets)")
            return df
        except Exception as e:
            print(f"Error creating Excel file: {e}")
            return df


    async def update_google_sheet(self, data):
        """Update Google Sheet with one sheet for all data + county-wise sheets"""
        if not self.service:
            print("Google Sheets service not initialized. Skipping update.")
            return False

        try:
            sheet = self.service.spreadsheets()
            df = pd.DataFrame(data)

            # --- First tab: All Data ---
            all_sheet_name = "All Data"
            try:
                add_sheet_request = {"addSheet": {"properties": {"title": all_sheet_name}}}
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={"requests": [add_sheet_request]}
                ).execute()
                print(f"Created new sheet: {all_sheet_name}")
            except HttpError as e:
                if e.resp.status == 400 and "already exists" in str(e):
                    pass
                else:
                    raise

            # Prepare all-data values
            all_values = [list(df.columns)]
            for _, row in df.iterrows():
                row_values = []
                for col in df.columns:
                    val = row[col]
                    if isinstance(val, (pd.Timestamp, datetime)):
                        row_values.append(val.strftime("%Y-%m-%d"))
                    else:
                        row_values.append("" if pd.isna(val) else str(val))
                all_values.append(row_values)

            # Clear & update "All Data"
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{all_sheet_name}'!A:Z"
            ).execute()
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{all_sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": all_values}
            ).execute()
            print(f"✓ Updated Google Sheet tab: {all_sheet_name} ({len(df)} rows)")

            # --- County-wise tabs ---
            for county, county_df in df.groupby("County"):
                sheet_name = county[:30]

                try:
                    add_sheet_request = {"addSheet": {"properties": {"title": sheet_name}}}
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=SPREADSHEET_ID,
                        body={"requests": [add_sheet_request]}
                    ).execute()
                    print(f"Created new sheet: {sheet_name}")
                except HttpError as e:
                    if e.resp.status == 400 and "already exists" in str(e):
                        pass
                    else:
                        raise

                # Prepare county values
                values = [list(county_df.columns)]
                for _, row in county_df.iterrows():
                    row_values = []
                    for col in county_df.columns:
                        val = row[col]
                        if isinstance(val, (pd.Timestamp, datetime)):
                            row_values.append(val.strftime("%Y-%m-%d"))
                        else:
                            row_values.append("" if pd.isna(val) else str(val))
                    values.append(row_values)

                # Clear old + write new
                sheet.values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"'{sheet_name}'!A:Z"
                ).execute()
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"'{sheet_name}'!A1",
                    valueInputOption="USER_ENTERED",
                    body={"values": values}
                ).execute()

                print(f"✓ Updated Google Sheet tab: {sheet_name} ({len(county_df)} rows)")

            return True

        except Exception as e:
            print(f"✗ Google Sheets update error: {e}")
            return False

    def save_to_csv(self, data):
        """Save data to local CSV file"""
        try:
            df = pd.DataFrame(data)
            df.to_csv(LOCAL_CSV_PATH, index=False, encoding='utf-8')
            print(f"✓ Data saved to local CSV: {LOCAL_CSV_PATH}")
            return True
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return False

    async def scrape_all_counties(self):
        all_data = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for county in TARGET_COUNTIES:
                try:
                    county_data = await self.scrape_county_sales(page, county)
                    all_data.extend(county_data)
                    print(f"✓ Completed {county['county_name']}: {len(county_data)} records")
                    
                    # Delay between counties to avoid rate limiting
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    print(f"Failed to scrape {county['county_name']}: {str(e)}")
                    continue
            
            await browser.close()
        
        return all_data

    async def scheduled_scrape(self):
        print(f"Starting scheduled scrape at {datetime.now()}")
        data = await self.scrape_all_counties()
        
        if data:
            # Save raw data to CSV
            self.save_to_csv(data)
            
            # Clean dates and create Excel file
            cleaned_data = self.clean_dates_and_create_excel(data)
            
            # Update Google Sheets if configured
            if self.service:
                await self.update_google_sheet(cleaned_data.to_dict('records'))
            else:
                print("Google Sheets not configured, skipping online update")
        else:
            print("No data scraped")
            
        print(f"Finished scheduled scrape at {datetime.now()}")

    def start_scheduler(self):
        # Schedule for every day at 9:30 AM EST
        trigger = CronTrigger(hour=9, minute=30, timezone="America/New_York")
        self.scheduler.add_job(self.scheduled_scrape, trigger)
        self.scheduler.start()
        print("Scheduler started. Press Ctrl+C to exit.")
        
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            self.scheduler.shutdown()
            print("Scheduler stopped.")

async def main():
    scraper = ForeclosureScraper()
    
    # For initial run
    await scraper.scheduled_scrape()
    
    # For scheduled execution (uncomment the next line)
    # scraper.start_scheduler()

if __name__ == "__main__":
    asyncio.run(main())