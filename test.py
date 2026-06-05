
import io
import zipfile
import cloudscraper

FY2026_URL = "https://www.fsis.usda.gov/sites/default/files/media_file/documents/raw_poultry_sampling_data_fy2026.zip"

def test_download_fy2026_cloudscraper():
    print(f"Downloading FY2026 ZIP via cloudscraper:\n  {FY2026_URL}\n")

    # Create a browser-like scraper session
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    # Important: include a referer to look more like genuine browser navigation
    headers = {
        "Referer": "https://www.fsis.usda.gov/news-events/publications/raw-poultry-sampling",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    resp = scraper.get(FY2026_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    print("Download OK — size:", len(resp.content), "bytes")

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    print("\nZIP file contents:")
    for name in z.namelist():
        print(" •", name)

    print("\nFY2026 cloudscraper test completed successfully.")
    return z

test_download_fy2026_cloudscraper()