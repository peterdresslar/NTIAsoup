import requests
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

allowed_domains = [
    "broadbandusa.ntia.doc.gov",
]  # List of allowed domains


def log_message(message, log_file):
    with open(log_file, "a") as file:
        file.write(f"{datetime.now()} - {message}\n")


def download_pdf(url, folder, log_file):
    start_time = datetime.now()
    response = requests.get(url)
    if response.status_code == 200:
        filename = url.split("/")[-1]
        filepath = os.path.join(folder, filename)
        with open(filepath, "wb") as file:
            file.write(response.content)
        end_time = datetime.now()
        filesize = os.path.getsize(filepath)
        log_message(
            f"Downloaded PDF - Start: {start_time}, End: {end_time}, Filename: {filename}, Filesize: {filesize} bytes",
            log_file,
        )
    else:
        log_message(f"Failed to download {url}", log_file)


def crawl_and_scrape(url, folder, visited, throttle, log_file):
    if url in visited:
        return
    visited.add(url)

    parsed_url = urlparse(url)
    if parsed_url.netloc not in allowed_domains:
        return  # Skip crawling if the domain is not in the allowed list

    log_message(f"Crawling URL: {url}", log_file)
    print(f"Crawling URL: {url}")

    time.sleep(throttle / 1000)  # Throttle requests

    response = requests.get(url, verify=True)
    if response.status_code != 200:
        log_message(f"Failed to retrieve {url}", log_file)
        return

    soup = BeautifulSoup(response.content, "html.parser")

    # print (soup.find_all("a", href=True)) ## major debug

    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(url, href)

        # Debugging
        # print(f"Original href: {href}, Full URL: {full_url}")

        # Check if '.pdf' is in the href attribute
        if ".pdf" in href:
            log_message(f"PDF found: {full_url}", log_file)
            download_pdf(full_url, folder, log_file)
        elif href.startswith("http") or href.startswith("/"):
            print(f"No PDF for {full_url} - crawling")
            crawl_and_scrape(full_url, folder, visited, throttle, log_file)


ntia_urls = [
    "https://broadbandusa.ntia.doc.gov/technical-assistance-hub",
    "https://broadbandusa.ntia.doc.gov/",
]

data_folder = "../data/ntia_pdf"
log_file = "crawl_log.txt"
throttle_millis = 10
file_cap = 2000

if not os.path.exists(data_folder):
    print(f"Creating folder: {data_folder}")
    os.makedirs(data_folder)

visited_urls = set()

start_time = datetime.now()
log_message(f"Script started at {start_time}", log_file)
print(f"Script started at {start_time}")

for ntia_url in ntia_urls:
    print(f"Crawling {ntia_url}")
    crawl_and_scrape(ntia_url, data_folder, visited_urls, throttle_millis, log_file)
    if len(os.listdir(data_folder)) >= file_cap:
        print(f"Reached file cap of {file_cap}. Exiting.")
        break

end_time = datetime.now()
print(f"Script ended at {end_time}")
log_message(f"Script ended at {end_time}", log_file)
log_message(f"Total time: {end_time - start_time}", log_file)
