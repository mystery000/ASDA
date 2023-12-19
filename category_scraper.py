import os
import sys
import csv
import math
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver import Remote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection


load_dotenv()

class AsdaScraper:
    _aisle_links: List[str]
    _sbr_connection: ChromiumRemoteConnection
    
    def __init__(self, aisle_links: List[str], sbr_connection: ChromiumRemoteConnection):
        self._aisle_links = aisle_links
        self._sbr_connection = sbr_connection
    
    def get_asda_product_links_by_aisle(self, aisle_link: str) -> List[str]:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        product_links: List[str] = []
        
        try:
            max_page: int = 1
            with Remote(self._sbr_connection, options=chrome_options) as driver:
                driver.get(aisle_link)
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "cms-modules")))
                html = driver.page_source
                page = BeautifulSoup(html, "html5lib")
                page_navigation_element = page.find('div', class_="page-navigation")
                
                if page_navigation_element is None: return product_links
                
                max_page_element = page_navigation_element.find("div", class_="co-pagination__max-page")
                max_page = int(max_page_element.get_text()) if max_page_element else 1
                
            for page_no in range(max_page):
                try:
                    with Remote(self._sbr_connection, options=chrome_options) as page_driver:
                        page_driver.get(f"{aisle_link}?page={page_no + 1}")
                        WebDriverWait(page_driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "cms-modules")))
                        html = page_driver.page_source
                        page = BeautifulSoup(html, "html5lib")
                        page_navigation_element = page.find('div', class_="page-navigation")
                        if page_navigation_element is None: continue
                        cms_module_element = page_navigation_element.find_parent("div", class_="cms-modules")
                        items = cms_module_element.find_all('li', class_="co-item")
                        product_links.extend([f"https://groceries.asda.com{item.find('a', class_='co-product__anchor')['href']}" for item in items])
                        print(f"{aisle_link}?page={page_no + 1}", len(items))
                        
                except Exception as e:
                    logging.warning(f"Exception: {str(e)}")
                
        except Exception as e:
            logging.info(f"Exception: {str(e)}")
        
        return product_links
            
    def run(self):
        for aisle_link in self._aisle_links:
            asda_product_links = self.get_asda_product_links_by_aisle(aisle_link)
            csv_file_name = "asda_product_links.csv"
            with open(csv_file_name, "a", newline="") as csv_file:
                fieldnames = ["Link"]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if csv_file.tell() == 0:
                    writer.writeheader()
                for product in asda_product_links:
                    writer.writerow({"Link": product})
            
def run_category_scraper():
    print("Asda category scraper running...")
    
    csv_file_name = "asda_product_links.csv"
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    processes: List[mp.Process] = []
    
    try:
        SBR_WEBDRIVER = f"http://{os.getenv('SELENIUM_WEBDRIVER_AUTH')}@{os.getenv('SELENIUM_SERVER_IP')}:{os.getenv('SELENIUM_SERVER_PORT')}"
        
        try:
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
        except Exception as e:
            logging.warning("Scraping Browser connection failed")
            raise e

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        
        aisle_links: List[str] = []
            
        with Remote(sbr_connection, options=chrome_options) as driver:
            driver.get("https://groceries.asda.com/sitemap")
            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "cat__taxonomy")))
            html = driver.page_source
            page = BeautifulSoup(html, "html5lib")
            categories = page.find_all('div', class_="cat__taxonomy")
            
            for category in categories:
                departments = category.find_all('div', class_="dept")
                for department in departments:
                    asda_links = [f"https://groceries.asda.com{asda.a['href']}" for asda in department.find_all("li")]
                    
                    view_all_link = None

                    for asda_link in asda_links:
                        if asda_link.find('/view-all') > 0:
                            view_all_link = asda_link
                            break
                    
                    aisle_links.append(view_all_link) if view_all_link else aisle_links.extend(asda_links)
                    
        process_count = 6
        unit = math.floor(len(aisle_links) / process_count)
        
        processes = [
            mp.Process(target=AsdaScraper(aisle_links[unit * i : ], sbr_connection).run)
            if i == process_count - 1
            else mp.Process(target=AsdaScraper(aisle_links[unit * i : unit * (i + 1)], sbr_connection).run)
            for i in range(process_count)
        ] if len(aisle_links) >= process_count else [mp.Process(target=AsdaScraper(aisle_links, sbr_connection).run)]

        for process in processes:
            process.start()
        for process in processes:
            process.join()
           
    except KeyboardInterrupt:
        logging.info("Quitting...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
    finally:
        for process in processes:
            process.terminate()
        logging.info("Asda category scraper finished")

if __name__ == "__main__":
    run_category_scraper()    