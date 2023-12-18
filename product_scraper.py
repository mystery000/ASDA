import os
import sys
import csv
import math
import pandas
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import Remote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection


def get_product_page_links() -> List[str]:
    csv_file_name = "asda_product_links.csv"
    links: List[str] = []
    try:
        if os.path.exists(csv_file_name):
            products = pandas.read_csv(csv_file_name)
            products.drop_duplicates(subset="Link", inplace=True)
            links.extend(products["Link"].values.tolist())
    except pandas.errors.EmptyDataError as e:
        logging.error(f"Error: {str(e)}")
    finally:
        return links
    
class AsdaProductScraper:
    _sbr_connection: ChromiumRemoteConnection
    _product_links: List[str]
    
    def __init__(self, product_links: List[str], sbr_connection: ChromiumRemoteConnection) -> None:
        self._sbr_connection = sbr_connection
        self._product_links = product_links
    
    def scrape(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        for product_link in self._product_links:
            try:
                print(product_link)
                with Remote(self._sbr_connection, options=chrome_options) as driver:
                    driver.get(product_link)
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "pdp-main-details")))
                    html = driver.page_source
                    page = BeautifulSoup(html, "html5lib")
                    
                    with open("asda_products.csv", 'a', newline='') as csv_file:
                        fieldnames = [
                        'source',
                        'title', 
                        'description',
                        'item_price',
                        'offer_price',
                        'unit_price',
                        'average_rating',
                        'review_count',
                        'tags',                       
                        'categories',
                        'product_url',
                        'image_url',                            
                        'last_updated']
                                           
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        
                        if csv_file.tell() == 0:
                            writer.writeheader()
                        
                        product_page = page.find("div", class_="product-detail-page__main-detail-cntr")
                        
                        if product_page is None: continue
                        
                        source = "ASDA"
                        
                        title_element = product_page.find("h1", class_="pdp-main-details__title")
                        title = title_element.get_text(strip=True) if title_element else None
                        
                        
                        picture_element = page.find("div", class_="product-detail-page__left-cntr").find("picture")
                        image_url = picture_element.source["srcset"] if picture_element else None
                        
                        category_element = page.find("div", class_="pdp-breadcrumb")
                        categories = "".join([category.get_text() for category in (category_element.find_all("a") if category_element else [])]).replace("breadcrumb", "").strip()
                        
                        tag_element = product_page.find("div", class_="pdp-main-details__icons-container")
                        tags = ",".join([tag.get_text(strip=True) for tag in (tag_element.find_all("li") if tag_element else [])])
                        
                        review_count_element = product_page.find("span", class_="co-product__review-count")
                        review_count = review_count_element.get_text(strip=True)[1:-1] if review_count_element else 0
                        
                        rating_element = product_page.find("div", class_="co-product__rating pdp-main-details__rating")
                        
                        try:
                            average_rating = rating_element["aria-label"][:rating_element["aria-label"].find("star")].strip() if rating_element else 0
                        except:
                            average_rating = 0
                        
                        unit_price_element = product_page.find("span", class_="co-product__price-per-uom")    
                        unit_price = unit_price_element.get_text(strip=True)[1:-1] if unit_price_element else None
                        
                        item_price_container = product_page.find("div", class_="pdp-main-details__price-container")
                        
                        try:
                            item_price_element = list(item_price_container.children)[0]
                            item_price_element.span.decompose()
                            item_price = item_price_element.get_text(strip=True)
                        except:
                            item_price = None
                        
                        try:
                            offer_price_element = list(item_price_container.children)[1]
                            offer_price_element.span.decompose()
                            offer_price = offer_price_element.get_text(strip=True)
                        except:
                            offer_price = None
                        
                        description: str = ""
                        
                        try:
                            pdp_reviews = product_page.find_all("div", class_="pdp-description-reviews__product-details-cntr")
                            for pdp_review in pdp_reviews:
                                if list(pdp_review.children)[0].get_text(strip=True) == "Features":
                                    description = list(pdp_review.children)[1].get_text(strip=True)
                                    break
                        except:
                            pass
                        
                        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")    

                        logging.info({
                            'source': source,
                            'title': title, 
                            'description': description,
                            'item_price': item_price,
                            'offer_price': offer_price,
                            'unit_price': unit_price,
                            'average_rating': average_rating,
                            'review_count': review_count,
                            'tags': tags,
                            'categories': categories,
                            'product_url': product_link,
                            'image_url': image_url,
                            'last_updated': now
                            })
                        
                        writer.writerow({
                        'source': source,
                        'title': title, 
                        'description': description,
                        'item_price': item_price,
                        'offer_price': offer_price,
                        'unit_price': unit_price,
                        'average_rating': average_rating,
                        'review_count': review_count,
                        'tags': tags,
                        'categories': categories,
                        'product_url': product_link,
                        'image_url': image_url,
                        'last_updated': now
                        })
                    
            except Exception as e:
                logging.info(f"Exception: {str(e)}")

def run_product_scraper():
    print("Asda product scraper running...")
    
    csv_file_name = "asda_products.csv"
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    logging.basicConfig(
        format="[%(asctime)s] %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    processes: List[mp.Process] = []
    
    try:
        SBR_WEBDRIVER = f"http://65.21.129.16:9515"
        
        try:
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
        except Exception as e:
            logging.warning("Scraping Browser connection failed")
            raise e

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        
        asda_product_links = get_product_page_links()
                    
        process_count = 10
        unit = math.floor(len(asda_product_links) / process_count)
        
        processes = [
            mp.Process(target=AsdaProductScraper(asda_product_links[unit * i : ], sbr_connection).scrape)
            if i == process_count - 1
            else mp.Process(target=AsdaProductScraper(asda_product_links[unit * i : unit * (i + 1)], sbr_connection).scrape)
            for i in range(process_count)
        ] if len(asda_product_links) >= process_count else [mp.Process(target=AsdaProductScraper(asda_product_links, sbr_connection).scrape)]

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
        logging.info("Asda product scraper finished")
    

if __name__ == "__main__":
    run_product_scraper()