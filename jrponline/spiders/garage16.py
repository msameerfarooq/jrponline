import os
import csv
import sys
import scrapy
import traceback
import threading
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common import fetch_or_create_file, export_datafrane_insights

load_dotenv()
current_directory = os.getcwd()
print("Current Directory:", current_directory)
load_dotenv('./jrponline/spiders/.config')

class WareHouseScraper(scrapy.Spider):
    name = "garage16"
    start_urls = [
        'https://www.garage16.ca/'
    ]
    
    def __init__(self):
        self.log_file = fetch_or_create_file(os.getenv('GARAGE_16_LOG_FILE'), "w")
        self.csv_file_path = os.getenv('GARAGE_16_DATA_FILE')
        
        self.newly_scraped_data_file_headers = [header.strip() for header in os.getenv('NEW_SCRAPED_FILE_HEADERS').split(',')]

        if os.path.exists(self.csv_file_path):
            self.csv_file = fetch_or_create_file(self.csv_file_path, "a")
            self.csv_writer = csv.writer(self.csv_file)
            self.df = pd.read_csv(self.csv_file_path, names=self.newly_scraped_data_file_headers)
        else:
            self.csv_file = fetch_or_create_file(self.csv_file_path, "w")
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(self.newly_scraped_data_file_headers)
            self.df = pd.DataFrame(columns=self.newly_scraped_data_file_headers)
        
        # EXTRA WORK
        self.unmatched_csv_file = 'data/scraped/competitors/garage16/garag16_unmatched.csv'
        if os.path.exists(self.unmatched_csv_file):
            self.unmatched_csv_file = fetch_or_create_file(self.unmatched_csv_file, "a")
            self.unmatched_csv_writer = csv.writer(self.unmatched_csv_file)
        else:
            self.unmatched_csv_file = fetch_or_create_file(self.unmatched_csv_file, "w")
            self.unmatched_csv_writer = csv.writer(self.unmatched_csv_file)
            self.unmatched_csv_writer.writerow(['Omesku ID', 'Part Number', 'SKU', 'URL'])
              
        # self.lock = threading.Lock()          
        # self.buffer = []
        # self.batch_size = os.getenv('BATCH_SIZE')
        
        self.df_collections = pd.read_csv(os.getenv('GIVEN_DATA_FILE'))
        # print("df_collections", self.df_collections)
        # export_datafrane_insights(os.getenv('SCRAPED_DATA_INSIGHTS'), self.df_collections)

    def parse(self, response):
        # for omesku_id in self.df_collections['OEMSKU'].sample(n=2000).to_list():
        for omesku_id in self.df_collections['OEMSKU'].to_list():
            try:
                yield response.follow(response.url + 'isearch3?searchterm=' + str(omesku_id), callback=self.fetch_products, cb_kwargs={'omesku_id': omesku_id})
            except Exception as e:
                self.log_file.write(f"parse | Error parsing url : {omesku_id}\n")
                self.log_file.write(f"parse | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())   
                   
    # async def fetch_products(self, response, omesku_id):
    def fetch_products(self, response, omesku_id):
        try:
            total_results = response.xpath('//span[@id="MaxResultsCount"]/text()').get()
            current_count = response.xpath('//span[@id="LastIndex"]/text()').get()
            
            # while current_count < total_results:
            #     # Scroll down by sending a 'PageCoroutine' to the Playwright browser
            #     await response.meta['playwright_page'].evaluate('window.scrollTo(0, document.body.scrollHeight)')
            #     # Wait for the page to load new content (you may adjust this wait time based on the actual behavior)
            #     await response.meta['playwright_page'].wait_for_timeout(2000)  # Wait for 2 seconds

            #     # Re-extract the counts after loading more content
            #     current_count = int(await response.meta['playwright_page'].locator('//span[@id="LastIndex"]/text()').text_content())
            #     total_results = int(await response.meta['playwright_page'].locator('//span[@id="MaxResultsCount"]/text()').text_content())

            #     # Optionally, yield or log the data as more products are loaded
            #     self.logger.info(f"Loaded {current_count} out of {total_results} products")

            # print(1/0)
            # product_elements = await response.meta['playwright_page'].locator('//div[@class="product"]').all_text_contents()
            # for product in product_elements:
            #     yield {
            #         'product': product
            #     }
            
            # if total_results > 0:
            #     self.csv_writer.writerow([omesku_id, 10, response.url])
                
                
            
            li_items = response.xpath('//div[@id="productholder"]/ul/li/div[1]/a').getall()
            # self.log_file.write(str(li_items))
            
            for li in li_items:
                try:
                    # href = li.xpath('.//div[1]/a/@href').get()
                    soup = BeautifulSoup(li, 'html.parser')
                
                    href = soup.find('a')['href']
                    yield response.follow(self.start_urls[0] + href, callback=self.scrape_products, cb_kwargs={'omesku_id': omesku_id})
                except Exception as e:
                    self.log_file.write(f"listing fetch_products | Error fetching product : {omesku_id}\n")
                    self.log_file.write(f"listing fetch_products | Error : {str(e)}\n")
                    self.log_file.write(traceback.format_exc())
        except Exception as e:
                self.log_file.write(f"fetch_products | Error fetching product : {omesku_id}\n")
                self.log_file.write(f"fetch_products | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())
                
    def scrape_products(self, response, omesku_id):
        part_no = response.xpath('//*[@id="variant-info-wrap"]/div/h5/span[2]/text()').get()
        sku_id = response.xpath('//*[@id="variant-info-wrap"]/div/h5/span[4]/text()').get()
        
        if omesku_id == sku_id or omesku_id == part_no:
            product_name = response.xpath('//div[@id="maincontent"]/main/div/h1/text()')
            price = response.xpath('//*[@id="variant-info-wrap"]/div[1]/div/div[2]/div/text()')
            self.csv_writer.writerow([product_name, omesku_id, price, response.url])
        else:
            self.unmatched_csv_writer.writerow([omesku_id, part_no, sku_id, response.url])
        
        