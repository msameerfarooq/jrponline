import os
import csv
import sys
import scrapy
import traceback
import threading
import urllib.parse
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common import fetch_or_create_file, export_datafrane_insights

load_dotenv()
load_dotenv('.config')

class WareHouseScraper(scrapy.Spider):
    name = "tdotperformance"
    start_urls = [
        'https://www.tdotperformance.ca/catalogsearch/result/?q={}'
    ]
    
    def __init__(self):
        self.log_file = fetch_or_create_file(os.getenv('SCRAPED_ERROR_LOG_FILE'), "w")
        self.csv_file_path = os.getenv('SCRAPED_DATA_FILE')
        self.scraped_data_file_headers = [header.strip() for header in os.getenv('SCRAPED_FILE_HEADERS').split(',')]

        if os.path.exists(self.csv_file_path):
            self.csv_file = fetch_or_create_file(self.csv_file_path, "a")
            self.csv_writer = csv.writer(self.csv_file)
            self.df = pd.read_csv(self.csv_file_path, names=self.scraped_data_file_headers)
        else:
            self.csv_file = fetch_or_create_file(self.csv_file_path, "w")
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(self.scraped_data_file_headers)
            self.df = pd.DataFrame(columns=self.scraped_data_file_headers)
              
        self.lock = threading.Lock()          
        self.buffer = []
        self.batch_size = os.getenv('BATCH_SIZE')
        self.is_auth_enable = os.getenv('AUTH_ENABLE')
        
        self.df_collections = pd.read_csv(os.getenv('CRAWLED_URL_FILE'), names=[header.strip() for header in os.getenv('CRAWLED_FILE_HEADERS').split(',')], encoding='latin-1')
        print("df_collections", self.df_collections)
        export_datafrane_insights(os.getenv('CRAWLER_DATA_INSIGHTS'), self.df_collections)

    def parse(self, response):
        if self.is_auth_enable:
            yield scrapy.FormRequest.from_response( response,
                                                    formdata={'login_username': os.getenv('JRP_USERNAME'), 'login_password': os.getenv('JRP_PASSWORD')},
                                                    callback=self.parse_category)
        else:
            yield response.follow(response.url, callback=self.parse_category)
                        
    # Extract all categories from the homepage
    def parse_category(self, response):
        if self.is_auth_enable:
            is_login_failed = response.xpath('//span[contains(text(), "Login failed")]').get()

            if is_login_failed:
                self.log_file.write("parse_category | Login Failed!")
                return
            
        self.df_collections = self.df_collections[~self.df_collections['URL'].isin(self.df['URL'])]
        for index, row in self.df_collections.iterrows():
            try:
                yield response.follow(row['URL'], callback=self.parse_product, cb_kwargs={'category_name': row['Category Name'], 'sub_category': row['Product Category'], 'product_name': row['Product Name']})
            except:
                self.log_file.write(f"parse_category | Error parsing url : {row['URL']}\n")
                self.log_file.write(f"parse_category | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())  
    # Extract details of product
    def parse_product(self, response, category_name, sub_category, product_name):
        try:
            product_number = self.extract_product_number(response.url)
            msrp_price = self.retrieve_value(response, '//td[contains(text(), "MSRP Price:")]/parent::tr/td[2]/span/text()')
            our_price = self.retrieve_value(response, '//td[contains(text(), "Our Price:")]/parent::tr/td[2]/span/text()')
            your_price = self.retrieve_value(response, '//td[contains(text(), "Your Price:")]/parent::tr/td[2]/text()')
            in_stock_status = self.retrieve_value(response, '//td[contains(text(), "In Stock")]/text()')
            product_url = response.url
        except Exception as e:
            self.log_file.write("parse_product | Error in fetching product details : " + response.url)
            self.log_file.write(traceback.format_exc())
        else:
            with self.lock:
                self.buffer.append([category_name, sub_category, product_name, product_number, msrp_price, our_price, your_price, in_stock_status, product_url])
                
                # If buffer reaches batch size, write to CSV
                if len(self.buffer) >= int(self.batch_size):
                    self.flush_buffer_to_csv()
                            
    def retrieve_value(self, response, xpath):
        value = response.xpath(xpath).get()
        if value:
            return value.strip()
        return None
             
    def extract_product_number(self, url):
        # Parse the URL using urlparse
        parsed_url = urllib.parse.urlparse(url)

        # Extract the query string
        query_string = parsed_url.query

        # Split the query string into key-value pairs
        params = urllib.parse.parse_qs(query_string)

        encoded_value = params.get('pn', [])  # Use get to handle potential missing key
        return urllib.parse.unquote(encoded_value[0] if encoded_value else "")  # Handle empty list
    
    def flush_buffer_to_csv(self):
        self.csv_writer.writerows(self.buffer)
        self.buffer.clear()

        df_scraped = pd.read_csv(os.getenv('SCRAPED_DATA_FILE'), names=[header.strip() for header in os.getenv('SCRAPED_FILE_HEADERS').split(',')])
        export_datafrane_insights(os.getenv('SCRAPED_DATA_INSIGHTS'), df_scraped)

    # Ensure any remaining rows are written to the CSV when the spider is closed.
    def close(self, reason):    
        if self.buffer:
            self.flush_buffer_to_csv()
        self.csv_file.close()
        self.log_file.close()