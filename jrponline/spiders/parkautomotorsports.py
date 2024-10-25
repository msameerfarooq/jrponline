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

class WareHouseScraper(scrapy.Spider):
    name = "parkautomotorsports"
    start_urls = [
        'https://www.parkautomotorsports.ca/'
    ]
    
    def __init__(self):
        self.log_file = fetch_or_create_file(os.getenv('PARK_AUTO_MOTOR_SPORTS_LOG_FILE'), "w")
        self.csv_file_path = os.getenv('PARK_AUTO_MOTOR_SPORTS_DATA_FILE')
        
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
              
        # self.lock = threading.Lock()          
        # self.buffer = []
        # self.batch_size = os.getenv('BATCH_SIZE')
        
        self.df_collections = pd.read_csv(os.getenv('SCRAPED_DATA_FILE'))
        print("df_collections", self.df_collections)
        # export_datafrane_insights(os.getenv('SCRAPED_DATA_INSIGHTS'), self.df_collections)

    def parse(self, response):
        self.df_collections = self.df_collections[~self.df_collections['URL'].isin(self.df['URL'])]
        for product_number in self.df_collections['Product Number'].sample(n=2000).to_list():
            try:
                yield response.follow(response.url + 'products/' + str(product_number), callback=self.scrape_products, cb_kwargs={'product_number': product_number})
            except Exception as e:
                self.log_file.write(f"parse | Error parsing url : {product_number}\n")
                self.log_file.write(f"parse | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())   
                   
    def scrape_products(self, response, product_number):
        try:
            new_price = response.xpath('//*[@id="price-template--15143801651313__main"]/span/text()').get()
            if new_price:
                self.csv_writer.writerow([product_number, new_price, response.url])
        except Exception as e:
                self.log_file.write(f"scrape_products | Error fetching product : {product_number}\n")
                self.log_file.write(f"scrape_products | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())