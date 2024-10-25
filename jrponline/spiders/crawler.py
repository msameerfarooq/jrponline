import os
import re
import csv
import sys
import scrapy
import traceback
import threading
import urllib.parse
from bs4 import BeautifulSoup
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common import fetch_or_create_file

load_dotenv()
load_dotenv('./jrponline/spiders/.config')

class WareHouseCrawler(scrapy.Spider):
    name = "crawler"
    start_urls = [
        'https://store.jrponline.com/webstore/'
    ]
    
    def __init__(self):
        # This mode('w') opens a file for writing. If the file exists, its contents are overwritten. If it doesn’t exist, a new file is created.
        self.csv_file = fetch_or_create_file(os.getenv('CRAWLED_URL_FILE'), "w")
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow([header.strip() for header in os.getenv('CRAWLED_FILE_HEADERS').split(',')])
        
        self.log_file = fetch_or_create_file(os.getenv('CRAWLER_ERROR_LOG_FILE'), "w")
        
        self.lock = threading.Lock()
        self.buffer = []
        self.batch_size = os.getenv('BATCH_SIZE')
        self.is_auth_enable = os.getenv('AUTH_ENABLE')

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
            
        category_links = response.xpath("//div[@id='ProductCategories']//table/tr[2]/td/ul/li/div/a").getall()
        
        for link in category_links:
            try:
                soup = BeautifulSoup(link, 'html.parser')
                
                category_name = soup.find('a').text.strip()
                href = soup.find('a')['href']
                
                if category_name == "Other":
                    yield response.follow(self.start_urls[0] + href, callback=self.parse_pages_of_sub_category, cb_kwargs={'category_name': category_name, 'sub_category': "Other"})
                else:
                    yield response.follow(self.start_urls[0] + href, callback=self.parse_sub_category, cb_kwargs={'category_name': category_name})
            except Exception as e:
                self.log_file.write(f"parse_category | Error parsing url : {href}\n")
                self.log_file.write(f"parse_category | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())

    # Extract all the sub categories
    def parse_sub_category(self, response, category_name):
        product_links = response.xpath('//table[@class="BrowseContent"]//tr/td[2]/a').getall()

        for link in product_links:
            try:
                soup = BeautifulSoup(link, 'html.parser')
                
                sub_category = soup.find('a').text.strip()
                href = soup.find('a')['href']

                yield response.follow(self.start_urls[0] + href, callback=self.parse_pages_of_sub_category, cb_kwargs={'category_name': category_name, 'sub_category': sub_category})
            except Exception as e:
                self.log_file.write(f"parse_sub_category | Error parsing url : {href}\n")
                self.log_file.write(f"parse_sub_category | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())
                            
    # Get the count of total pages
    def parse_pages_of_sub_category(self, response, category_name, sub_category):
        # Try to find the page number if pagination exist
        try:
            last_page_button = response.xpath('//div[@class="Border"]//a[contains(text(), "Last")]/@href').get()
            total_pages = int(re.search(r'page=(\d+)', last_page_button).group(1))
        except:
            total_pages = 1
        
        for page_number in range(total_pages):
            try:
                if total_pages == 1:
                    # Paginations doesn't exist
                    print("Paginations doesn't exist as it is unable to retrieve total page number from string : ", last_page_button, ", so it'll now only crawl the first page!")
                    yield response.follow(response.url, callback=self.parse_list_of_products, cb_kwargs={'category_name': category_name, 'sub_category': sub_category})
                else:
                    yield response.follow(response.url + '&page=' + str(page_number+1), callback=self.parse_list_of_products, cb_kwargs={'category_name': category_name, 'sub_category': sub_category})
            except Exception as e:
                self.log_file.write(f"parse_pages_of_sub_category | Error parsing url : {response.url} for page number {page_number}\n")
                self.log_file.write(f"parse_pages_of_sub_category | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc()) 
                    
          
    # Retrieve the list of all products on a single page
    def parse_list_of_products(self, response, category_name, sub_category):
        list_of_products = response.xpath('//*[@class="ProductHeader"]/a').getall()

        for product in list_of_products:
            try:
                soup = BeautifulSoup(product, 'html.parser')
                
                href = soup.find('a')['href']
                product_url = self.start_urls[0] + href
                product_name = soup.find('a').text.strip()
                
                with self.lock:
                    self.buffer.append([category_name, sub_category, product_name, product_url])
                    
                    # If buffer reaches batch size, write to CSV
                    if len(self.buffer) >= int(self.batch_size):
                        self.flush_buffer_to_csv()
            except Exception as e:
                self.log_file.write(f"parse_list_of_products | Error parsing url : {href}\n")
                self.log_file.write(f"parse_list_of_products | Error : {str(e)}\n")
                self.log_file.write(traceback.format_exc())        
                
    def flush_buffer_to_csv(self):
        self.csv_writer.writerows(self.buffer)
        self.buffer.clear()
        
    def close(self, reason):
        # Ensure any remaining rows are written to the CSV when the spider is closed.
        if self.buffer:
            self.flush_buffer_to_csv()
        self.csv_file.close()
        self.log_file.close()