import re
import csv
import scrapy
import logging
import traceback
from bs4 import BeautifulSoup

USERNAME    = "username"
PASSWORD    = "password"
AUTH_ENABLE = False

ERROR_LOG_FILE      = "err1or.log"
SCRAPPED_DATA_FILE  = "scrapData.csv"


class WareHouseScraper(scrapy.Spider):
    name = "warehouse"
    start_urls = [
        'https://store.jrponline.com/webstore/'
    ]
    
    def __init__(self):
        # This mode('w') opens a file for writing. If the file exists, its contents are overwritten. If it doesn’t exist, a new file is created.
        self.csv_file = open(SCRAPPED_DATA_FILE, "w", newline="")
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(["Category Name", "Product Category", "Product Name", "MSRP Price", "Our Price", "Your Price", "In Stick", "URL"])
        
        self.log_file = open(ERROR_LOG_FILE, "w", newline="")

    def parse(self, response):
        if AUTH_ENABLE:
            yield scrapy.FormRequest.from_response(response,
                                            formdata={'login_username': USERNAME, 'login_password': PASSWORD},
                                            callback=self.parse_category)
        else:
            yield response.follow(response.url, callback=self.parse_category)
            
    # Extract all categories from the homepage
    def parse_category(self, response):
        if AUTH_ENABLE:
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
                self.log_file.write("parse_category | Error parsing href : " + href)
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
                self.log_file.write("parse_sub_category | Error parsing href : " + href)
                self.log_file.write(traceback.format_exc())
                
    # Get the count of total pages
    def parse_pages_of_sub_category(self, response, category_name, sub_category):
        # Try to find the page number if pagination exist
        try:
            last_page_button = response.xpath('//div[@class="Border"]//a[contains(text(), "Last")]/@href').get()
            total_pages = int(re.search(r'page=(\d+)', last_page_button).group(1))
        
        # Paginations doesn't exist
        except:
            print("Paginations doesn't exist as it is unable to retrieve total page number from string : ", last_page_button, " , so it'll now only crawl the first page!")
            yield response.follow(response.url + '&page=', callback=self.parse_list_of_products, cb_kwargs={'category_name': category_name, 'sub_category': sub_category})
        
        else:
            for page_number in range(total_pages):
                yield response.follow(response.url + '&page=' + str(page_number+1), callback=self.parse_list_of_products, cb_kwargs={'category_name': category_name, 'sub_category': sub_category})

    # Retrieve the list of all products on a single page
    def parse_list_of_products(self, response, category_name, sub_category):
        list_of_products = response.xpath('//*[@class="ProductHeader"]/a').getall()

        for product in list_of_products:
            try:
                soup = BeautifulSoup(product, 'html.parser')
                
                product_name = soup.find('a').text.strip()
                href = soup.find('a')['href']
                
                yield response.follow(self.start_urls[0] + href, callback=self.parse_product, cb_kwargs={'category_name': category_name, 'sub_category': sub_category, 'product_name': product_name})
            except Exception as e:
                self.log_file.write("parse_list_of_products | Error parsing href : " + href)
                self.log_file.write(traceback.format_exc())
                
    # Extract details of product
    def parse_product(self, response, category_name, sub_category, product_name):
        try:
            msrp_price = self.retrieve_value(response, '//span[@id="strike"]/text()')
            our_price = self.retrieve_value(response, '//td[contains(text(), "Our Price:")]/parent::tr/td[2]/text()')
            your_price = self.retrieve_value(response, '//td[contains(text(), "Your Price:")]/parent::tr/td[2]/text()')
            in_stock_status = self.retrieve_value(response, '//td[contains(text(), "In Stock")]/text()')
            product_url = response.url
        except Exception as e:
            self.log_file.write("parse_product | Error in fetching product details : " + response.url)
            self.log_file.write(traceback.format_exc())
        else:
            self.csv_writer.writerow([category_name, sub_category, product_name, msrp_price, our_price, your_price, in_stock_status, product_url])
          
    def retrieve_value(self, response, xpath):
        value = response.xpath(xpath).get()
        if value:
            return value.strip()
        return None