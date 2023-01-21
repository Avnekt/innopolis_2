import scrapy
from scrapy.spiders import CrawlSpider, Spider
import re

class ComputersSpider(CrawlSpider):
    name = 'notik'
    allowed_domains = ['notik.ru']
    start_urls = [
        "https://www.notik.ru/search_catalog/filter/brand.htm",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=2",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=3",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=4",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=5",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=6",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=7",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=8",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=9",
        "https://www.notik.ru/search_catalog/filter/brand.htm?page=10"
    ]

    default_headers = {}

    def scrap_computers(self, response):

        for card in response.xpath("//tr[@class='goods-list-table']"):
            price_selector = card.xpath(".//td[@class='glt-cell gltc-cart']")
            price = re.findall(r'\d+', price_selector.xpath(".//b").css("::text").get())
            price = int("".join(price))
            ecname = price_selector.xpath(".//a").attrib.get("ecname")
            yield {ecname : {"price" : price}}

    def parse_start_url(self, response, **kwargs):
        url = self.start_urls[0]
        for url in self.start_urls:
            yield response.follow(
                url, callback=self.scrap_computers, headers=self.default_headers
            )

# class NotikSpider(scrapy.Spider):
#     name = 'notik'
#     allowed_domains = ['notik.ru']
#     start_urls = ['http://notik.ru/']

#     def parse(self, response):
#         pass
