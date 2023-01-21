import scrapy
from scrapy.spiders import CrawlSpider, Spider, Rule
from scrapy.linkextractors import  LinkExtractor
import re
from datetime import datetime
from sqlalchemy import Table, ForeignKey, Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, Session
from sqlalchemy.engine import URL
from sqlalchemy.sql import func

database_name = 'sqlite'

Base = declarative_base()

class notebook(Base):
    __tablename__ = 'items'

    id: Mapped[int] = Column(Integer, primary_key=True , autoincrement=True, comment="Идентификатор товара")
    url: Mapped[str] = Column(String(2000), comment="Ссылка на товар")
    visit_time: Mapped[DateTime] = Column(DateTime(timezone=True), default=func.now(), nullable=False, comment="Дата прсмотра страницы")
    name: Mapped[str] = Column(String(2000), comment="Наименование товара")
    cpu_hhz: Mapped[float] = Column(Float, comment="Частота процессора")
    ram_gb: Mapped[int] = Column(Integer, comment="Количество оперативной памяти")
    ssd_gb: Mapped[int] = Column(Integer, comment="Объем диска SSD")
    weight: Mapped[float] = Column(Float, comment="Вес товара")
    battery: Mapped[float] = Column(Float, comment="Объем батареи")
    price: Mapped[int] = Column(Integer, comment="Цена товара")
    rank: Mapped[float] = Column(Float, comment="Ранк товара")

    def __repr__(self) -> str:
        return f'item(id={self.id!r}, name={self.name!r}, price={self.price!r}, rank={self.rank!r})'

class ComputersSpider(CrawlSpider):
    name = 'spcomputer'
    allowed_domains = ['sp-computer.ru']
    start_urls = [
        'https://www.sp-computer.ru/catalog/noutbuki/',
        'https://www.sp-computer.ru/catalog/noutbuki/?PAGEN_1=2',
        'https://www.sp-computer.ru/catalog/noutbuki/?PAGEN_1=3',
        'https://www.sp-computer.ru/catalog/noutbuki/?PAGEN_1=4'
    ]

    url_object = URL.create(
        drivername='sqlite',
        database='comps.db',
    )
    engine = create_engine(url_object, echo=True)
    Base.metadata.create_all(engine)

    # rules = (
    #     # Extract links matching 'category.php' (but not matching 'subsection.php')
    #     # and follow links from them (since no callback means follow=True by default).
    #     Rule(LinkExtractor(allow=('category\.php', ), deny=('subsection\.php', ))),

    #     # Extract links matching 'item.php' and parse them with the spider's method parse_item
    #     Rule(LinkExtractor(allow=('item\.php', )), callback='scrap_computers', follow=True),
    # )

    default_headers = {}

    def parse_cards(self, response):
        for link in response.xpath('//div[@class="product-item item_squares"]'):
            href = link.xpath('.//div[@class="product-item-title"]/a').attrib['href']
            url = response.urljoin(href)
            yield scrapy.Request(url, callback=self.scrap_computers, headers=self.default_headers)

    def scrap_computers(self, response):
        item = {
            'name': '',
            'cpu_name': '',
            'cpu_model': '',
            'cpu_freq': '',
            'cpu_cors': '',
            'os': '',
            'screen_size': '',
            'memory': '',
            'disk_type': '',
            'disk_volume': '',
            'weight': '',
            'battery': '',
            'display_type': '',
            'price': '',
            'url': '',
            'visit_time': ''
        }
        item['price'] = int(''.join(re.findall(r'\d+', response.xpath('//td[@class="price"]/span/text()').get())))
        item['price'] = item['price'] if item['price'] else 0
        item['name'] = response.xpath('//div[@class="head_title pad_mobi"]/h1/text()').get()
        item['url'] = response.url
        item['visit_time'] = datetime.now()
        props = response.xpath('//div[@class="props_table"]')
        for row in props.xpath('.//div[@class="props_row"]'):
            prop_name = row.xpath('.//div/text()')[0].get()
            prop_value = row.xpath('.//div/text()')[1].get()
            if prop_name == 'Операционная система':
                item['os'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Диагональ экрана в дюймах':
                item['screen_size'] = float(''.join(re.findall(r'\d+[,|.]?\d?', prop_value)))
            elif prop_name == 'Тип дисплея':
                item['display_type'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Процессор':
                item['cpu_name'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Модель процессора':
                item['cpu_model'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Характеристики процессора':
                item['cpu_freq'] = float(''.join(re.findall(r'(\d+[,|.]?\d?)\s+ГГц', prop_value)))
                item['cpu_cors'] = int(''.join(re.findall(r'(\d+)\s+яд[е]?р', prop_value)))
            elif prop_name == 'Оперативная память':
                item['memory'] = int(''.join(re.findall(r'\d+', prop_value)))
            elif prop_name == 'Тип накопителя':
                item['disk_type'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Объем накопителя':
                item['disk_volume'] = int(''.join(re.findall(r'\d+', prop_value)))
            elif prop_name == 'Емкость аккумулятора, мАч':
                item['battery'] = int(''.join(re.findall(r'\d+', prop_value)))                
            elif prop_name == 'Вес':
                item['weight'] = float(''.join(re.findall(r'\d+[,|.]?\d?', prop_value)))                
        item['cpu_freq'] = item['cpu_freq'] if item['cpu_freq'] else 0
        item['memory'] = item['memory'] if item['memory'] else 0
        item['disk_volume'] = item['disk_volume'] if item['disk_volume'] else 0
        item['battery'] = item['battery'] if item['battery'] else 0
        item['weight'] = item['weight'] if item['weight'] else 0
        self.logger.info(item)
        with Session(self.engine) as session:
            nb = [
                notebook(
                    name = item['name'],
                    url = item['url'],
                    cpu_hhz = item['cpu_freq'],
                    ram_gb = item['memory'],
                    ssd_gb = item['disk_volume'] if 'SSD' in str(item['disk_type']).upper() else 0,
                    weight = item['weight'],
                    price = item['price'],
                    rank = (
                        item['cpu_freq'] * 10 + \
                        item['memory'] * 4 + \
                        item['disk_volume'] * 0.002 + \
                        item['weight'] * -1 + \
                        item['battery'] * 0.001 + \
                        item['price'] * -0.0001
                    )
                )
            ]
            session.add_all(nb)
            session.commit()
        yield item

    def parse_start_url(self, response, **kwargs):
        # url = self.start_urls[0]
        # for url in self.start_urls:
        # self.logger.info('A response from %s just arrived!', response.url)
        yield response.follow(
            response.url, callback=self.parse_cards, headers=self.default_headers
        )
