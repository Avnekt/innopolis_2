import scrapy
from scrapy.spiders import CrawlSpider, Spider
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
    name = 'topcomputer'
    allowed_domains = ['topcomputer.ru']
    start_urls = [
        'https://topcomputer.ru/katalog/1-noutbuki/',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=2',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=3',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=4',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=5',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=6',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=7',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=8',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=9',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=10',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=11',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=12',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=13',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=14',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=15',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=16',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=17',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=18',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=19',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=20',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=21',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=22',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=23',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=24',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=25',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=26',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=27',
        'https://topcomputer.ru/katalog/1-noutbuki/?PAGEN_1=28',
    ]

    url_object = URL.create(
        drivername='sqlite',
        database='comps.db',
    )
    engine = create_engine(url_object, echo=True)
    Base.metadata.create_all(engine)

    default_headers = {}

    def parse_cards(self, response):
        for link in response.xpath('//div[@class="col-xs-12 col-sm-4 col-md-4 col-lg-3 item"]'):
            href = link.xpath('.//div[@class="item-name-box"]/a').attrib['href']
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
        item['price'] = int(''.join(re.findall(r'\d+', response.xpath('//span[@class="product-price current-price price-currency-rub"]').attrib['content'])))
        item['price'] = item['price'] if item['price'] else 0
        item['name'] = response.xpath('//h1[@class="product-title"]/text()').get()
        item['url'] = response.url
        item['visit_time'] = datetime.now()
        props = response.xpath('//table[@class="catalog-product-props"]')
        for row in props.xpath('.//tr'):
            try:
                prop_name = ' '.join(x for x in row.xpath('.//td/text()')[0].get().split() if x)
                prop_value = ' '.join(x for x in row.xpath('.//td/text()')[1].get().split() if x)
                print(prop_name, prop_value)
            except IndexError:
                continue
            if prop_name == 'Операционная система:':
                item['os'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Диагональ экрана, дюймы:':
                try:
                    item['screen_size'] = float(''.join(re.findall(r'\d+[,|.]?\d?', prop_value)))
                except:
                    item['screen_size'] = 0
            elif prop_name == 'Тип матрицы экрана:':
                item['display_type'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Семейство процессора:':
                item['cpu_name'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Модель процессора:':
                item['cpu_model'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Частота процессора базовая, МГц:':
                try:
                    item['cpu_freq'] = float(''.join(re.findall(r'(\d+)', prop_value))) / 1000
                except:
                    item['cpu_freq'] = 0
            elif prop_name == 'Количество ядер процессора:':
                try:
                    item['cpu_cors'] = int(''.join(re.findall(r'(\d+)', prop_value)))
                except:
                    item['cpu_cors'] = 0
            elif prop_name == 'Память:':
                try:
                    item['memory'] = int(''.join(re.findall(r'\d+', prop_value)))
                except:
                    item['memory'] = 0
            elif prop_name == 'Тип накопителя:':
                item['disk_type'] = ' '.join([x for x in prop_value.split() if x])
            elif prop_name == 'Общий полезный объём накопителей:':
                try:
                    item['disk_volume'] = int(''.join(re.findall(r'\d+', prop_value)))
                except:
                    item['disk_volume'] = 0
            elif prop_name == 'Объем накопителя (SSD):' and item['disk_volume'] == 0:
                try:
                    item['disk_volume'] = int(''.join(re.findall(r'\d+', prop_value)))
                except:
                    item['disk_volume'] = 0
            elif prop_name == 'Емкость аккумулятора:':
                try:
                    item['battery'] = int(''.join(re.findall(r'\d+', prop_value)[0])) * 1000 / 12
                    item['battery'] = item['battery'] if item['battery'] < 10000 else 0
                except:
                    item['battery'] = 0              
            elif prop_name == 'Вес:':
                try:
                    item['weight'] = float(''.join(re.findall(r'\d+[,|.]?\d?', prop_value)))
                except:
                    item['weight'] = 0
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
        # # url = self.start_urls[0]
        # for url in self.start_urls:
        # self.logger.info('A response from %s just arrived!', response.url)
        yield response.follow(
            response.url, callback=self.parse_cards, headers=self.default_headers
        )