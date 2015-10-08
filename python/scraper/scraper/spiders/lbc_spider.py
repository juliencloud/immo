import scrapy

class LbcSpider(scrapy.Spider):
    name = "lbc_spider"
    allowed_domains = ["leboncoin.fr"]
    start_urls = ["http://www.leboncoin.fr/ventes_immobilieres/?o=1"]

    def parse(self, response):
        for href_selector in response.selector.css('.lbc-list > a::attr(href)'):
            url = response.urljoin(href_selector.extract())
            yield scrapy.Request(url, callback=self.parse_annonce)

    def parse_annonce(self, response):
        yield response.xpath('//h1[@id="ad_subject"]/text()').extract()
