#!/usr/bin/python
# -*- coding:  utf-8 -*-

from __future__ import print_function

import datetime, time
import re
from threading import Thread
from Queue import Queue
from bs4 import BeautifulSoup
import requests
import sys

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('leboncoin')


import settings
import postgres
import s3




# Remove duplicate spaces
def cleanup(text):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


# Parse LBC date/time strings to ISO string and Unix epoch
def parse_timestamp(datestr):
    months = {
        u'janvier': 1,
        u'février': 2,
        u'mars': 3,
        u'avril': 4,
        u'mai': 5,
        u'juin': 6,
        u'juillet': 7,
        u'août': 8,
        u'septembre': 9,
        u'octobre': 10,
        u'novembre': 11,
        u'décembre': 12}
    today = datetime.datetime.now()
    if 'Aujourd\'hui' in datestr:
        expr = re.compile(ur'([0-9]+):([0-9]+)', re.UNICODE)
        groups = re.search(expr, datestr)
        day = today.day
        month = today.month
        year = today.year
        hours = int(groups.group(1))
        minutes = int(groups.group(2))
    elif 'Hier' in datestr:
        expr = re.compile(ur'([0-9]+):([0-9]+)', re.UNICODE)
        groups = re.search(expr, datestr)
        yesterday = today - datetime.timedelta(days=1)
        day = yesterday.day
        month = yesterday.month
        year = yesterday.year
        hours = int(groups.group(1))
        minutes = int(groups.group(2))
    else:
        expr = re.compile(ur'([0-9]+)\s([\w]+)\sà\s([0-9]+):([0-9]+)', re.UNICODE)
        groups = re.search(expr, datestr)
        day = int(groups.group(1))
        month = months[groups.group(2)]
        if today.month < month:
            year = today.year - 1
        else:
            year = today.year
        hours = int(groups.group(3))
        minutes = int(groups.group(4))
    return datetime.datetime(year, month, day, hours, minutes)


def site_from_soup(soup, url):
    return 'lbc'


# Extract URL
def url_from_soup(soup, url):
    return url.encode('utf-8')


# Build id from URL and site
def id_from_soup(soup, url):
    try:
        m = re.search(r'\/(\d+)\.htm', url)
        return site_from_soup(soup, url) + m.group(1).encode('utf-8')
    except:
        return None


# Extract ad title
def title_from_soup(soup, url):
    try:
        title = soup.find('h1', {'itemprop': 'name'})
        return title.get_text(strip=True)
    except:
        return None


# Extract image urls
def image_urls_from_soup(soup, url):
    try:
        image_urls = []
        for node in soup.findAll('meta', {'itemprop': 'image'}):
            if node.get('content') is not None:
                image_urls.append(node.get('content').encode('utf-8'))
        return image_urls
    except:
        return None


# Extract image urls
def thumbnail_urls_from_soup(soup, url):
    try:
        thumbail_urls = []
        for node in soup.findAll('span', {'class': 'thumbs'}):
            if node.get('style') is not None:
                groups = re.search(r'url\(\'(\S+)\'\)', node.get('style'))
                if groups is not None:
                    thumbail_urls.append(groups.group(1).encode('utf-8'))
        return thumbail_urls
    except:
        return None


# Extract ad text
def text_from_soup(soup, url):
    try:
        text = soup.find('div', {'class': 'content'})
        return cleanup(text.get_text())
    except:
        return None


# Extract author name
def author_from_soup(soup, url):
    try:
        author = soup.find('div', {'class': 'upload_by'})
        return cleanup(author.find('a').get_text())
    except:
        return None


# Extract timestamp from 'Mise en ligne le... à...'
def timestamp_from_soup(soup, url):
    try:
        timestamp = soup.find('div', {'class': 'upload_by'})
        return parse_timestamp(timestamp.get_text())
    except:
        return None


# Extract price
def price_from_soup(soup, url):
    try:
        price = soup.find('span', {'itemprop': 'price'})
        return int(re.sub(r'\D', '', price.get_text()))
    except:
        return None


# Extract city and zip code
def city_from_soup(soup, url):
    try:
        city = soup.find('td', {'itemprop': 'addressLocality'})
        return cleanup(city.get_text())
    except:
        return None


def zip_code_from_soup(soup, url):
    try:
        zip_code = soup.find('td', {'itemprop': 'postalCode'})
        return re.sub(r'\D', '', zip_code.get_text()).encode('utf-8')
    except:
        return None


# Extract geo coordinates
def latitude_from_soup(soup, url):
    try:
        latitude = soup.find('meta', {'itemprop': 'latitude'})
        return float(re.sub(r'[^\d\.]', '', latitude.get('content')))
    except:
        return None


def longitude_from_soup(soup, url):
    try:
        longitude = soup.find('meta', {'itemprop': 'longitude'})
        return float(re.sub(r'[^\d\.]', '', longitude.get('content')))
    except:
        return None


# Extract the table of optional criterias
def criterias_from_soup(soup, url):
    criterias = {}
    try:
        nodes = soup.find('div', {'class': 'lbcParams criterias'}).findAll('tr')
        for node in nodes:
            header = cleanup(node.find('th').get_text())
            data = node.find('td')
            if header == u'Frais d\'agence inclus :':
                criterias['ad_agency_fees'] = (cleanup(data.get_text()) == 'Oui')
            elif header == u'Type de bien :':
                criterias['ad_immo_type'] = cleanup(data.get_text()).lower()
            elif header == u'Référence :':
                criterias['ad_reference'] = cleanup(data.get_text())
            elif header == u'Surface :':
                criterias['ad_surface'] = int(re.sub(r'\D', '', data.get_text())[0:-1])
                if 'ad_immo_type' in criterias and criterias['ad_immo_type'] == 'terrain':
                    criterias['ad_terrain_surface'] = criterias['ad_surface']
                elif 'ad_immo_type' in criterias:
                    criterias['ad_building_surface'] = criterias['ad_surface']
            elif header == u'Pièces :':
                criterias['ad_rooms'] = int(re.sub(r'\D', '', data.get_text()))
            elif header == u'GES :':
                criterias['ad_gaz_emissions'] = data.find('a').get_text()[0].encode('utf-8')
            elif header == u'Classe énergie :':
                criterias['ad_energy_class'] = data.find('a').get_text()[0].encode('utf-8')
        return criterias
    except:
        return criterias


def process_index(queue, thread_index, session, connection, bucket):
    while True:
        start = time.clock()
        index_url = queue.get()
        try:
            index_count = index_url.split('=')[-1]
            logger.info('thread %s > index #%s: start' % (thread_index, index_count))
            index_request = session.get(index_url, stream=False)
            soup = BeautifulSoup(index_request.text, 'html.parser')
        except:
            e = sys.exc_info()[0].msg
            logger.error('thread %s >>> error index %s: %s' % (thread_index, index_url, e))
        else:
            link_nodes = soup.findAll('div', {'class': 'lbc'})
            ads = []
            for link_node in link_nodes:
                #link_timestamp = parse_timestamp(link_node.find('div', {'class': 'date'}).get_text())
                ad_url = link_node.parent['href']
                ad = get_ad(ad_url, session)
                ads.append(ad)
            start_write_db = time.clock()
            postgres.write_ads(connection, 'immo.ads', ads)
            logger.info('thread %s > index #%s: write %s ads to db in %s seconds' % (thread_index, index_count, len(ads), time.clock()-start_write_db))
        finally:
            logger.info('thread %s > index #%s: completed in %s seconds' % (thread_index, index_count, time.clock()-start))
            queue.task_done()


def get_ad(ad_url, session):
    try:
        start = time.clock()
        ad_request = session.get(ad_url, stream=False)
        soup = BeautifulSoup(ad_request.text, 'html.parser')
        ad = {}
    except:
        e = sys.exc_info()[0].msg
        logger.error('error ad %s: %s' % (ad_url, e))
    else:
        ad['visit_timestamp'] = datetime.datetime.now()
        ad['ad_id'] = id_from_soup(soup, ad_url)
        ad['ad_site'] = site_from_soup(soup, ad_url)
        ad['ad_url'] = url_from_soup(soup, ad_url)
        ad['ad_title'] = title_from_soup(soup, ad_url)
        if ad['ad_title'] is None:
            # If there is no ad_subject then this is not an ad (the ad has been deactivated)
            ad['ad_status'] = False
        else:
            # Else this is an ad so let's parse it
            ad['ad_status'] = True
            ad['ad_image_urls'] = thumbnail_urls_from_soup(soup, ad_url)
            ad['ad_text'] = text_from_soup(soup, ad_url)
            ad['ad_author'] = author_from_soup(soup, ad_url)
            ad['ad_timestamp'] = timestamp_from_soup(soup, ad_url)
            ad['ad_price'] = price_from_soup(soup, ad_url)
            ad['ad_city'] = city_from_soup(soup, ad_url)
            ad['ad_zip_code'] = zip_code_from_soup(soup, ad_url)
            ad['ad_latitude'] = latitude_from_soup(soup, ad_url)
            ad['ad_longitude'] = longitude_from_soup(soup, ad_url)
            criterias = criterias_from_soup(soup, ad_url)
            for criteria in criterias:
                ad[criteria] = criterias[criteria]
            #logger.info('completed scrape ad %s in %s seconds' % (ad_url, time.clock()-start))
            return ad


def process_images(ad, thread_index, bucket):
    if 'ad_image_urls' in ad:
        start = time.clock()
        for image_url in ad['ad_image_urls']:
            try:
                image = requests.get(image_url).content
                path = ad['ad_id'] + '/' + image_url.split('/')[-1]
                bucket.write_file(bucket, path, image)
            except:
                e = sys.exc_info()[0].msg
                logger.error('thread %s >>> error image %s: %s' % (thread_index, image_url, e))


def crawl(settings):
    logger.info('crawl started')
    try:
        connection = postgres.get_connection(settings)
    except:
        logger.error('could not connect to postgres db')
    else:
        bucket = s3.get_bucket(settings)
        queue = Queue(maxsize=0)
        session = requests.session()
        for thread_index in range(settings['threading']['num_threads']):
            worker = Thread(target=process_index, args=(queue, thread_index, session, connection, bucket))
            worker.setDaemon(True)
            logger.info('thread %s > starting' % (thread_index))
            worker.start()
        for index_page in range(settings['leboncoin']['start_page'], settings['leboncoin']['end_page']):
            queue.put('http://www.leboncoin.fr/ventes_immobilieres/offres/?o={page}'.format(page=index_page))
        queue.join()
        logger.info('crawl ended')


crawl(settings.get())
