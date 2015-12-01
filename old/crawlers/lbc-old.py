#!/usr/bin/python
# -*- coding:  utf-8 -*-

from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import re
from decimal import *
from threading import Thread
from Queue import Queue
from bs4 import BeautifulSoup
import requests
import sys


dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')


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
    today = datetime.now()
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
        yesterday = today - timedelta(days=1)
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
    dt = datetime(year, month, day, hours, minutes)
    timestamp = int((dt - datetime(1970, 1, 1)).total_seconds())
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ'), timestamp


def get_now_timestamp():
    today = datetime.now()
    timestamp = int((today - datetime(1970, 1, 1)).total_seconds())
    return today.strftime('%Y-%m-%dT%H:%M:%SZ'), timestamp


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
        return Decimal(re.sub(r'[^\d\.]', '', latitude.get('content')))
    except:
        return None


def longitude_from_soup(soup, url):
    try:
        longitude = soup.find('meta', {'itemprop': 'longitude'})
        return Decimal(re.sub(r'[^\d\.]', '', longitude.get('content')))
    except:
        return None


# Extract the table of optional criterias
def criterias_from_soup(soup, url):
    try:
        criterias = {}
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
                criterias['ad_surface'] = int(re.sub(r'\D', '', data.get_text()))
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
        return None


def process_index(queue, thread_index, batch_writer, bucket):
    while True:
        index_url = queue.get()
        try:
            index_count = index_url.split('=')[-1]
            print('[thread {thread}] get index #{index_count}'.format(thread=thread_index, index_count=index_count))
            index_request = requests.get(index_url)
            soup = BeautifulSoup(index_request.text, 'html.parser')
        except:
            print('[thread {thread}] >>> error index {url}'.format(thread=thread_index, url=index_url))
        else:
            link_nodes = soup.findAll('div', {'class': 'lbc'})
            for link_node in link_nodes:
                link_date, link_timestamp = parse_timestamp(link_node.find('div', {'class': 'date'}).get_text())
                ad_url = link_node.parent['href']
                process_ad(ad_url, thread_index, batch_writer, bucket)
        finally:
            queue.task_done()


def process_ad(ad_url, thread_index, batch_writer, bucket):
    try:
        print('[thread {thread}] get ad {url}'.format(thread=thread_index, url=ad_url))
        ad_request = requests.get(ad_url)
        soup = BeautifulSoup(ad_request.text, 'html.parser')
    except:
        print('[thread {thread}] >>> error ad {url}'.format(thread=thread_index, url=ad_url))
    else:
        ad = {}
        ad['ad_id'] = id_from_soup(soup, ad_url)
        ad['ad_site'] = site_from_soup(soup, ad_url)
        ad['ad_url'] = url_from_soup(soup, ad_url)
        ad['ad_title'] = title_from_soup(soup, ad_url)
        ad['ad_visit_date'], ad['ad_visit_timestamp'] = get_now_timestamp()
        if ad['ad_title'] is None:
            # If there is no ad_subject then this is not an ad (the ad has been deactivated)
            ad['ad_status'] = 'inactive'
        else:
            # Else this is an ad so let's parse it
            ad['ad_status'] = 'active'
            ad['ad_image_urls'] = thumbnail_urls_from_soup(soup, ad_url)
            ad['ad_text'] = text_from_soup(soup, ad_url)
            ad['ad_author'] = author_from_soup(soup, ad_url)
            ad['ad_date'], ad['ad_timestamp'] = timestamp_from_soup(soup, ad_url)
            ad['ad_price'] = price_from_soup(soup, ad_url)
            ad['ad_city'] = city_from_soup(soup, ad_url)
            ad['ad_zip_code'] = zip_code_from_soup(soup, ad_url)
            ad['ad_latitude'] = latitude_from_soup(soup, ad_url)
            ad['ad_longitude'] = longitude_from_soup(soup, ad_url)
            criterias = criterias_from_soup(soup, ad_url)
            for criteria in criterias:
                ad[criteria] = criterias[criteria]
            process_images(ad, thread_index, bucket)
            batch_writer.put_item(Item=ad)
            try:
                1
            except:
                e = sys.exc_info()[0]
                print('[thread {thread}] >>> error ad {url}: {err}'.format(thread=thread_index, url=ad_url, err=e.msg))


def process_images(ad, thread_index, bucket):
    if 'ad_image_urls' in ad:
        for image_url in ad['ad_image_urls']:
            try:
                image = requests.get(image_url).content
                file_name = ad['ad_id'] + '/' + image_url.split('/')[-1]
                bucket.put_object(Key=file_name, ACL='private', Body=image)
            except:
                print('[thread {thread}] error image {url}'.format(thread=thread_index, url=image_url))


def crawl(event, context):
    table = dynamodb.Table('immo-ads')
    bucket = s3.Bucket('immo-thumbs')
    queue = Queue(maxsize=0)
    NUM_THREADS = 20
    with table.batch_writer() as batch_writer:
        for i in range(NUM_THREADS):
            worker = Thread(target=process_index, args=(queue, i, batch_writer, bucket))
            worker.setDaemon(True)
            worker.start()

    for index_page in range(1, 1000):
        queue.put('http://www.leboncoin.fr/ventes_immobilieres/offres/?o={page}'.format(page=index_page))

    queue.join()


crawl({}, {})
