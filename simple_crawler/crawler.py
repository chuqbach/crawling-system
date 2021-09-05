import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import time
import random
import logging
import uuid
from datetime import datetime
from logger import LogException


# Create and configure logger
log_format = '%(levelname)s %(asctime)s - %(message)s'
log_file = 'G:/Self Projects/crawling-system/simple_crawler/logs/crawl.log'
logging.basicConfig(filename=log_file,
                    level=logging.DEBUG,
                    format=log_format,
                    filemode='a')

logger = logging.getLogger()

# @LogException


def get_soup(url):
    count = 0
    try:
        while count < 5:
            r = requests.get(url)
            status_code = r.status_code
            reason = r.reason
            if status_code == 200:
                logger.info(f'Receive response from {url} with status code is 200. Generating soup')
                soup = BeautifulSoup(r.content, 'html5lib')
                return soup
            else:
                logger.warning(f'Request status code from {url} is not 200. Retry again. Reason: {reason}')
                time.sleep(random.randint(1, 10))
                count += 1
        logger.error(f'Timeout error when connecting to {url}. Reason: {reason}')
    except ConnectionError:
        logger.error(f'Error connecting to {url}')


def get_int(text):
    if text.isdigit():
        return int(text)
    return text


def get_float(text):
    try:
        float(text)
        return float(text)
    except ValueError:
        return text


def crawl(obj):
    manga = dict()
    manga['id'] = str(uuid.uuid4())
    manga['timestamp'] = str(datetime.now())
    manga['title'] = obj.find('div', attrs={'class': 'overlay-title'}).find('a', attrs={'class': 'text-yellow'}).text
    manga['link'] = obj.find('div', attrs={'class': 'overlay-title'}).find('a', attrs={'class': 'text-yellow'}).get('href')
    manga_soup = get_soup(manga.get('link'))
    if manga_soup is not None:
        score = manga_soup.find('small', attrs={'style': 'display:block'}) \
                    .text[8: manga_soup.find('small', attrs={'style': 'display:block'}).text.find('/')]
        manga['score'] = get_float(score)
        info = manga_soup.find('ul', attrs={'class': 'list list-simple-mini'})
        manga['other_name'] = info.find('li', attrs={'class': 'text-muted'}).text
        manga['source'] = manga_soup.find('small', attrs={'class': 'text-danger'}).text.strip('[]')
        for tag in info.findAll('li', attrs={'class': 'text-primary'}):
            if tag.find('b').text == 'Parody':
                manga['parody'] = [mini_tag.text for mini_tag in tag.findAll('a') if mini_tag.text != '-']
            if tag.find('b').text == 'Ranking':
                ranking = tag.find('a').text[:-2]
                manga['ranking'] = get_int(ranking)
            if tag.find('b').text == 'Status':
                manga['status'] = tag.find('a').text.strip('\n')
            if tag.find('b').text == 'Release Year':
                release_year = tag.find('a').text
                manga['release_year'] = get_int(release_year)
            if tag.find('b').text == 'View':
                view = tag.find('a').text.strip().strip(' views').replace(',', '')
                manga['view'] = get_int(view)
            if tag.find('b').text == 'Page':
                page = tag.find('a').text[:tag.find('a').text.find('pages')].strip()
                manga['page'] = get_int(page)
            if tag.find('b').text == 'Author':
                manga['author'] = [mini_tag.text for mini_tag in tag.findAll('a')]
            if tag.find('b').text == 'Artist':
                manga['artist'] = [mini_tag.text for mini_tag in tag.findAll('a')]
            if tag.find('b').text == 'Category':
                manga['category'] = [mini_tag.text for mini_tag in tag.findAll('a')]
            if tag.find('b').text == 'Content':
                manga['content'] = [mini_tag.text for mini_tag in tag.findAll('a')]
            if tag.find('b').text == 'Character':
                manga['character'] = [mini_tag.text for mini_tag in tag.findAll('a') if mini_tag.text != '-']
            if tag.find('b').text == 'Language':
                manga['language'] = [mini_tag.text for mini_tag in tag.findAll('a')]
            if tag.find('b').text == 'Storyline':
                manga['story'] = tag.find('p').text
        logger.info('Generate manga information')
        return manga


def sent_producer(url, data):
    count = 0
    try:
        while count < 5:
            r = requests.post(url=url, json=data)
            status_code = r.status_code
            reason = r.reason
            if status_code == 200:
                logger.info(f'Sent message to Kafka Producer {url}')
                print(r.content)
                break
            else:
                time.sleep(random.randint(1, 10))
                count += 1
        logger.error(f'Timeout error when connecting to {url}. Reason: {reason}')
    except ConnectionError:
        logger.error(f'Error sending message to Kafka Producer {url}')


def export_json(data, path):
    with open(path + '/data.json', 'a', encoding='utf-8') as f:
        json.dump(data, f)


def main():
    early_added_url = 'https://hentai2read.com/hentai-list/all/any/all/early-added/'
    early_added_soup = get_soup(early_added_url)
    kafka_producer_url = 'http://192.168.0.100:9090/'
    if early_added_soup is not None:
        count = get_int(early_added_soup.find('ul', attrs={'class': 'pagination pagination-sm push-5-t push-5'}).findAll('li')[-2].find('a').text)
        for page in range(1, count + 1):
            page_soup = get_soup(early_added_url + str(page))
            if page_soup is not None:
                list_soup = early_added_soup.findAll('div', attrs={'class': 'col-xs-6 col-sm-4 col-md-3 col-xl-2'})
                for obj in list_soup:
                    manga = crawl(obj)
                    if manga is not None:
                        sent_producer(url=kafka_producer_url, data=manga)
                        # export_json(manga, path='G:/Self Projects/crawling-system/simple_crawler/data')
                    time.sleep(random.randint(1, 5))
            time.sleep(random.randint(1, 5))


if __name__ == "__main__":
    main()
