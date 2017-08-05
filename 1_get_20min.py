# -*- coding: utf-8 -*-
'''
Author: Fabio D'Elia
Description: Scraps all comments and article content from all articles
    linked in the home page of 20min.ch and saves them to CSV-files.
    The CSV-files are rotated monthly.

    Every scrapped item is saved, even if it was saved before.
    To ensure there are no duplicates, after each run, duplicates are removed.
'''
import time
import datetime
import urllib.request, urllib.error, urllib.parse
import re
import csv
import traceback
import os
from collections import defaultdict

import json
from bs4 import BeautifulSoup

# configs
hdr = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       # 'Accept-Charset': 'de-DE,de;q=0.8,en-US;q=0.6,en;q=0.4,id;q=0.2,nl;q=0.2',
       'Accept-Encoding': 'none',
       'Accept-Language': 'de-DE,de;q=0.8,en-US;q=0.6,en;q=0.4,id;q=0.2,nl;q=0.2',
       'Connection': 'keep-alive'}

DELAY_MS = 100.0 # before each request
DEV_PRINT = False
SHOW_ERR = True
PARSER = "html.parser"
DELIMITER = ',' # TODO check it works with this delimiter, maybe encode text?


def get_article_links():
    links = []

    site = BeautifulSoup(urllib.request.urlopen("http://www.20min.ch", timeout=10), PARSER)
    regexp_article_link = re.compile(r'((?=[^\d])[a-zA-Z-\/]*\d{8})')
    for a in site.findAll('a', href=True):
        s = regexp_article_link.search(a['href'])
        if s is not None and len(s.group(0)) > 8:
            links.append(a['href'])

    return links


# header = .story_head h1
# subtitle = .story_head h3
# text = .story_text p's
# anzahl kommentare = .left .more_link_top
# kommentar = .entry .title / .content / .author / .time / .viamobile
def get_and_parse_article(link):
    global hdr
    if DEV_PRINT: print('get article ' + link)
    time.sleep(DELAY_MS / 1000.0) # delay
    req = urllib.request.Request("http://www.20min.ch" + link, headers=hdr)
    uo = urllib.request.urlopen(req, timeout=5)
    # div_content = SoupStrainer("div", "content")
    site = BeautifulSoup(uo, PARSER)
    if DEV_PRINT:
        print('     parsed')

    story_head = site.find("div", class_='story_head')
    if story_head is None: #then its not an typical article site
        return False

    text = ''
    for t in site.find("div", class_='story_text').findAll("p"):
        if t is None: continue
        text += t.get_text()
        # text += '\n\n'

    more_link_top = site.find('div', class_='more_link_top')
    if more_link_top:
        num_comments = re.findall(r'\d+', more_link_top.get_text())[0]
    else:
        num_comments = 0

    regex = re.compile(r'(?=[^\d])[a-zA-Z-\/]*(\d{8})')
    m = regex.match(link)
    if m is None: return False
    article_id = m.group(1)

    talkback_id = site.find("div", id='talkback')
    if talkback_id is not None: talkback_id = talkback_id['data-talkbackid']
    if not talkback_id: talkback_id = article_id

    article =  {
        'article_id': int(article_id),
        'talkback_id': int(talkback_id),
        'num_comments': int(num_comments),
        'updated': time.time(),
        'link': link,
        'header': story_head.find("h1").get_text(),
        'subtitle': story_head.find("h3").get_text(),
        'text': text
    }

    # remove newlines for CSV
    for v in ['header', 'subtitle', 'text']:
        article[v] = article[v].replace('\n', ' ')

    part_comments = site.find('ul', class_='comments') # not saved
    return (article, part_comments)

# some of the comments are still not parsed since a user would have to make one more request by clicking "Kommentare anzeigen (2)" on answers
# i thought i'd be enough comments for a first analysis
def get_comments(talkback_id):
    global hdr
    if DEV_PRINT: print('get and save comments ' + talkback_id)
    time.sleep(DELAY_MS / 1000.0) # delay

    url = 'http://www.20min.ch/community/storydiscussion/messageoverview.tmpl?storyid=' + str(talkback_id) + '&type=1&l=0'
    req = urllib.request.Request(url, headers=hdr)
    site = BeautifulSoup(urllib.request.urlopen(req, timeout=5), PARSER)
    return site.findAll('li', class_='comment')


def parse_comment(comment, talkback_id):

    if comment.find('span', class_='viamobile') is None:
        viamobile = 0
    else:
        viamobile = 1

    # if not comment['id']:

    cId = comment['id'].replace('thread', '').replace('msg', '')
    comment_dict = {
        'tId': talkback_id,
        'cId': int(cId),
        'mob': viamobile,
        'vup': int(comment['data-voteup']),
        'vdo': int(comment['data-votedown']),
        'title': comment.find('h3', class_='title').get_text(),
        'author': comment.find('span', class_='author').get_text(),
        'time': comment.find('span', class_='time').get_text(),
        'text': comment.find('p', class_='content').get_text()
    }

    # remove newlines for CSV
    for v in ['title', 'author', 'text']:
        comment_dict[v] = comment_dict[v].replace('\n', ' ')

    return comment_dict


def save_article(article, writer_articles):
    # attention: order of values is important!
    writer_articles.writerow(article.values())

def save_comment(comment, writer_comments):
    # attention: order of values is important!
    writer_comments.writerow(comment.values())


def main():
    print('20min.ch   ' + time.strftime('%c'))
    starting_time = time.time()

    # filenames
    today = datetime.date.today()
    filename_articles = "articles_{:04d}_{:02d}.csv".format(today.year, today.month)
    filename_comments = "comments_{:04d}_{:02d}.csv".format(today.year, today.month)


    # init csv-files for saving
    file_articles = open(filename_articles, 'w+', encoding='utf-8')
    file_comments = open(filename_comments, 'w+', encoding='utf-8')
    writer_articles = csv.writer(file_articles, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)
    writer_comments = csv.writer(file_comments, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)


    # get all links
    article_links = get_article_links()
    article_links = list(set(article_links)) # remove doubles

    # go through links
    count_saved_articles = 0
    count_saved_comments = 0
    for article_link in article_links:
        print('get ' + article_link)
        try:
            (article, part_comments) = get_and_parse_article(article_link)
            if not article:
                continue
        except Exception as e: # it usually fails on stories which are promos because of some redirect (there are comments too there, but whatever)
            if SHOW_ERR:
                print('     err: exception getting article from ' + article_link)
                print(e)
                traceback.print_exc()
            continue

        save_article(article, writer_articles)
        count_saved_articles += 1

        # if not possible to comment
        if not article['talkback_id']:
            continue

        try:
            comments_html = get_comments(article['talkback_id'])
            if part_comments:
                comments_html += part_comments.findAll('li', class_='comment')

            for comment in comments_html:
                comment_parsed = parse_comment(comment, article['talkback_id'])
                save_comment(comment_parsed, writer_comments)
                count_saved_comments += 1

        except Exception as e: # timeouts sometimes
            if SHOW_ERR:
                print('     err: exception getting comments for ' + str(article['talkback_id']))
                print(e)
                traceback.print_exc()
            continue

        break

    file_articles.close()
    file_comments.close()


    # TODO checking for doubles in the end
    # for safety reasons files are moved to temp
    os.rename(filename_articles, filename_articles + '_temp')
    os.rename(filename_comments, filename_comments + '_temp')

    # checking for and removing doubles
    file_articles = open(filename_articles + '_temp', 'r', encoding='utf-8')
    file_comments = open(filename_comments + '_temp', 'r', encoding='utf-8')
    reader_articles = csv.reader(file_articles)
    reader_comments = csv.reader(file_comments)

    print(reader_articles)

    # for articles and comments
    # 1. get all IDs in a set
    # 2. for each ID take the latest row with that ID from the csv
    # 3. save that row into a new csv
    # 4. correct files / move temp to permanent

    # reader_articles = csv.reader(file_articles)
    # reader_comments = csv.reader(file_comments)
    #
    # articles_saved = defaultdict(list)
    # comments_saved = defaultdict(list)
    # for article in reader_articles:
    #     # [article_id] = num_comments
    #     articles_saved[article[0]] = [article[2]]
    #
    # for comment in reader_comments:
    #     print(comment)



    print('    saved articles: {}'.format(count_saved_articles))
    print('    saved comments: {}'.format(count_saved_comments))
    print('    time taken: {} Min.'.format((time.time() - starting_time)/60))



if __name__ == "__main__":
    main()
