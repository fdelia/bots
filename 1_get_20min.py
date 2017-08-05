# -*- coding: utf-8 -*-
'''
Author: Fabio D'Elia
Description: Scraps all comments and article content from all articles
    linked in the home page of 20min.ch and saves them to CSV-files.
    The CSV-files are rotated monthly.
'''
import time
import datetime
import urllib.request, urllib.error, urllib.parse
import re
import threading
import queue
import zlib
import csv

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
MAX_THREADS = 8
DEV_PRINT = False
SHOW_ERR = False
PARSER = "html.parser"



def get_article_links():
    links = []

    site = BeautifulSoup(urllib.request.urlopen("http://www.20min.ch", timeout=10), PARSER)
    regexp_article_link = re.compile(r'((?=[^\d])[a-zA-Z-\/]*\d{8})')
    for a in site.findAll('a', href=True):
    # if not a.has_attr('href'): continue

    # if regexp_article_link.search(a['href']) is not None:
        s = regexp_article_link.search(a['href'])
        if s is not None and len(s.group(0)) > 8:
            links.append(a['href'])

    return links


# header = .story_head h1
# subtitle = .story_head h3
# text = .story_text p's
# anzahl kommentare = .left .more_link_top
# kommentar = .entry .title / .content / .author / .time / .viamobile
def get_article(link):
    if DEV_PRINT: print('get article ' + link)
    global hdr
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
        text += '\n\n'

    more_link_top = site.find('div', class_='more_link_top')
    if more_link_top:
        num_comments = more_link_top.get_text()
    else:
        num_comments = 0

    regex = re.compile(r'(?=[^\d])[a-zA-Z-\/]*(\d{8})')
    m = regex.match(link)
    if m is None: return False
    article_id = m.group(1)

    talkback_id = site.find("div", id='talkback')
    if talkback_id is not None: talkback_id = talkback_id['data-talkbackid']
    if not talkback_id: talkback_id = article_id

    return {
    'article_id': article_id,
    'talkback_id': talkback_id,
    'header': story_head.find("h1").get_text(),
    'link': link,
    'subtitle': story_head.find("h3").get_text(),
    'text': text,
    'num_comments': num_comments,
    'updated': time.time(),
    'comments_part': site.find('ul', class_='comments')
    }


# some of the comments are still not parsed since a user would have to make one more request by clicking "Kommentare anzeigen (2)" on answers
# i thought i'd be enough comments for a first analysis
def get_and_save_comments(talkback_id, db_comments, queue):
    if DEV_PRINT: print('get and save comments ' + talkback_id)
    global hdr
    time.sleep(DELAY_MS / 1000.0) # delay

    # req_time = time.time()
    url = 'http://www.20min.ch/community/storydiscussion/messageoverview.tmpl?storyid=' + str(talkback_id) + '&type=1&l=0'
    req = urllib.request.Request(url, headers=hdr)
    site = BeautifulSoup(urllib.request.urlopen(req, timeout=5), PARSER)
    # print '   req + parsing time: ' + str((time.time() - req_time)) + ' Sec'

    num_saved = save_comments(site, db_comments, talkback_id)
    queue.put(num_saved)
    return num_saved


def save_comments(site, db_comments, talkback_id):
    found_comments = 0
    saved_comments = 0
    for comment in site.findAll('li', class_='comment'):
        found_comments += 1

        viamobile = 1
        if comment.find('span', class_='viamobile') is None: viamobile = 0

        comment_dict = {
        'tId': talkback_id,
        'cId': comment['id'],
        'tit': comment.find('h3', class_='title').get_text(),
        'aut': comment.find('span', class_='author').get_text(),
        'time': comment.find('span', class_='time').get_text(),
        'con': comment.find('p', class_='content').get_text(),
        'mob': viamobile,
        'vup': comment['data-voteup'],
        'vdo': comment['data-votedown']
        }

        '''
        27.01.2017 23:00
        RENAMES = {
        'comment_id': 'cId',
        'data-votedown': 'vdo',
        'data-voteup': 'vup',
        'viamobile': 'mob',
        'title': 'tit',
        'talkback_id': 'tId',
        'author': 'aut',
        'content': 'con'
        }
        '''


        if not comment_dict['cId']: continue
        comment_dict['cId'] = comment_dict['cId'].replace('thread', '').replace('msg', '')
        if save_comment_if_needed(comment_dict, db_comments) is not False:
            saved_comments += 1

    # http://www.20min.ch/community/storydiscussion/messageoverview.tmpl?storyid=14623185&type=1&l=0&channel=de/leben

    # http://www.20min.ch/schweiz/news/story/So-haben-die-AKW-Gemeinden-gestimmt-13471081
    # talkback id 21748185 = story id hier

    return found_comments



def save_article_if_needed(article, db_articles):
    dumped = json.dumps(article)
    res = db_articles.get(article['article_id'])


    if res is None or res != dumped:
        return db_articles.set(article['article_id'], dumped)
    else:
        return False

def save_comment_if_needed(comment, db_comments):
    key = comment['tId'] + '_' + comment['cId']

    dumped = json.dumps(comment)
    # dumped = zlib.compress(dumped, 3)
    # res = db_comments.get(key) # takes  ~ 8.3 Min. (300ms delay), ~ 4.5 (50ms delay), 3.2 (10ms), parallel: 3.4 (50ms)
    res = None # save always, for performance, takes  ~  7.2 Min. (300ms delay), ~ 4.7 (50ms delay), parallel: 3.6 (50ms)

    if res is None or res != dumped:
        return db_comments.set(key, dumped)
    else:
        return False


def main():
    print('20min.ch   ' + time.strftime('%c'))
    starting_time = time.time()

    # TODO load IDs of already saved comments and articles
    # (don't save num of comments into article row)
    # TODO get article_links
    # TODO get article content and comments
    # TODO save line by line to CSV-file

    # init csv-files
    today = datetime.date.today()
    filename_articles = "articles_{:04d}_{:02d}.csv".format(today.year, today.month)
    filename_comments = "comments_{:04d}_{:02d}.csv".format(today.year, today.month)
    
    # TODO check if this works, maybe encode text?
    DELIMITER = ','

    with open(filename_articles, 'wb') as file_articles:
        writer_articles = csv.writer(file_articles, delimiter=DELIMITER)
    with open(filename_comments, 'wb') as file_comments:
        writer_comments = csv.writer(file_comments, delimiter=DELIMITER)

    article_links = get_article_links()
    article_links = list(set(article_links)) # remove doubles

    return False



    saved_articles = 0
    saved_comments = 0
    threads = []
    result = queue.Queue()
    for article_link in article_links:
        try:
            article = get_article(article_link)
            if not article: continue
        except Exception as e: # it usually fails on stories which are promos because of some redirect (there are comments too there, but whatever)
        # print '  error: ... continuing'
            if SHOW_ERR:
                print('     err: exception getting article from ' + article_link)
                print(e)
            continue

        # save temporarly
        comments_part = article['comments_part']
        article['comments_part'] = None

        if save_article_if_needed(article, db_articles) is not False:
            saved_articles += 1

        if not article['talkback_id']: continue

        try:
            if comments_part is not None:
                saved_comments += save_comments(comments_part, db_comments, article['talkback_id'])


            # saved_comments += get_and_save_comments(article['talkback_id'], db_comments)
            threads.append(threading.Thread(target=get_and_save_comments, args=(article['talkback_id'], db_comments, result)))
            if len(threads) >= MAX_THREADS:
                for t in threads: t.start()
                for t in threads:
                    t.join()
                    saved_comments += result.get()
                threads = []


        except Exception as e: # timeouts sometimes
            if SHOW_ERR:
                print('     err: exception getting comments for ' + article['talkback_id'])
                print(e)
            continue

    # this will happen all the time if parallel threading is used
    # if article['num_comments'] and saved_comments < int(article['num_comments'].replace(' Kommentare', '')):
    #     print '     info: found ' + str(saved_comments) + ', expected ' + str(int(article['num_comments'].replace(' Kommentare', ''))) + ' comments, for ' + str(article['talkback_id'])




    print('   article links: ' + str(len(article_links)) + ', saved/updated articles: ' + str(saved_articles) + ', saved comments: '+ str(saved_comments))
    print('   new articles: ' + str(len(list(db_articles.keys())) - num_articles_before))
    # print '   new comments: ' + str(len(db_comments.keys()) - num_comments_before)
    print('   time taken: ' + str((time.time() - starting_time)/60) + ' Min.')



if __name__ == "__main__":
    main()
