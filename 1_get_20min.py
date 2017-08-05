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
SHOW_ERR = True
PARSER = "html.parser"



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
def get_and_save_comments(talkback_id, db_comments, queue):
    if DEV_PRINT: print('get and save comments ' + talkback_id)
    global hdr
    time.sleep(DELAY_MS / 1000.0) # delay

    url = 'http://www.20min.ch/community/storydiscussion/messageoverview.tmpl?storyid=' + str(talkback_id) + '&type=1&l=0'
    req = urllib.request.Request(url, headers=hdr)
    site = BeautifulSoup(urllib.request.urlopen(req, timeout=5), PARSER)

    num_saved = save_comments(site, db_comments, talkback_id)
    queue.put(num_saved)
    return num_saved

def save_article(article, writer_articles):
    # attention: order of values is important!
    writer_articles.writerow(list(article.values()))

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

# TODO check if article_id already there
def article_exists(article):
    return False

# TODO check if comment_id already there and up/downvotes the same
def comment_exists(comment):
    return False


def main():
    print('20min.ch   ' + time.strftime('%c'))
    starting_time = time.time()

    # TODO load IDs of already saved comments and articles
    # (don't save num of comments into article row)

    # TODO get article content and comments
    # TODO save line by line to CSV-file

    # init csv-files
    today = datetime.date.today()
    filename_articles = "articles_{:04d}_{:02d}.csv".format(today.year, today.month)
    filename_comments = "comments_{:04d}_{:02d}.csv".format(today.year, today.month)

    # TODO check it works with this delimiter, maybe encode text?
    DELIMITER = ','

    file_articles = open(filename_articles, 'w', encoding='utf-8')
    writer_articles = csv.writer(file_articles, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)
    file_comments = open(filename_comments, 'w', encoding='utf-8')
    writer_comments = csv.writer(file_comments, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)
    # for line in data: writer.writerow(line)



    article_links = get_article_links()
    article_links = list(set(article_links)) # remove doubles

    saved_articles = 0
    saved_comments = 0
    for article_link in article_links:
        print('get '+article_link)
        try:
            (article, part_comments) = get_article(article_link)
            if not article: continue
        except Exception as e: # it usually fails on stories which are promos because of some redirect (there are comments too there, but whatever)
            if SHOW_ERR:
                print('     err: exception getting article from ' + article_link)
                print(e)
            continue

        if not article_exists(article):
            save_article(article, writer_articles)
            saved_articles += 1


        # if not possible to comment
        if not article['talkback_id']: continue

        break

        try:
            if comments_part is not None:
                saved_comments += save_comments(comments_part, db_comments, article['talkback_id'])


            saved_comments += get_and_save_comments(article['talkback_id'], db_comments)
            # threads.append(threading.Thread(target=get_and_save_comments, args=(article['talkback_id'], db_comments, result)))
            # if len(threads) >= MAX_THREADS:
            #     for t in threads: t.start()
            #     for t in threads:
            #         t.join()
            #         saved_comments += result.get()
            #     threads = []


        except Exception as e: # timeouts sometimes
            if SHOW_ERR:
                print('     err: exception getting comments for ' + article['talkback_id'])
                print(e)
            continue

    # this will happen all the time if parallel threading is used
    # if article['num_comments'] and saved_comments < int(article['num_comments'].replace(' Kommentare', '')):
    #     print '     info: found ' + str(saved_comments) + ', expected ' + str(int(article['num_comments'].replace(' Kommentare', ''))) + ' comments, for ' + str(article['talkback_id'])




    # print('   article links: ' + str(len(article_links)) + ', saved/updated articles: ' + str(saved_articles) + ', saved comments: '+ str(saved_comments))
    # print('   new articles: ' + str(len(list(db_articles.keys())) - num_articles_before))
    # # print '   new comments: ' + str(len(db_comments.keys()) - num_comments_before)
    # print('   time taken: ' + str((time.time() - starting_time)/60) + ' Min.')



if __name__ == "__main__":
    main()
