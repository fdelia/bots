# -*- coding: utf-8 -*-
'''
Author: Fabio D'Elia
Description: Scraps all comments and article content from all articles
    linked in the home page of 20min.ch and saves into a redis DB.

    The contents need to be exported into CSV.
'''
import time
import datetime
import urllib.request, urllib.error, urllib.parse
import re
import redis
import traceback

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

    # should not happen
    # if not comment['id']:
    #     continue

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

# def article_uptodate(article, db_articles):
#     return True
#
# def comment_uptodate(comment, db_comments):
#     return True

def save_article(article, db_articles):
    key = "{}".format(article['article_id'])
    db_articles.set(key, json.dumps(article))

def save_comment(comment, db_comments):
    key = "{}_{}".format(comment['tId'], comment['cId'])
    db_comments.set(key, json.dumps(comment))


def main():
    print('20min.ch   ' + time.strftime('%c'))
    starting_time = time.time()

    # init DBs
    db_articles = redis.StrictRedis(host='localhost', port=6379, db=0)
    db_comments = redis.StrictRedis(host='localhost', port=6379, db=1)

    # get all links
    article_links = get_article_links()
    article_links = list(set(article_links)) # remove doubles

    # go through links
    count_saved_articles = 0
    count_saved_comments = 0
    for article_link in article_links:
        if DEV_PRINT:
            print('  get {}'.format(article_link))

        # those raise exceptions because they result in 403
        if '/promotion/' in article_link:
            continue
        if '/immobilien/reportagen/' in article_link:
            continue

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

        # if not article_uptodate(article):
        save_article(article, db_articles)
        count_saved_articles += 1

        # if not possible to comment # it's more performant to write anyways
        if not article['talkback_id']:
            continue

        try:
            comments_html = get_comments(article['talkback_id'])
            if part_comments:
                comments_html += part_comments.findAll('li', class_='comment')

            for comment in comments_html:
                # if not comment_uptodate(comment): # it's more performant to write anyways
                comment_parsed = parse_comment(comment, article['talkback_id'])
                save_comment(comment_parsed, db_comments)
                count_saved_comments += 1

        except Exception as e: # timeouts sometimes
            if SHOW_ERR:
                print('     err: exception getting comments for ' + str(article['talkback_id']))
                print(e)
                traceback.print_exc()
            continue




    print('    saved articles: {}'.format(count_saved_articles))
    print('    saved comments: {}'.format(count_saved_comments))
    print('    total # comments in DB: {}'.format(len(db_comments.keys())))
    print('    time taken: {} Min.'.format((time.time() - starting_time)/60))



if __name__ == "__main__":
    main()
