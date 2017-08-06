# -*- coding: utf-8 -*-
'''
Author: Fabio D'Elia
Description: Exports articles and comments into a CSV file
    which can be used for further analysis.
'''

import datetime
import redis
import json
import csv

DELIMITER = ','

'''
values_order: ensures same order in CSV
                since the python dict is not ordered
'''
def export_table(table, writer, values_order):
    writer.writerow(values_order) # header

    counter = 0
    for key in table.scan_iter():
        item = json.loads(table.get(key))

        # TODO how to handle missing value?
        values = []
        for v in values_order:
            values.append(item[v])

        writer.writerow(values)
        counter += 1

    return counter

def main():
    # init files
    today = datetime.date.today()
    filename_articles = "articles_{:04d}_{:02d}.csv".format(today.year, today.month)
    filename_comments = "comments_{:04d}_{:02d}.csv".format(today.year, today.month)
    file_articles = open(filename_articles, 'w')
    file_comments = open(filename_comments, 'w')
    writer_articles = csv.writer(file_articles, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)
    writer_comments = csv.writer(file_comments, delimiter=DELIMITER, quoting=csv.QUOTE_ALL)

    # init DBs
    db_articles = redis.StrictRedis(host='localhost', port=6379, db=0)
    db_comments = redis.StrictRedis(host='localhost', port=6379, db=1)

    # export to csv
    res_a = export_table(db_articles, writer_articles,
        ['tId', 'article_id', 'updated', 'num_comments', 'link', 'header', 'sub', 'text'])
    res_c = export_table(db_comments, writer_comments,
        ['tId', 'cId', 'mob', 'vup', 'vdo', 'tit', 'aut', 'time', 'con'])

    # report
    print("exported articles: {} ".format(res_a))
    print("exported comments: {}".format(res_c))


if __name__ == "__main__":
    main()
