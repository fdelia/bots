import datetime
import json
import redis
import numpy as np

def compute_stats_articles(table):
    '''
    Returns: count, mean, stdev, max, min, missing
        for features:
            * num_comments
    '''
    num_comments = []
    num_comments_missing = 0
    for key in table.scan_iter():
        item = json.loads(table.get(key).decode('utf-8'))
        if 'num_comments' in item:
            num_comments.append(item['num_comments'])
        else:
            num_comments_missing += 1

    num_comments = np.array(num_comments, dtype='float')
    return {
    'num_comments': {
        'count': len(num_comments),
        'mean': np.mean(num_comments),
        'std': np.std(num_comments),
        'max': np.max(num_comments),
        'min': np.min(num_comments),
        'missing': num_comments_missing
        }
    }

def compute_stats_comments(table):
    '''
    Returns: count, mean, stdev, max, min, missing
        grouped by:
        * all items
        * last day
        * last 7 days

        for features:
            * number of comments
            * via mobile
            * upvotes
            * downvotes
            * if comment is an answer ("@")
    '''
    stats = {}
    for key in table.scan_iter():
        item = json.loads(table.get(key).decode('utf-8'))

        print(item)
        break


    return stats

def save_obj(obj, file):
    dump = json.dumps(obj)
    file.write(dump)

def main():
    # init file
    today = datetime.datetime.now()
    filename = "stats{:04d}_{:02d}_{:02d}_{:02d}h.csv".format(today.year, today.month, today.day, today.hour)
    f_open = open(filename, 'w')

    # init DBs
    db_articles = redis.StrictRedis(host='localhost', port=6379, db=0)
    db_comments = redis.StrictRedis(host='localhost', port=6379, db=1)

    # compute
    stats_articles = compute_stats_articles(db_articles)
    stats_comments = compute_stats_comments(db_comments)

    print(stats_articles)
    print(stats_comments)

    # save stats
    save_obj([stats_articles, stats_comments], f_open)

    # close file
    f_open.close()



if __name__ == "__main__":
    main()
