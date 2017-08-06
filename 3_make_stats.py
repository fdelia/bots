from datetime import datetime, timedelta
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
            * upvotes
            * downvotes
            TODO * via mobile
            TODO * if comment is an answer ("@")
    '''
    features = ['vup', 'vdo']

    # TODO make this more abstract
    times = {
        'all': timedelta(days=9999),
        '7 days': timedelta(days=7),
        '1 day': timedelta(days=1)
    }
    stats_all = {}
    stats_last1 = {}
    stats_last7 = {}
    missing_all = {}
    missing_last1 = {}
    missing_last7 = {}

    for f in features:
        stats_all[f] = []
        stats_last1[f] = []
        stats_last7[f] = []
        missing_all[f] = 0
        missing_last1[f] = 0
        missing_last7[f] = 0

    now = datetime.now()
    time_first = now
    time_last = datetime(1970, 1, 1)
    for key in table.scan_iter():
        item = json.loads(table.get(key).decode('utf-8'))
        dt = datetime.strptime(item['time'].replace('am ', ''), '%d.%m.%Y %H:%M')

        time_first = min(time_first, dt)
        time_last = max(time_last, dt)

        for f in features:
            if f in item:
                stats_all[f].append(item[f])
                if (now - dt) < timedelta(days=7):
                    stats_last7[f].append(item[f])
                if (now - dt) < timedelta(days=1):
                    stats_last1[f].append(item[f])

            else:
                missing_all[f] += 1
                if (now - dt) < timedelta(days=7):
                    missing_last7[f] += 1
                if (now - dt) < timedelta(days=1):
                    missing_last1[f] += 1


    stats = {
        'time_first': time_first.strftime("%Y-%m-%d %H:%M:%S"),
        'time_last': time_last.strftime("%Y-%m-%d %H:%M:%S")
    }
    for t, val in times.items():
        stats[t] = {}
        if t == 'all':
            s = stats_all
            m = missing_all
        if t == '1 day':
            s = stats_last1
            m = missing_last1
        if t == '7 days':
            s = stats_last7
            m = missing_last7

        for f in features:
            arr = np.array(s[f], dtype='float')
            stats[t][f] = {
                'count': len(arr),
                'mean': np.mean(arr),
                'std': np.std(arr),
                'max': np.max(arr),
                'min': np.min(arr),
                'missing': m[f]
            }

    return stats

def save_obj(obj, file):
    dump = json.dumps(obj)
    file.write(dump)

def main():
    # init file
    now = datetime.now()
    filename = "stats_{:04d}_{:02d}".format(now.year, now.month)
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
