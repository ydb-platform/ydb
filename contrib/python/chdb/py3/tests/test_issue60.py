import threading
import time
import unittest
import chdb

data = """url('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"""
# data = """file('/home/Clickhouse/server/chdb-server/house_0.parquet', Parquet)"""

query_str = f'''
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price
FROM {data}
GROUP BY
    town,
    district
ORDER BY c DESC
LIMIT 10
'''

expected_result = '''"BIRMINGHAM","BIRMINGHAM",35326,146648
"LEEDS","LEEDS",30640,160353
"SHEFFIELD","SHEFFIELD",22420,153128
"MANCHESTER","MANCHESTER",21917,156390
"BRISTOL","CITY OF BRISTOL",21662,217596
"LIVERPOOL","LIVERPOOL",19689,128179
"LONDON","WANDSWORTH",18442,456216
"CARDIFF","CARDIFF",16449,177420
"BRADFORD","BRADFORD",14468,100065
"COVENTRY","COVENTRY",13927,149269
'''

result = ""

class myThread(threading.Thread):
    def __init__(self, threadID, name, delay):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.delay = delay

    def run(self):
        print_chdb(self.name, self.delay)


def print_chdb(threadName, delay):
    global result
    result = chdb.query(query_str, 'CSV')
    print(result)
    time.sleep(delay)
    print("%s: %s" % (threadName, time.ctime(time.time())))


class TestQueryInThread(unittest.TestCase):
    def test_query_in_thread(self):
        thread1 = myThread(1, "Thread-1", 1)
        thread1.start()
        thread1.join()
        self.assertEqual(str(result), expected_result)

if __name__ == '__main__':
    unittest.main()
