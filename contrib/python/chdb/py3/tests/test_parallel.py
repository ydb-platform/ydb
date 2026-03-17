#!python3
import concurrent.futures
import time
import sys
import unittest
import chdb
from utils import data_file

# run query parallel in n thread and benchmark
thread_count = 10
query_count = 50

if len(sys.argv) == 2:
    thread_count = int(sys.argv[1])
elif len(sys.argv) == 3:
    thread_count = int(sys.argv[1])
    query_count = int(sys.argv[2])

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=thread_count)


def run_query(query, fmt):
    res = chdb.query(query, fmt)
    # print(res)
    if len(res) < 100:
        print(f"Error: result size is not correct {len(res)}")
        # exit(1)


def run_queries(query, fmt, count=query_count):
    for i in range(count):
        if i % 5 == 0:
            print(f"Running {i} queries")
        run_query(query, fmt)


def run_queries_parallel(query, fmt, parallel=thread_count, count=query_count):
    for _ in range(parallel):
        thread_pool.submit(run_queries, query, fmt, count // parallel)


def wait():
    thread_pool.shutdown(wait=True)


def benchmark(query, fmt, parallel=thread_count, count=query_count):
    time_start = time.time()
    run_queries_parallel(query, fmt, parallel, count)
    wait()
    time_end = time.time()
    print("Time cost:", time_end - time_start, "s")
    print("QPS:", count / (time_end - time_start))


class TestParallel(unittest.TestCase):
    def test_parallel(self):
        benchmark(f"SELECT * FROM file('{data_file}', Parquet) LIMIT 10", "Arrow")


if __name__ == '__main__':
    unittest.main(verbosity=2)
