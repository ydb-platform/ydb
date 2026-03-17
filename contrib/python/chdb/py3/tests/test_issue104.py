#!python3

import os
import time
import shutil
import unittest
import psutil
from chdb import session as chs

tmp_dir = ".state_tmp_auxten_issue104"


class TestIssue104(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return super().setUp()

    def tearDown(self):
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return super().tearDown()

    def test_issue104(self):
        with chs.Session(tmp_dir) as sess:
            sess.query("CREATE DATABASE IF NOT EXISTS test_db ENGINE = Atomic;")
            # sess.query("CREATE DATABASE IF NOT EXISTS test_db ENGINE = Atomic;", "Debug")
            sess.query("CREATE TABLE IF NOT EXISTS test_db.test_table (x String, y String) ENGINE = MergeTree ORDER BY tuple()")
            sess.query("INSERT INTO test_db.test_table (x, y) VALUES ('A', 'B'), ('C', 'D');")

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            print("Original values:")
            ret = sess.query("SELECT * FROM test_db.test_table", "Debug")
            print(ret)
            # self.assertEqual(str(ret), '"A","B"\n"C","D"\n')

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            print('Values after ALTER UPDATE in same query expected:')
            ret = sess.query(
                "ALTER TABLE test_db.test_table UPDATE y = 'updated1' WHERE x = 'A';"
                "SELECT * FROM test_db.test_table WHERE x = 'A';")
            print(ret)
            self.assertEqual(str(ret), '"A","updated1"\n')

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            # print("Values after UPDATE in same query (expected 'A', 'updated'):")
            # ret = sess.query(
            #     "UPDATE test_db.test_table SET y = 'updated2' WHERE x = 'A';"
            #     "SELECT * FROM test_db.test_table WHERE x = 'A';")
            # print(ret)
            # self.assertEqual(str(ret), '"A","updated2"\n')

            print('Values after UPDATE expected:')
            sess.query("ALTER TABLE test_db.test_table UPDATE y = 'updated2' WHERE x = 'A';"
                    "ALTER TABLE test_db.test_table UPDATE y = 'updated3' WHERE x = 'A'")
            ret = sess.query("SELECT * FROM test_db.test_table WHERE x = 'A'")
            print(ret)
            self.assertEqual(str(ret), '"A","updated3"\n')

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            print("Values after DELETE expected:")
            sess.query("ALTER TABLE test_db.test_table DELETE WHERE x = 'A'")
            ret = sess.query("SELECT * FROM test_db.test_table")
            print(ret)
            self.assertEqual(str(ret), '"C","D"\n')

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            print("Values after ALTER then OPTIMIZE expected:")
            sess.query("ALTER TABLE test_db.test_table DELETE WHERE x = 'C'; OPTIMIZE TABLE test_db.test_table FINAL")
            ret = sess.query("SELECT * FROM test_db.test_table")
            print(ret)
            self.assertEqual(str(ret), "")

            print("Inserting 1000 rows")
            sess.query("INSERT INTO test_db.test_table (x, y) SELECT toString(number), toString(number) FROM numbers(1000);")
            ret = sess.query("SELECT count() FROM test_db.test_table", "Debug")
            count = str(ret).count("\n")
            print("Number of newline characters:", count)

            # show final thread count
            print("Final thread count:", len(psutil.Process().threads()))

            time.sleep(3)
            print("Final thread count after 3s:", len(psutil.Process().threads()))

            # unittest will run tests in one process, but numpy and pyarrow will spawn threads like these:
            # name "python3"
            #   #0  futex_wait_cancelable (private=0, expected=0, futex_word=0x7fdd3f756560 <thread_status+2784>) at ../sysdeps/nptl/futex-internal.h:186
            #   #1  __pthread_cond_wait_common (abstime=0x0, clockid=0, mutex=0x7fdd3f756510 <thread_status+2704>, cond=0x7fdd3f756538 <thread_status+2744>) at pthread_cond_wait.c:508
            #   #2  __pthread_cond_wait (cond=0x7fdd3f756538 <thread_status+2744>, mutex=0x7fdd3f756510 <thread_status+2704>) at pthread_cond_wait.c:638
            #   #3  0x00007fdd3dcbd43b in blas_thread_server () from /usr/local/lib/python3.9/dist-packages/numpy/core/../../numpy.libs/libopenblas64_p-r0-15028c96.3.21.so
            #   #4  0x00007fdd8fab5ea7 in start_thread (arg=<optimized out>) at pthread_create.c:477
            #   #5  0x00007fdd8f838a2f in clone () at ../sysdeps/unix/sysv/linux/x86_64/clone.S:95
            # and "AwsEventLoop"
            #   #0  0x00007fdd8f838d56 in epoll_wait (epfd=109, events=0x7fdb131fe950, maxevents=100, timeout=100000) at ../sysdeps/unix/sysv/linux/epoll_wait.c:30
            #   #1  0x00007fdc97033d06 in aws_event_loop_thread () from /usr/local/lib/python3.9/dist-packages/pyarrow/libarrow.so.1200
            #   #2  0x00007fdc97053232 in thread_fn () from /usr/local/lib/python3.9/dist-packages/pyarrow/libarrow.so.1200
            #   #3  0x00007fdd8fab5ea7 in start_thread (arg=<optimized out>) at pthread_create.c:477
            #   #4  0x00007fdd8f838a2f in clone () at ../sysdeps/unix/sysv/linux/x86_64/clone.S:95
            # will try to address them all for numpy and pyarrow
            # self.assertEqual(len(psutil.Process().threads()), 1)


if __name__ == "__main__":
    unittest.main()
