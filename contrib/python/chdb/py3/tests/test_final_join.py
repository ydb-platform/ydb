#!python3

import unittest
import psutil
from chdb import session


current_process = psutil.Process()
check_thread_count = False


class TestStateful(unittest.TestCase):
    def test_zfree_thread_count(self):
        sess2 = session.Session()
        ret = sess2.query("SELECT sleep(2)", "Debug")
        # self.assertEqual(str(ret), "")
        thread_count = current_process.num_threads()
        print("Number of threads using psutil library: ", thread_count)
        if check_thread_count:
            self.assertEqual(thread_count, 1)
        sess2.cleanup()

if __name__ == "__main__":
    check_thread_count = True
    unittest.main()
