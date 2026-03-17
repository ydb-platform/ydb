#!python3

import unittest
import os
import shutil
import signal
import threading
import time
from chdb import session

test_signal_handler_dir = ".tmp_test_signal_handler"
insert_counter = 0
exit_event1 = threading.Event()
exit_event2 = threading.Event()

class TestSignalHandler(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_signal_handler_dir, ignore_errors=True)
        self.sess = session.Session(test_signal_handler_dir)
        self.signal_received = False
        return super().setUp()

    def tearDown(self) -> None:
        self.sess.close()
        shutil.rmtree(test_signal_handler_dir, ignore_errors=True)
        return super().tearDown()

    def background_sender(self, delay):
        time.sleep(delay)
        print(f"send signal after {delay} seconds")
        os.kill(os.getpid(), signal.SIGINT)

    def test_signal_response(self):
        sender_thread = threading.Thread(
            target=self.background_sender,
            daemon=True,
            args=(3,)
        )
        sender_thread.start()

        start_time = time.time()
        try:
            while time.time() - start_time < 10:
                time.sleep(0.1)
                self.sess.query("SELECT 1")
        except KeyboardInterrupt:
            print("receive signal")
            self.signal_received = True

        self.assertTrue(self.signal_received)

    def test_data_integrity_after_interrupt(self):
        def data_writer():
            global insert_counter
            i = 500000
            while not exit_event1.is_set():
                self.sess.query(f"INSERT INTO signal_handler_table VALUES ({i})")
                insert_counter += 1
                i += 1
            exit_event2.set()

        self.sess.query("CREATE DATABASE IF NOT EXISTS test")
        self.sess.query("USE test")
        self.sess.query("CREATE TABLE signal_handler_table (id Int64) ENGINE = MergeTree() ORDER BY id")
        self.sess.query("INSERT INTO signal_handler_table SELECT number FROM numbers(500000)")

        writer_thread = threading.Thread(
            target=data_writer,
            daemon=False
        )
        sender_thread = threading.Thread(
            target=self.background_sender,
            daemon=True,
            args=(30,)
        )

        writer_thread.start()
        sender_thread.start()

        start_time = time.time()
        try:
            while time.time() - start_time < 60:
                self.sess.query("SELECT * FROM signal_handler_table")
        except KeyboardInterrupt:
            print("receive signal")
            exit_event1.set()
            self.signal_received = True

        self.assertTrue(self.signal_received)

        while not exit_event2.is_set():
            continue
        self.sess.close()
        self.sess = session.Session(test_signal_handler_dir)

        final_count = self.sess.query("SELECT * FROM test.signal_handler_table").rows_read()
        print(f"final count is {final_count}")
        self.assertEqual(int(final_count), 500000 + insert_counter)

if __name__ == "__main__":
    unittest.main()
