import os
import signal
import time


def test_segfault():
    os.kill(os.getpid(), signal.SIGSEGV)


def test_timeout():
    while True:
        time.sleep(1)
