#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import random
import string
import time
from multiprocessing import Process
import os
import sys


pack_size = 500
time_interval = 1000000

random_patterns = [
    """{{"time":{0}, "key":"fqdn1", "ev_type":"login",      "event_class":"event_class1", "vpn":true, "message":"{1}"}}""",
    """{{"time":{0}, "key":"fqdn1", "ev_type":"delete_all", "event_class":"event_class2", "vpn":false, "message":"{1}"}}""",
    """{{"time":{0}, "key":"fqdn1", "ev_type":"delete_all", "event_class":"event_class3", "vpn":false, "message":"{1}"}}""",
]

match_patterns = """{{"time":{0}, "key":"fqdn1", "ev_type":"login",      "event_class":"event_class777", "vpn":true, "message":"{1}"}}"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream writter tool",
        epilog='Example: ./writter /tmp/pipe',
    )
   # parser.add_argument('pipe_name')
    args = parser.parse_args()
    return args


class Producer(object):
    def __init__(self):
        self.args = parse_args()
        self.random_string = []

        for i in range(100):
            self.random_string.append(''.join(random.choices(string.ascii_lowercase, k=9000)))

    def send(self, dt):
        for r in self.records:
            sys.stdout.write(r)

        self.records = []

    def get_random_string(self):
        return random.choice(self.random_string)


    def loop(self):
        self.records = []
        self.begin_time = time.time()
        self.records = []

        match_pattern_time = int(time.time()) + 10

        while True:
            time.sleep(0.001)
            # 0.002  12.0MB/s avg -
            # 0.003   8.2MB/s avg +
            # 0.004   6.2MB/s avg +

            dt = int(time.time() * 1000000)
            self.send(dt)
            # user1 = ''.join(random.choices(string.ascii_lowercase, k=5))
            for row in random_patterns:
                dt = int(time.time() * 1000000)
                data = row.format(dt, self.get_random_string()) + "\n"
                self.records.append(data)

            if int(time.time()) > match_pattern_time:
                match_pattern_time = int(time.time()) + 10
                dt = int(time.time() * 1000000)
                data = match_patterns.format(dt, self.get_random_string()) + "\n"
                self.records.append(data)


def producer_process():
    producer = Producer()
    producer.loop()


def main():
    args = parse_args()
    list_of_processes = []

    producer = Producer()
    producer.loop()

if __name__ == "__main__":
    main()
