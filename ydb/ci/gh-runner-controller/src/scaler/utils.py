import base64
import os
import time
import logging
from typing import Callable, Optional


def generate_short_id(n=6):
    return base64.b32encode(os.urandom(n)).replace(b"=", b"").decode("utf8").lower()


logger = logging.getLogger(__name__)


class ExpireItemsSet:
    def __init__(self, ttl, set_name: Optional[str]):
        self.items = {}
        self.ttl = ttl
        self.set_name = set_name

    def add(self, item, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        self.items[item] = timestamp

    def evict_expired(self):
        t = time.time()
        for k in list(self.items.keys()):
            if t > self.items[k] + self.ttl:
                del self.items[k]
                if self.set_name:
                    logger.info("remove %s from %s", k, self.set_name)

    def __len__(self):
        self.evict_expired()
        return len(self.items)

    def __getitem__(self, item):
        self.evict_expired()
        return item in self.items

    def __repr__(self):
        self.evict_expired()
        current_time = time.time()
        result = []
        for item, item_time in self.items.items():
            alive_time = current_time - item_time
            result.append(f"{item}:{alive_time:.2f}s")
        return ','.join(result)

    def remove(self, item):
        del self.items[item]

    def remove_if_contains(self, item):
        self.evict_expired()
        if item in self.items:
            del self.items[item]
            return True
        return False
