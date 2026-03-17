import collections
import datetime
from datetime import timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field


class CachedItem(BaseModel):
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(tz=timezone.utc)
    )
    value: Any


class LRUCache:
    def __init__(self, capacity: int, expiration_time: timedelta):
        self.capacity: int = capacity
        self.expiration_time: timedelta = expiration_time
        self.cache: collections.OrderedDict = collections.OrderedDict()

    def get(self, key) -> Optional[CachedItem]:
        try:
            item: CachedItem = self.cache.pop(key)
            if (
                datetime.datetime.now(tz=timezone.utc) - item.timestamp
                > self.expiration_time
            ):
                return None
            self.cache[key] = item
            return item.value
        except KeyError:
            return None

    def set(self, key, value) -> None:
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = CachedItem(value=value)

    @staticmethod
    def create_key(*args):
        return str(args)  # TODO think about this
