# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from threading import local


class StateItem(object):
    def __init__(self):
        self.requests_time = 0
        self.requests_count = 0
        self.sql_time = 0
        self.sql_count = 0
        self.redis_time = 0
        self.redis_count = 0

    def add_requests_time(self, requests_time):
        self.requests_time += requests_time
        self.requests_count += 1

    def add_sql_time(self, sql_time):
        self.sql_time += sql_time
        self.sql_count += 1

    def add_redis_time(self, redis_time):
        self.redis_time += redis_time
        self.redis_count += 1

    def __repr__(self):
        return 'requests(%d, %d), sql(%d, %d), redis(%d, %d)' % (
            self.requests_time, self.requests_count,
            self.sql_time, self.sql_count,
            self.redis_time, self.redis_count,
        )



class ThreadLocalState(local):
    def __init__(self):
        self.items = []
        self._enabled = True

    def reset(self):
        self.items = []

    def add_requests_time(self, requests_time):
        last_item = self.get_last_item()
        if last_item:
            last_item.add_requests_time(requests_time)

    def add_sql_time(self, sql_time):
        last_item = self.get_last_item()
        if last_item:
            last_item.add_sql_time(sql_time)

    def add_redis_time(self, redis_time):
        last_item = self.get_last_item()
        if last_item:
            last_item.add_redis_time(redis_time)

    def put_state(self):
        self.items.append(StateItem())

    def pop_state(self):
        last_item = self.items.pop()
        new_last_item = self.get_last_item()
        if new_last_item:
            new_last_item.requests_time += last_item.requests_time
            new_last_item.requests_count += last_item.requests_count
            new_last_item.sql_time += last_item.sql_time
            new_last_item.sql_count += last_item.sql_count
            new_last_item.redis_time += last_item.redis_time
            new_last_item.redis_count += last_item.redis_count
        return last_item

    def get_last_item(self):
        if self.items:
            return self.items[-1]

    def is_enabled(self):
        return self._enabled and bool(self.items)

    def enable(self):
        self._enabled = True

    def disable(self):
        self._enabled = False


_state = ThreadLocalState()


def get_state():
    return _state


def reset_state(sender, **kwargs):
    state = get_state()
    state.reset()
