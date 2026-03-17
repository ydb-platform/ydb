# coding=utf-8

from functools import wraps
from contextlib import contextmanager

from django.core.signals import request_finished


def close_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        finally:
            request_finished.send(sender='greenlet')
    return wrapper


class NullContextRLock:
    def __init__(self, enter_result=None):
        self._enter_result = enter_result

    def __enter__(self):
        return self._enter_result

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None
