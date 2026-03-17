import logging
from functools import wraps

from django.core.cache import cache


def generate_key(function, *args, **kwargs):
    args_string = ','.join([str(arg) for arg in args] +
                           ['{}={}'.format(k, v) for k, v in kwargs.items()])
    return '{}({})'.format(function.__name__, args_string)


def enqueue(function, *args, **kwargs):
    key = generate_key(function, *args, **kwargs)
    logging.info('Scheduling task %s', key)

    if cache.get(key):
        logging.info('Dropping task %s', key)
        return

    cache.set(key, 'lock')
    function.delay(*args, **kwargs)


def no_duplicates(function, *args, **kwargs):
    """
    Makes sure that no duplicated tasks are enqueued.
    """
    @wraps(function)
    def wrapper(self, *args, **kwargs):
        key = generate_key(function, *args, **kwargs)
        try:
            function(self, *args, **kwargs)
        finally:
            logging.info('Removing key %s', key)
            cache.delete(key)

    return wrapper
