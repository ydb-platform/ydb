import logging
import functools

__all__ = ['Logged']


class Logged(object):
    '''Class which automatically adds a named logger to your class when
    interiting

    Adds easy access to debug, info, warning, error, exception and log methods

    >>> class MyClass(Logged):
    ...     def __init__(self):
    ...         Logged.__init__(self)
    >>> my_class = MyClass()
    >>> my_class.debug('debug')
    >>> my_class.info('info')
    >>> my_class.warning('warning')
    >>> my_class.error('error')
    >>> my_class.exception('exception')
    >>> my_class.log(0, 'log')
    '''
    def __new__(cls, *args, **kwargs):
        cls.logger = logging.getLogger(
            cls.__get_name(cls.__module__, cls.__name__))
        return super(Logged, cls).__new__(cls)

    @classmethod
    def __get_name(cls, *name_parts):
        return '.'.join(n.strip() for n in name_parts if n.strip())

    @classmethod
    @functools.wraps(logging.debug)
    def debug(cls, msg, *args, **kwargs):
        cls.logger.debug(msg, *args, **kwargs)

    @classmethod
    @functools.wraps(logging.info)
    def info(cls, msg, *args, **kwargs):
        cls.logger.info(msg, *args, **kwargs)

    @classmethod
    @functools.wraps(logging.warning)
    def warning(cls, msg, *args, **kwargs):
        cls.logger.warning(msg, *args, **kwargs)

    @classmethod
    @functools.wraps(logging.error)
    def error(cls, msg, *args, **kwargs):
        cls.logger.error(msg, *args, **kwargs)

    @classmethod
    @functools.wraps(logging.exception)
    def exception(cls, msg, *args, **kwargs):
        cls.logger.exception(msg, *args, **kwargs)

    @classmethod
    @functools.wraps(logging.log)
    def log(cls, lvl, msg, *args, **kwargs):
        cls.logger.log(lvl, msg, *args, **kwargs)

