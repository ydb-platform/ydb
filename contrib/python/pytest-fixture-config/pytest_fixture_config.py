""" Fixture configuration
"""
import functools

import pytest


class Config(object):
    __slots__ = ()

    def __init__(self, **kwargs):
        [setattr(self, k, v) for (k, v) in kwargs.items()]

    def update(self, cfg):
        for k in cfg:
            if k not in self.__slots__:
                raise ValueError("Unknown config option: {0}".format(k))
            setattr(self, k, cfg[k])


def requires_config(cfg, vars_):
    """ Decorator for fixtures that will skip tests if the required config variables
        are missing or undefined in the configuration
    """
    def decorator(f):
        # We need to specify 'request' in the args here to satisfy pytest's fixture logic
        @functools.wraps(f)
        def wrapper(request, *args, **kwargs):
            for var in vars_:
                if not getattr(cfg, var):
                    pytest.skip('config variable {0} missing, skipping test'.format(var))
            return f(request, *args, **kwargs)
        return wrapper
    return decorator


def yield_requires_config(cfg, vars_):
    """ As above but for py.test yield_fixtures
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for var in vars_:
                if not getattr(cfg, var):
                    pytest.skip('config variable {0} missing, skipping test'.format(var))
            gen = f(*args, **kwargs)
            yield next(gen)
        return wrapper
    return decorator
