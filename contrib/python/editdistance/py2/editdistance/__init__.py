from .bycython import eval


def distance(*args, **kwargs):
    """"An alias to eval"""
    return eval(*args, **kwargs)


__all__ = ('eval', 'distance')
