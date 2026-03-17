from .bycython import eval, eval_criterion


def distance(*args, **kwargs):
    """"An alias to eval"""
    return eval(*args, **kwargs)

def distance_le_than(*args, **kwargs):
    """"An alias to eval"""
    return eval_criterion(*args, **kwargs)

__all__ = ('eval', 'distance', "eval_criterion", "distance_le_than")
