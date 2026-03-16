import trafaret as t
from trafaret.lib import py3metafix


class ConstructMeta(type):
    def __or__(self, other):
        return construct(other)

    def __and__(self, other):
        return construct(other)


@py3metafix
class C(object):
    '''
    Start object. It has `|` and `&` operations defined that will use construct to it args

    Use it like `C & int & check_less_500`
    '''
    __metaclass__ = ConstructMeta


def construct(arg):
    '''
    Shortcut syntax to define trafarets.

    - int, str, float and bool will return t.Int, t.String, t.Float and t.Bool
    - one element list will return t.List
    - tuple or list with several args will return t.Tuple
    - dict will return t.Dict. If key has '?' at the and it will be optional and '?' will be removed
    - any callable will be t.Call
    - otherwise it will be returned as is

    construct is recursive and will try construct all lists, tuples and dicts args
    '''
    if isinstance(arg, t.Trafaret):
        return arg
    elif isinstance(arg, tuple) or (isinstance(arg, list) and len(arg) > 1):
        return t.Tuple(*(construct(a) for a in arg))
    elif isinstance(arg, list):
        # if len(arg) == 1
        return t.List(construct(arg[0]))
    elif isinstance(arg, dict):
        return t.Dict({construct_key(key): construct(value) for key, value in arg.items()})
    elif isinstance(arg, str):
        return t.Atom(arg)
    elif isinstance(arg, type):
        if arg is int:
            return t.ToInt()
        elif arg is float:
            return t.ToFloat()
        elif arg is str:
            return t.String()
        elif arg is bool:
            return t.Bool()
        else:
            return t.Type(arg)
    elif callable(arg):
        return t.Call(arg)
    else:
        return arg


def construct_key(key):
    if isinstance(key, t.Key):
        return key
    elif isinstance(key, str):
        if key.endswith('?'):
            return t.Key(key[:-1], optional=True)
        return t.Key(key)
    raise ValueError()
