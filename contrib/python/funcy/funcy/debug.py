import re
import traceback
from itertools import chain
from functools import partial
from timeit import default_timer as timer

from .decorators import decorator, wraps, Call


__all__ = [
    'tap',
    'log_calls', 'print_calls',
    'log_enters', 'print_enters',
    'log_exits', 'print_exits',
    'log_errors', 'print_errors',
    'log_durations', 'print_durations',
    'log_iter_durations', 'print_iter_durations',
]

REPR_LEN = 25


def tap(x, label=None):
    """Prints x and then returns it."""
    if label:
        print('%s: %s' % (label, x))
    else:
        print(x)
    return x


@decorator
def log_calls(call, print_func, errors=True, stack=True, repr_len=REPR_LEN):
    """Logs or prints all function calls,
       including arguments, results and raised exceptions."""
    signature = signature_repr(call, repr_len)
    try:
        print_func('Call %s' % signature)
        result = call()
        # NOTE: using full repr of result
        print_func('-> %s from %s' % (smart_repr(result, max_len=None), signature))
        return result
    except BaseException as e:
        if errors:
            print_func('-> ' + _format_error(signature, e, stack))
        raise

def print_calls(errors=True, stack=True, repr_len=REPR_LEN):
    if callable(errors):
        return log_calls(print)(errors)
    else:
        return log_calls(print, errors, stack, repr_len)
print_calls.__doc__ = log_calls.__doc__


@decorator
def log_enters(call, print_func, repr_len=REPR_LEN):
    """Logs each entrance to a function."""
    print_func('Call %s' % signature_repr(call, repr_len))
    return call()


def print_enters(repr_len=REPR_LEN):
    """Prints on each entrance to a function."""
    if callable(repr_len):
        return log_enters(print)(repr_len)
    else:
        return log_enters(print, repr_len)


@decorator
def log_exits(call, print_func, errors=True, stack=True, repr_len=REPR_LEN):
    """Logs exits from a function."""
    signature = signature_repr(call, repr_len)
    try:
        result = call()
        # NOTE: using full repr of result
        print_func('-> %s from %s' % (smart_repr(result, max_len=None), signature))
        return result
    except BaseException as e:
        if errors:
            print_func('-> ' + _format_error(signature, e, stack))
        raise

def print_exits(errors=True, stack=True, repr_len=REPR_LEN):
    """Prints on exits from a function."""
    if callable(errors):
        return log_exits(print)(errors)
    else:
        return log_exits(print, errors, stack, repr_len)


class LabeledContextDecorator(object):
    """
    A context manager which also works as decorator, passing call signature as its label.
    """
    def __init__(self, print_func, label=None, repr_len=REPR_LEN):
        self.print_func = print_func
        self.label = label
        self.repr_len = repr_len

    def __call__(self, label=None, **kwargs):
        if callable(label):
            return self.decorator(label)
        else:
            return self.__class__(self.print_func, label, **kwargs)

    def decorator(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            # Recreate self with a new label so that nested and recursive calls will work
            cm = self.__class__.__new__(self.__class__)
            cm.__dict__.update(self.__dict__)
            cm.label = signature_repr(Call(func, args, kwargs), self.repr_len)
            with cm:
                return func(*args, **kwargs)
        return inner


class log_errors(LabeledContextDecorator):
    """Logs or prints all errors within a function or block."""
    def __init__(self, print_func, label=None, stack=True, repr_len=REPR_LEN):
        LabeledContextDecorator.__init__(self, print_func, label=label, repr_len=repr_len)
        self.stack = stack

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type:
            if self.stack:
                exc_message = ''.join(traceback.format_exception(exc_type, exc_value, tb))
            else:
                exc_message = '%s: %s' % (exc_type.__name__, exc_value)
            self.print_func(_format_error(self.label, exc_message, self.stack))

print_errors = log_errors(print)


# Duration utils

def format_time(sec):
    if sec < 1e-6:
        return '%8.2f ns' % (sec * 1e9)
    elif sec < 1e-3:
        return '%8.2f mks' % (sec * 1e6)
    elif sec < 1:
        return '%8.2f ms' % (sec * 1e3)
    else:
        return '%8.2f s' % sec

time_formatters = {
    'auto': format_time,
    'ns': lambda sec: '%8.2f ns' % (sec * 1e9),
    'mks': lambda sec: '%8.2f mks' % (sec * 1e6),
    'ms': lambda sec: '%8.2f ms' % (sec * 1e3),
    's': lambda sec: '%8.2f s' % sec,
}


class log_durations(LabeledContextDecorator):
    """Times each function call or block execution."""
    def __init__(self, print_func, label=None, unit='auto', threshold=-1, repr_len=REPR_LEN):
        LabeledContextDecorator.__init__(self, print_func, label=label, repr_len=repr_len)
        if unit not in time_formatters:
            raise ValueError('Unknown time unit: %s. It should be ns, mks, ms, s or auto.' % unit)
        self.format_time = time_formatters[unit]
        self.threshold = threshold

    def __enter__(self):
        self.start = timer()
        return self

    def __exit__(self, *exc):
        duration = timer() - self.start
        if duration >= self.threshold:
            duration_str = self.format_time(duration)
            self.print_func("%s in %s" % (duration_str, self.label) if self.label else duration_str)

print_durations = log_durations(print)


def log_iter_durations(seq, print_func, label=None, unit='auto'):
    """Times processing of each item in seq."""
    if unit not in time_formatters:
        raise ValueError('Unknown time unit: %s. It should be ns, mks, ms, s or auto.' % unit)
    _format_time = time_formatters[unit]
    suffix = " of %s" % label if label else ""
    it = iter(seq)
    for i, item in enumerate(it):
        start = timer()
        yield item
        duration = _format_time(timer() - start)
        print_func("%s in iteration %d%s" % (duration, i, suffix))

def print_iter_durations(seq, label=None, unit='auto'):
    """Times processing of each item in seq."""
    return log_iter_durations(seq, print, label, unit=unit)


### Formatting utils

def _format_error(label, e, stack=True):
    if isinstance(e, Exception):
        if stack:
            e_message = traceback.format_exc()
        else:
            e_message = '%s: %s' % (e.__class__.__name__, e)
    else:
        e_message = e

    if label:
        template = '%s    raised in %s' if stack else '%s raised in %s'
        return template % (e_message, label)
    else:
        return e_message


### Call signature stringification utils

def signature_repr(call, repr_len=REPR_LEN):
    if isinstance(call._func, partial):
        if hasattr(call._func.func, '__name__'):
            name = '<%s partial>' % call._func.func.__name__
        else:
            name = '<unknown partial>'
    else:
        name = getattr(call._func, '__name__', '<unknown>')
    args_repr = (smart_repr(arg, repr_len) for arg in call._args)
    kwargs_repr = ('%s=%s' % (key, smart_repr(value, repr_len))
                   for key, value in call._kwargs.items())
    return '%s(%s)' % (name, ', '.join(chain(args_repr, kwargs_repr)))

def smart_repr(value, max_len=REPR_LEN):
    if isinstance(value, (bytes, str)):
        res = repr(value)
    else:
        res = str(value)

    res = re.sub(r'\s+', ' ', res)
    if max_len and len(res) > max_len:
        res = res[:max_len-3] + '...'
    return res
