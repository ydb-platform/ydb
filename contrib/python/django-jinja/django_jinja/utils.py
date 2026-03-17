import functools
from importlib import import_module

from django.utils.safestring import mark_safe
from django.core.exceptions import ImproperlyConfigured



def load_class(path):
    """
    Load class from path.
    """

    try:
        mod_name, klass_name = path.rsplit('.', 1)
        mod = import_module(mod_name)
    except AttributeError as e:
        raise ImproperlyConfigured(f'Error importing {mod_name}: "{e}"')

    try:
        klass = getattr(mod, klass_name)
    except AttributeError:
        raise ImproperlyConfigured(f'Module "{mod_name}" does not define a "{klass_name}" class')

    return klass


def safe(function):
    @functools.wraps(function)
    def _decorator(*args, **kwargs):
        return mark_safe(function(*args, **kwargs))
    return _decorator


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value

