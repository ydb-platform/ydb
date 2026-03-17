import six

from .utils import translit

__title__ = 'transliterate.decorators'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'transliterate_function',
    'transliterate_method',
)


class TransliterateFunction(object):
    """Function decorator."""

    def __init__(self, language_code, reversed=False):
        self.language_code = language_code
        self.reversed = reversed

    def __call__(self, func):
        def inner(*args, **kwargs):
            if not six.PY3:
                value = unicode(func(*args, **kwargs))
            else:
                value = func(*args, **kwargs)

            return translit(value,
                            language_code=self.language_code,
                            reversed=self.reversed)
        return inner


transliterate_function = TransliterateFunction


class TransliterateMethod(object):
    """Method decorator."""

    def __init__(self, language_code, reversed=False):
        self.language_code = language_code
        self.reversed = reversed

    def __call__(self, func):
        def inner(this, *args, **kwargs):
            if not six.PY3:
                value = unicode(func(this, *args, **kwargs))
            else:
                value = func(this, *args, **kwargs)

            return translit(value,
                            language_code=self.language_code,
                            reversed=self.reversed)
        return inner


transliterate_method = TransliterateMethod
