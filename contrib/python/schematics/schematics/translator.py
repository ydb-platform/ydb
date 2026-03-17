# -*- coding: utf-8 -*-

from .compat import str_compat


@str_compat
class LazyText(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        translator = _.real_translator
        return translator(self.message) if translator else self.message

    def __mod__(self, other):
        return str(self) % other

    def format(self, *args, **kwargs):
        return str(self).format(*args, **kwargs)


class Translator(object):
    """A placeholder which could call a function like lazy_gettext and make messages translatable."""
    def __init__(self):
        self.real_translator = None

    def __call__(self, message, lazy=True, *args, **kwargs):
        return LazyText(message) if lazy else str(LazyText(message))

    def register_translator(self, new_translator):
        self.real_translator = new_translator

_ = Translator()
register_translator = _.register_translator
