# code snippet copied from https://gist.github.com/NotSqrt/5f3c76cd15e40ef62d09
from pytest_django.lazy_django import get_django_version


class DisableMigrations(object):
    def __init__(self):
        self._django_version = get_django_version()

    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        if self._django_version >= (1, 9):
            return None
        else:
            return "notmigrations"
