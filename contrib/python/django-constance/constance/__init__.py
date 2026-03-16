from django.utils.functional import LazyObject

__version__ = '3.1.0'


class LazyConfig(LazyObject):
    def _setup(self):
        from .base import Config
        self._wrapped = Config()


config = LazyConfig()
