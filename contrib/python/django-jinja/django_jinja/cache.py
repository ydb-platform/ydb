from django.core.cache import caches
from django.utils.functional import cached_property
from jinja2 import BytecodeCache as _BytecodeCache


class BytecodeCache(_BytecodeCache):
    """
    A bytecode cache for Jinja2 that uses Django's caching framework.
    """

    def __init__(self, cache_name):
        self._cache_name = cache_name

    @cached_property
    def backend(self):
        return caches[self._cache_name]

    def load_bytecode(self, bucket):
        key = f'jinja2_{str(bucket.key)}'
        bytecode = self.backend.get(key)
        if bytecode:
            bucket.bytecode_from_string(bytecode)

    def dump_bytecode(self, bucket):
        key = f'jinja2_{str(bucket.key)}'
        self.backend.set(key, bucket.bytecode_to_string())
