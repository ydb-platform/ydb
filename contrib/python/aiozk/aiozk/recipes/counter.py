import logging

from aiozk import exc

from .recipe import Recipe


log = logging.getLogger(__name__)


class Counter(Recipe):
    def __init__(self, base_path, default=0):
        super().__init__(base_path)

        self.value = None
        self.numeric_type = type(default)
        self._default = default
        self._version = 0

    async def _fetch(self):
        data, stat = await self.client.get(self.base_path)
        self._version = stat.version
        self.value = self.numeric_type(data)
        return (self.value, stat.version)

    async def start(self):
        base, _leaf = self.base_path.rsplit('/', 1)
        if base:
            await self.client.ensure_path(base)
        try:
            await self.client.create(self.base_path, str(self._default))
            self.value = self._default
            self._version = 0
        except exc.NodeExists:
            await self._fetch()

    async def get_value(self):
        data, _version = await self._fetch()
        return data

    async def set_value(self, value, force=True):
        data = str(value)
        version = -1 if force else self._version
        stat = await self.client.set(self.base_path, data, version)
        self.value = value
        self._version = stat.version
        log.debug("Set value to '%s': successful", data)

    async def apply_operation(self, operation):
        success = False
        while not success:
            new_value = operation(self.value)
            data = str(new_value)
            try:
                stat = await self.client.set(self.base_path, data, self._version)
                log.debug("Operation '%s': successful", operation.__name__)
                self.value = new_value
                self._version = stat.version
                success = True
            except exc.BadVersion:
                await self._fetch()
                log.debug("Operation '%s': version mismatch, retrying", operation.__name__)

    async def incr(self, by=1):
        def increment(value):
            return value + self.numeric_type(by)

        await self.apply_operation(increment)

    async def decr(self, by=1):
        def decrement(value):
            return value - self.numeric_type(by)

        await self.apply_operation(decrement)

    def stop(self):
        """Deprecated. Counter no longer needs to be stopped"""
