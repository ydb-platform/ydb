from typing import TYPE_CHECKING

from ydb import scheme

if TYPE_CHECKING:
    from .driver import Driver as AsyncDriver


class SchemeClient(scheme.BaseSchemeClient["AsyncDriver"]):
    def __init__(self, driver: "AsyncDriver") -> None:
        super(SchemeClient, self).__init__(driver)

    async def make_directory(self, path, settings=None):
        return await super(SchemeClient, self).make_directory(path, settings)

    async def remove_directory(self, path, settings=None):
        return await super(SchemeClient, self).remove_directory(path, settings)

    async def list_directory(self, path, settings=None):
        return await super(SchemeClient, self).list_directory(path, settings)

    async def describe_path(self, path, settings=None):
        return await super(SchemeClient, self).describe_path(path, settings)

    async def modify_permissions(self, path, settings):
        return await super(SchemeClient, self).modify_permissions(path, settings)
