from ydb import scheme


class SchemeClient(scheme.BaseSchemeClient):
    def __init__(self, driver):
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
