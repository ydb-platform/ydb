from starlette_context.header_keys import HeaderKeys
from starlette_context.plugins.base import Plugin


class ApiKeyPlugin(Plugin):
    key = HeaderKeys.api_key
