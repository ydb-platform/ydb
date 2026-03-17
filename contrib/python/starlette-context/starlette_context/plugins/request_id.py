from starlette_context.header_keys import HeaderKeys
from starlette_context.plugins.base import PluginUUIDBase


class RequestIdPlugin(PluginUUIDBase):
    key = HeaderKeys.request_id
