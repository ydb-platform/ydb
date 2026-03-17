from starlette_context.header_keys import HeaderKeys
from starlette_context.plugins.base import PluginUUIDBase


class CorrelationIdPlugin(PluginUUIDBase):
    key = HeaderKeys.correlation_id
