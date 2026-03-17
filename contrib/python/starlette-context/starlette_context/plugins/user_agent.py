from starlette_context.header_keys import HeaderKeys
from starlette_context.plugins.base import Plugin


class UserAgentPlugin(Plugin):
    key = HeaderKeys.user_agent
