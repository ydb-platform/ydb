"""Control remote side information.

Properly sets up host, scheme and remote properties of
aiohttp.web.Request if the server is deployed behind reverse proxy.

"""

__version__ = "1.3.0"


from typing_extensions import Protocol

from aiohttp import web

from .allowed_hosts import AllowedHosts
from .basic_auth import BasicAuth
from .cloudflare import Cloudflare
from .forwarded import ForwardedRelaxed, ForwardedStrict
from .secure import Secure
from .x_forwarded import XForwardedFiltered, XForwardedRelaxed, XForwardedStrict


class _Tool(Protocol):
    async def setup(self, app: web.Application) -> None: ...


async def setup(app: web.Application, *tools: _Tool) -> None:
    for tool in tools:
        await tool.setup(app)


__all__ = (
    "AllowedHosts",
    "BasicAuth",
    "Cloudflare",
    "ForwardedRelaxed",
    "ForwardedStrict",
    "Secure",
    "XForwardedFiltered",
    "XForwardedRelaxed",
    "XForwardedStrict",
    "setup",
)
