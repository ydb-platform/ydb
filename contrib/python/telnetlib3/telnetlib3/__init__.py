"""telnetlib3: an asyncio Telnet Protocol implemented in python."""

# std imports
import sys

# flake8: noqa: F405
# fmt: off
# isort: off
# Import order matters: server_shell symbols must be exported before server
# import due to function_lookup("telnetlib3.telnet_server_shell") at server.py load time
from . import server_base
from . import server_shell
from .server_shell import *  # noqa - Must export before server import
from . import server
from . import stream_writer
from . import stream_reader
from . import client_base
from . import client_shell
from . import color_filter
from . import client
from . import telopt
from . import mud
from . import slc
from . import telnetlib
from . import guard_shells
from . import fingerprinting
from . import server_fingerprinting
if sys.platform != "win32":
    from . import fingerprinting_display  # noqa: F401
from . import encodings  # noqa: F401 - registers custom codecs (petscii, atarist)
from . import sync
from ._session_context import TelnetSessionContext  # noqa: F401
from .server_base import *  # noqa
from .server import *  # noqa
from .stream_writer import *  # noqa
from .stream_reader import *  # noqa
from .client_base import *  # noqa
from .client_shell import *  # noqa
from .client import *  # noqa
from .telopt import *  # noqa
from .mud import *  # noqa
from .slc import *  # noqa
from .telnetlib import *  # noqa
from .guard_shells import *  # noqa
from .fingerprinting import *  # noqa
from .server_fingerprinting import *  # noqa
if sys.platform != "win32":
    from .fingerprinting_display import *  # noqa
from .sync import *  # noqa
try:
    from . import server_pty_shell
    from .server_pty_shell import *  # noqa
    PTY_SUPPORT = True
except ImportError:
    server_pty_shell = None  # type: ignore[assignment]
    PTY_SUPPORT = False
from .accessories import get_version as _get_version
# isort: on
# fmt: on

__all__ = tuple(
    dict.fromkeys(
        # server,
        server_base.__all__
        + server.__all__
        + server_shell.__all__
        + guard_shells.__all__
        + fingerprinting.__all__
        + server_fingerprinting.__all__
        + (server_pty_shell.__all__ if PTY_SUPPORT else ())
        # client,
        + client_base.__all__
        + client.__all__
        + client_shell.__all__
        # telnet protocol stream / decoders,
        + stream_writer.__all__
        + stream_reader.__all__
        # blocking i/o wrapper
        + sync.__all__
        # protocol bits, bytes, and names
        + telopt.__all__
        + mud.__all__
        + slc.__all__
        # python's legacy stdlib api
        + telnetlib.__all__
    )
)  # noqa - deduplicate, preserving order

__author__ = "Jeff Quast"
__url__ = "https://github.com/jquast/telnetlib3/"
__copyright__ = "Copyright 2013"
__credits__ = ["Jim Storch", "Wijnand Modderman-Lenstra"]
__license__ = "ISC"
__version__ = _get_version()
