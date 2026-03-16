from ..exceptions import ConfigurationError


RANGE_255 = list(range(256))


class Modifier:

    _submod = None
    subcodes = None

    code = None
    submod = property()

    def __init__(self, submod=None):
        self.submod = submod

    @classmethod
    def _get_code_str(cls, src) -> str:
        """
        :param src:

        """
        return str(src) if src is not None else ''

    def __str__(self):
        return self._get_code_str(self.code)

    @submod.getter
    def submod(self) -> str:
        """Modifier 2 value."""
        return self._get_code_str(self._submod)

    @submod.setter
    def submod(self, val):
        allowed = self.subcodes

        if val is not None and (allowed is not None and val not in allowed):
            raise ConfigurationError(
                f'Unsupported submod (modifier2) value `{val}` '
                f'for {self.__class__.__name__} (modifier1) `{self.code}`')

        self._submod = val


class ModifierWsgi(Modifier):
    """Standard WSGI request followed by the HTTP request body."""
    subcodes = [0]
    code = 0


class ModifierPsgi(Modifier):
    """Standard PSGI request followed by the HTTP request body."""
    subcodes = [0]
    code = 5


class ModifierLua(Modifier):
    """Standard LUA/WSAPI request followed by the HTTP request body."""
    subcodes = [0]
    code = 6


class ModifierRack(Modifier):
    """Standard RACK request followed by the HTTP request body."""
    subcodes = [0]
    code = 7


class ModifierJvm(Modifier):
    """Standard JVM request for The JWSGI interface and
    The Clojure/Ring JVM request handler followed by the HTTP request body.

    """
    SUB_RING = 1
    """Use Clojure/Ring JVM request handler."""

    subcodes = [0, SUB_RING]
    code = 8


class ModifierCgi(Modifier):
    """Standard Running CGI scripts on uWSGI request followed by the HTTP request body."""
    subcodes = [0]
    code = 9


class ModifierManage(Modifier):
    """Management interface request: setup flag specified by ``modifier2``.

    For a list of management flag look at ``ManagementFlag``.

    """
    subcodes = RANGE_255
    code = 10


class ModifierGccgo(Modifier):
    code = 11


class ModifierPhp(Modifier):
    """Standard Running PHP scripts in uWSGI request followed by the HTTP request body."""
    subcodes = [0]
    code = 14


class ModifierMono(Modifier):
    """Standard The Mono ASP.NET plugin request followed by the HTTP request body."""
    subcodes = [0]
    code = 15


class ModifierSpooler(Modifier):
    """The uWSGI Spooler request, the block vars is converted
    to a dictionary/hash/table and passed to the spooler callable.

    """
    subcodes = RANGE_255
    code = 17


class ModifierSymcall(Modifier):
    """Direct call to C-like symbols."""
    subcodes = RANGE_255
    code = 18


class ModifierSsi(Modifier):
    code = 19


class ModifierEval(Modifier):
    """Raw Code evaluation. The interpreter is chosen by the ``modifier2``.

    ..note:: It does not return a valid uwsgi response, but a raw string (that may be an HTTP response).

    """
    SUB_PYTHON = 0
    SUB_PERL = 5

    subcodes = [SUB_PYTHON, SUB_PERL]
    code = 22


class ModifierXslt(Modifier):
    """Invoke the The XSLT plugin."""
    subcodes = RANGE_255
    code = 23


class ModifierV8(Modifier):
    """Invoke the uWSGI V8 support."""
    subcodes = RANGE_255
    code = 24


class ModifierGridfs(Modifier):
    """Invoke the The GridFS plugin."""
    subcodes = RANGE_255
    code = 25


class ModifierFastfunc(Modifier):
    """Call the FastFuncs specified by the ``modifier2`` field."""
    subcodes = RANGE_255
    code = 26


class ModifierGlusterfs(Modifier):
    """Invoke the The GlusterFS plugin."""
    subcodes = RANGE_255
    code = 27


class ModifierRados(Modifier):
    """Invoke the The RADOS plugin."""
    subcodes = RANGE_255
    code = 28


class ModifierManagePathInfo(Modifier):
    """Standard WSGI request followed by the HTTP request body.

    The ``PATH_INFO`` is automatically modified, removing the ``SCRIPT_NAME`` from it.

    """
    code = 30


class ModifierMessage(Modifier):
    """Generic message passing (reserved)."""
    subcodes = RANGE_255
    code = 31


class ModifierMessageArray(Modifier):
    """Array of char passing (reserved)."""
    subcodes = RANGE_255
    code = 32


class ModifierMessageMarshal(Modifier):
    """Marshalled/serialzed object passing (reserved)."""
    subcodes = RANGE_255
    code = 33


class ModifierWebdav(Modifier):
    code = 35


class ModifierSnmp(Modifier):
    """Identify a SNMP request/response (mainly via UDP)."""
    code = 48


class ModifierRaw(Modifier):
    """Corresponds to the ``HTTP`` string and signals that
    this is a raw HTTP response.

    """
    code = 72


class ModifierMulticastAnnounce(Modifier):
    """Announce message."""
    code = 73


class ModifierMulticast(Modifier):
    """Array of chars; a custom multicast message managed by uwsgi."""
    subcodes = [0]
    code = 74


class ModifierClusterNode(Modifier):
    """Add/remove/enable/disable node from a cluster.

    Add action requires a dict of at least 3 keys:

        * hostname
        * address
        * workers

    """
    SUB_ADD = 0
    SUB_REMOVE = 1
    SUB_ENABLE = 2
    SUB_DISABLE = 3

    subcodes = [
        SUB_ADD,
        SUB_REMOVE,
        SUB_ENABLE,
        SUB_DISABLE,
    ]
    code = 95


class ModifierRemoteLogging(Modifier):
    """Remote logging (clustering/multicast/unicast)."""
    subcodes = [0]
    code = 96


class ModifierReload(Modifier):
    """Graceful reload request."""

    SUB_REQUEST = 0
    SUB_CONFIRMATION = 1

    subcodes = [SUB_REQUEST, SUB_CONFIRMATION]
    code = 98


class ModifierReloadBrutal(ModifierReload):
    """Brutal reload request."""
    code = 97


class ModifierConfigFromNode(Modifier):
    """Request configuration data from a uwsgi node (even via multicast)."""
    subcodes = [0, 1]
    code = 99


class ModifierPing(Modifier):
    """PING-PONG. Useful for cluster health check."""

    SUB_PING = 0
    """Request."""

    SUB_PONG = 1
    """Response."""

    subcodes = [SUB_PING, SUB_PONG]
    code = 100


class ModifierEcho(Modifier):
    """ECHO service."""
    subcodes = [0]
    code = 101


class ModifierLegionMsg(Modifier):
    """Legion msg (UDP, the body is encrypted)."""
    subcodes = RANGE_255
    code = 109


class ModifierSignal(Modifier):
    """uwsgi_signal framework (payload is optional).

    .. note:: ``modifier2`` is the signal num.

    """
    subcodes = RANGE_255
    code = 110


class ModifierCache(Modifier):
    """Cache operations."""

    SUB_GET = 0
    """Simple cache get for values not bigger than 64k."""

    SUB_SET = 1
    """Simple cache set for values not bigger than 64k."""

    SUB_DELETE = 2
    """Simple cache del."""

    SUB_DICT_BASED = 3
    """Simple dict based get command."""

    SUB_STREAM = 5
    """Get and stream."""

    SUB_DUMP = 6
    """Dump the whole cache."""

    SUB_MAGIC = 17
    """Magic interface for plugins remote access."""

    subcodes = [
        SUB_GET,
        SUB_SET,
        SUB_DELETE,
        SUB_DICT_BASED,
        SUB_STREAM,
        SUB_DUMP,
        SUB_MAGIC,
    ]
    code = 111


class ModifierCorerouterSignal(Modifier):
    """Special modifier for signaling corerouters about special conditions."""
    code = 123


class ModifierRpc(Modifier):
    """RPC. The packet is an uwsgi array where

        * the first item - the name of the function
        * the following - the args

    """
    SUB_DEFAULT = 0
    """Return uwsgi header + rpc response."""

    SUB_RAW = 1
    """Return raw rpc response, uwsgi header included, if available."""

    SUB_USE_PATH_INFO = 2
    """Split PATH_INFO to get func name and args and return as HTTP response 
    with content_type as application/binary or Accept request header (if different from *).
    
    """

    SUB_XMLRPC = 3
    """Set xmlrpc wrapper (requires libxml2)."""

    SUB_JSONRPC = 4
    """Set jsonrpc wrapper (requires libjansson)."""

    SUB_DICT = 5
    """Used in uwsgi response to signal the response is a uwsgi dictionary 
    followed by the body (the dictionary must contains a CONTENT_LENGTH key).
    
    """

    subcodes = [
        SUB_DEFAULT,
        SUB_RAW,
        SUB_USE_PATH_INFO,
        SUB_XMLRPC,
        SUB_JSONRPC,
        SUB_DICT,
    ]
    code = 173


class ModifierPersistentClose(Modifier):
    """Close mark for persistent connections."""
    subcodes = [0]
    code = 200


class ModifierSubscription(Modifier):
    """Subscription packet. See ``subscriptions``."""
    subcodes = [0]
    code = 224


class ModifierExample(Modifier):
    """Modifier used in dummy example plugin."""
    code = 250


class ModifierResponse(Modifier):
    """Generic response. Request dependent.

    Example: a spooler response set 0 for a failed spool or 1 for a successful one.

    """
    subcodes = RANGE_255
    code = 255
