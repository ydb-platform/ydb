from ..base import OptionsGroup, ParametrizedValue
from ..utils import KeyValue, filter_locals
from .routing_modifiers import Modifier, ModifierWsgi
from .networking_sockets import SocketShared


class RouterBase(OptionsGroup):

    alias = None
    on_command = None

    def _set_aliased(self, *args, **kwargs):
        args = list(args)
        args[0] = f'{self.alias}-{args[0]}'
        self._set(*args, **kwargs)

    def __init__(self, on=None):
        """
        :param SocketShared|str on: Activates the router on the given address.
        """
        self._make_section_like()

        command = self.on_command or self.alias

        self._set(command, on)

        super().__init__()

    def _contribute_to_opts(self, target):
        target_section = target._section
        networking = target_section.networking

        def handle_shared(value):
            # Handle shared sockets as addresses.
            networking.register_socket(value)
            return networking._get_shared_socket_idx(value)

        for key, value in self._section._opts.items():

            if isinstance(value, KeyValue):
                on = value.locals_dict.get('on')

                if isinstance(on, SocketShared):
                    # Replace address with shared socket index.
                    value.locals_dict['on'] = handle_shared(on)

            else:
                if isinstance(value, SocketShared):
                    value = handle_shared(value)

            target_section._opts[key] = value


class _RouterCommon(RouterBase):

    def set_basic_params(
            self, *, workers=None, zerg_server=None, fallback_node=None, concurrent_events=None,
            cheap_mode=None, stats_server=None):
        """

        :param int workers: Number of worker processes to spawn.

        :param str zerg_server: Attach the router to a zerg server.

        :param str fallback_node: Fallback to the specified node in case of error.

        :param int concurrent_events: Set the maximum number of concurrent events router can manage.

            Default: system dependent.

        :param bool cheap_mode: Enables cheap mode. When the router is in cheap mode,
            it will not respond to requests until a node is available.
            This means that when there are no nodes subscribed, only your local app (if any) will respond.
            When all of the nodes go down, the router will return in cheap mode.

        :param str stats_server: Router stats server address to run at.

        """
        self._set_aliased('workers', workers)
        self._set_aliased('zerg', zerg_server)
        self._set_aliased('fallback', fallback_node)
        self._set_aliased('events', concurrent_events)
        self._set_aliased('cheap', cheap_mode, cast=bool)
        self._set_aliased('stats', stats_server)

        return self

    def set_connections_params(self, *, harakiri=None, timeout_socket=None, retry_delay=None):
        """Sets connection-related parameters.

        :param int harakiri: Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds). Default: 60.

        :param int retry_delay: Retry connections to dead static nodes after the specified
            amount of seconds. Default: 30.

        """
        self._set_aliased('harakiri', harakiri)
        self._set_aliased('timeout', timeout_socket)
        self._set_aliased('gracetime', retry_delay)

        return self


class Forwarder(ParametrizedValue):

    opt_key = True


class ForwarderPath(Forwarder):
    """Use the specified base (allows %s pattern) for mapping requests to UNIX sockets.

    Examples:
        * /tmp/sockets/
        * /tmp/sockets/%s/uwsgi.sock

    * http://uwsgi.readthedocs.io/en/latest/Fastrouter.html#way-1-fastrouter-use-base
    * http://uwsgi.readthedocs.io/en/latest/Fastrouter.html#way-2-fastrouter-use-pattern

    """

    name = 'use-base'

    def __init__(self, sockets_dir):
        """
        :param str sockets_dir: UNIX sockets directory.
            Allows %s to denote key (domain).

        """
        if '%s' in sockets_dir:
            self.name = 'use-pattern'

        super().__init__(sockets_dir)


class ForwarderCode(Forwarder):
    """Forwards requests to nodes returned by a function.

    This allows using user defined functions to calculate.
    Function must accept key (domain).

    * http://uwsgi.readthedocs.io/en/latest/Fastrouter.html#way-5-fastrouter-use-code-string

    .. warning:: Remember to not put blocking code in your functions.
        The router is totally non-blocking, do not ruin it!

    """
    name = 'use-code-string'
    args_joiner = ':'

    def __init__(self, script, func, *, modifier=None):
        """
        :param str script: Script (module for Python) name to get function from.

        :param str func:  Function name.

        :param Modifier modifier: Routing modifier.

        """
        modifier = modifier or ModifierWsgi
        super().__init__(modifier.code, script, func)


class ForwarderCache(Forwarder):
    """Uses uWSGI cache to get target nodes from.

    * http://uwsgi.readthedocs.io/en/latest/Fastrouter.htmlway-3-fastrouter-use-cache

    """
    name = 'use-cache'

    def __init__(self, cache_name=None):
        """
        :param str cache_name: Cache name to use.
        """
        super().__init__(cache_name)


class ForwarderSocket(Forwarder):
    """Forwards request to the specified uwsgi socket."""

    name = 'use-socket'

    def __init__(self, socket):
        """
        :param str socket: Socket filepath.
        """
        super().__init__(socket)


class ForwarderSubscriptionServer(Forwarder):
    """Forwards requests to nodes returned by the subscription server.

    Subscriptions are simple UDP packets that instruct the router which domain maps to which instance or instances.

    To subscribe to such a subscription server use `.subscriptions.subscribe()`.

    * http://uwsgi.readthedocs.io/en/latest/Fastrouter.html#way-4-fastrouter-subscription-server

    """
    name = 'subscription-server'

    def __init__(self, address):
        """
        :param str address: Address (including port) to run the subscription server on.
        """
        super().__init__(address)


class _RouterWithForwarders(_RouterCommon):

    class forwarders:
        """Forwarders are kind of registries containing information
        on where to forward requests.

        """
        cache = ForwarderCache
        code = ForwarderCode
        path = ForwarderPath
        socket = ForwarderSocket
        subscription_server = ForwarderSubscriptionServer

    def __init__(self, on=None, *, forward_to=None):
        """Activates the router on the given address.

        :param SocketShared|str on: Activates the router on the given address.

        :param Forwarder|str|list[str] forward_to: Where to forward requests.
            Expects a forwarder instance or one or more node names.

        """
        super().__init__(on)

        if forward_to is not None:
            if isinstance(forward_to, Forwarder):

                value = f'{forward_to}'
                self._set_aliased(forward_to.name, value, multi=True)

            else:
                self._set_aliased('to', forward_to, multi=True)

    def set_basic_params(
            self, *, workers=None, zerg_server=None, fallback_node=None, concurrent_events=None,
            cheap_mode=None, stats_server=None, quiet=None, buffer_size=None):
        """

        :param int workers: Number of worker processes to spawn.

        :param str zerg_server: Attach the router to a zerg server.

        :param str fallback_node: Fallback to the specified node in case of error.

        :param int concurrent_events: Set the maximum number of concurrent events router can manage.

            Default: system dependent.

        :param bool cheap_mode: Enables cheap mode. When the router is in cheap mode,
            it will not respond to requests until a node is available.
            This means that when there are no nodes subscribed, only your local app (if any) will respond.
            When all of the nodes go down, the router will return in cheap mode.

        :param str stats_server: Router stats server address to run at.

        :param bool quiet: Do not report failed connections to instances.

        :param int buffer_size: Set the internal buffer max size - size of a request (request-body excluded),
            this generally maps to the size of request headers.  Default: 4096 bytes (4k) / page size.

            The amount of variables you can add per-request is limited by the uwsgi packet buffer ().
            You can increase it up to 65535 (64k) with this option.

            .. note: If you receive a bigger request (for example with big cookies or query string)
                so that "invalid request block size" is logged in your logs you may need to increase it.
                It is a security measure too, so adapt to your app needs instead of maxing it out.

        :param forward_to: Forward request to the given node.

        """
        super().set_basic_params(**filter_locals(locals(), drop=[
            'quiet',
            'buffer_size',
        ]))

        self._set_aliased('quiet', quiet, cast=bool)
        self._set_aliased('buffer-size', buffer_size)

        return self


class RouterHttp(_RouterWithForwarders):
    """uWSGI includes an HTTP router/proxy/load-balancer that can forward requests to uWSGI workers.

    The server can be used in two ways:

        * embedded - automatically spawn workers and setup the communication socket
        * standalone - you have to specify the address of a uwsgi socket to connect to

            See `subscribe_to` argument to `.set_basic_params()`

    .. note:: If you want to go massive (virtualhosting and zero-conf scaling) combine the HTTP router
        with the uWSGI Subscription Server.

    """
    alias = 'http'
    plugin = alias

    # todo consider adding
    # http-to-https
    # http-var
    # http-modifier1 / 2
    # http-raw-body - to support chunked input.
    # http-stud-prefix
    # http-server-name-as-http-host
    # http-backend-http
    # http-manage-expect

    def set_basic_params(
            self, *, workers=None, zerg_server=None, fallback_node=None, concurrent_events=None,
            cheap_mode=None, stats_server=None, quiet=None, buffer_size=None,
            keepalive=None, resubscribe_addresses=None):
        """
        :param int workers: Number of worker processes to spawn.

        :param str zerg_server: Attach the router to a zerg server.

        :param str fallback_node: Fallback to the specified node in case of error.

        :param int concurrent_events: Set the maximum number of concurrent events router can manage.

            Default: system dependent.

        :param bool cheap_mode: Enables cheap mode. When the router is in cheap mode,
            it will not respond to requests until a node is available.
            This means that when there are no nodes subscribed, only your local app (if any) will respond.
            When all of the nodes go down, the router will return in cheap mode.

        :param str stats_server: Router stats server address to run at.

        :param bool quiet: Do not report failed connections to instances.

        :param int buffer_size: Set internal buffer size in bytes. Default: page size.

        :param int keepalive: Allows holding the connection open even if the request has a body.

            * http://uwsgi.readthedocs.io/en/latest/HTTP.html#http-keep-alive

            .. note:: See http11 socket type for an alternative.

        :param str|list[str] resubscribe_addresses: Forward subscriptions
            to the specified subscription server.


        """
        super().set_basic_params(**filter_locals(locals(), drop=[
            'keepalive',
            'resubscribe_addresses',
        ]))

        self._set_aliased('keepalive', keepalive)
        self._set_aliased('resubscribe', resubscribe_addresses, multi=True)

        return self

    def set_connections_params(
            self, *, harakiri=None, timeout_socket=None, retry_delay=None, timeout_headers=None, timeout_backend=None):
        """Sets connection-related parameters.

        :param int harakiri: Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds).
            Used to set the SPDY timeout. This is the maximum amount of inactivity after
            the SPDY connection is closed.

            Default: 60.

        :param int retry_delay: Retry connections to dead static nodes after the specified
            amount of seconds. Default: 30.

        :param int timeout_headers: Defines the timeout (seconds) while waiting for http headers.

            Default: `socket_timeout`.

        :param int timeout_backend: Defines the timeout (seconds) when connecting to backend instances.

            Default: `socket_timeout`.

        """

        super().set_connections_params(
            **filter_locals(locals(), drop=['timeout_headers', 'timeout_backend']))

        self._set_aliased('headers-timeout', timeout_headers)
        self._set_aliased('connect-timeout', timeout_backend)

        return self

    def set_manage_params(
            self, *, chunked_input=None, chunked_output=None, gzip=None, websockets=None, source_method=None,
            rtsp=None, proxy_protocol=None):
        """Allows enabling various automatic management mechanics.

        * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.html#http-router-keepalive-auto-chunking-auto-gzip-and-transparent-websockets

        :param bool chunked_input: Automatically detect chunked input requests and put the session in raw mode.

        :param bool chunked_output: Automatically transform output to chunked encoding
            during HTTP 1.1 keepalive (if needed).

        :param bool gzip: Automatically gzip content if uWSGI-Encoding header is set to gzip,
            but content size (Content-Length/Transfer-Encoding) and Content-Encoding are not specified.

        :param bool websockets: Automatically detect websockets connections and put the session in raw mode.

        :param bool source_method: Automatically put the session in raw mode for `SOURCE` HTTP method.

            * http://uwsgi.readthedocs.io/en/latest/Changelog-2.0.5.html#icecast2-protocol-helpers

        :param bool rtsp: Allow the HTTP router to detect RTSP and chunked requests automatically.

        :param bool proxy_protocol: Allows the HTTP router to manage PROXY1 protocol requests,
            such as those made by Haproxy or Amazon Elastic Load Balancer (ELB).

        """
        self._set_aliased('chunked-input', chunked_input, cast=bool)
        self._set_aliased('auto-chunked', chunked_output, cast=bool)
        self._set_aliased('auto-gzip', gzip, cast=bool)
        self._set_aliased('websockets', websockets, cast=bool)
        self._set_aliased('manage-source', source_method, cast=bool)
        self._set_aliased('manage-rtsp', rtsp, cast=bool)
        self._set_aliased('enable-proxy-protocol', proxy_protocol, cast=bool)

        return self

    def set_owner_params(self, uid=None, gid=None):
        """Drop http router privileges to specified user and group.

        :param str|int uid: Set uid to the specified username or uid.

        :param str|int gid: Set gid to the specified groupname or gid.

        """
        self._set_aliased('uid', uid)
        self._set_aliased('gid', gid)

        return self


class RouterHttps(RouterHttp):
    """uWSGI includes an HTTPS router/proxy/load-balancer that can forward requests to uWSGI workers.

    The server can be used in two ways:

        * embedded - automatically spawn workers and setup the communication socket
        * standalone - you have to specify the address of a uwsgi socket to connect to

            See `subscribe_to` argument to `.set_basic_params()`

    .. note:: If you want to go massive (virtualhosting and zero-conf scaling) combine the HTTP router
        with the uWSGI Subscription Server.

    """
    alias = 'http'  # Shares options with http.
    plugin = alias
    on_command = 'https2'

    def __init__(
            self, on, *, cert, key, ciphers=None, client_ca=None, session_context=None, use_spdy=None,
            export_cert_var=None):
        """Binds https router to run on the given address.

        :param SocketShared|str on: Activates the router on the given address.

        :param str cert: Certificate file.

        :param str key: Private key file.

        :param str ciphers: Ciphers [alias] string.

            Example:
                * DEFAULT
                * HIGH
                * DHE, EDH

            * https://www.openssl.org/docs/man1.1.0/apps/ciphers.html

        :param str client_ca: Client CA file for client-based auth.

            .. note: You can prepend ! (exclamation mark) to make client certificate
                authentication mandatory.

        :param str session_context: Session context identifying string. Can be set to static shared value
            to avoid session rejection.

            Default: a value built from the HTTP server address.

            * http://uwsgi.readthedocs.io/en/latest/SSLScaling.html#setup-2-synchronize-caches-of-different-https-routers

        :param bool use_spdy: Use SPDY.

        :param bool export_cert_var: Export uwsgi variable `HTTPS_CC` containing the raw client certificate.

        """
        on = KeyValue(
            filter_locals(locals(), drop=['session_context']),
            aliases={'on': 'addr', 'use_spdy': 'spdy'},
            bool_keys=['use_spdy'],
        )

        super().__init__(on)

        self._set_aliased('session-context', session_context)


class RouterSsl(_RouterWithForwarders):
    """Works in the same way as the RouterRaw, but will terminate ssl connections.

    Supports SNI for implementing virtual hosting.

    """
    alias = 'sslrouter'
    plugin = alias
    on_command = 'sslrouter2'

    def __init__(self, on, cert, key, forward_to=None, ciphers=None, client_ca=None, session_context=None, use_sni=None):
        """Activates the router on the given address.

        :param SocketShared|str on: Activates the router on the given address.

        :param str cert: Certificate file.

        :param str key: Private key file.

        :param Forwarder|str|list[str] forward_to: Where to forward requests.
            Expects a forwarder instance or one or more node names.

        :param str ciphers: Ciphers [alias] string.

            Example:
                * DEFAULT
                * HIGH
                * DHE, EDH

            * https://www.openssl.org/docs/man1.1.0/apps/ciphers.html

        :param str client_ca: Client CA file for client-based auth.

        :param str session_context: Session context identifying string. Can be set to static shared value
            to avoid session rejection.

            Default: a value built from the HTTP server address.

            * http://uwsgi.readthedocs.io/en/latest/SSLScaling.html#setup-2-synchronize-caches-of-different-https-routers

        :param bool use_sni: Use SNI to route requests.

        """
        on = KeyValue(
            filter_locals(locals(), drop=['session_context', 'use_sni']),
            aliases={'on': 'addr'},
        )

        self._set_aliased('session-context', session_context)
        self._set_aliased('sni', use_sni, cast=bool)

        super().__init__(on, forward_to=forward_to)

    def set_connections_params(self, harakiri=None, timeout_socket=None, retry_delay=None, retry_max=None):
        """Sets connection-related parameters.

        :param int harakiri: Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds). Default: 60.

        :param int retry_delay: Retry connections to dead static nodes after the specified
            amount of seconds. Default: 30.

        :param int retry_max: Maximum number of retries/fallbacks to other nodes. Default: 3.

        """
        super().set_connections_params(**filter_locals(locals(), drop=['retry_max']))

        self._set_aliased('max-retries', retry_max)

        return self


class RouterFast(_RouterWithForwarders):
    """A proxy/load-balancer/router speaking the uwsgi protocol.

    You can put it between your webserver and real uWSGI instances to have more control
    over the routing of HTTP requests to your application servers.

    """
    alias = 'fastrouter'
    plugin = alias

    def set_basic_params(
            self, *, workers=None, zerg_server=None, fallback_node=None, concurrent_events=None,
            cheap_mode=None, stats_server=None, quiet=None, buffer_size=None,
            fallback_nokey=None, subscription_key=None, emperor_command_socket=None):
        """
        :param int workers: Number of worker processes to spawn.

        :param str zerg_server: Attach the router to a zerg server.

        :param str fallback_node: Fallback to the specified node in case of error.

        :param int concurrent_events: Set the maximum number of concurrent events router can manage.

            Default: system dependent.

        :param bool cheap_mode: Enables cheap mode. When the router is in cheap mode,
            it will not respond to requests until a node is available.
            This means that when there are no nodes subscribed, only your local app (if any) will respond.
            When all of the nodes go down, the router will return in cheap mode.

        :param str stats_server: Router stats server address to run at.

        :param bool quiet: Do not report failed connections to instances.

        :param int buffer_size: Set internal buffer size in bytes. Default: page size.

        :param bool fallback_nokey: Move to fallback node even if a subscription key is not found.

        :param str subscription_key: Skip uwsgi parsing and directly set a key.

        :param str emperor_command_socket: Set the emperor command socket that will receive spawn commands.

            See `.empire.set_emperor_command_params()`.

        """
        super().set_basic_params(**filter_locals(locals(), drop=[
            'fallback_nokey',
            'subscription_key',
            'emperor_command_socket',
        ]))

        self._set_aliased('fallback-on-no-key', fallback_nokey, cast=bool)
        self._set_aliased('force-key', subscription_key)
        self._set_aliased('emperor-socket', emperor_command_socket)

        return self

    def set_resubscription_params(self, addresses=None, bind_to=None):
        """You can specify a dgram address (udp or unix) on which all of the subscriptions
        request will be forwarded to (obviously changing the node address to the router one).

        The system could be useful to build 'federated' setup.

        * http://uwsgi.readthedocs.io/en/latest/Changelog-2.0.1.html#resubscriptions

        :param str|list[str] addresses: Forward subscriptions to the specified subscription server.

        :param str|list[str] bind_to: Bind to the specified address when re-subscribing.

        """
        self._set_aliased('resubscribe', addresses, multi=True)
        self._set_aliased('resubscribe-bind', bind_to)

        return self

    def set_connections_params(self, harakiri=None, timeout_socket=None, retry_delay=None, retry_max=None, defer=None):
        """Sets connection-related parameters.

        :param int harakiri:  Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds). Default: 60.

        :param int retry_delay: Retry connections to dead static nodes after the specified
            amount of seconds. Default: 30.

        :param int retry_max: Maximum number of retries/fallbacks to other nodes. Default: 3

        :param int defer: Defer connection delay, seconds. Default: 5.

        """
        super().set_connections_params(**filter_locals(locals(), drop=['retry_max', 'defer']))

        self._set_aliased('max-retries', retry_max)
        self._set_aliased('defer-connect-timeout', defer)

        return self

    def set_postbuffering_params(self, size=None, store_dir=None):
        """Sets buffering params.

        Web-proxies like nginx are "buffered", so they wait til the whole request (and its body)
        has been read, and then it sends it to the backends.

        :param int size: The size (in bytes) of the request body after which the body will
            be stored to disk (as a temporary file) instead of memory.

        :param str store_dir: Put buffered files to the specified directory. Default: TMPDIR, /tmp/

        """
        self._set_aliased('post-buffering', size)
        self._set_aliased('post-buffering-dir', store_dir)

        return self

    def set_owner_params(self, uid=None, gid=None):
        """Drop http router privileges to specified user and group.

        :param str|int uid: Set uid to the specified username or uid.

        :param str|int gid: Set gid to the specified groupname or gid.

        """
        self._set_aliased('uid', uid)
        self._set_aliased('gid', gid)

        return self


class RouterRaw(_RouterWithForwarders):
    """A pure-TCP load balancer.

    Can be used to load balance between the various HTTPS routers.

    """
    alias = 'rawrouter'
    plugin = alias

    def set_connections_params(
            self, harakiri=None, timeout_socket=None, retry_delay=None, retry_max=None, use_xclient=None):
        """Sets connection-related parameters.

        :param int harakiri:  Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds). Default: 60.

        :param int retry_delay: Retry connections to dead static nodes after the specified
            amount of seconds. Default: 30.

        :param int retry_max: Maximum number of retries/fallbacks to other nodes. Default: 3.

        :param bool use_xclient: Use the xclient protocol to pass the client address.

        """
        super().set_connections_params(**filter_locals(locals(), drop=['retry_max', 'use_xclient']))

        self._set_aliased('max-retries', retry_max)
        self._set_aliased('xclient', use_xclient)

        return self


class RouterForkPty(_RouterCommon):
    """Allows allocation of pseudoterminals in jails.

    Dealing with containers is now a common deployment pattern.
    One of the most annoying tasks when dealing with jails/namespaces is 'attaching' to already running instances.
    The forkpty router aims at simplifyng the process giving a pseudoterminal server to your uWSGI instances.
    A client connect to the socket exposed by the forkpty router and get a new pseudoterminal
    connected to a process (generally a shell, but can be whatever you want).

    .. note:: To be used in cooperation with `pty` plugin.

    """
    alias = 'forkptyrouter'
    plugin = alias

    def __init__(self, on=None, undeferred=False):
        """Binds router to run on the given address.

        :param SocketShared|str on: Activates the router on the given address.

        :param bool undeferred: Run router in undeferred mode.

        """
        router_name = self.alias

        if undeferred:
            router_name = 'forkptyurouter'

        self.on_command = router_name

        super().__init__(on)

    def set_basic_params(
            self, *, workers=None, zerg_server=None, fallback_node=None, concurrent_events=None,
            cheap_mode=None, stats_server=None, run_command=None):
        """

        :param int workers: Number of worker processes to spawn.

        :param str zerg_server: Attach the router to a zerg server.

        :param str fallback_node: Fallback to the specified node in case of error.

        :param int concurrent_events: Set the maximum number of concurrent events router can manage.

            Default: system dependent.

        :param bool cheap_mode: Enables cheap mode. When the router is in cheap mode,
            it will not respond to requests until a node is available.
            This means that when there are no nodes subscribed, only your local app (if any) will respond.
            When all of the nodes go down, the router will return in cheap mode.

        :param str stats_server: Router stats server address to run at.

        :param str run_command: Run the specified command
            on every connection. Default: /bin/sh.

        """
        super().set_basic_params(**filter_locals(locals(), drop=['run_command']))

        self._set_aliased('command', run_command)

        return self

    def set_connections_params(self, *, harakiri=None, timeout_socket=None):
        """Sets connection-related parameters.

        :param int harakiri: Set gateway harakiri timeout (seconds).

        :param int timeout_socket: Node socket timeout (seconds). Default: 60.

        """
        self._set_aliased('harakiri', harakiri)
        self._set_aliased('timeout', timeout_socket)

        return self

    def set_window_params(self, cols=None, rows=None):
        """Sets pty window params.

        :param int cols:
        :param int rows:

        """
        self._set_aliased('cols', cols)
        self._set_aliased('rows', rows)

        return self


class RouterTunTap(RouterBase):
    """The tuntap router is a non-blocking highly optimized ip router
    translating from tuntap device to socket streams.

    Allows full user-space networking in jails.

    It is meant as a replacement for the currently available networking namespaces approaches.
    Compared to `veth` or `macvlan` it is really simple and allows total control over the routing subsystem
    (in addition to a simple customizable firewalling engine).

    Generally you spawn the tuntap router in the Emperor instance.
    Vassals will run in new namespaces in which they create a tuntap device attached to the tuntap router.
    UNIX sockets are the only way to connect to the tuntap router after jailing.

    Vassals should connect to tuntap device.

    """
    alias = 'tuntap'
    plugin = alias

    def __init__(self, on=None, *, device=None, stats_server=None, gateway=None):
        """Passing params will create a router device.

        :param str on: Socket file.

        :param str device: Device name.

        :param str stats_server: Router stats server address to run at.

        :param str gateway: Gateway address.

        """
        super().__init__()

        if on is not None:
            value = [device or 'uwsgidev', on]

            if stats_server:
                value.append(stats_server)

                if gateway:
                    value.append(gateway)

            self._set_aliased('router', ' '.join(value), multi=True)

    def set_basic_params(self, *, use_credentials=None, stats_server=None):
        """
        :param str use_credentials: Enable check of SCM_CREDENTIALS for tuntap client/server.

        :param str stats_server: Router stats server address to run at.

        """
        self._set_aliased('use-credentials', use_credentials)
        self._set_aliased('router-stats', stats_server)

        return self

    def register_route(self, src, dst, *, gateway):
        """Adds a routing rule to the tuntap router.

        :param str src: Source/mask.

        :param str dst: Destination/mask.

        :param str gateway: Gateway address.

        """
        self._set_aliased('router-route', ' '.join((src, dst, gateway)), multi=True)

        return self

    def device_connect(self, socket, *, device_name):
        """Add a tuntap device to the instance.

        To be used in a vassal.

        :param str socket: Router socket.

            Example: `/run/tuntap_router.socket`.

        :param str device_name: Device.

            Example: `uwsgi0`.

        """
        self._set_aliased('device', f'{device_name} {socket}')

        return self

    def device_add_rule(self, *, direction, action, src, dst, target=None):
        """Adds a tuntap device rule.

        To be used in a vassal.

        :param str direction: Direction:

            * in
            * out.

        :param str action: Action:

            * allow
            * deny
            * route
            * gateway.

        :param str src: Source/mask.

        :param str dst: Destination/mask.

        :param str target: Depends on action.

            * Route / Gateway: Accept addr:port

        """
        value = [direction, src, dst, action]

        if target:
            value.append(target)

        self._set_aliased('device-rule', ' '.join(value), multi=True)

        return self

    def add_firewall_rule(self, *, direction, action, src=None, dst=None):
        """Adds a firewall rule to the router.

        The TunTap router includes a very simple firewall for governing vassal's traffic.
        The first matching rule stops the chain, if no rule applies, the policy is "allow".

        :param str direction: Direction:

            * in
            * out

        :param str action: Action:

            * allow
            * deny

        :param str src: Source/mask.

        :param str dst: Destination/mask

        """
        value = [action]

        if src:
            value.extend((src, dst))

        self._set_aliased(f'router-firewall-{direction.lower()}', ' '.join(value), multi=True)

        return self
