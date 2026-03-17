##############################################################################
#
# Copyright (c) 2013 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Command line runner.
"""

from __future__ import print_function, unicode_literals

import getopt
import os
import os.path
import re
import sys

from waitress import serve
from waitress.adjustments import Adjustments

HELP = """\
Usage:

    {0} [OPTS] MODULE:OBJECT

Standard options:

    --help
        Show this information.

    --call
        Call the given object to get the WSGI application.

    --host=ADDR
        Hostname or IP address on which to listen, default is '0.0.0.0',
        which means "all IP addresses on this host".

        Note: May not be used together with --listen

    --port=PORT
        TCP port on which to listen, default is '8080'

        Note: May not be used together with --listen

    --listen=ip:port
        Tell waitress to listen on an ip port combination.

        Example:

            --listen=127.0.0.1:8080
            --listen=[::1]:8080
            --listen=*:8080

        This option may be used multiple times to listen on multiple sockets.
        A wildcard for the hostname is also supported and will bind to both
        IPv4/IPv6 depending on whether they are enabled or disabled.

    --[no-]ipv4
        Toggle on/off IPv4 support.

        Example:

            --no-ipv4

        This will disable IPv4 socket support. This affects wildcard matching
        when generating the list of sockets.

    --[no-]ipv6
        Toggle on/off IPv6 support.

        Example:

            --no-ipv6

        This will turn on IPv6 socket support. This affects wildcard matching
        when generating a list of sockets.

    --unix-socket=PATH
        Path of Unix socket. If a socket path is specified, a Unix domain
        socket is made instead of the usual inet domain socket.

        Not available on Windows.

    --unix-socket-perms=PERMS
        Octal permissions to use for the Unix domain socket, default is
        '600'.

    --url-scheme=STR
        Default wsgi.url_scheme value, default is 'http'.

   --url-prefix=STR
        The ``SCRIPT_NAME`` WSGI environment value.  Setting this to anything
        except the empty string will cause the WSGI ``SCRIPT_NAME`` value to be
        the value passed minus any trailing slashes you add, and it will cause
        the ``PATH_INFO`` of any request which is prefixed with this value to
        be stripped of the prefix.  Default is the empty string.

    --ident=STR
        Server identity used in the 'Server' header in responses. Default
        is 'waitress'.

Tuning options:

    --threads=INT
        Number of threads used to process application logic, default is 4.

    --backlog=INT
        Connection backlog for the server. Default is 1024.

    --recv-bytes=INT
        Number of bytes to request when calling socket.recv(). Default is
        8192.

    --send-bytes=INT
        Number of bytes to send to socket.send(). Default is 18000.
        Multiples of 9000 should avoid partly-filled TCP packets.

    --outbuf-overflow=INT
        A temporary file should be created if the pending output is larger
        than this. Default is 1048576 (1MB).

    --outbuf-high-watermark=INT
        The app_iter will pause when pending output is larger than this value
        and will resume once enough data is written to the socket to fall below
        this threshold. Default is 16777216 (16MB).

    --inbuf-overflow=INT
        A temporary file should be created if the pending input is larger
        than this. Default is 524288 (512KB).

    --connection-limit=INT
        Stop creating new channels if too many are already active.
        Default is 100.

    --cleanup-interval=INT
        Minimum seconds between cleaning up inactive channels. Default
        is 30. See '--channel-timeout'.

    --channel-timeout=INT
        Maximum number of seconds to leave inactive connections open.
        Default is 120. 'Inactive' is defined as 'has received no data
        from the client and has sent no data to the client'.

    --[no-]log-socket-errors
        Toggle whether premature client disconnect tracebacks ought to be
        logged. On by default.

    --max-request-header-size=INT
        Maximum size of all request headers combined. Default is 262144
        (256KB).

    --max-request-body-size=INT
        Maximum size of request body. Default is 1073741824 (1GB).

    --[no-]expose-tracebacks
        Toggle whether to expose tracebacks of unhandled exceptions to the
        client. Off by default.

    --asyncore-loop-timeout=INT
        The timeout value in seconds passed to asyncore.loop(). Default is 1.

    --asyncore-use-poll
        The use_poll argument passed to ``asyncore.loop()``. Helps overcome
        open file descriptors limit. Default is False.

"""

RUNNER_PATTERN = re.compile(
    r"""
    ^
    (?P<module>
        [a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*
    )
    :
    (?P<object>
        [a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*
    )
    $
    """,
    re.I | re.X,
)


def match(obj_name):
    matches = RUNNER_PATTERN.match(obj_name)
    if not matches:
        raise ValueError("Malformed application '{0}'".format(obj_name))
    return matches.group("module"), matches.group("object")


def resolve(module_name, object_name):
    """Resolve a named object in a module."""
    # We cast each segments due to an issue that has been found to manifest
    # in Python 2.6.6, but not 2.6.8, and may affect other revisions of Python
    # 2.6 and 2.7, whereby ``__import__`` chokes if the list passed in the
    # ``fromlist`` argument are unicode strings rather than 8-bit strings.
    # The error triggered is "TypeError: Item in ``fromlist '' not a string".
    # My guess is that this was fixed by checking against ``basestring``
    # rather than ``str`` sometime between the release of 2.6.6 and 2.6.8,
    # but I've yet to go over the commits. I know, however, that the NEWS
    # file makes no mention of such a change to the behaviour of
    # ``__import__``.
    segments = [str(segment) for segment in object_name.split(".")]
    obj = __import__(module_name, fromlist=segments[:1])
    for segment in segments:
        obj = getattr(obj, segment)
    return obj


def show_help(stream, name, error=None):  # pragma: no cover
    if error is not None:
        print("Error: {0}\n".format(error), file=stream)
    print(HELP.format(name), file=stream)


def show_exception(stream):
    exc_type, exc_value = sys.exc_info()[:2]
    args = getattr(exc_value, "args", None)
    print(
        ("There was an exception ({0}) importing your module.\n").format(
            exc_type.__name__,
        ),
        file=stream,
    )
    if args:
        print("It had these arguments: ", file=stream)
        for idx, arg in enumerate(args, start=1):
            print("{0}. {1}\n".format(idx, arg), file=stream)
    else:
        print("It had no arguments.", file=stream)


def run(argv=sys.argv, _serve=serve):
    """Command line runner."""
    name = os.path.basename(argv[0])

    try:
        kw, args = Adjustments.parse_args(argv[1:])
    except getopt.GetoptError as exc:
        show_help(sys.stderr, name, str(exc))
        return 1

    if kw["help"]:
        show_help(sys.stdout, name)
        return 0

    if len(args) != 1:
        show_help(sys.stderr, name, "Specify one application only")
        return 1

    try:
        module, obj_name = match(args[0])
    except ValueError as exc:
        show_help(sys.stderr, name, str(exc))
        show_exception(sys.stderr)
        return 1

    # Add the current directory onto sys.path
    sys.path.append(os.getcwd())

    # Get the WSGI function.
    try:
        app = resolve(module, obj_name)
    except ImportError:
        show_help(sys.stderr, name, "Bad module '{0}'".format(module))
        show_exception(sys.stderr)
        return 1
    except AttributeError:
        show_help(sys.stderr, name, "Bad object name '{0}'".format(obj_name))
        show_exception(sys.stderr)
        return 1
    if kw["call"]:
        app = app()

    # These arguments are specific to the runner, not waitress itself.
    del kw["call"], kw["help"]

    _serve(app, **kw)
    return 0
