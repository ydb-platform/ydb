# Copyright (c) 2015, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

import dataclasses
import functools
import imaplib
import itertools
import re
import select
import socket
import ssl as ssl_lib
import sys
import warnings
from datetime import date, datetime
from logging import getLogger, LoggerAdapter
from operator import itemgetter
from typing import List, Optional

from . import exceptions, imap4, response_lexer, tls
from .datetime_util import datetime_to_INTERNALDATE, format_criteria_date
from .imap_utf7 import decode as decode_utf7
from .imap_utf7 import encode as encode_utf7
from .response_parser import parse_fetch_response, parse_message_list, parse_response
from .util import assert_imap_protocol, chunk, to_bytes, to_unicode

if hasattr(select, "poll"):
    POLL_SUPPORT = True
else:
    # Fallback to select() on systems that don't support poll()
    POLL_SUPPORT = False


logger = getLogger(__name__)

__all__ = [
    "IMAPClient",
    "SocketTimeout",
    "DELETED",
    "SEEN",
    "ANSWERED",
    "FLAGGED",
    "DRAFT",
    "RECENT",
]


# We also offer the gmail-specific XLIST command...
if "XLIST" not in imaplib.Commands:
    imaplib.Commands["XLIST"] = ("NONAUTH", "AUTH", "SELECTED")

# ...and IDLE
if "IDLE" not in imaplib.Commands:
    imaplib.Commands["IDLE"] = ("NONAUTH", "AUTH", "SELECTED")

# ..and STARTTLS
if "STARTTLS" not in imaplib.Commands:
    imaplib.Commands["STARTTLS"] = ("NONAUTH",)

# ...and ID. RFC2971 says that this command is valid in all states,
# but not that some servers (*cough* FastMail *cough*) don't seem to
# accept it in state NONAUTH.
if "ID" not in imaplib.Commands:
    imaplib.Commands["ID"] = ("NONAUTH", "AUTH", "SELECTED")

# ... and UNSELECT. RFC3691 does not specify the state but there is no
# reason to use the command without AUTH state and a mailbox selected.
if "UNSELECT" not in imaplib.Commands:
    imaplib.Commands["UNSELECT"] = ("AUTH", "SELECTED")

# .. and ENABLE.
if "ENABLE" not in imaplib.Commands:
    imaplib.Commands["ENABLE"] = ("AUTH",)

# .. and MOVE for RFC6851.
if "MOVE" not in imaplib.Commands:
    imaplib.Commands["MOVE"] = ("AUTH", "SELECTED")

# System flags
DELETED = rb"\Deleted"
SEEN = rb"\Seen"
ANSWERED = rb"\Answered"
FLAGGED = rb"\Flagged"
DRAFT = rb"\Draft"
RECENT = rb"\Recent"  # This flag is read-only

# Special folders, see RFC6154
# \Flagged is omitted because it is the same as the flag defined above
ALL = rb"\All"
ARCHIVE = rb"\Archive"
DRAFTS = rb"\Drafts"
JUNK = rb"\Junk"
SENT = rb"\Sent"
TRASH = rb"\Trash"

# Personal namespaces that are common among providers
# used as a fallback when the server does not support the NAMESPACE capability
_POPULAR_PERSONAL_NAMESPACES = (("", ""), ("INBOX.", "."))

# Names of special folders that are common among providers
_POPULAR_SPECIAL_FOLDERS = {
    SENT: ("Sent", "Sent Items", "Sent items", "Sent Messages"),
    DRAFTS: ("Drafts",),
    ARCHIVE: ("Archive",),
    TRASH: ("Trash", "Deleted Items", "Deleted Messages", "Deleted"),
    JUNK: ("Junk", "Spam"),
}

_RE_SELECT_RESPONSE = re.compile(rb"\[(?P<key>[A-Z-]+)( \((?P<data>.*)\))?\]")


class Namespace(tuple):
    def __new__(cls, personal, other, shared):
        return tuple.__new__(cls, (personal, other, shared))

    personal = property(itemgetter(0))
    other = property(itemgetter(1))
    shared = property(itemgetter(2))


@dataclasses.dataclass
class SocketTimeout:
    """Represents timeout configuration for an IMAP connection.

    :ivar connect: maximum time to wait for a connection attempt to remote server
    :ivar read: maximum time to wait for performing a read/write operation

    As an example, ``SocketTimeout(connect=15, read=60)`` will make the socket
    timeout if the connection takes more than 15 seconds to establish but
    read/write operations can take up to 60 seconds once the connection is done.
    """

    connect: float
    read: float


@dataclasses.dataclass
class MailboxQuotaRoots:
    """Quota roots associated with a mailbox.

    Represents the response of a GETQUOTAROOT command.

    :ivar mailbox: the mailbox
    :ivar quota_roots: list of quota roots associated with the mailbox
    """

    mailbox: str
    quota_roots: List[str]


@dataclasses.dataclass
class Quota:
    """Resource quota.

    Represents the response of a GETQUOTA command.

    :ivar quota_roots: the quota roots for which the limit apply
    :ivar resource: the resource being limited (STORAGE, MESSAGES...)
    :ivar usage: the current usage of the resource
    :ivar limit: the maximum allowed usage of the resource
    """

    quota_root: str
    resource: str
    usage: bytes
    limit: bytes


def require_capability(capability):
    """Decorator raising CapabilityError when a capability is not available."""

    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(client, *args, **kwargs):
            if not client.has_capability(capability):
                raise exceptions.CapabilityError(
                    "Server does not support {} capability".format(capability)
                )
            return func(client, *args, **kwargs)

        return wrapper

    return actual_decorator


class IMAPClient:
    """A connection to the IMAP server specified by *host* is made when
    this class is instantiated.

    *port* defaults to 993, or 143 if *ssl* is ``False``.

    If *use_uid* is ``True`` unique message UIDs be used for all calls
    that accept message ids (defaults to ``True``).

    If *ssl* is ``True`` (the default) a secure connection will be made.
    Otherwise an insecure connection over plain text will be
    established.

    If *ssl* is ``True`` the optional *ssl_context* argument can be
    used to provide an ``ssl.SSLContext`` instance used to
    control SSL/TLS connection parameters. If this is not provided a
    sensible default context will be used.

    If *stream* is ``True`` then *host* is used as the command to run
    to establish a connection to the IMAP server (defaults to
    ``False``). This is useful for exotic connection or authentication
    setups.

    Use *timeout* to specify a timeout for the socket connected to the
    IMAP server. The timeout can be either a float number, or an instance
    of :py:class:`imapclient.SocketTimeout`.

    * If a single float number is passed, the same timeout delay applies
      during the  initial connection to the server and for all future socket
      reads and writes.

    * In case of a ``SocketTimeout``, connection timeout and
      read/write operations can have distinct timeouts.

    * The default is ``None``, where no timeout is used.

    The *normalise_times* attribute specifies whether datetimes
    returned by ``fetch()`` are normalised to the local system time
    and include no timezone information (native), or are datetimes
    that include timezone information (aware). By default
    *normalise_times* is True (times are normalised to the local
    system time). This attribute can be changed between ``fetch()``
    calls if required.

    Can be used as a context manager to automatically close opened connections:

    >>> with IMAPClient(host="imap.foo.org") as client:
    ...     client.login("bar@foo.org", "passwd")

    """

    # Those exceptions are kept for backward-compatibility, since
    # previous versions included these attributes as references to
    # imaplib original exceptions
    Error = exceptions.IMAPClientError
    AbortError = exceptions.IMAPClientAbortError
    ReadOnlyError = exceptions.IMAPClientReadOnlyError

    def __init__(
        self,
        host: str,
        port: int = None,
        use_uid: bool = True,
        ssl: bool = True,
        stream: bool = False,
        ssl_context: Optional[ssl_lib.SSLContext] = None,
        timeout: Optional[float] = None,
    ):
        if stream:
            if port is not None:
                raise ValueError("can't set 'port' when 'stream' True")
            if ssl:
                raise ValueError("can't use 'ssl' when 'stream' is True")
        elif port is None:
            port = ssl and 993 or 143

        if ssl and port == 143:
            logger.warning(
                "Attempting to establish an encrypted connection "
                "to a port (143) often used for unencrypted "
                "connections"
            )

        self.host = host
        self.port = port
        self.ssl = ssl
        self.ssl_context = ssl_context
        self.stream = stream
        self.use_uid = use_uid
        self.folder_encode = True
        self.normalise_times = True

        # If the user gives a single timeout value, assume it is the same for
        # connection and read/write operations
        if not isinstance(timeout, SocketTimeout):
            timeout = SocketTimeout(timeout, timeout)

        self._timeout = timeout
        self._starttls_done = False
        self._cached_capabilities = None
        self._idle_tag = None

        self._imap = self._create_IMAP4()
        logger.debug(
            "Connected to host %s over %s",
            self.host,
            "SSL/TLS" if ssl else "plain text",
        )

        self._set_read_timeout()
        # Small hack to make imaplib log everything to its own logger
        imaplib_logger = IMAPlibLoggerAdapter(getLogger("imapclient.imaplib"), {})
        self._imap.debug = 5
        self._imap._mesg = imaplib_logger.debug

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Logout and closes the connection when exiting the context manager.

        All exceptions during logout and connection shutdown are caught because
        an error here usually means the connection was already closed.
        """
        try:
            self.logout()
        except Exception:
            try:
                self.shutdown()
            except Exception as e:
                logger.info("Could not close the connection cleanly: %s", e)

    def _create_IMAP4(self):
        if self.stream:
            return imaplib.IMAP4_stream(self.host)

        connect_timeout = getattr(self._timeout, "connect", None)

        if self.ssl:
            return tls.IMAP4_TLS(
                self.host,
                self.port,
                self.ssl_context,
                connect_timeout,
            )

        return imap4.IMAP4WithTimeout(self.host, self.port, connect_timeout)

    def _set_read_timeout(self):
        if self._timeout is not None:
            self.socket().settimeout(self._timeout.read)

    @property
    def _sock(self):
        warnings.warn("_sock is deprecated. Use socket().", DeprecationWarning)
        return self.socket()

    def socket(self):
        """Returns socket used to connect to server.

        The socket is provided for polling purposes only.
        It can be used in,
        for example, :py:meth:`selectors.BaseSelector.register`
        and :py:meth:`asyncio.loop.add_reader` to wait for data.

        .. WARNING::
           All other uses of the returned socket are unsupported.
           This includes reading from and writing to the socket,
           as they are likely to break internal bookkeeping of messages.
        """
        # In py2, imaplib has sslobj (for SSL connections), and sock for non-SSL.
        # In the py3 version it's just sock.
        return getattr(self._imap, "sslobj", self._imap.sock)

    @require_capability("STARTTLS")
    def starttls(self, ssl_context=None):
        """Switch to an SSL encrypted connection by sending a STARTTLS command.

        The *ssl_context* argument is optional and should be a
        :py:class:`ssl.SSLContext` object. If no SSL context is given, a SSL
        context with reasonable default settings will be used.

        You can enable checking of the hostname in the certificate presented
        by the server  against the hostname which was used for connecting, by
        setting the *check_hostname* attribute of the SSL context to ``True``.
        The default SSL context has this setting enabled.

        Raises :py:exc:`Error` if the SSL connection could not be established.

        Raises :py:exc:`AbortError` if the server does not support STARTTLS
        or an SSL connection is already established.
        """
        if self.ssl or self._starttls_done:
            raise exceptions.IMAPClientAbortError("TLS session already established")

        typ, data = self._imap._simple_command("STARTTLS")
        self._checkok("starttls", typ, data)

        self._starttls_done = True

        self._imap.sock = tls.wrap_socket(self._imap.sock, ssl_context, self.host)
        self._imap.file = self._imap.sock.makefile("rb")
        return data[0]

    def login(self, username: str, password: str):
        """Login using *username* and *password*, returning the
        server response.
        """
        try:
            rv = self._command_and_check(
                "login",
                to_unicode(username),
                to_unicode(password),
                unpack=True,
            )
        except exceptions.IMAPClientError as e:
            raise exceptions.LoginError(str(e))

        logger.debug("Logged in as %s", username)
        return rv

    def oauth2_login(
        self,
        user: str,
        access_token: str,
        mech: str = "XOAUTH2",
        vendor: Optional[str] = None,
    ):
        """Authenticate using the OAUTH2 or XOAUTH2 methods.

        Gmail and Yahoo both support the 'XOAUTH2' mechanism, but Yahoo requires
        the 'vendor' portion in the payload.
        """
        auth_string = "user=%s\1auth=Bearer %s\1" % (user, access_token)
        if vendor:
            auth_string += "vendor=%s\1" % vendor
        auth_string += "\1"
        try:
            return self._command_and_check("authenticate", mech, lambda x: auth_string)
        except exceptions.IMAPClientError as e:
            raise exceptions.LoginError(str(e))

    def oauthbearer_login(self, identity, access_token):
        """Authenticate using the OAUTHBEARER method.

        This is supported by Gmail and is meant to supersede the non-standard
        'OAUTH2' and 'XOAUTH2' mechanisms.
        """
        # https://tools.ietf.org/html/rfc5801#section-4
        # Technically this is the authorization_identity, but at least for Gmail it's
        # mandatory and practically behaves like the regular username/identity.
        if identity:
            gs2_header = "n,a=%s," % identity.replace("=", "=3D").replace(",", "=2C")
        else:
            gs2_header = "n,,"
        # https://tools.ietf.org/html/rfc6750#section-2.1
        http_authz = "Bearer %s" % access_token
        # https://tools.ietf.org/html/rfc7628#section-3.1
        auth_string = "%s\1auth=%s\1\1" % (gs2_header, http_authz)
        try:
            return self._command_and_check(
                "authenticate", "OAUTHBEARER", lambda x: auth_string
            )
        except exceptions.IMAPClientError as e:
            raise exceptions.LoginError(str(e))

    def plain_login(self, identity, password, authorization_identity=None):
        """Authenticate using the PLAIN method (requires server support)."""
        if not authorization_identity:
            authorization_identity = ""
        auth_string = "%s\0%s\0%s" % (authorization_identity, identity, password)
        try:
            return self._command_and_check(
                "authenticate", "PLAIN", lambda _: auth_string, unpack=True
            )
        except exceptions.IMAPClientError as e:
            raise exceptions.LoginError(str(e))

    def sasl_login(self, mech_name, mech_callable):
        """Authenticate using a provided SASL mechanism (requires server support).

        The *mech_callable* will be called with one parameter (the server
        challenge as bytes) and must return the corresponding client response
        (as bytes, or as string which will be automatically encoded).

        It will be called as many times as the server produces challenges,
        which will depend on the specific SASL mechanism. (If the mechanism is
        defined as "client-first", the server will nevertheless produce a
        zero-length challenge.)

        For example, PLAIN has just one step with empty challenge, so a handler
        might look like this::

            plain_mech = lambda _: "\\0%s\\0%s" % (username, password)

            imap.sasl_login("PLAIN", plain_mech)

        A more complex but still stateless handler might look like this::

            def example_mech(challenge):
                if challenge == b"Username:"
                    return username.encode("utf-8")
                elif challenge == b"Password:"
                    return password.encode("utf-8")
                else:
                    return b""

            imap.sasl_login("EXAMPLE", example_mech)

        A stateful handler might look like this::

            class ScramSha256SaslMechanism():
                def __init__(self, username, password):
                    ...

                def __call__(self, challenge):
                    self.step += 1
                    if self.step == 1:
                        response = ...
                    elif self.step == 2:
                        response = ...
                    return response

            scram_mech = ScramSha256SaslMechanism(username, password)

            imap.sasl_login("SCRAM-SHA-256", scram_mech)
        """
        try:
            return self._command_and_check(
                "authenticate", mech_name, mech_callable, unpack=True
            )
        except exceptions.IMAPClientError as e:
            raise exceptions.LoginError(str(e))

    def logout(self):
        """Logout, returning the server response."""
        typ, data = self._imap.logout()
        self._check_resp("BYE", "logout", typ, data)
        logger.debug("Logged out, connection closed")
        return data[0]

    def shutdown(self) -> None:
        """Close the connection to the IMAP server (without logging out)

        In most cases, :py:meth:`.logout` should be used instead of
        this. The logout method also shutdown down the connection.
        """
        self._imap.shutdown()
        logger.info("Connection closed")

    @require_capability("ENABLE")
    def enable(self, *capabilities):
        """Activate one or more server side capability extensions.

        Most capabilities do not need to be enabled. This is only
        required for extensions which introduce backwards incompatible
        behaviour. Two capabilities which may require enable are
        ``CONDSTORE`` and ``UTF8=ACCEPT``.

        A list of the requested extensions that were successfully
        enabled on the server is returned.

        Once enabled each extension remains active until the IMAP
        connection is closed.

        See :rfc:`5161` for more details.
        """
        if self._imap.state != "AUTH":
            raise exceptions.IllegalStateError(
                "ENABLE command illegal in state %s" % self._imap.state
            )

        resp = self._raw_command_untagged(
            b"ENABLE",
            [to_bytes(c) for c in capabilities],
            uid=False,
            response_name="ENABLED",
            unpack=True,
        )
        if not resp:
            return []
        return resp.split()

    @require_capability("ID")
    def id_(self, parameters=None):
        """Issue the ID command, returning a dict of server implementation
        fields.

        *parameters* should be specified as a dictionary of field/value pairs,
        for example: ``{"name": "IMAPClient", "version": "0.12"}``
        """
        if parameters is None:
            args = "NIL"
        else:
            if not isinstance(parameters, dict):
                raise TypeError("'parameters' should be a dictionary")
            args = seq_to_parenstr(
                _quote(v) for v in itertools.chain.from_iterable(parameters.items())
            )

        typ, data = self._imap._simple_command("ID", args)
        self._checkok("id", typ, data)
        typ, data = self._imap._untagged_response(typ, data, "ID")
        return parse_response(data)

    def capabilities(self):
        """Returns the server capability list.

        If the session is authenticated and the server has returned an
        untagged CAPABILITY response at authentication time, this
        response will be returned. Otherwise, the CAPABILITY command
        will be issued to the server, with the results cached for
        future calls.

        If the session is not yet authenticated, the capabilities
        requested at connection time will be returned.
        """
        # Ensure cached capabilities aren't used post-STARTTLS. As per
        # https://tools.ietf.org/html/rfc2595#section-3.1
        if self._starttls_done and self._imap.state == "NONAUTH":
            self._cached_capabilities = None
            return self._do_capabilites()

        # If a capability response has been cached, use that.
        if self._cached_capabilities:
            return self._cached_capabilities

        # If the server returned an untagged CAPABILITY response
        # (during authentication), cache it and return that.
        untagged = _dict_bytes_normaliser(self._imap.untagged_responses)
        response = untagged.pop("CAPABILITY", None)
        if response:
            self._cached_capabilities = self._normalise_capabilites(response[0])
            return self._cached_capabilities

        # If authenticated, but don't have a capability response, ask for one
        if self._imap.state in ("SELECTED", "AUTH"):
            self._cached_capabilities = self._do_capabilites()
            return self._cached_capabilities

        # Return capabilities that imaplib requested at connection
        # time (pre-auth)
        return tuple(to_bytes(c) for c in self._imap.capabilities)

    def _do_capabilites(self):
        raw_response = self._command_and_check("capability", unpack=True)
        return self._normalise_capabilites(raw_response)

    def _normalise_capabilites(self, raw_response):
        raw_response = to_bytes(raw_response)
        return tuple(raw_response.upper().split())

    def has_capability(self, capability):
        """Return ``True`` if the IMAP server has the given *capability*."""
        # FIXME: this will not detect capabilities that are backwards
        # compatible with the current level. For instance the SORT
        # capabilities may in the future be named SORT2 which is
        # still compatible with the current standard and will not
        # be detected by this method.
        return to_bytes(capability).upper() in self.capabilities()

    @require_capability("NAMESPACE")
    def namespace(self):
        """Return the namespace for the account as a (personal, other,
        shared) tuple.

        Each element may be None if no namespace of that type exists,
        or a sequence of (prefix, separator) pairs.

        For convenience the tuple elements may be accessed
        positionally or using attributes named *personal*, *other* and
        *shared*.

        See :rfc:`2342` for more details.
        """
        data = self._command_and_check("namespace")
        parts = []
        for item in parse_response(data):
            if item is None:
                parts.append(item)
            else:
                converted = []
                for prefix, separator in item:
                    if self.folder_encode:
                        prefix = decode_utf7(prefix)
                    converted.append((prefix, to_unicode(separator)))
                parts.append(tuple(converted))
        return Namespace(*parts)

    def list_folders(self, directory="", pattern="*"):
        """Get a listing of folders on the server as a list of
        ``(flags, delimiter, name)`` tuples.

        Specifying *directory* will limit returned folders to the
        given base directory. The directory and any child directories
        will returned.

        Specifying *pattern* will limit returned folders to those with
        matching names. The wildcards are supported in
        *pattern*. ``*`` matches zero or more of any character and
        ``%`` matches 0 or more characters except the folder
        delimiter.

        Calling list_folders with no arguments will recursively list
        all folders available for the logged in user.

        Folder names are always returned as unicode strings, and
        decoded from modified UTF-7, except if folder_decode is not
        set.
        """
        return self._do_list("LIST", directory, pattern)

    @require_capability("XLIST")
    def xlist_folders(self, directory="", pattern="*"):
        """Execute the XLIST command, returning ``(flags, delimiter,
        name)`` tuples.

        This method returns special flags for each folder and a
        localized name for certain folders (e.g. the name of the
        inbox may be localized and the flags can be used to
        determine the actual inbox, even if the name has been
        localized.

        A ``XLIST`` response could look something like::

            [((b'\\HasNoChildren', b'\\Inbox'), b'/', u'Inbox'),
             ((b'\\Noselect', b'\\HasChildren'), b'/', u'[Gmail]'),
             ((b'\\HasNoChildren', b'\\AllMail'), b'/', u'[Gmail]/All Mail'),
             ((b'\\HasNoChildren', b'\\Drafts'), b'/', u'[Gmail]/Drafts'),
             ((b'\\HasNoChildren', b'\\Important'), b'/', u'[Gmail]/Important'),
             ((b'\\HasNoChildren', b'\\Sent'), b'/', u'[Gmail]/Sent Mail'),
             ((b'\\HasNoChildren', b'\\Spam'), b'/', u'[Gmail]/Spam'),
             ((b'\\HasNoChildren', b'\\Starred'), b'/', u'[Gmail]/Starred'),
             ((b'\\HasNoChildren', b'\\Trash'), b'/', u'[Gmail]/Trash')]

        This is a *deprecated* Gmail-specific IMAP extension (See
        https://developers.google.com/gmail/imap_extensions#xlist_is_deprecated
        for more information).

        The *directory* and *pattern* arguments are as per
        list_folders().
        """
        return self._do_list("XLIST", directory, pattern)

    def list_sub_folders(self, directory="", pattern="*"):
        """Return a list of subscribed folders on the server as
        ``(flags, delimiter, name)`` tuples.

        The default behaviour will list all subscribed folders. The
        *directory* and *pattern* arguments are as per list_folders().
        """
        return self._do_list("LSUB", directory, pattern)

    def _do_list(self, cmd, directory, pattern):
        directory = self._normalise_folder(directory)
        pattern = self._normalise_folder(pattern)
        typ, dat = self._imap._simple_command(cmd, directory, pattern)
        self._checkok(cmd, typ, dat)
        typ, dat = self._imap._untagged_response(typ, dat, cmd)
        return self._proc_folder_list(dat)

    def _proc_folder_list(self, folder_data):
        # Filter out empty strings and None's.
        # This also deals with the special case of - no 'untagged'
        # responses (ie, no folders). This comes back as [None].
        folder_data = [item for item in folder_data if item not in (b"", None)]

        ret = []
        parsed = parse_response(folder_data)
        for flags, delim, name in chunk(parsed, size=3):
            if isinstance(name, int):
                # Some IMAP implementations return integer folder names
                # with quotes. These get parsed to ints so convert them
                # back to strings.
                name = str(name)
            elif self.folder_encode:
                name = decode_utf7(name)

            ret.append((flags, delim, name))
        return ret

    def find_special_folder(self, folder_flag):
        """Try to locate a special folder, like the Sent or Trash folder.

        >>> server.find_special_folder(imapclient.SENT)
        'INBOX.Sent'

        This function tries its best to find the correct folder (if any) but
        uses heuristics when the server is unable to precisely tell where
        special folders are located.

        Returns the name of the folder if found, or None otherwise.
        """
        # Detect folder by looking for known attributes
        # TODO: avoid listing all folders by using extended LIST (RFC6154)
        for folder in self.list_folders():
            if folder and len(folder[0]) > 0 and folder_flag in folder[0]:
                return folder[2]

        # Detect folder by looking for common names
        # We only look for folders in the "personal" namespace of the user
        if self.has_capability("NAMESPACE"):
            personal_namespaces = self.namespace().personal
        else:
            personal_namespaces = _POPULAR_PERSONAL_NAMESPACES

        for personal_namespace in personal_namespaces:
            for pattern in _POPULAR_SPECIAL_FOLDERS.get(folder_flag, tuple()):
                pattern = personal_namespace[0] + pattern
                sent_folders = self.list_folders(pattern=pattern)
                if sent_folders:
                    return sent_folders[0][2]

        return None

    def select_folder(self, folder, readonly=False):
        """Set the current folder on the server.

        Future calls to methods such as search and fetch will act on
        the selected folder.

        Returns a dictionary containing the ``SELECT`` response. At least
        the ``b'EXISTS'``, ``b'FLAGS'`` and ``b'RECENT'`` keys are guaranteed
        to exist. An example::

            {b'EXISTS': 3,
             b'FLAGS': (b'\\Answered', b'\\Flagged', b'\\Deleted', ... ),
             b'RECENT': 0,
             b'PERMANENTFLAGS': (b'\\Answered', b'\\Flagged', b'\\Deleted', ... ),
             b'READ-WRITE': True,
             b'UIDNEXT': 11,
             b'UIDVALIDITY': 1239278212}
        """
        self._command_and_check("select", self._normalise_folder(folder), readonly)
        return self._process_select_response(self._imap.untagged_responses)

    @require_capability("UNSELECT")
    def unselect_folder(self):
        r"""Unselect the current folder and release associated resources.

        Unlike ``close_folder``, the ``UNSELECT`` command does not expunge
        the mailbox, keeping messages with \Deleted flag set for example.

        Returns the UNSELECT response string returned by the server.
        """
        logger.debug("< UNSELECT")
        # IMAP4 class has no `unselect` method so we can't use `_command_and_check` there
        _typ, data = self._imap._simple_command("UNSELECT")
        return data[0]

    def _process_select_response(self, resp):
        untagged = _dict_bytes_normaliser(resp)
        out = {}

        # imaplib doesn't parse these correctly (broken regex) so replace
        # with the raw values out of the OK section
        for line in untagged.get("OK", []):
            match = _RE_SELECT_RESPONSE.match(line)
            if match:
                key = match.group("key")
                if key == b"PERMANENTFLAGS":
                    out[key] = tuple(match.group("data").split())

        for key, value in untagged.items():
            key = key.upper()
            if key in (b"OK", b"PERMANENTFLAGS"):
                continue  # already handled above
            if key in (
                b"EXISTS",
                b"RECENT",
                b"UIDNEXT",
                b"UIDVALIDITY",
                b"HIGHESTMODSEQ",
            ):
                value = int(value[0])
            elif key == b"READ-WRITE":
                value = True
            elif key == b"FLAGS":
                value = tuple(value[0][1:-1].split())
            out[key] = value
        return out

    def noop(self):
        """Execute the NOOP command.

        This command returns immediately, returning any server side
        status updates. It can also be used to reset any auto-logout
        timers.

        The return value is the server command response message
        followed by a list of status responses. For example::

            (b'NOOP completed.',
             [(4, b'EXISTS'),
              (3, b'FETCH', (b'FLAGS', (b'bar', b'sne'))),
              (6, b'FETCH', (b'FLAGS', (b'sne',)))])

        """
        tag = self._imap._command("NOOP")
        return self._consume_until_tagged_response(tag, "NOOP")

    @require_capability("IDLE")
    def idle(self):
        """Put the server into IDLE mode.

        In this mode the server will return unsolicited responses
        about changes to the selected mailbox. This method returns
        immediately. Use ``idle_check()`` to look for IDLE responses
        and ``idle_done()`` to stop IDLE mode.

        .. note::

            Any other commands issued while the server is in IDLE
            mode will fail.

        See :rfc:`2177` for more information about the IDLE extension.
        """
        self._idle_tag = self._imap._command("IDLE")
        resp = self._imap._get_response()
        if resp is not None:
            raise exceptions.IMAPClientError("Unexpected IDLE response: %s" % resp)

    def _poll_socket(self, sock, timeout=None):
        """
        Polls the socket for events telling us it's available to read.
        This implementation is more scalable because it ALLOWS your process
        to have more than 1024 file descriptors.
        """
        poller = select.poll()
        poller.register(sock.fileno(), select.POLLIN)
        timeout = timeout * 1000 if timeout is not None else None
        return poller.poll(timeout)

    def _select_poll_socket(self, sock, timeout=None):
        """
        Polls the socket for events telling us it's available to read.
        This implementation is a fallback because it FAILS if your process
        has more than 1024 file descriptors.
        We still need this for Windows and some other niche systems.
        """
        return select.select([sock], [], [], timeout)[0]

    @require_capability("IDLE")
    def idle_check(self, timeout=None):
        """Check for any IDLE responses sent by the server.

        This method should only be called if the server is in IDLE
        mode (see ``idle()``).

        By default, this method will block until an IDLE response is
        received. If *timeout* is provided, the call will block for at
        most this many seconds while waiting for an IDLE response.

        The return value is a list of received IDLE responses. These
        will be parsed with values converted to appropriate types. For
        example::

            [(b'OK', b'Still here'),
             (1, b'EXISTS'),
             (1, b'FETCH', (b'FLAGS', (b'\\NotJunk',)))]
        """
        sock = self.socket()

        # make the socket non-blocking so the timeout can be
        # implemented for this call
        sock.settimeout(None)
        sock.setblocking(0)

        if POLL_SUPPORT:
            poll_func = self._poll_socket
        else:
            poll_func = self._select_poll_socket

        try:
            resps = []
            events = poll_func(sock, timeout)
            if events:
                while True:
                    try:
                        line = self._imap._get_line()
                    except (socket.timeout, socket.error):
                        break
                    except IMAPClient.AbortError:
                        # An imaplib.IMAP4.abort with "EOF" is raised
                        # under Python 3
                        err = sys.exc_info()[1]
                        if "EOF" in err.args[0]:
                            break
                        raise
                    else:
                        resps.append(_parse_untagged_response(line))
            return resps
        finally:
            sock.setblocking(1)
            self._set_read_timeout()

    @require_capability("IDLE")
    def idle_done(self):
        """Take the server out of IDLE mode.

        This method should only be called if the server is already in
        IDLE mode.

        The return value is of the form ``(command_text,
        idle_responses)`` where *command_text* is the text sent by the
        server when the IDLE command finished (eg. ``b'Idle
        terminated'``) and *idle_responses* is a list of parsed idle
        responses received since the last call to ``idle_check()`` (if
        any). These are returned in parsed form as per
        ``idle_check()``.
        """
        logger.debug("< DONE")
        self._imap.send(b"DONE\r\n")
        return self._consume_until_tagged_response(self._idle_tag, "IDLE")

    def folder_status(self, folder, what=None):
        """Return the status of *folder*.

        *what* should be a sequence of status items to query. This
        defaults to ``('MESSAGES', 'RECENT', 'UIDNEXT', 'UIDVALIDITY',
        'UNSEEN')``.

        Returns a dictionary of the status items for the folder with
        keys matching *what*.
        """
        if what is None:
            what = ("MESSAGES", "RECENT", "UIDNEXT", "UIDVALIDITY", "UNSEEN")
        else:
            what = normalise_text_list(what)
        what_ = "(%s)" % (" ".join(what))

        fname = self._normalise_folder(folder)
        data = self._command_and_check("status", fname, what_)
        response = parse_response(data)
        status_items = response[-1]
        return dict(as_pairs(status_items))

    def close_folder(self):
        """Close the currently selected folder, returning the server
        response string.
        """
        return self._command_and_check("close", unpack=True)

    def create_folder(self, folder):
        """Create *folder* on the server returning the server response string."""
        return self._command_and_check(
            "create", self._normalise_folder(folder), unpack=True
        )

    def rename_folder(self, old_name, new_name):
        """Change the name of a folder on the server."""
        return self._command_and_check(
            "rename",
            self._normalise_folder(old_name),
            self._normalise_folder(new_name),
            unpack=True,
        )

    def delete_folder(self, folder):
        """Delete *folder* on the server returning the server response string."""
        return self._command_and_check(
            "delete", self._normalise_folder(folder), unpack=True
        )

    def folder_exists(self, folder):
        """Return ``True`` if *folder* exists on the server."""
        return len(self.list_folders("", folder)) > 0

    def subscribe_folder(self, folder):
        """Subscribe to *folder*, returning the server response string."""
        return self._command_and_check("subscribe", self._normalise_folder(folder))

    def unsubscribe_folder(self, folder):
        """Unsubscribe to *folder*, returning the server response string."""
        return self._command_and_check("unsubscribe", self._normalise_folder(folder))

    def search(self, criteria="ALL", charset=None):
        """Return a list of messages ids from the currently selected
        folder matching *criteria*.

        *criteria* should be a sequence of one or more criteria
        items. Each criteria item may be either unicode or
        bytes. Example values::

            [u'UNSEEN']
            [u'SMALLER', 500]
            [b'NOT', b'DELETED']
            [u'TEXT', u'foo bar', u'FLAGGED', u'SUBJECT', u'baz']
            [u'SINCE', date(2005, 4, 3)]

        IMAPClient will perform conversion and quoting as
        required. The caller shouldn't do this.

        It is also possible (but not recommended) to pass the combined
        criteria as a single string. In this case IMAPClient won't
        perform quoting, allowing lower-level specification of
        criteria. Examples of this style::

            u'UNSEEN'
            u'SMALLER 500'
            b'NOT DELETED'
            u'TEXT "foo bar" FLAGGED SUBJECT "baz"'
            b'SINCE 03-Apr-2005'

        To support complex search expressions, criteria lists can be
        nested. IMAPClient will insert parentheses in the right
        places. The following will match messages that are both not
        flagged and do not have "foo" in the subject::

            ['NOT', ['SUBJECT', 'foo', 'FLAGGED']]

        *charset* specifies the character set of the criteria. It
        defaults to US-ASCII as this is the only charset that a server
        is required to support by the RFC. UTF-8 is commonly supported
        however.

        Any criteria specified using unicode will be encoded as per
        *charset*. Specifying a unicode criteria that can not be
        encoded using *charset* will result in an error.

        Any criteria specified using bytes will be sent as-is but
        should use an encoding that matches *charset* (the character
        set given is still passed on to the server).

        See :rfc:`3501#section-6.4.4` for more details.

        Note that criteria arguments that are 8-bit will be
        transparently sent by IMAPClient as IMAP literals to ensure
        adherence to IMAP standards.

        The returned list of message ids will have a special *modseq*
        attribute. This is set if the server included a MODSEQ value
        to the search response (i.e. if a MODSEQ criteria was included
        in the search).

        """
        return self._search(criteria, charset)

    @require_capability("X-GM-EXT-1")
    def gmail_search(self, query, charset="UTF-8"):
        """Search using Gmail's X-GM-RAW attribute.

        *query* should be a valid Gmail search query string. For
        example: ``has:attachment in:unread``. The search string may
        be unicode and will be encoded using the specified *charset*
        (defaulting to UTF-8).

        This method only works for IMAP servers that support X-GM-RAW,
        which is only likely to be Gmail.

        See https://developers.google.com/gmail/imap_extensions#extension_of_the_search_command_x-gm-raw
        for more info.
        """
        return self._search([b"X-GM-RAW", query], charset)

    def _search(self, criteria, charset):
        args = []
        if charset:
            args.extend([b"CHARSET", to_bytes(charset)])
        args.extend(_normalise_search_criteria(criteria, charset))

        try:
            data = self._raw_command_untagged(b"SEARCH", args)
        except imaplib.IMAP4.error as e:
            # Make BAD IMAP responses easier to understand to the user, with a link to the docs
            m = re.match(r"SEARCH command error: BAD \[(.+)\]", str(e))
            if m:
                raise exceptions.InvalidCriteriaError(
                    "{original_msg}\n\n"
                    "This error may have been caused by a syntax error in the criteria: "
                    "{criteria}\nPlease refer to the documentation for more information "
                    "about search criteria syntax..\n"
                    "https://imapclient.readthedocs.io/en/master/#imapclient.IMAPClient.search".format(
                        original_msg=m.group(1),
                        criteria=(
                            '"%s"' % criteria
                            if not isinstance(criteria, list)
                            else criteria
                        ),
                    )
                )

            # If the exception is not from a BAD IMAP response, re-raise as-is
            raise

        return parse_message_list(data)

    @require_capability("SORT")
    def sort(self, sort_criteria, criteria="ALL", charset="UTF-8"):
        """Return a list of message ids from the currently selected
        folder, sorted by *sort_criteria* and optionally filtered by
        *criteria*.

        *sort_criteria* may be specified as a sequence of strings or a
        single string. IMAPClient will take care any required
        conversions. Valid *sort_criteria* values::

            ['ARRIVAL']
            ['SUBJECT', 'ARRIVAL']
            'ARRIVAL'
            'REVERSE SIZE'

        The *criteria* and *charset* arguments are as per
        :py:meth:`.search`.

        See :rfc:`5256` for full details.

        Note that SORT is an extension to the IMAP4 standard so it may
        not be supported by all IMAP servers.
        """
        args = [
            _normalise_sort_criteria(sort_criteria),
            to_bytes(charset),
        ]
        args.extend(_normalise_search_criteria(criteria, charset))
        ids = self._raw_command_untagged(b"SORT", args, unpack=True)
        return [int(i) for i in ids.split()]

    def thread(self, algorithm="REFERENCES", criteria="ALL", charset="UTF-8"):
        """Return a list of messages threads from the currently
        selected folder which match *criteria*.

        Each returned thread is a list of messages ids. An example
        return value containing three message threads::

            ((1, 2), (3,), (4, 5, 6))

        The optional *algorithm* argument specifies the threading
        algorithm to use.

        The *criteria* and *charset* arguments are as per
        :py:meth:`.search`.

        See :rfc:`5256` for more details.
        """
        algorithm = to_bytes(algorithm)
        if not self.has_capability(b"THREAD=" + algorithm):
            raise exceptions.CapabilityError(
                "The server does not support %s threading algorithm" % algorithm
            )

        args = [algorithm, to_bytes(charset)] + _normalise_search_criteria(
            criteria, charset
        )
        data = self._raw_command_untagged(b"THREAD", args)
        return parse_response(data)

    def get_flags(self, messages):
        """Return the flags set for each message in *messages* from
        the currently selected folder.

        The return value is a dictionary structured like this: ``{
        msgid1: (flag1, flag2, ... ), }``.
        """
        response = self.fetch(messages, ["FLAGS"])
        return self._filter_fetch_dict(response, b"FLAGS")

    def add_flags(self, messages, flags, silent=False):
        """Add *flags* to *messages* in the currently selected folder.

        *flags* should be a sequence of strings.

        Returns the flags set for each modified message (see
        *get_flags*), or None if *silent* is true.
        """
        return self._store(b"+FLAGS", messages, flags, b"FLAGS", silent=silent)

    def remove_flags(self, messages, flags, silent=False):
        """Remove one or more *flags* from *messages* in the currently
        selected folder.

        *flags* should be a sequence of strings.

        Returns the flags set for each modified message (see
        *get_flags*), or None if *silent* is true.
        """
        return self._store(b"-FLAGS", messages, flags, b"FLAGS", silent=silent)

    def set_flags(self, messages, flags, silent=False):
        """Set the *flags* for *messages* in the currently selected
        folder.

        *flags* should be a sequence of strings.

        Returns the flags set for each modified message (see
        *get_flags*), or None if *silent* is true.
        """
        return self._store(b"FLAGS", messages, flags, b"FLAGS", silent=silent)

    def get_gmail_labels(self, messages):
        """Return the label set for each message in *messages* in the
        currently selected folder.

        The return value is a dictionary structured like this: ``{
        msgid1: (label1, label2, ... ), }``.

        This only works with IMAP servers that support the X-GM-LABELS
        attribute (eg. Gmail).
        """
        response = self.fetch(messages, [b"X-GM-LABELS"])
        response = self._filter_fetch_dict(response, b"X-GM-LABELS")
        return {msg: utf7_decode_sequence(labels) for msg, labels in response.items()}

    def add_gmail_labels(self, messages, labels, silent=False):
        """Add *labels* to *messages* in the currently selected folder.

        *labels* should be a sequence of strings.

        Returns the label set for each modified message (see
        *get_gmail_labels*), or None if *silent* is true.

        This only works with IMAP servers that support the X-GM-LABELS
        attribute (eg. Gmail).
        """
        return self._gm_label_store(b"+X-GM-LABELS", messages, labels, silent=silent)

    def remove_gmail_labels(self, messages, labels, silent=False):
        """Remove one or more *labels* from *messages* in the
        currently selected folder, or None if *silent* is true.

        *labels* should be a sequence of strings.

        Returns the label set for each modified message (see
        *get_gmail_labels*).

        This only works with IMAP servers that support the X-GM-LABELS
        attribute (eg. Gmail).
        """
        return self._gm_label_store(b"-X-GM-LABELS", messages, labels, silent=silent)

    def set_gmail_labels(self, messages, labels, silent=False):
        """Set the *labels* for *messages* in the currently selected
        folder.

        *labels* should be a sequence of strings.

        Returns the label set for each modified message (see
        *get_gmail_labels*), or None if *silent* is true.

        This only works with IMAP servers that support the X-GM-LABELS
        attribute (eg. Gmail).
        """
        return self._gm_label_store(b"X-GM-LABELS", messages, labels, silent=silent)

    def delete_messages(self, messages, silent=False):
        """Delete one or more *messages* from the currently selected
        folder.

        Returns the flags set for each modified message (see
        *get_flags*).
        """
        return self.add_flags(messages, DELETED, silent=silent)

    def fetch(self, messages, data, modifiers=None):
        """Retrieve selected *data* associated with one or more
        *messages* in the currently selected folder.

        *data* should be specified as a sequence of strings, one item
        per data selector, for example ``['INTERNALDATE',
        'RFC822']``.

        *modifiers* are required for some extensions to the IMAP
        protocol (eg. :rfc:`4551`). These should be a sequence of strings
        if specified, for example ``['CHANGEDSINCE 123']``.

        A dictionary is returned, indexed by message number. Each item
        in this dictionary is also a dictionary, with an entry
        corresponding to each item in *data*. Returned values will be
        appropriately typed. For example, integer values will be returned as
        Python integers, timestamps will be returned as datetime
        instances and ENVELOPE responses will be returned as
        :py:class:`Envelope <imapclient.response_types.Envelope>` instances.

        String data will generally be returned as bytes (Python 3) or
        str (Python 2).

        In addition to an element for each *data* item, the dict
        returned for each message also contains a *SEQ* key containing
        the sequence number for the message. This allows for mapping
        between the UID and sequence number (when the *use_uid*
        property is ``True``).

        Example::

            >> c.fetch([3293, 3230], ['INTERNALDATE', 'FLAGS'])
            {3230: {b'FLAGS': (b'\\Seen',),
                    b'INTERNALDATE': datetime.datetime(2011, 1, 30, 13, 32, 9),
                    b'SEQ': 84},
             3293: {b'FLAGS': (),
                    b'INTERNALDATE': datetime.datetime(2011, 2, 24, 19, 30, 36),
                    b'SEQ': 110}}

        """
        if not messages:
            return {}

        args = [
            "FETCH",
            join_message_ids(messages),
            seq_to_parenstr_upper(data),
            seq_to_parenstr_upper(modifiers) if modifiers else None,
        ]
        if self.use_uid:
            args.insert(0, "UID")
        tag = self._imap._command(*args)
        typ, data = self._imap._command_complete("FETCH", tag)
        self._checkok("fetch", typ, data)
        typ, data = self._imap._untagged_response(typ, data, "FETCH")
        return parse_fetch_response(data, self.normalise_times, self.use_uid)

    def append(self, folder, msg, flags=(), msg_time=None):
        """Append a message to *folder*.

        *msg* should be a string contains the full message including
        headers.

        *flags* should be a sequence of message flags to set. If not
        specified no flags will be set.

        *msg_time* is an optional datetime instance specifying the
        date and time to set on the message. The server will set a
        time if it isn't specified. If *msg_time* contains timezone
        information (tzinfo), this will be honoured. Otherwise the
        local machine's time zone sent to the server.

        Returns the APPEND response as returned by the server.
        """
        if msg_time:
            time_val = '"%s"' % datetime_to_INTERNALDATE(msg_time)
            time_val = to_unicode(time_val)
        else:
            time_val = None
        return self._command_and_check(
            "append",
            self._normalise_folder(folder),
            seq_to_parenstr(flags),
            time_val,
            to_bytes(msg),
            unpack=True,
        )

    @require_capability("MULTIAPPEND")
    def multiappend(self, folder, msgs):
        """Append messages to *folder* using the MULTIAPPEND feature from :rfc:`3502`.

        *msgs* must be an iterable. Each item must be either a string containing the
        full message including headers, or a dict containing the keys "msg" with the
        full message as before, "flags" with a sequence of message flags to set, and
        "date" with a datetime instance specifying the internal date to set.
        The keys "flags" and "date" are optional.

        Returns the APPEND response from the server.
        """

        def chunks():
            for m in msgs:
                if isinstance(m, dict):
                    if "flags" in m:
                        yield to_bytes(seq_to_parenstr(m["flags"]))
                    if "date" in m:
                        yield to_bytes('"%s"' % datetime_to_INTERNALDATE(m["date"]))
                    yield _literal(to_bytes(m["msg"]))
                else:
                    yield _literal(to_bytes(m))

        msgs = list(chunks())

        return self._raw_command(
            b"APPEND",
            [self._normalise_folder(folder)] + msgs,
            uid=False,
        )

    def copy(self, messages, folder):
        """Copy one or more messages from the current folder to
        *folder*. Returns the COPY response string returned by the
        server.
        """
        return self._command_and_check(
            "copy",
            join_message_ids(messages),
            self._normalise_folder(folder),
            uid=True,
            unpack=True,
        )

    @require_capability("MOVE")
    def move(self, messages, folder):
        """Atomically move messages to another folder.

        Requires the MOVE capability, see :rfc:`6851`.

        :param messages: List of message UIDs to move.
        :param folder: The destination folder name.
        """
        return self._command_and_check(
            "move",
            join_message_ids(messages),
            self._normalise_folder(folder),
            uid=True,
            unpack=True,
        )

    def expunge(self, messages=None):
        """Use of the *messages* argument is discouraged.
        Please see the ``uid_expunge`` method instead.

        When, no *messages* are specified, remove all messages
        from the currently selected folder that have the
        ``\\Deleted`` flag set.

        The return value is the server response message
        followed by a list of expunge responses. For example::

            ('Expunge completed.',
             [(2, 'EXPUNGE'),
              (1, 'EXPUNGE'),
              (0, 'RECENT')])

        In this case, the responses indicate that the message with
        sequence numbers 2 and 1 where deleted, leaving no recent
        messages in the folder.

        See :rfc:`3501#section-6.4.3` section 6.4.3 and
        :rfc:`3501#section-7.4.1` section 7.4.1 for more details.

        When *messages* are specified, remove the specified messages
        from the selected folder, provided those messages also have
        the ``\\Deleted`` flag set. The return value is ``None`` in
        this case.

        Expunging messages by id(s) requires that *use_uid* is
        ``True`` for the client.

        See :rfc:`4315#section-2.1` section 2.1 for more details.
        """
        if messages:
            if not self.use_uid:
                raise ValueError("cannot EXPUNGE by ID when not using uids")
            return self._command_and_check(
                "EXPUNGE", join_message_ids(messages), uid=True
            )
        tag = self._imap._command("EXPUNGE")
        return self._consume_until_tagged_response(tag, "EXPUNGE")

    @require_capability("UIDPLUS")
    def uid_expunge(self, messages):
        """Expunge deleted messages with the specified message ids from the
        folder.

        This requires the UIDPLUS capability.

        See :rfc:`4315#section-2.1` section 2.1 for more details.
        """
        return self._command_and_check("EXPUNGE", join_message_ids(messages), uid=True)

    @require_capability("ACL")
    def getacl(self, folder):
        """Returns a list of ``(who, acl)`` tuples describing the
        access controls for *folder*.
        """
        data = self._command_and_check("getacl", self._normalise_folder(folder))
        parts = list(response_lexer.TokenSource(data))
        parts = parts[1:]  # First item is folder name
        return [(parts[i], parts[i + 1]) for i in range(0, len(parts), 2)]

    @require_capability("ACL")
    def setacl(self, folder, who, what):
        """Set an ACL (*what*) for user (*who*) for a folder.

        Set *what* to an empty string to remove an ACL. Returns the
        server response string.
        """
        return self._command_and_check(
            "setacl", self._normalise_folder(folder), who, what, unpack=True
        )

    @require_capability("QUOTA")
    def get_quota(self, mailbox="INBOX"):
        """Get the quotas associated with a mailbox.

        Returns a list of Quota objects.
        """
        return self.get_quota_root(mailbox)[1]

    @require_capability("QUOTA")
    def _get_quota(self, quota_root=""):
        """Get the quotas associated with a quota root.

        This method is not private but put behind an underscore to show that
        it is a low-level function. Users probably want to use `get_quota`
        instead.

        Returns a list of Quota objects.
        """
        return _parse_quota(self._command_and_check("getquota", _quote(quota_root)))

    @require_capability("QUOTA")
    def get_quota_root(self, mailbox):
        """Get the quota roots for a mailbox.

        The IMAP server responds with the quota root and the quotas associated
        so there is usually no need to call `get_quota` after.

        See :rfc:`2087` for more details.

        Return a tuple of MailboxQuotaRoots and list of Quota associated
        """
        quota_root_rep = self._raw_command_untagged(
            b"GETQUOTAROOT", to_bytes(mailbox), uid=False, response_name="QUOTAROOT"
        )
        quota_rep = self._imap.untagged_responses.pop("QUOTA", [])
        quota_root_rep = parse_response(quota_root_rep)
        quota_root = MailboxQuotaRoots(
            to_unicode(quota_root_rep[0]), [to_unicode(q) for q in quota_root_rep[1:]]
        )
        return quota_root, _parse_quota(quota_rep)

    @require_capability("QUOTA")
    def set_quota(self, quotas):
        """Set one or more quotas on resources.

        :param quotas: list of Quota objects
        """
        if not quotas:
            return

        quota_root = None
        set_quota_args = []

        for quota in quotas:
            if quota_root is None:
                quota_root = quota.quota_root
            elif quota_root != quota.quota_root:
                raise ValueError("set_quota only accepts a single quota root")

            set_quota_args.append("{} {}".format(quota.resource, quota.limit))

        set_quota_args = " ".join(set_quota_args)
        args = [to_bytes(_quote(quota_root)), to_bytes("({})".format(set_quota_args))]

        response = self._raw_command_untagged(
            b"SETQUOTA", args, uid=False, response_name="QUOTA"
        )
        return _parse_quota(response)

    def _check_resp(self, expected, command, typ, data):
        """Check command responses for errors.

        Raises IMAPClient.Error if the command fails.
        """
        if typ != expected:
            raise exceptions.IMAPClientError(
                "%s failed: %s" % (command, to_unicode(data[0]))
            )

    def _consume_until_tagged_response(self, tag, command):
        tagged_commands = self._imap.tagged_commands
        resps = []
        while True:
            line = self._imap._get_response()
            if tagged_commands[tag]:
                break
            resps.append(_parse_untagged_response(line))
        typ, data = tagged_commands.pop(tag)
        self._checkok(command, typ, data)
        return data[0], resps

    def _raw_command_untagged(
        self, command, args, response_name=None, unpack=False, uid=True
    ):
        # TODO: eventually this should replace _command_and_check (call it _command)
        typ, data = self._raw_command(command, args, uid=uid)
        if response_name is None:
            response_name = command
        typ, data = self._imap._untagged_response(typ, data, to_unicode(response_name))
        self._checkok(to_unicode(command), typ, data)
        if unpack:
            return data[0]
        return data

    def _raw_command(self, command, args, uid=True):
        """Run the specific command with the arguments given. 8-bit arguments
        are sent as literals. The return value is (typ, data).

        This sidesteps much of imaplib's command sending
        infrastructure because imaplib can't send more than one
        literal.

        *command* should be specified as bytes.
        *args* should be specified as a list of bytes.
        """
        command = command.upper()

        # Check for LITERAL+ now because if capabilities haven't been cached
        # yet, we can't call CAPABILITY while sending another command.
        has_literal_plus = self.has_capability("LITERAL+")

        if isinstance(args, tuple):
            args = list(args)
        if not isinstance(args, list):
            args = [args]

        tag = self._imap._new_tag()
        prefix = [to_bytes(tag)]
        if uid and self.use_uid:
            prefix.append(b"UID")
        prefix.append(command)

        line = []
        for item, is_last in _iter_with_last(prefix + args):
            if not isinstance(item, bytes):
                raise ValueError("command args must be passed as bytes")

            if _is8bit(item):
                # If a line was already started send it
                if line:
                    out = b" ".join(line)
                    logger.debug("> %s", out)
                    self._imap.send(out)
                    line = []

                # Now send the (unquoted) literal
                if isinstance(item, _quoted):
                    item = item.original
                self._send_literal(tag, item, has_literal_plus)
                if not is_last:
                    self._imap.send(b" ")
            else:
                line.append(item)

        if line:
            out = b" ".join(line)
            logger.debug("> %s", out)
            self._imap.send(out)

        self._imap.send(b"\r\n")

        return self._imap._command_complete(to_unicode(command), tag)

    def _send_literal(self, tag, item, has_literal_plus):
        """Send a single literal for the command with *tag*."""
        if has_literal_plus:
            out = b" {" + str(len(item)).encode("ascii") + b"+}\r\n" + item
            logger.debug("> %s", debug_trunc(out, 64))
            self._imap.send(out)
            return

        out = b" {" + str(len(item)).encode("ascii") + b"}\r\n"
        logger.debug("> %s", out)
        self._imap.send(out)

        # Wait for continuation response
        while self._imap._get_response():
            tagged_resp = self._imap.tagged_commands.get(tag)
            if tagged_resp:
                raise exceptions.IMAPClientAbortError(
                    "unexpected response while waiting for continuation response: "
                    + repr(tagged_resp)
                )

        logger.debug("   (literal) > %s", debug_trunc(item, 256))
        self._imap.send(item)

    def _command_and_check(
        self, command, *args, unpack: bool = False, uid: bool = False
    ):
        if uid and self.use_uid:
            command = to_unicode(command)  # imaplib must die
            typ, data = self._imap.uid(command, *args)
        else:
            meth = getattr(self._imap, to_unicode(command))
            typ, data = meth(*args)
        self._checkok(command, typ, data)
        if unpack:
            return data[0]
        return data

    def _checkok(self, command, typ, data):
        self._check_resp("OK", command, typ, data)

    def _gm_label_store(self, cmd, messages, labels, silent):
        response = self._store(
            cmd, messages, self._normalise_labels(labels), b"X-GM-LABELS", silent=silent
        )
        return (
            {msg: utf7_decode_sequence(labels) for msg, labels in response.items()}
            if response
            else None
        )

    def _store(self, cmd, messages, flags, fetch_key, silent):
        """Worker function for the various flag manipulation methods.

        *cmd* is the STORE command to use (eg. '+FLAGS').
        """
        if not messages:
            return {}
        if silent:
            cmd += b".SILENT"

        data = self._command_and_check(
            "store", join_message_ids(messages), cmd, seq_to_parenstr(flags), uid=True
        )
        if silent:
            return None
        return self._filter_fetch_dict(parse_fetch_response(data), fetch_key)

    def _filter_fetch_dict(self, fetch_dict, key):
        return dict((msgid, data[key]) for msgid, data in fetch_dict.items())

    def _normalise_folder(self, folder_name):
        if isinstance(folder_name, bytes):
            folder_name = folder_name.decode("ascii")
        if self.folder_encode:
            folder_name = encode_utf7(folder_name)
        return _quote(folder_name)

    def _normalise_labels(self, labels):
        if isinstance(labels, (str, bytes)):
            labels = (labels,)
        return [_quote(encode_utf7(label)) for label in labels]

    @property
    def welcome(self):
        """access the server greeting message"""
        try:
            return self._imap.welcome
        except AttributeError:
            pass


def _quote(arg):
    if isinstance(arg, str):
        arg = arg.replace("\\", "\\\\")
        arg = arg.replace('"', '\\"')
        q = '"'
    else:
        arg = arg.replace(b"\\", b"\\\\")
        arg = arg.replace(b'"', b'\\"')
        q = b'"'
    return q + arg + q


def _normalise_search_criteria(criteria, charset=None):
    if not criteria:
        raise exceptions.InvalidCriteriaError("no criteria specified")
    if not charset:
        charset = "us-ascii"

    if isinstance(criteria, (str, bytes)):
        return [to_bytes(criteria, charset)]

    out = []
    for item in criteria:
        if isinstance(item, int):
            out.append(str(item).encode("ascii"))
        elif isinstance(item, (datetime, date)):
            out.append(format_criteria_date(item))
        elif isinstance(item, (list, tuple)):
            # Process nested criteria list and wrap in parens.
            inner = _normalise_search_criteria(item)
            inner[0] = b"(" + inner[0]
            inner[-1] = inner[-1] + b")"
            out.extend(inner)  # flatten
        else:
            out.append(_quoted.maybe(to_bytes(item, charset)))
    return out


def _normalise_sort_criteria(criteria, charset=None):
    if isinstance(criteria, (str, bytes)):
        criteria = [criteria]
    return b"(" + b" ".join(to_bytes(item).upper() for item in criteria) + b")"


class _literal(bytes):
    """Hold message data that should always be sent as a literal."""


class _quoted(bytes):
    """
    This class holds a quoted bytes value which provides access to the
    unquoted value via the *original* attribute.

    They should be created via the *maybe* classmethod.
    """

    @classmethod
    def maybe(cls, original):
        """Maybe quote a bytes value.

        If the input requires no quoting it is returned unchanged.

        If quoting is required a *_quoted* instance is returned. This
        holds the quoted version of the input while also providing
        access to the original unquoted source.
        """
        quoted = original.replace(b"\\", b"\\\\")
        quoted = quoted.replace(b'"', b'\\"')
        if quoted != original or b" " in quoted or not quoted:
            out = cls(b'"' + quoted + b'"')
            out.original = original
            return out
        return original


# normalise_text_list, seq_to_parentstr etc have to return unicode
# because imaplib handles flags and sort criteria assuming these are
# passed as unicode
def normalise_text_list(items):
    return list(_normalise_text_list(items))


def seq_to_parenstr(items):
    return _join_and_paren(_normalise_text_list(items))


def seq_to_parenstr_upper(items):
    return _join_and_paren(item.upper() for item in _normalise_text_list(items))


def _join_and_paren(items):
    return "(" + " ".join(items) + ")"


def _normalise_text_list(items):
    if isinstance(items, (str, bytes)):
        items = (items,)
    return (to_unicode(c) for c in items)


def join_message_ids(messages):
    """Convert a sequence of messages ids or a single integer message id
    into an id byte string for use with IMAP commands
    """
    if isinstance(messages, (str, bytes, int)):
        messages = (to_bytes(messages),)
    return b",".join(_maybe_int_to_bytes(m) for m in messages)


def _maybe_int_to_bytes(val):
    if isinstance(val, int):
        return str(val).encode("us-ascii")
    return to_bytes(val)


def _parse_untagged_response(text):
    assert_imap_protocol(text.startswith(b"* "))
    text = text[2:]
    if text.startswith((b"OK ", b"NO ")):
        return tuple(text.split(b" ", 1))
    return parse_response([text])


def as_pairs(items):
    i = 0
    last_item = None
    for item in items:
        if i % 2:
            yield last_item, item
        else:
            last_item = item
        i += 1


def as_triplets(items):
    a = iter(items)
    return zip(a, a, a)


def _is8bit(data):
    return isinstance(data, _literal) or any(b > 127 for b in data)


def _iter_with_last(items):
    last_i = len(items) - 1
    for i, item in enumerate(items):
        yield item, i == last_i


_not_present = object()


class _dict_bytes_normaliser:
    """Wrap a dict with unicode/bytes keys and normalise the keys to
    bytes.
    """

    def __init__(self, d):
        self._d = d

    def iteritems(self):
        for key, value in self._d.items():
            yield to_bytes(key), value

    # For Python 3 compatibility.
    items = iteritems

    def __contains__(self, ink):
        for k in self._gen_keys(ink):
            if k in self._d:
                return True
        return False

    def get(self, ink, default=_not_present):
        for k in self._gen_keys(ink):
            try:
                return self._d[k]
            except KeyError:
                pass
        if default == _not_present:
            raise KeyError(ink)
        return default

    def pop(self, ink, default=_not_present):
        for k in self._gen_keys(ink):
            try:
                return self._d.pop(k)
            except KeyError:
                pass
        if default == _not_present:
            raise KeyError(ink)
        return default

    def _gen_keys(self, k):
        yield k
        if isinstance(k, bytes):
            yield to_unicode(k)
        else:
            yield to_bytes(k)


def debug_trunc(v, maxlen):
    if len(v) < maxlen:
        return repr(v)
    hl = maxlen // 2
    return repr(v[:hl]) + "..." + repr(v[-hl:])


def utf7_decode_sequence(seq):
    return [decode_utf7(s) for s in seq]


def _parse_quota(quota_rep):
    quota_rep = parse_response(quota_rep)
    rv = []
    for quota_root, quota_resource_infos in as_pairs(quota_rep):
        for quota_resource_info in as_triplets(quota_resource_infos):
            rv.append(
                Quota(
                    quota_root=to_unicode(quota_root),
                    resource=to_unicode(quota_resource_info[0]),
                    usage=quota_resource_info[1],
                    limit=quota_resource_info[2],
                )
            )
    return rv


class IMAPlibLoggerAdapter(LoggerAdapter):
    """Adapter preventing IMAP secrets from going to the logging facility."""

    def process(self, msg, kwargs):
        # msg is usually unicode but see #367. Convert bytes to
        # unicode if required.
        if isinstance(msg, bytes):
            msg = msg.decode("ascii", "ignore")

        for command in ("LOGIN", "AUTHENTICATE"):
            if msg.startswith(">") and command in msg:
                msg_start = msg.split(command)[0]
                msg = "{}{} **REDACTED**".format(msg_start, command)
                break
        return super().process(msg, kwargs)
