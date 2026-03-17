# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements I/O streams over FTP."""

import logging
import ssl
import urllib.parse
import smart_open.utils
from ftplib import FTP, FTP_TLS, error_reply
import types

logger = logging.getLogger(__name__)

SCHEMES = ("ftp", "ftps")

"""Supported URL schemes."""

DEFAULT_PORT = 21

URI_EXAMPLES = (
    "ftp://username@host/path/file",
    "ftp://username:password@host/path/file",
    "ftp://username:password@host:port/path/file",
    "ftps://username@host/path/file",
    "ftps://username:password@host/path/file",
    "ftps://username:password@host:port/path/file",
)


def _unquote(text):
    return text and urllib.parse.unquote(text)


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES
    return dict(
        scheme=split_uri.scheme,
        uri_path=_unquote(split_uri.path),
        user=_unquote(split_uri.username),
        host=split_uri.hostname,
        port=int(split_uri.port or DEFAULT_PORT),
        password=_unquote(split_uri.password),
    )


def open_uri(uri, mode, transport_params):
    smart_open.utils.check_kwargs(open, transport_params)
    parsed_uri = parse_uri(uri)
    uri_path = parsed_uri.pop("uri_path")
    scheme = parsed_uri.pop("scheme")
    secure_conn = True if scheme == "ftps" else False
    return open(
        uri_path,
        mode,
        secure_connection=secure_conn,
        transport_params=transport_params,
        **parsed_uri,
    )


def convert_transport_params_to_args(transport_params):
    supported_keywords = [
        "timeout",
        "source_address",
        "encoding",
    ]
    unsupported_keywords = [k for k in transport_params if k not in supported_keywords]
    kwargs = {k: v for (k, v) in transport_params.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning(
            "ignoring unsupported ftp keyword arguments: %r", unsupported_keywords
        )

    return kwargs


def _connect(hostname, username, port, password, secure_connection, transport_params):
    kwargs = convert_transport_params_to_args(transport_params)
    if secure_connection:
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ftp = FTP_TLS(context=ssl_context, **kwargs)
    else:
        ftp = FTP(**kwargs)
    try:
        ftp.connect(hostname, port)
    except Exception as e:
        logger.error("Unable to connect to FTP server: try checking the host and port!")
        raise e
    try:
        ftp.login(username, password)
    except error_reply as e:
        logger.error(
            "Unable to login to FTP server: try checking the username and password!"
        )
        raise e
    if secure_connection:
        ftp.prot_p()
    return ftp


def open(
    path,
    mode="rb",
    host=None,
    user=None,
    password=None,
    port=DEFAULT_PORT,
    secure_connection=False,
    transport_params=None,
):
    """Open a file for reading or writing via FTP/FTPS.

    Parameters
    ----------
    path: str
        The path on the remote server
    mode: str
        Must be "rb" or "wb"
    host: str
        The host to connect to
    user: str
        The username to use for the connection
    password: str
        The password for the specified username
    port: int
        The port to connect to
    secure_connection: bool
        True for FTPS, False for FTP
    transport_params: dict
        Additional parameters for the FTP connection.
        Currently supported parameters: timeout, source_address, encoding.
    """
    if not host:
        raise ValueError("you must specify the host to connect to")
    if not user:
        raise ValueError("you must specify the user")
    if not transport_params:
        transport_params = {}
    conn = _connect(host, user, port, password, secure_connection, transport_params)
    mode_to_ftp_cmds = {
        "rb": ("RETR", "rb"),
        "wb": ("STOR", "wb"),
        "ab": ("APPE", "wb"),
    }
    try:
        ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    except KeyError:
        raise ValueError(f"unsupported mode: {mode!r}")
    ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    conn.voidcmd("TYPE I")
    socket = conn.transfercmd(f"{ftp_mode} {path}")
    fobj = socket.makefile(file_obj_mode)

    def full_close(self):
        self.orig_close()
        self.socket.close()
        self.conn.close()

    fobj.orig_close = fobj.close
    fobj.socket = socket
    fobj.conn = conn
    fobj.close = types.MethodType(full_close, fobj)
    return fobj
