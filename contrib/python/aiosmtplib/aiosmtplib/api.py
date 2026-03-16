"""
Main public API.
"""

import email.message
import socket
import ssl
from collections.abc import Sequence
from typing import cast

from .response import SMTPResponse
from .smtp import DEFAULT_TIMEOUT, SMTP
from .typing import SMTPTokenGenerator, SocketPathType


__all__ = ("send",)


async def send(
    message: email.message.EmailMessage | email.message.Message | str | bytes,
    /,
    *,
    sender: str | None = None,
    recipients: str | Sequence[str] | None = None,
    mail_options: Sequence[str] | None = None,
    rcpt_options: Sequence[str] | None = None,
    hostname: str | None = "localhost",
    port: int | None = None,
    username: str | bytes | None = None,
    password: str | bytes | None = None,
    oauth_token_generator: SMTPTokenGenerator | None = None,
    local_hostname: str | None = None,
    source_address: tuple[str, int] | None = None,
    timeout: float | None = DEFAULT_TIMEOUT,
    use_tls: bool = False,
    start_tls: bool | None = None,
    validate_certs: bool = True,
    client_cert: str | None = None,
    client_key: str | None = None,
    tls_context: ssl.SSLContext | None = None,
    cert_bundle: str | None = None,
    socket_path: SocketPathType | None = None,
    sock: socket.socket | None = None,
) -> tuple[dict[str, SMTPResponse], str]:
    """
    Send an email message. On await, connects to the SMTP server using the details
    provided, sends the message, then disconnects.

    :param message:  Message text. Either an :py:class:`email.message.EmailMessage`
        object, ``str`` or ``bytes``. If an :py:class:`email.message.EmailMessage`
        object is provided, sender and recipients set in the message headers will be
        used, unless overridden by the respective keyword arguments.
    :keyword sender:  From email address. Not required if an
        :py:class:`email.message.EmailMessage` object is provided for the `message`
        argument.
    :keyword recipients: Recipient email addresses. Not required if an
        :py:class:`email.message.EmailMessage` object is provided for the `message`
        argument.
    :keyword hostname:  Server name (or IP) to connect to. Defaults to "localhost".
    :keyword port: Server port. Defaults ``465`` if ``use_tls`` is ``True``,
        ``587`` if ``start_tls`` is ``True``, or ``25`` otherwise.
    :keyword username:  Username to login as after connect.
    :keyword password:  Password for login after connect. Mutually exclusive with
        ``oauth_token_generator``.
    :keyword oauth_token_generator: An async callable that returns an OAuth2 access
        token for XOAUTH2 authentication. Mutually exclusive with ``password``.
    :keyword local_hostname: The hostname of the client.  If specified, used as the
        FQDN of the local host in the HELO/EHLO command. Otherwise, the result of
        :func:`socket.getfqdn`.
    :keyword source_address: Takes a 2-tuple (host, port) for the socket to bind to
        as its source address before connecting. If the host is '' and port is 0,
        the OS default behavior will be used.
    :keyword timeout: Default timeout value for the connection, in seconds.
        Defaults to 60.
    :keyword use_tls: If True, make the initial connection to the server
        over TLS/SSL. Mutually exclusive with ``start_tls``; if the server uses
        STARTTLS, ``use_tls`` should be ``False``.
    :keyword start_tls: Flag to initiate a STARTTLS upgrade on connect.
        If ``None`` (the default), upgrade will be initiated if supported by the
        server.
        If ``True``, and upgrade will be initiated regardless of server support.
        If ``False``, no upgrade will occur.
        Mutually exclusive with ``use_tls``.
    :keyword validate_certs: Determines if server certificates are
        validated. Defaults to ``True``.
    :keyword client_cert: Path to client side certificate, for TLS.
    :keyword client_key: Path to client side key, for TLS.
    :keyword tls_context: An existing :py:class:`ssl.SSLContext`, for TLS.
        Mutually exclusive with ``client_cert``/``client_key``.
    :keyword cert_bundle: Path to certificate bundle, for TLS verification.
    :keyword socket_path: Path to a Unix domain socket. Not compatible with
        `port` or `sock`. Accepts str, bytes, or a pathlike object.
    :keyword sock: An existing, connected socket object. Not compatible with `port`,
        or `socket_path`. Passing a socket object will transfer control of it to the
        asyncio connection, and it will be closed when the client disconnects.

    :raises ValueError: required arguments missing or mutually exclusive options
        provided
    """
    if not isinstance(message, (email.message.EmailMessage, email.message.Message)):
        if not recipients:
            raise ValueError("Recipients must be provided with raw messages.")
        if not sender:
            raise ValueError("Sender must be provided with raw messages.")

    sender = cast(str, sender)
    recipients = cast(str | Sequence[str], recipients)

    client = SMTP(
        hostname=hostname,
        port=port,
        local_hostname=local_hostname,
        source_address=source_address,
        timeout=timeout,
        use_tls=use_tls,
        start_tls=start_tls,
        validate_certs=validate_certs,
        client_cert=client_cert,
        client_key=client_key,
        tls_context=tls_context,
        cert_bundle=cert_bundle,
        socket_path=socket_path,
        sock=sock,
        username=username,
        password=password,
        oauth_token_generator=oauth_token_generator,
    )

    async with client:
        if isinstance(message, (email.message.EmailMessage, email.message.Message)):
            result = await client.send_message(
                message,
                sender=sender,
                recipients=recipients,
                mail_options=mail_options,
                rcpt_options=rcpt_options,
            )
        else:
            result = await client.sendmail(
                sender,
                recipients,
                message,
                mail_options=mail_options,
                rcpt_options=rcpt_options,
            )

    return result
