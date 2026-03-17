# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import typing

from spnego._context import ContextProxy, ContextReq
from spnego._credential import Credential, NTLMHash, unify_credentials
from spnego._credssp import CredSSPProxy
from spnego._gss import GSSAPIProxy
from spnego._negotiate import NegotiateProxy
from spnego._ntlm import NTLMProxy
from spnego._sspi import SSPIProxy
from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import NegotiateOptions


def _new_context(
    username: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]],
    password: typing.Optional[str],
    hostname: str,
    service: str,
    channel_bindings: typing.Optional[GssChannelBindings],
    context_req: ContextReq,
    protocol: str,
    options: NegotiateOptions,
    usage: str,
    **kwargs: typing.Any,
) -> ContextProxy:
    proto = protocol.lower()
    credentials = unify_credentials(username, password)
    sspi_protocols = SSPIProxy.available_protocols(options=options)
    gssapi_protocols = GSSAPIProxy.available_protocols(options=options)

    # Unless otherwise specified, we always favour the platform implementations (SSPI/GSSAPI) if they are available.
    # Otherwise fallback to the Python implementations (NegotiateProxy/NTLMProxy).
    use_flags = (
        NegotiateOptions.use_sspi
        | NegotiateOptions.use_gssapi
        | NegotiateOptions.use_negotiate
        | NegotiateOptions.use_ntlm
    )
    use_specified = options & use_flags != 0

    # Filter protocols that aren't compatible with the credentials provided.
    sspi_remove = set()
    for cred in credentials:
        # NTLMHash cannot be used with SSPI. The code falls back to using NTLMProxy if this credential is present.
        if isinstance(cred, NTLMHash):
            sspi_remove.add("negotiate")
            sspi_remove.add("ntlm")

    if sspi_remove:
        for protocol in sspi_remove:
            if protocol in sspi_protocols:
                sspi_protocols.remove(protocol)

    proxy: typing.Type[typing.Union[CredSSPProxy, NTLMProxy, SSPIProxy, GSSAPIProxy, NegotiateProxy]]

    # If the protocol is CredSSP then we can only use CredSSPProxy. The use_flags still control what underlying
    # Negotiate auth is used in the CredSSP authentication process.
    if proto == "credssp":
        proxy = CredSSPProxy

    elif options & NegotiateOptions.use_sspi or (not use_specified and proto in sspi_protocols):
        proxy = SSPIProxy

    elif options & NegotiateOptions.use_gssapi or (
        not use_specified and (proto == "kerberos" or proto in gssapi_protocols)
    ):
        proxy = GSSAPIProxy

    elif options & NegotiateOptions.use_negotiate or (not use_specified and proto == "negotiate"):
        # If GSSAPI does not offer full negotiate support, use our own wrapper.
        proxy = NegotiateProxy

    elif options & NegotiateOptions.use_ntlm or (not use_specified and proto == "ntlm"):
        # Finally if GSSAPI does not support ntlm, use our own wrapper.
        proto = "ntlm" if proto == "negotiate" else proto
        proxy = NTLMProxy

    else:
        raise ValueError("Invalid protocol specified '%s', must be kerberos, negotiate, or ntlm" % protocol)

    return proxy(
        username=credentials,
        password=None,
        hostname=hostname,
        service=service,
        channel_bindings=channel_bindings,
        context_req=context_req,
        usage=usage,
        protocol=proto,
        options=options,
        **kwargs,
    )


def client(
    username: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]] = None,
    password: typing.Optional[str] = None,
    hostname: str = "unspecified",
    service: str = "host",
    channel_bindings: typing.Optional[GssChannelBindings] = None,
    context_req: ContextReq = ContextReq.default,
    protocol: str = "negotiate",
    options: NegotiateOptions = NegotiateOptions.none,
    **kwargs: typing.Any,
) -> ContextProxy:
    """Create a client context to be used for authentication.

    Credentials can be provides in 3 different ways:

        * omitted entirely
        * `username` set to a string with an optional password
        * `username` set to a single or list of :class:`Credential` objects

    If the credential is omitted the authentication protocol will use a
    protocol specific mechanism to find the credential in some cache. If no
    cache or credentials are found an exception is raised.

    If `username` is a string but `password` is not defined then the provider
    specific cache is used but for the principal specified. If `password` is
    specified then a new credential is retrieved using the username/password
    combiniation. Using both `username` and `password` is the same as using
    :class:`Password` as a credential.

    If `username` is a, or list of, `Credential` object the object(s) are
    passed into the authentication implementation for use. The behaviour of
    these credentials are defined by the authentication protocol
    implementation that uses the credential(s). Currently the :class:`Passowrd`
    credential is supported by all authentication protocols and is the same
    as specifying the username/password as separate arugments.

    The ``negotiate`` protocol can be given a list of credentials for each of
    the sub auth protocols it negotiates to use. For example to have Negotiate
    use a Kerberos CCache for Kerberos and an NT/LM hash value for NTLM the
    following can be specified::

        import spnego

        credential = [
            spnego.KerberosCCache(ccache="FILE:/tmp/my-ccache"),
            spnego.NTLMHash(
                username="user",
                nt_hash="8ADB9B997580D69E69CAA2BBB68F4697",
            )
        ]
        client = spnego.auth(credential, hostname="server")

    In the example above, the negotiate protocol will attempt use the CCache
    for Kerberos auth and fallback to the NT/LM hash for NTLM auth if
    Kerberos fails. If a credential for a specific sub protocol is not
    available then Negotiate will move onto the next available one.

    Args:
        username: The username/credential(s) to authenticate with. Certain providers can use a cache if omitted.
        password: The password to authenticate with. Should only be specified when username is a string.
        hostname: The principal part of the SPN. This is required for Kerberos auth to build the SPN.
        service: The service part of the SPN. This is required for Kerberos auth to build the SPN.
        channel_bindings: The optional :class:`spnego.channel_bindings.GssChannelBindings` for the context.
        context_req: The :class:`spnego.ContextReq` flags to use when setting up the context.
        protocol: The protocol to authenticate with, can be `ntlm`, `kerberos`, `negotiate`, or `credssp`.
        options: The :class:`spnego.NegotiateOptions` that define pyspnego specific options to control the negotiation.
        kwargs: Optional arguments to pass through to the authentiction context.

    Returns:
        ContextProxy: The context proxy for a client.
    """
    return _new_context(
        username, password, hostname, service, channel_bindings, context_req, protocol, options, "initiate", **kwargs
    )


def server(
    hostname: str = "unspecified",
    service: str = "host",
    channel_bindings: typing.Optional[GssChannelBindings] = None,
    context_req: ContextReq = ContextReq.default,
    protocol: str = "negotiate",
    options: NegotiateOptions = NegotiateOptions.none,
    *,
    credentials: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]] = None,
    **kwargs: typing.Any,
) -> ContextProxy:
    """Create a server context to be used for authentication.

    It is only possible to specify a credential when running on Windows and
    using Kerberos either through Negotiate or Kerberos directly. Other
    platforms and protocols may be supported in the future.

    Args:
        hostname: The principal part of the SPN. This is required for Kerberos auth to build the SPN.
        service: The service part of the SPN. This is required for Kerberos auth to build the SPN.
        channel_bindings: The optional :class:`spnego.channel_bindings.GssChannelBindings` for the context.
        context_req: The :class:`spnego.ContextReq` flags to use when setting up the context.
        protocol: The protocol to authenticate with, can be `ntlm`, `kerberos`, `negotiate`, or `credssp`.
        options: The :class:`spnego.NegotiateOptions` that define pyspnego specific options to control the negotiation.
        credentials: A credential or list or credentials to use with the server auth context.
        kwargs: Optional arguments to pass through to the authentiction context.

    Returns:
        ContextProxy: The context proxy for a client.
    """
    return _new_context(
        credentials, None, hostname, service, channel_bindings, context_req, protocol, options, "accept", **kwargs
    )
