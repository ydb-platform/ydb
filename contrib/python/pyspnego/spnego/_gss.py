# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import base64
import copy
import logging
import sys
import typing

from spnego._context import (
    IOV,
    ContextProxy,
    ContextReq,
    GSSMech,
    IOVUnwrapResult,
    IOVWrapResult,
    SecPkgContextSizes,
    UnwrapResult,
    WinRMWrapResult,
    WrapResult,
    wrap_system_error,
)
from spnego._credential import (
    Credential,
    CredentialCache,
    KerberosCCache,
    KerberosKeytab,
    Password,
    unify_credentials,
)
from spnego._text import to_bytes, to_text
from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import GSSError as NativeError
from spnego.exceptions import (
    InvalidCredentialError,
    NegotiateOptions,
    NoContextError,
    SpnegoError,
)
from spnego.iov import BufferType, IOVBuffer, IOVResBuffer

log = logging.getLogger(__name__)

HAS_GSSAPI = True
GSSAPI_IMP_ERR = None
try:
    import gssapi
    import krb5
    from gssapi.raw import ChannelBindings, GSSError
    from gssapi.raw import exceptions as gss_errors
    from gssapi.raw import inquire_sec_context_by_oid, set_cred_option
except ImportError as e:
    GSSAPI_IMP_ERR = str(e)
    HAS_GSSAPI = False
    log.debug("Python gssapi not available, cannot use any GSSAPIProxy protocols: %s" % e)


HAS_IOV = True
GSSAPI_IOV_IMP_ERR = None
try:
    from gssapi.raw import IOV as GSSIOV
    from gssapi.raw import IOVBuffer as GSSIOVBuffer
    from gssapi.raw import IOVBufferType, unwrap_iov, wrap_iov, wrap_iov_length
except ImportError as e:
    GSSAPI_IOV_IMP_ERR = str(e)
    HAS_IOV = False
    log.debug("Python gssapi IOV extension not available: %s" % GSSAPI_IOV_IMP_ERR)

_GSS_C_INQ_SSPI_SESSION_KEY = "1.2.840.113554.1.2.2.5.5"

_GSS_KRB5_CRED_NO_CI_FLAGS_X = "1.2.752.43.13.29"


def _create_iov_result(iov: "GSSIOV") -> typing.Tuple[IOVResBuffer, ...]:
    """Converts GSSAPI IOV buffer to generic IOVBuffer result."""
    buffers = []
    for i in iov:
        buffer_entry = IOVResBuffer(type=BufferType(i.type), data=i.value)
        buffers.append(buffer_entry)

    return tuple(buffers)


def _get_gssapi_credential(
    mech: "gssapi.OID",
    usage: str,
    credentials: typing.List[Credential],
    context_req: typing.Optional[ContextReq] = None,
) -> typing.Optional["gssapi.creds.Credentials"]:
    """Gets the GSSAPI credential.

    Will get a GSSAPI credential for the mech specified. If the username and password is specified then a new
    set of credentials are explicitly required for the mech specified. Otherwise the credentials are retrieved based on
    the credential type specified.

    Args:
        mech: The mech OID to get the credentials for, only Kerberos is supported.
        usage: Either `initiate` for a client context or `accept` for a server context.
        credentials: List of credentials to retreive from.
        context_req: Context requirement flags that can control how the credential is retrieved.

    Returns:
        gssapi.creds.Credentials: The credential set that was created/retrieved.
    """
    name_type = getattr(gssapi.NameType, "user" if usage == "initiate" else "hostbased_service")
    forwardable = bool(context_req and (context_req & ContextReq.delegate or context_req & ContextReq.delegate_policy))

    for cred in credentials:
        if isinstance(cred, CredentialCache):
            principal = None
            if cred.username:
                principal = gssapi.Name(base=cred.username, name_type=name_type)

            elif usage == "initiate":
                # https://github.com/jborean93/pyspnego/issues/15
                # Using None as a credential when creating the sec context is better than getting the default
                # credential as the former takes into account the target SPN when selecting the principal to use.
                return None

            gss_cred = gssapi.Credentials(name=principal, usage=usage, mechs=[mech])

            # We don't need to check the actual lifetime, just trying to get the valid will have gssapi check the
            # lifetime and raise an ExpiredCredentialsError if it is expired.
            _ = gss_cred.lifetime

            return gss_cred

        elif isinstance(cred, KerberosCCache):
            if usage != "initiate":
                log.debug("Skipping %s as it can only be used for an initiate Kerberos context", cred)
                continue

            ctx = krb5.init_context()
            ccache = krb5.cc_resolve(ctx, to_bytes(cred.ccache))
            krb5_principal: typing.Optional[krb5.Principal] = None
            if cred.principal:
                krb5_principal = krb5.parse_name_flags(ctx, to_bytes(cred.principal))

            return gssapi.Credentials(base=_gss_acquire_cred_from_ccache(ccache, krb5_principal), usage=usage)

        elif isinstance(cred, (KerberosKeytab, Password)):
            if usage != "initiate":
                log.debug("Skipping %s as it can only be used for an initiate Kerberos context", cred)
                continue

            if isinstance(cred, KerberosKeytab):
                username = cred.principal or ""
                password = to_bytes(cred.keytab)
                is_keytab = True
            else:
                username = cred.username
                password = _encode_kerb_password(cred.password)
                is_keytab = False

            raw_cred = _kinit(
                to_bytes(username),
                password,
                forwardable=forwardable,
                is_keytab=is_keytab,
            )

            return gssapi.Credentials(base=raw_cred, usage=usage)

        else:
            log.debug("Skipping credential %s as it does not support required mech type", cred)
            continue

    raise InvalidCredentialError(context_msg="No applicable credentials available")


def _gss_sasl_description(mech: "gssapi.OID") -> typing.Optional[bytes]:
    """Attempts to get the SASL description of the mech specified."""
    try:
        res = _gss_sasl_description.result  # type: ignore
        return res[mech.dotted_form]

    except (AttributeError, KeyError):
        res = getattr(_gss_sasl_description, "result", {})

    try:
        sasl_desc = gssapi.raw.inquire_saslname_for_mech(mech).mech_description
    except Exception as e:
        log.debug("gss_inquire_saslname_for_mech(%s) failed: %s" % (mech.dotted_form, str(e)))
        sasl_desc = None

    res[mech.dotted_form] = sasl_desc
    _gss_sasl_description.result = res  # type: ignore
    return _gss_sasl_description(mech)


def _kinit(
    username: bytes,
    password: bytes,
    forwardable: typing.Optional[bool] = None,
    is_keytab: bool = False,
) -> "gssapi.raw.Creds":
    """Gets a Kerberos credential.

    This will get the GSSAPI credential that contains the Kerberos TGT inside
    it. This is used instead of gss_acquire_cred_with_password as the latter
    does not expose a way to request a forwardable ticket or to retrieve a TGT
    from a keytab. This way makes it possible to request whatever is needed
    before making it usable in GSSAPI.

    Args:
        username: The username to get the credential for.
        password: The password to use to retrieve the credential.
        forwardable: Whether to request a forwardable credential.
        is_keytab: Whether password is a keytab or just a password.

    Returns:
        gssapi.raw.Creds: The GSSAPI credential for the Kerberos mech.
    """
    ctx = krb5.init_context()

    kt: typing.Optional[krb5.KeyTab] = None
    princ: typing.Optional[krb5.Principal] = None
    if is_keytab:
        kt = krb5.kt_resolve(ctx, password)

        # If the username was not specified get the principal of the first entry.
        if not username:
            # The principal handle is deleted once the entry is deallocated. Make sure it is stored in a var before
            # being copied.
            first_entry = list(kt)[0]
            princ = copy.copy(first_entry.principal)

    if not princ:
        princ = krb5.parse_name_flags(ctx, username)

    init_opt = krb5.get_init_creds_opt_alloc(ctx)

    if hasattr(krb5, "get_init_creds_opt_set_default_flags"):
        # Heimdal requires this to be set in order to load the default options from krb5.conf. This follows the same
        # code that it's own gss_acquire_cred_with_password does.
        realm = krb5.principal_get_realm(ctx, princ)
        krb5.get_init_creds_opt_set_default_flags(ctx, init_opt, b"gss_krb5", realm)

    krb5.get_init_creds_opt_set_canonicalize(init_opt, True)
    if forwardable is not None:
        krb5.get_init_creds_opt_set_forwardable(init_opt, forwardable)

    if kt:
        cred = krb5.get_init_creds_keytab(ctx, princ, init_opt, keytab=kt)
    else:
        cred = krb5.get_init_creds_password(ctx, princ, init_opt, password=password)

    mem_ccache = krb5.cc_new_unique(ctx, b"MEMORY")
    krb5.cc_initialize(ctx, mem_ccache, princ)
    krb5.cc_store_cred(ctx, mem_ccache, cred)

    return _gss_acquire_cred_from_ccache(mem_ccache, None)


def _gss_acquire_cred_from_ccache(
    ccache: "krb5.CCache",
    principal: typing.Optional["krb5.Principal"],
) -> "gssapi.raw.Creds":
    """Acquire GSSAPI credential from CCache.

    Args:
        ccache: The CCache to acquire the credential from.
        principal: The optional principal to acquire the cred for.

    Returns:
        gssapi.raw.Creds: The GSSAPI credentials from the ccache.
    """
    # acquire_cred_from is less dangerous than krb5_import_cred which uses a raw pointer to access the ccache. Heimdal
    # has only recently added this API (not in a release as of 2021) so there's a fallback to the latter API.
    if hasattr(gssapi.raw, "acquire_cred_from"):
        kerberos = gssapi.OID.from_int_seq(GSSMech.kerberos.value)
        name = None
        if principal:
            name = gssapi.Name(base=to_text(principal.name), name_type=gssapi.NameType.user)

        ccache_name = ccache.name or b""
        if ccache.cache_type:
            ccache_name = ccache.cache_type + b":" + ccache_name

        return gssapi.raw.acquire_cred_from(
            {b"ccache": ccache_name},
            name=name,
            mechs=[kerberos],
            usage="initiate",
        ).creds

    else:
        gssapi_creds = gssapi.raw.Creds()
        gssapi.raw.krb5_import_cred(
            gssapi_creds, cache=ccache.addr, keytab_principal=principal.addr if principal else None
        )

        return gssapi_creds


def _encode_kerb_password(
    value: str,
) -> bytes:
    """Encode string to use for Kerberos passwords.

    Encodes the input string to use with Kerberos functions as a password. This
    is a special encoding method to ensure that any invalid surrogate chars are
    encoded as the replacement char U+FFFD. This is needed when dealing with
    randomly generated passwords like gMSA or machine accounts. The raw UTF-16
    bytes can be encoded in a string with the following:

        b"...".decode("utf-16-le", errors="surrogatepass")

    The invalid surrogate pairs in the UTF-16 byte sequence will be preserved
    in the str value allowing this function to replace it as needed. This
    means the value can be used with both NTLM and Kerberos authentication with
    the same value.

    Args:
        value: The string to encode to bytes.

    Returns:
        bytes: The encoded string value.
    """
    b_data = []
    for c in value:
        try:
            b_data.append(c.encode("utf-8", errors="strict"))
        except UnicodeEncodeError:
            b_data.append(b"\xef\xbf\xbd")

    return b"".join(b_data)


class GSSAPIProxy(ContextProxy):
    """GSSAPI proxy class for GSSAPI on Linux.

    This proxy class for GSSAPI exposes GSSAPI calls into a common interface for Kerberos authentication. This context
    uses the Python gssapi library to interface with the gss_* calls to provider Kerberos.
    """

    def __init__(
        self,
        username: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]] = None,
        password: typing.Optional[str] = None,
        hostname: typing.Optional[str] = None,
        service: typing.Optional[str] = None,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
        context_req: ContextReq = ContextReq.default,
        usage: str = "initiate",
        protocol: str = "kerberos",
        options: NegotiateOptions = NegotiateOptions.none,
        **kwargs: typing.Any,
    ) -> None:
        if not HAS_GSSAPI:
            raise ImportError("GSSAPIProxy requires the Python gssapi library: %s" % GSSAPI_IMP_ERR)

        credentials = unify_credentials(username, password)
        super(GSSAPIProxy, self).__init__(
            credentials, hostname, service, channel_bindings, context_req, usage, protocol, options
        )

        self._mech = gssapi.OID.from_int_seq(GSSMech.kerberos.value)

        gssapi_credential = kwargs.get("_gssapi_credential", None)
        if not gssapi_credential:
            try:
                gssapi_credential = _get_gssapi_credential(
                    self._mech,
                    self.usage,
                    credentials=credentials,
                    context_req=context_req,
                )
            except GSSError as gss_err:
                raise SpnegoError(base_error=gss_err, context_msg="Getting GSSAPI credential") from gss_err

        if context_req & ContextReq.no_integrity and self.usage == "initiate":
            if gssapi_credential is None:
                gssapi_credential = gssapi.Credentials(usage=self.usage, mechs=[self._mech])

            set_cred_option(
                gssapi.OID.from_int_seq(_GSS_KRB5_CRED_NO_CI_FLAGS_X),
                gssapi_credential,
            )

        self._credential = gssapi_credential
        self._context: typing.Optional[gssapi.SecurityContext] = None

    @classmethod
    def available_protocols(cls, options: typing.Optional[NegotiateOptions] = None) -> typing.List[str]:
        # We can't offer Kerberos if the caller requires WinRM wrapping and IOV isn't available.
        avail = []
        if not (options and options & NegotiateOptions.wrapping_winrm and not HAS_IOV):
            avail.append("kerberos")

        return avail

    @classmethod
    def iov_available(cls) -> bool:
        return HAS_IOV

    @property
    def client_principal(self) -> typing.Optional[str]:
        # Looks like a bug in python-gssapi where the value still has the terminating null char.
        if self._context and self.usage == "accept":
            return to_text(self._context.initiator_name).rstrip("\x00")
        else:
            return None

    @property
    def complete(self) -> bool:
        return self._context is not None and self._context.complete

    @property
    def negotiated_protocol(self) -> typing.Optional[str]:
        return "kerberos"

    @property
    @wrap_system_error(NativeError, "Retrieving session key")
    def session_key(self) -> bytes:
        if self._context:
            return inquire_sec_context_by_oid(self._context, gssapi.OID.from_int_seq(_GSS_C_INQ_SSPI_SESSION_KEY))[0]
        else:
            raise NoContextError(context_msg="Retrieving session key failed as no context was initialized")

    def new_context(self) -> "GSSAPIProxy":
        return GSSAPIProxy(
            hostname=self._hostname,
            service=self._service,
            channel_bindings=self.channel_bindings,
            context_req=self.context_req,
            usage=self.usage,
            protocol=self.protocol,
            options=self.options,
            _gssapi_credential=self._credential,
        )

    @wrap_system_error(NativeError, "Processing security token")
    def step(
        self,
        in_token: typing.Optional[bytes] = None,
        *,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
    ) -> typing.Optional[bytes]:
        if not self._is_wrapped:
            log.debug("GSSAPI step input: %s", base64.b64encode(in_token or b"").decode())

        if not self._context:
            context_kwargs: typing.Dict[str, typing.Any] = {}

            channel_bindings = channel_bindings or self.channel_bindings
            if channel_bindings:
                context_kwargs["channel_bindings"] = ChannelBindings(
                    initiator_address_type=channel_bindings.initiator_addrtype,
                    initiator_address=channel_bindings.initiator_address,
                    acceptor_address_type=channel_bindings.acceptor_addrtype,
                    acceptor_address=channel_bindings.acceptor_address,
                    application_data=channel_bindings.application_data,
                )

            if self.usage == "initiate":
                spn = "%s@%s" % (self._service or "host", self._hostname or "unspecified")
                context_kwargs["name"] = gssapi.Name(spn, name_type=gssapi.NameType.hostbased_service)
                context_kwargs["mech"] = self._mech
                context_kwargs["flags"] = self._context_req

            self._context = gssapi.SecurityContext(creds=self._credential, usage=self.usage, **context_kwargs)

        out_token = self._context.step(in_token)

        try:
            self._context_attr = int(self._context.actual_flags)
        except gss_errors.MissingContextError:  # pragma: no cover
            # MIT krb5 before 1.14.x will raise this error if the context isn't
            # complete. We should only treat it as an error if it happens when
            # the context is complete (last step).
            # https://github.com/jborean93/pyspnego/issues/55
            if self._context.complete:
                raise

        if not self._is_wrapped:
            log.debug("GSSAPI step output: %s", base64.b64encode(out_token or b"").decode())

        return out_token

    @wrap_system_error(NativeError, "Getting context sizes")
    def query_message_sizes(self) -> SecPkgContextSizes:
        if not self._context:
            raise NoContextError(context_msg="Cannot get message sizes until context has been established")

        iov = GSSIOV(
            IOVBufferType.header,
            b"",
            std_layout=False,
        )
        wrap_iov_length(self._context, iov)
        return SecPkgContextSizes(header=len(iov[0].value or b""))

    @wrap_system_error(NativeError, "Wrapping data")
    def wrap(self, data: bytes, encrypt: bool = True, qop: typing.Optional[int] = None) -> WrapResult:
        if not self._context:
            raise NoContextError(context_msg="Cannot wrap until context has been established")
        res = gssapi.raw.wrap(self._context, data, confidential=encrypt, qop=qop)

        return WrapResult(data=res.message, encrypted=res.encrypted)

    @wrap_system_error(NativeError, "Wrapping IOV buffer")
    def wrap_iov(
        self,
        iov: typing.Iterable[IOV],
        encrypt: bool = True,
        qop: typing.Optional[int] = None,
    ) -> IOVWrapResult:
        if not self._context:
            raise NoContextError(context_msg="Cannot wrap until context has been established")

        buffers = self._build_iov_list(iov, self._convert_iov_buffer)
        iov_buffer = GSSIOV(*buffers, std_layout=False)
        encrypted = wrap_iov(self._context, iov_buffer, confidential=encrypt, qop=qop)

        return IOVWrapResult(buffers=_create_iov_result(iov_buffer), encrypted=encrypted)

    def wrap_winrm(self, data: bytes) -> WinRMWrapResult:
        iov = self.wrap_iov([BufferType.header, data, BufferType.padding]).buffers
        header = iov[0].data or b""
        enc_data = iov[1].data or b""
        padding = iov[2].data or b""

        return WinRMWrapResult(header=header, data=enc_data + padding, padding_length=len(padding))

    @wrap_system_error(NativeError, "Unwrapping data")
    def unwrap(self, data: bytes) -> UnwrapResult:
        if not self._context:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        res = gssapi.raw.unwrap(self._context, data)

        return UnwrapResult(data=res.message, encrypted=res.encrypted, qop=res.qop)

    @wrap_system_error(NativeError, "Unwrapping IOV buffer")
    def unwrap_iov(
        self,
        iov: typing.Iterable[IOV],
    ) -> IOVUnwrapResult:
        if not self._context:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        buffers = self._build_iov_list(iov, self._convert_iov_buffer)
        iov_buffer = GSSIOV(*buffers, std_layout=False)
        res = unwrap_iov(self._context, iov_buffer)

        return IOVUnwrapResult(buffers=_create_iov_result(iov_buffer), encrypted=res.encrypted, qop=res.qop)

    def unwrap_winrm(self, header: bytes, data: bytes) -> bytes:
        # This is an extremely weird setup, Kerberos depends on the underlying provider that is used. Right now the
        # proper IOV buffers required to work on both AES and RC4 encrypted only works for MIT KRB5 whereas Heimdal
        # fails. It currently mandates a padding buffer of a variable size which we cannot achieve in the way that
        # WinRM encrypts the data. This is fixed in the source code but until it is widely distributed we just need to
        # use a way that is known to just work with AES. To ensure that MIT works on both RC4 and AES we check the
        # description which differs between the 2 implemtations. It's not perfect but I don't know of another way to
        # achieve this until more time has passed.
        # https://github.com/heimdal/heimdal/issues/739
        if not self._context:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        sasl_desc = _gss_sasl_description(self._context.mech)

        # https://github.com/krb5/krb5/blob/f2e28f13156785851819fc74cae52100e0521690/src/lib/gssapi/krb5/gssapi_krb5.c#L686
        if sasl_desc and sasl_desc == b"Kerberos 5 GSS-API Mechanism":
            iov = self.unwrap_iov([(IOVBufferType.header, header), data, IOVBufferType.data]).buffers
            return iov[1].data or b""

        else:
            return self.unwrap(header + data).data

    @wrap_system_error(NativeError, "Signing message")
    def sign(self, data: bytes, qop: typing.Optional[int] = None) -> bytes:
        if not self._context:
            raise NoContextError(context_msg="Cannot sign until context has been established")

        return gssapi.raw.get_mic(self._context, data, qop=qop)

    @wrap_system_error(NativeError, "Verifying message")
    def verify(self, data: bytes, mic: bytes) -> int:
        if not self._context:
            raise NoContextError(context_msg="Cannot verify until context has been established")

        return gssapi.raw.verify_mic(self._context, data, mic)

    @property
    def _context_attr_map(self) -> typing.List[typing.Tuple[ContextReq, int]]:
        attr_map = [
            (ContextReq.delegate, "delegate_to_peer"),
            (ContextReq.mutual_auth, "mutual_authentication"),
            (ContextReq.replay_detect, "replay_detection"),
            (ContextReq.sequence_detect, "out_of_sequence_detection"),
            (ContextReq.confidentiality, "confidentiality"),
            (ContextReq.integrity, "integrity"),
            (ContextReq.dce_style, "dce_style"),
            # Only present when the DCE extensions are installed.
            (ContextReq.identify, "identify"),
            # Only present with newer versions of python-gssapi https://github.com/pythongssapi/python-gssapi/pull/218.
            (ContextReq.delegate_policy, "ok_as_delegate"),
        ]
        attrs = []
        for spnego_flag, gssapi_name in attr_map:
            if hasattr(gssapi.RequirementFlag, gssapi_name):
                attrs.append((spnego_flag, getattr(gssapi.RequirementFlag, gssapi_name)))

        return attrs

    def _convert_iov_buffer(self, buffer: IOVBuffer) -> "GSSIOVBuffer":
        buffer_data = None
        buffer_alloc = False

        if isinstance(buffer.data, bytes):
            buffer_data = buffer.data
        elif isinstance(buffer.data, bool):
            buffer_alloc = buffer.data
        elif isinstance(buffer.data, int):
            # This shouldn't really occur on GSSAPI but is here to mirror what SSPI does.
            buffer_data = b"\x00" * buffer.data
        else:
            auto_alloc = [BufferType.header, BufferType.padding, BufferType.trailer]
            buffer_alloc = buffer.type in auto_alloc

        buffer_type = buffer.type
        if buffer.type == BufferType.data_readonly:
            # GSSAPI doesn't have the SSPI equivalent of SECBUFFER_READONLY.
            # the GSS_IOV_BUFFER_TYPE_EMPTY seems to produce the same behaviour
            # so that's going to be used instead.
            buffer_type = BufferType.empty

        return GSSIOVBuffer(IOVBufferType(buffer_type), buffer_alloc, buffer_data)
