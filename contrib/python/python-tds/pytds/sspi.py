"""
This module implements wrapper for Windows SSPI API
"""
import logging

from ctypes import (  # type: ignore # needs fixing
    c_ulong,
    c_ushort,
    c_void_p,
    c_ulonglong,
    POINTER,
    Structure,
    c_wchar_p,
    WINFUNCTYPE,
    windll,
    byref,
    cast,
)

logger = logging.getLogger(__name__)


class Status(object):
    SEC_E_OK = 0
    SEC_I_CONTINUE_NEEDED = 0x00090312
    SEC_I_COMPLETE_AND_CONTINUE = 0x00090314
    SEC_I_INCOMPLETE_CREDENTIALS = 0x00090320
    SEC_E_INSUFFICIENT_MEMORY = 0x80090300 - 0x100000000
    SEC_E_INVALID_HANDLE = 0x80090301 - 0x100000000
    SEC_E_UNSUPPORTED_FUNCTION = 0x80090302 - 0x100000000
    SEC_E_INTERNAL_ERROR = 0x80090304 - 0x100000000
    SEC_E_SECPKG_NOT_FOUND = 0x80090305 - 0x100000000
    SEC_E_NOT_OWNER = 0x80090306 - 0x100000000
    SEC_E_INVALID_TOKEN = 0x80090308 - 0x100000000
    SEC_E_NO_IMPERSONATION = 0x8009030B - 0x100000000
    SEC_E_LOGON_DENIED = 0x8009030C - 0x100000000
    SEC_E_UNKNOWN_CREDENTIALS = 0x8009030D - 0x100000000
    SEC_E_NO_CREDENTIALS = 0x8009030E - 0x100000000
    SEC_E_OUT_OF_SEQUENCE = 0x80090310 - 0x100000000
    SEC_E_NO_AUTHENTICATING_AUTHORITY = 0x80090311 - 0x100000000
    SEC_E_BUFFER_TOO_SMALL = 0x80090321 - 0x100000000
    SEC_E_WRONG_PRINCIPAL = 0x80090322 - 0x100000000
    SEC_E_ALGORITHM_MISMATCH = 0x80090331 - 0x100000000

    @classmethod
    def getname(cls, value):
        for name in dir(cls):
            if name.startswith("SEC_E_") and getattr(cls, name) == value:
                return name
        return "unknown value {0:x}".format(0x100000000 + value)


# define SECBUFFER_EMPTY             0   // Undefined, replaced by provider
# define SECBUFFER_DATA              1   // Packet data
SECBUFFER_TOKEN = 2
# define SECBUFFER_PKG_PARAMS        3   // Package specific parameters
# define SECBUFFER_MISSING           4   // Missing Data indicator
# define SECBUFFER_EXTRA             5   // Extra data
# define SECBUFFER_STREAM_TRAILER    6   // Security Trailer
# define SECBUFFER_STREAM_HEADER     7   // Security Header
# define SECBUFFER_NEGOTIATION_INFO  8   // Hints from the negotiation pkg
# define SECBUFFER_PADDING           9   // non-data padding
# define SECBUFFER_STREAM            10  // whole encrypted message
# define SECBUFFER_MECHLIST          11
# define SECBUFFER_MECHLIST_SIGNATURE 12
# define SECBUFFER_TARGET            13  // obsolete
# define SECBUFFER_CHANNEL_BINDINGS  14
# define SECBUFFER_CHANGE_PASS_RESPONSE 15
# define SECBUFFER_TARGET_HOST       16
# define SECBUFFER_ALERT             17

SECPKG_CRED_INBOUND = 0x00000001
SECPKG_CRED_OUTBOUND = 0x00000002
SECPKG_CRED_BOTH = 0x00000003
SECPKG_CRED_DEFAULT = 0x00000004
SECPKG_CRED_RESERVED = 0xF0000000

SECBUFFER_VERSION = 0

# define ISC_REQ_DELEGATE                0x00000001
# define ISC_REQ_MUTUAL_AUTH             0x00000002
ISC_REQ_REPLAY_DETECT = 4
# define ISC_REQ_SEQUENCE_DETECT         0x00000008
ISC_REQ_CONFIDENTIALITY = 0x10
ISC_REQ_USE_SESSION_KEY = 0x00000020
ISC_REQ_PROMPT_FOR_CREDS = 0x00000040
ISC_REQ_USE_SUPPLIED_CREDS = 0x00000080
ISC_REQ_ALLOCATE_MEMORY = 0x00000100
ISC_REQ_USE_DCE_STYLE = 0x00000200
ISC_REQ_DATAGRAM = 0x00000400
ISC_REQ_CONNECTION = 0x00000800
# define ISC_REQ_CALL_LEVEL              0x00001000
# define ISC_REQ_FRAGMENT_SUPPLIED       0x00002000
# define ISC_REQ_EXTENDED_ERROR          0x00004000
# define ISC_REQ_STREAM                  0x00008000
# define ISC_REQ_INTEGRITY               0x00010000
# define ISC_REQ_IDENTIFY                0x00020000
# define ISC_REQ_NULL_SESSION            0x00040000
# define ISC_REQ_MANUAL_CRED_VALIDATION  0x00080000
# define ISC_REQ_RESERVED1               0x00100000
# define ISC_REQ_FRAGMENT_TO_FIT         0x00200000
# // This exists only in Windows Vista and greater
# define ISC_REQ_FORWARD_CREDENTIALS     0x00400000
# define ISC_REQ_NO_INTEGRITY            0x00800000 // honored only by SPNEGO
# define ISC_REQ_USE_HTTP_STYLE          0x01000000
# define ISC_REQ_UNVERIFIED_TARGET_NAME  0x20000000
# define ISC_REQ_CONFIDENTIALITY_ONLY    0x40000000 // honored by SPNEGO/Kerberos

SECURITY_NETWORK_DREP = 0
SECURITY_NATIVE_DREP = 0x10

SECPKG_CRED_ATTR_NAMES = 1

ULONG = c_ulong
USHORT = c_ushort
PULONG = POINTER(ULONG)
PVOID = c_void_p
TimeStamp = c_ulonglong
PTimeStamp = POINTER(c_ulonglong)
PLUID = POINTER(c_ulonglong)


class SecHandle(Structure):
    _fields_ = [
        ("lower", c_void_p),
        ("upper", c_void_p),
    ]


PSecHandle = POINTER(SecHandle)
CredHandle = SecHandle
PCredHandle = PSecHandle
PCtxtHandle = PSecHandle


class SecBuffer(Structure):
    _fields_ = [
        ("cbBuffer", ULONG),
        ("BufferType", ULONG),
        ("pvBuffer", PVOID),
    ]


PSecBuffer = POINTER(SecBuffer)


class SecBufferDesc(Structure):
    _fields_ = [
        ("ulVersion", ULONG),
        ("cBuffers", ULONG),
        ("pBuffers", PSecBuffer),
    ]


PSecBufferDesc = POINTER(SecBufferDesc)


class SEC_WINNT_AUTH_IDENTITY(Structure):
    _fields_ = [
        ("User", c_wchar_p),
        ("UserLength", c_ulong),
        ("Domain", c_wchar_p),
        ("DomainLength", c_ulong),
        ("Password", c_wchar_p),
        ("PasswordLength", c_ulong),
        ("Flags", c_ulong),
    ]


class SecPkgInfo(Structure):
    _fields_ = [
        ("fCapabilities", ULONG),
        ("wVersion", USHORT),
        ("wRPCID", USHORT),
        ("cbMaxToken", ULONG),
        ("Name", c_wchar_p),
        ("Comment", c_wchar_p),
    ]


PSecPkgInfo = POINTER(SecPkgInfo)


class SecPkgCredentials_Names(Structure):
    _fields_ = [("UserName", c_wchar_p)]


def ret_val(value):
    if value < 0:
        raise Exception("SSPI Error {0}".format(Status.getname(value)))
    return value


ENUMERATE_SECURITY_PACKAGES_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    POINTER(c_ulong),
    POINTER(POINTER(SecPkgInfo)),
)

ACQUIRE_CREDENTIALS_HANDLE_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    c_wchar_p,  # principal
    c_wchar_p,  # package
    ULONG,  # fCredentialUse
    PLUID,  # pvLogonID
    PVOID,  # pAuthData
    PVOID,  # pGetKeyFn
    PVOID,  # pvGetKeyArgument
    PCredHandle,  # phCredential
    PTimeStamp,  # ptsExpiry
)
FREE_CREDENTIALS_HANDLE_FN = WINFUNCTYPE(ret_val, POINTER(SecHandle))  # type: ignore # needs fixing
INITIALIZE_SECURITY_CONTEXT_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    PCredHandle,
    PCtxtHandle,  # phContext,
    c_wchar_p,  # pszTargetName,
    ULONG,  # fContextReq,
    ULONG,  # Reserved1,
    ULONG,  # TargetDataRep,
    PSecBufferDesc,  # pInput,
    ULONG,  # Reserved2,
    PCtxtHandle,  # phNewContext,
    PSecBufferDesc,  # pOutput,
    PULONG,  # pfContextAttr,
    PTimeStamp,  # ptsExpiry
)
COMPLETE_AUTH_TOKEN_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    PCtxtHandle,  # phContext
    PSecBufferDesc,  # pToken
)

FREE_CONTEXT_BUFFER_FN = WINFUNCTYPE(ret_val, PVOID)  # type: ignore # needs fixing

QUERY_CREDENTIAL_ATTRIBUTES_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    PCredHandle,  # cred
    ULONG,  # attribute
    PVOID,  # out buffer
)
ACCEPT_SECURITY_CONTEXT_FN = PVOID
DELETE_SECURITY_CONTEXT_FN = WINFUNCTYPE(ret_val, PCtxtHandle)  # type: ignore # needs fixing
APPLY_CONTROL_TOKEN_FN = PVOID
QUERY_CONTEXT_ATTRIBUTES_FN = PVOID
IMPERSONATE_SECURITY_CONTEXT_FN = PVOID
REVERT_SECURITY_CONTEXT_FN = PVOID
MAKE_SIGNATURE_FN = PVOID
VERIFY_SIGNATURE_FN = PVOID
QUERY_SECURITY_PACKAGE_INFO_FN = WINFUNCTYPE(
    ret_val,  # type: ignore # needs fixing
    c_wchar_p,  # package name
    POINTER(PSecPkgInfo),
)
EXPORT_SECURITY_CONTEXT_FN = PVOID
IMPORT_SECURITY_CONTEXT_FN = PVOID
ADD_CREDENTIALS_FN = PVOID
QUERY_SECURITY_CONTEXT_TOKEN_FN = PVOID
ENCRYPT_MESSAGE_FN = PVOID
DECRYPT_MESSAGE_FN = PVOID
SET_CONTEXT_ATTRIBUTES_FN = PVOID


class SECURITY_FUNCTION_TABLE(Structure):
    _fields_ = [
        ("dwVersion", c_ulong),
        ("EnumerateSecurityPackages", ENUMERATE_SECURITY_PACKAGES_FN),
        ("QueryCredentialsAttributes", QUERY_CREDENTIAL_ATTRIBUTES_FN),
        ("AcquireCredentialsHandle", ACQUIRE_CREDENTIALS_HANDLE_FN),
        ("FreeCredentialsHandle", FREE_CREDENTIALS_HANDLE_FN),
        ("Reserved2", c_void_p),
        ("InitializeSecurityContext", INITIALIZE_SECURITY_CONTEXT_FN),
        ("AcceptSecurityContext", ACCEPT_SECURITY_CONTEXT_FN),
        ("CompleteAuthToken", COMPLETE_AUTH_TOKEN_FN),
        ("DeleteSecurityContext", DELETE_SECURITY_CONTEXT_FN),
        ("ApplyControlToken", APPLY_CONTROL_TOKEN_FN),
        ("QueryContextAttributes", QUERY_CONTEXT_ATTRIBUTES_FN),
        ("ImpersonateSecurityContext", IMPERSONATE_SECURITY_CONTEXT_FN),
        ("RevertSecurityContext", REVERT_SECURITY_CONTEXT_FN),
        ("MakeSignature", MAKE_SIGNATURE_FN),
        ("VerifySignature", VERIFY_SIGNATURE_FN),
        ("FreeContextBuffer", FREE_CONTEXT_BUFFER_FN),
        ("QuerySecurityPackageInfo", QUERY_SECURITY_PACKAGE_INFO_FN),
        ("Reserved3", c_void_p),
        ("Reserved4", c_void_p),
        ("ExportSecurityContext", EXPORT_SECURITY_CONTEXT_FN),
        ("ImportSecurityContext", IMPORT_SECURITY_CONTEXT_FN),
        ("AddCredentials", ADD_CREDENTIALS_FN),
        ("Reserved8", c_void_p),
        ("QuerySecurityContextToken", QUERY_SECURITY_CONTEXT_TOKEN_FN),
        ("EncryptMessage", ENCRYPT_MESSAGE_FN),
        ("DecryptMessage", DECRYPT_MESSAGE_FN),
        ("SetContextAttributes", SET_CONTEXT_ATTRIBUTES_FN),
    ]


_PInitSecurityInterface = WINFUNCTYPE(POINTER(SECURITY_FUNCTION_TABLE))
InitSecurityInterface = _PInitSecurityInterface(
    ("InitSecurityInterfaceW", windll.secur32)
)

sec_fn = InitSecurityInterface()
if not sec_fn:
    raise Exception("InitSecurityInterface failed")
sec_fn = sec_fn.contents


class _SecContext(object):
    def __init__(self, cred: "SspiCredentials") -> None:
        self._cred = cred
        self._handle = SecHandle()
        self._ts = TimeStamp()
        self._attrs = ULONG()

    def close(self) -> None:
        if self._handle.lower and self._handle.upper:
            sec_fn.DeleteSecurityContext(self._handle)
            self._handle.lower = self._handle.upper = 0

    def __del__(self) -> None:
        self.close()

    def complete_auth_token(self, bufs):
        sec_fn.CompleteAuthToken(byref(self._handle), byref(_make_buffers_desc(bufs)))

    def next(
        self,
        flags,
        target_name=None,
        byte_ordering="network",
        input_buffers=None,
        output_buffers=None,
    ):
        input_buffers_desc = (
            _make_buffers_desc(input_buffers) if input_buffers else None
        )
        output_buffers_desc = (
            _make_buffers_desc(output_buffers) if output_buffers else None
        )
        status = sec_fn.InitializeSecurityContext(
            byref(self._cred._handle),
            byref(self._handle),
            target_name,
            flags,
            0,
            SECURITY_NETWORK_DREP
            if byte_ordering == "network"
            else SECURITY_NATIVE_DREP,
            byref(input_buffers_desc) if input_buffers_desc else None,
            0,
            byref(self._handle),
            byref(output_buffers_desc) if input_buffers_desc else None,
            byref(self._attrs),
            byref(self._ts),
        )
        result_buffers = []
        for i, (type, buf) in enumerate(output_buffers):
            buf = buf[: output_buffers_desc.pBuffers[i].cbBuffer]
            result_buffers.append((type, buf))
        return status, result_buffers


class SspiCredentials(object):
    def __init__(self, package, use, identity=None):
        self._handle = SecHandle()
        self._ts = TimeStamp()
        logger.debug("Acquiring credentials handle")
        sec_fn.AcquireCredentialsHandle(
            None,
            package,
            use,
            None,
            byref(identity) if identity and identity.Domain else None,
            None,
            None,
            byref(self._handle),
            byref(self._ts),
        )

    def close(self):
        if self._handle and (self._handle.lower or self._handle.upper):
            logger.debug("Releasing credentials handle")
            sec_fn.FreeCredentialsHandle(byref(self._handle))
            self._handle = None

    def __del__(self):
        self.close()

    def query_user_name(self):
        names = SecPkgCredentials_Names()
        try:
            sec_fn.QueryCredentialsAttributes(
                byref(self._handle), SECPKG_CRED_ATTR_NAMES, byref(names)
            )
            user_name = str(names.UserName)
        finally:
            p = c_wchar_p.from_buffer(names, SecPkgCredentials_Names.UserName.offset)
            sec_fn.FreeContextBuffer(p)
        return user_name

    def create_context(
        self,
        flags: int,
        target_name=None,
        byte_ordering="network",
        input_buffers=None,
        output_buffers=None,
    ):
        if self._handle is None:
            raise RuntimeError("Using closed SspiCredentials object")
        ctx = _SecContext(cred=self)
        input_buffers_desc = (
            _make_buffers_desc(input_buffers) if input_buffers else None
        )
        output_buffers_desc = (
            _make_buffers_desc(output_buffers) if output_buffers else None
        )
        logger.debug("Initializing security context")
        status = sec_fn.InitializeSecurityContext(
            byref(self._handle),
            None,
            target_name,
            flags,
            0,
            SECURITY_NETWORK_DREP
            if byte_ordering == "network"
            else SECURITY_NATIVE_DREP,
            byref(input_buffers_desc) if input_buffers_desc else None,
            0,
            byref(ctx._handle),
            byref(output_buffers_desc) if output_buffers_desc else None,
            byref(ctx._attrs),
            byref(ctx._ts),
        )
        result_buffers = []
        for i, (type, buf) in enumerate(output_buffers):
            buf = buf[: output_buffers_desc.pBuffers[i].cbBuffer]
            result_buffers.append((type, buf))
        return ctx, status, result_buffers


def _make_buffers_desc(buffers):
    desc = SecBufferDesc()
    desc.ulVersion = SECBUFFER_VERSION
    bufs_array = (SecBuffer * len(buffers))()
    for i, (type, buf) in enumerate(buffers):
        bufs_array[i].BufferType = type
        bufs_array[i].cbBuffer = len(buf)
        bufs_array[i].pvBuffer = cast(buf, PVOID)
    desc.pBuffers = bufs_array
    desc.cBuffers = len(buffers)
    return desc


def make_winnt_identity(domain, user_name, password):
    identity = SEC_WINNT_AUTH_IDENTITY()
    identity.Flags = 2  # SEC_WINNT_AUTH_IDENTITY_UNICODE
    identity.Password = password
    identity.PasswordLength = len(password)
    identity.Domain = domain
    identity.DomainLength = len(domain)
    identity.User = user_name
    identity.UserLength = len(user_name)
    return identity


# class SspiSecBuffer(object):
#    def __init__(self, type, buflen=4096):
#        self._buf = create_string_buffer(int(buflen))
#        self._desc = SecBuffer()
#        self._desc.cbBuffer = buflen
#        self._desc.BufferType = type
#        self._desc.pvBuffer = cast(self._buf, PVOID)
#
# class SspiSecBuffers(object):
#    def __init__(self):
#        self._desc = SecBufferDesc()
#        self._desc.ulVersion = SECBUFFER_VERSION
#        self._descrs = (SecBuffer * 8)()
#        self._desc.pBuffers = self._descrs
#
#    def append(self, buf):
#        if len(self._descrs) <= self._desc.cBuffers:
#            newdescrs = (SecBuffer * (len(self._descrs) * 2))(*self._descrs)
#            self._descrs = newdescrs
#            self._desc.pBuffers = newdescrs
#        self._descrs[self._desc.cBuffers] = buf._desc
#        self._desc.cBuffers += 1


def enum_security_packages():
    num = ULONG()
    infos = POINTER(SecPkgInfo)()
    sec_fn.EnumerateSecurityPackages(byref(num), byref(infos))
    try:
        return [
            {
                "caps": infos[i].fCapabilities,
                "version": infos[i].wVersion,
                "rpcid": infos[i].wRPCID,
                "max_token": infos[i].cbMaxToken,
                "name": infos[i].Name,
                "comment": infos[i].Comment,
            }
            for i in range(num.value)
        ]
    finally:
        sec_fn.FreeContextBuffer(infos)
