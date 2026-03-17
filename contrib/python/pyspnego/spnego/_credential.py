# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

"""Credentials that can be used by a client.

These are the various credentials that can be used as a client to authenticate
with a service. See the docstring for each credential type to identify what
credentials are usable for each authenticaton protocol.
"""

import dataclasses
import typing

from spnego._ntlm_raw.crypto import is_ntlm_hash
from spnego.exceptions import InvalidCredentialError, NoCredentialError

# FUTURE: Add Anonymous credential class


@dataclasses.dataclass
class Password:
    """Plaintext username and password credential.

    This is the simple username and password credential that is supported by
    all authentication protocols.

    Attributes:
        username: The username.
        password: The password for the user.
    """

    username: str
    password: str = dataclasses.field(repr=False)

    @property
    def supported_protocols(self) -> typing.List[str]:
        """List of protocols the credential can be used for."""
        return ["credssp", "kerberos", "ntlm"]


@dataclasses.dataclass
class CredentialCache:
    """Cached credential.

    Uses the provider specific cached credential. Can also specify a username
    to select a specific cached credential.

    Attributes:
        username: Optional username used to select a specific credential in the
            cache.
    """

    username: typing.Optional[str] = None

    @property
    def supported_protocols(self) -> typing.List[str]:
        return ["kerberos", "ntlm"]


@dataclasses.dataclass
class NTLMHash:
    """NTLM LM/NT Hash credential.

    Used with :class:`NTLMProxy` for NTLM authentication backed by an NT and/or
    LM hash value. In modern iterations of NTLM only the NT hash needs to be
    specified and LM is ignored but both can be specified and the NTLM code
    will use them as necessary.

    Attributes:
        username: The username the hashes are for.
        lm_hash: The LM hash as a hex string, can be `None` in most cases.
        nt_hash: The NT hash as a hex string.
    """

    username: str
    lm_hash: typing.Optional[str] = dataclasses.field(default=None, repr=False)
    nt_hash: typing.Optional[str] = dataclasses.field(default=None, repr=False)

    @property
    def supported_protocols(self) -> typing.List[str]:
        """List of protocols the credential can be used for."""
        return ["ntlm"]


@dataclasses.dataclass
class KerberosKeytab:
    """Kerberos Keytab Credential.

    Used with :class:`GSSAPIProxy` or :class:`SSPIProxy` for Kerberos
    authentication. It is used to retrieve a Kerberos ticket using a keytab for
    authentication rather than a password.

    Attributes:
        keytab: The keytab to use for authentication. The path will not be
            expanded of have variables substituted so should be the absolute
            path to the keytab.
        principal: The Kerberos principal to get the credential for. Should be
            in the UPN form `username@REALM.COM`. Set to `None` to use the
            first keytab entry.
    """

    keytab: str
    principal: typing.Optional[str] = None

    @property
    def supported_protocols(self) -> typing.List[str]:
        """List of protocols the credential can be used for."""
        return ["kerberos"]


@dataclasses.dataclass
class KerberosCCache:
    """Kerberos CCache Credential.

    Used with :class:`GSSAPIProxy` for Kerberos authentication. It is used to
    specify the credential cache that has the stored Kerberos credential for
    authentication. The ccache value is specified in the form ``TYPE:RESIDUAL``
    where the ``TYPE`` supported is down to the installed Kerberos/GSSAPI
    implementation and ``RESIDUAL`` is a value specific to the type. Common
    types are:

        DIR: The value is the path to a directory containing a collection of
            `FILE` caches.
        FILE: The value is the path to an individual cache.
        MEMORY: The value is a unique identifier to a cache stored in memory of
            the current process. It must be resolvable by the linked GSSAPI
            provider that this library uses.

    There are other ccache types but they are mostly platform or GSSAPI
    implementation specific.

    .. Note:
        This only works on Linux, Windows does not have the concept of
        separate CCaches.

    Attributes:
        ccache: The ccache in the form ``TYPE:RESIDUAL`` to use for a Kerberos
            credential. The path will not be expanded of have variables
            substituted so should be the absolute path to the ccache.
        principal: Optional principal to get in the credential cache specified.
    """

    ccache: str
    principal: typing.Optional[str] = None

    @property
    def supported_protocols(self) -> typing.List[str]:
        """List of protocols the credential can be used for."""
        return ["kerberos"]


# FUTURE: Expose this as an official credential
# @dataclasses.dataclass
# class CredSSPSmartCard:
#     """CredSSP Smart Card Credential.

#     Used with :class:`CredSSPProxy` for CredSSP authentication. It is used to
#     delegate a smart card credential to the peer rather than a
#     username/password. It must be supplied with another NTLM/Kerberos supported
#     credential as this is only used for the delegation part of the CredSSP
#     authentication process.

#     The values are modeled after `MS-CSSP TSSmartCardCreds`_.

#     Attributes:
#         pin: The user's smart card PIN.
#         key_spec: The specification of the user's smart card.
#         card_name: The name of the smart card.
#         reader_name: The name of the smart card reader.
#         container_name: Name of the certificate container.
#         csp_name: Name of the CSP.
#         user_hint: The user's account hint.
#         domain_hint: The user's domain name hint.

#     .. _MS-CSSP TSSmartCardCreds:
#         https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/4251d165-cf01-4513-a5d8-39ee4a98b7a4
#     """
#     pin: bytes
#     key_spec: int
#     card_name: typing.Optional[bytes] = None
#     reader_name: typing.Optional[bytes] = None
#     container_name: typing.Optional[bytes] = None
#     csp_name: typing.Optional[bytes] = None
#     user_hint: typing.Optional[bytes] = None
#     domain_hint: typing.Optional[bytes] = None

#     @property
#     def supported_protocols(self) -> typing.List[str]:
#         return ["credssp"]


# @dataclasses.dataclass
# class CredSSPRemoteGuardPackage:
#     """CredSSP Remote Guard Credential.

#     Used with :class:`CredSSPProxy` for CredSSP authentication. It is used to
#     delegate credential protected by `Remote Credential Guard`_. It must be
#     supplied with another NTLM/Kerberos supported credential as this is only
#     used for the delegation part of the CredSSP authentication process.
#     Multiple instance of this can be passed in with the first entry being the
#     logon credential and the remaining instances being the supplemental
#     credentials.

#     The values are modeled after `MS-CSSP TSRemoteGuardPackageCred`_.

#     .. _Remote Credential Guard:
#         https://docs.microsoft.com/en-us/windows/security/identity-protection/remote-credential-guard

#     .. _MS-CSSP TSRemoteGuardPackageCred:
#         https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/173eee44-1a2c-463f-b909-c15db01e68d7
#     """
#     package_name: str
#     cred_buffer: bytes

#     @property
#     def supported_protocols(self) -> typing.List[str]:
#         return ["credssp"]


Credential = typing.Union[CredentialCache, KerberosCCache, KerberosKeytab, NTLMHash, Password]


def unify_credentials(
    username: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]] = None,
    password: typing.Optional[str] = None,
    required_protocol: typing.Optional[str] = None,
) -> typing.List[Credential]:
    """Process user input credentials.

    Converts the user facing credential input into a list of credentials that
    is known by spnego. Also filters out any duplicate credentials/ones that
    are rendered obsolete by any ones preceding it. For example NTLMHash won't
    be used if it's specified after Password in the credential list.

    Args:
        username: The username or list of credentials to process.
        password: The password for username when it's a string.
        required_protocol: Optionally checks that at least 1 credential must
            support the protocol specified.

    Returns:
        typing.List[Credential]: A list of credentials based on the free-form
        input.
    """
    if username:
        if isinstance(username, str):
            if password is None:
                username = [CredentialCache(username=username)]
            elif is_ntlm_hash(password):
                lm, nt = password.split(":", 1)
                username = [NTLMHash(username=username, lm_hash=lm, nt_hash=nt)]
            else:
                username = [Password(username=username, password=password)]

        elif not isinstance(username, list):
            username = [username]

    else:
        username = [CredentialCache()]

    credentials: typing.List[Credential] = []
    used_protocols: typing.Set[str] = set()
    for cred in username:
        if not isinstance(cred, (CredentialCache, KerberosCCache, KerberosKeytab, NTLMHash, Password)):
            raise InvalidCredentialError(
                context_msg="Invalid username/credential specified, must be a string or Credential object."
            )

        # Remove any credential that only implements protocols that are already covered by one before it.
        # FUTURE: This might be problematic with the CredSSP ones
        cred_useful = False
        for cred_protocol in cred.supported_protocols:
            if cred_protocol not in used_protocols:
                used_protocols.add(cred_protocol)
                cred_useful = True

        if cred_useful:
            credentials.append(cred)

    if required_protocol and required_protocol not in used_protocols:
        found_protocols = ", ".join(sorted(used_protocols))
        raise NoCredentialError(
            context_msg=f"A credential for {required_protocol} is needed but only found "
            f"credentials for {found_protocols}"
        )

    return credentials
