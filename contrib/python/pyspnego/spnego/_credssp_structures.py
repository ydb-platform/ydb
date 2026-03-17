# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import typing

from spnego._asn1 import (
    ASN1Value,
    TagClass,
    get_sequence_value,
    pack_asn1,
    pack_asn1_integer,
    pack_asn1_octet_string,
    pack_asn1_sequence,
    unpack_asn1,
    unpack_asn1_integer,
    unpack_asn1_octet_string,
    unpack_asn1_tagged_sequence,
)


def unpack_text_field(
    sequence: typing.Dict[int, ASN1Value],
    idx: int,
    structure: str,
    name: str,
    **kwargs: typing.Optional[str],
) -> typing.Optional[str]:
    """Extracts a text field from a tagged ASN.1 sequence."""
    raw_value = get_sequence_value(sequence, idx, structure, name, unpack_asn1_octet_string)
    if raw_value is None:
        if "default" not in kwargs:
            raise ValueError("Missing mandatory text field '%s' in '%s'" % (name, structure))

        return kwargs["default"]

    return raw_value.decode("utf-16-le")


def unpack_sequence(data: typing.Union[ASN1Value, bytes]) -> typing.Dict[int, ASN1Value]:
    """Helper function that can unpack a sequence as either it's raw bytes or the already unpacked ASN.1 tuple."""
    if not isinstance(data, ASN1Value):
        data = unpack_asn1(data)[0]

    return unpack_asn1_tagged_sequence(data)


class NegoData:
    """CredSSP NegoData structure.

    The NegoData structure contains the SPNEGO tokens, the Kerberos messages, or the NTLM messages. While the
    structure is a SEQUENCE OF SEQUENCE this class just represents an individual SEQUENCE entry.

    The ASN.1 definition for the NegoData structure is defined in `MS-CSSP 2.2.1.1 NegoData`_::

        NegoData ::= SEQUENCE OF SEQUENCE {
                negoToken [0] OCTET STRING
        }

    Args:
        nego_token: One or more SPNEGO tokens and all Kerberos or NTLM messages, as negotiated by SPNEGO.

    Attributes:
        nego_token (bytes): See args.

    .. _MS-CSSP 2.2.1.1 NegoData:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/9664994d-0784-4659-b85b-83b8d54c2336
    """

    def __init__(self, nego_token: bytes) -> None:
        self.nego_token = nego_token

    def pack(self) -> bytes:
        """Packs the NegoData as a byte string."""
        return pack_asn1_sequence(
            [
                pack_asn1(TagClass.context_specific, True, 0, pack_asn1_octet_string(self.nego_token)),
            ]
        )

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "NegoData":
        """Unpacks the NegoData TLV value."""
        nego_data = unpack_sequence(b_data)
        nego_token = get_sequence_value(nego_data, 0, "NegoData", "negoToken", unpack_asn1_octet_string)

        return NegoData(nego_token)


class TSRequest:
    """CredSSP TSRequest structure.

    The TSRequest structure is the top-most structure used by the CredSSP client and CredSSP server. The TSRequest
    message is always sent over the TLS-encrypted channel between the client and server in a CredSSP protocol exchange.

    The ASN.1 definition for the TSRequest structure is defined in `MS-CSSP 2.2.1 TSRequest`_::

        TSRequest ::= SEQUENCE {
                version    [0] INTEGER,
                negoTokens [1] NegoData  OPTIONAL,
                authInfo   [2] OCTET STRING OPTIONAL,
                pubKeyAuth [3] OCTET STRING OPTIONAL,
                errorCode  [4] INTEGER OPTIONAL,
                clientNonce [5] OCTET STRING OPTIONAL
        }

    Args:
        version: THe supported version of the CredSSP protocol.
        nego_tokens: A list of NegoData structures that contains te SPNEGO tokens.
        auth_info: The encrypted TSCredentials structure that contains the user's credentials to be delegated to the
            server.
        pub_key_auth: The encrypted public key used in the TLS handshake between the client and the server.
        error_code: The error code that represents the NTSTATUS failure code from the server.
        client_nonce: A 32-byte array of cryptograpically random bytes for the pub_key_auth hash computation (version
            or above).

    Attributes:
        version (int): See args.
        nego_tokens (Optional[List[int]]): See args.
        auth_info (bytes): See args.
        pub_key_auth (bytes): See args.
        error_code (int): See args.
        client_nonce (bytes): See args.

    .. _MS-CSSP 2.2.1 TSRequest:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/9664994d-0784-4659-b85b-83b8d54c2336
    """

    def __init__(
        self,
        version: int,
        nego_tokens: typing.Optional[typing.Union[NegoData, typing.List[NegoData]]] = None,
        auth_info: typing.Optional[bytes] = None,
        pub_key_auth: typing.Optional[bytes] = None,
        error_code: typing.Optional[int] = None,
        client_nonce: typing.Optional[bytes] = None,
    ) -> None:
        self.version = version

        if nego_tokens is not None and not isinstance(nego_tokens, list):
            nego_tokens = [nego_tokens]
        self.nego_tokens = nego_tokens

        self.auth_info = auth_info
        self.pub_key_auth = pub_key_auth
        self.error_code = error_code
        self.client_nonce = client_nonce

    def pack(self) -> bytes:
        """Packs the TSRequest as a byte string."""
        elements = [pack_asn1(TagClass.context_specific, True, 0, pack_asn1_integer(self.version))]
        if self.nego_tokens:
            nego_tokens = [token.pack() for token in self.nego_tokens]
            elements.append(pack_asn1(TagClass.context_specific, True, 1, pack_asn1_sequence(nego_tokens)))

        value_map: typing.List[typing.Tuple[int, typing.Any, typing.Callable]] = [
            (2, self.auth_info, pack_asn1_octet_string),
            (3, self.pub_key_auth, pack_asn1_octet_string),
            (4, self.error_code, pack_asn1_integer),
            (5, self.client_nonce, pack_asn1_octet_string),
        ]
        for tag, value, pack_func in value_map:
            if value is not None:
                elements.append(pack_asn1(TagClass.context_specific, True, tag, pack_func(value)))

        return pack_asn1_sequence(elements)

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSRequest":
        """Unpacks the TSRequest TLV value."""
        request = unpack_sequence(b_data)

        version = get_sequence_value(request, 0, "TSRequest", "version", unpack_asn1_integer)

        nego_tokens = get_sequence_value(request, 1, "TSRequest", "negoTokens")
        if nego_tokens is not None:
            remaining_bytes = nego_tokens.b_data
            nego_tokens = []
            while remaining_bytes:
                nego_tokens.append(NegoData.unpack(remaining_bytes))
                remaining_bytes = unpack_asn1(remaining_bytes)[1]

        auth_info = get_sequence_value(request, 2, "TSRequest", "authInfo", unpack_asn1_octet_string)
        pub_key_auth = get_sequence_value(request, 3, "TSRequest", "pubKeyAuth", unpack_asn1_octet_string)
        error_code = get_sequence_value(request, 4, "TSRequest", "errorCode", unpack_asn1_integer)
        client_nonce = get_sequence_value(request, 5, "TSRequest", "clientNonce", unpack_asn1_octet_string)

        return TSRequest(
            version,
            nego_tokens=nego_tokens,
            auth_info=auth_info,
            pub_key_auth=pub_key_auth,
            error_code=error_code,
            client_nonce=client_nonce,
        )


class TSCredentials:
    """CredSSP TSCredentials structure.

    The TSCredentials structure contains both the user's credentials that are delegated to the server and their type.

    The ASN.1 definition for the TSCredentials structure is defined in `MS-CSSP 2.2.1.2 TSCredentials`_::

        TSCredentials ::= SEQUENCE {
                credType    [0] INTEGER,
                credentials [1] OCTET STRING
        }

    Args:
        credentials: The credential structure; TSPasswordCreds, TSSmartCardCreds, TSRemoteGuardCreds.

    Attributes:
        credentials (Union[TSPasswordCreds, TSSmartCardCreds, TSRemoteGuardCreds]): See args.

    .. _MS-CSSP 2.2.1.2 TSCredentials:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/94a1ab00-5500-42fd-8d3d-7a84e6c2cf03
    """

    def __init__(self, credentials: typing.Union["TSPasswordCreds", "TSSmartCardCreds", "TSRemoteGuardCreds"]) -> None:
        self.credentials = credentials

    @property
    def cred_type(self) -> int:
        """The credential type of credentials as an integer."""
        if isinstance(self.credentials, TSPasswordCreds):
            return 1

        elif isinstance(self.credentials, TSSmartCardCreds):
            return 2

        elif isinstance(self.credentials, TSRemoteGuardCreds):
            return 6

        else:
            raise ValueError("Invalid credential type set")

    def pack(self) -> bytes:
        """Packs the TSCredentials as a byte string."""
        cred_type = self.cred_type
        credentials = self.credentials.pack()

        return pack_asn1_sequence(
            [
                pack_asn1(TagClass.context_specific, True, 0, pack_asn1_integer(cred_type)),
                pack_asn1(TagClass.context_specific, True, 1, pack_asn1_octet_string(credentials)),
            ]
        )

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSCredentials":
        """Unpacks the TSCredentials TLV value."""
        credential = unpack_sequence(b_data)
        cred_type = get_sequence_value(credential, 0, "TSCredentials", "credType", unpack_asn1_integer)
        credentials_raw = get_sequence_value(credential, 1, "TSCredentials", "credentials", unpack_asn1_octet_string)

        cred_class: typing.Optional[
            typing.Union[typing.Type[TSPasswordCreds], typing.Type[TSSmartCardCreds], typing.Type[TSRemoteGuardCreds]]
        ] = {
            1: TSPasswordCreds,
            2: TSSmartCardCreds,
            6: TSRemoteGuardCreds,
        }.get(
            cred_type
        )
        if not cred_class:
            raise ValueError("Unknown credType %s in TSCredentials, cannot unpack" % cred_type)

        credentials = cred_class.unpack(credentials_raw)
        return TSCredentials(credentials)


class TSPasswordCreds:
    """CredSSP TSPasswordCreds structure.

    The TSPasswordCreds structure contains the user's password credentials that are delegated to the server.

    The ASN.1 definition for the TSPasswordCreds structure is defined in `MS-CSSP 2.2.1.2.1 TSPasswordCreds`_::

        TSPasswordCreds ::= SEQUENCE {
                domainName  [0] OCTET STRING,
                userName    [1] OCTET STRING,
                password    [2] OCTET STRING
        }

    Args:
        domain_name: The name of the user's account domain.
        username: The user's account name.
        password: The user's account password.

    Attributes:
        domain_name (str): See args.
        username (str): See args.
        password (str): See args.

    .. _MS-CSSP 2.2.1.2.1 TSPasswordCreds:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/9664994d-0784-4659-b85b-83b8d54c2336
    """

    def __init__(self, domain_name: str, username: str, password: str) -> None:
        self.domain_name = domain_name
        self.username = username
        self.password = password

    def pack(self) -> bytes:
        """Packs the TSPasswordCreds as a byte string."""
        elements = []
        for idx, value in enumerate([self.domain_name, self.username, self.password]):
            b_value = value.encode("utf-16-le")
            elements.append(pack_asn1(TagClass.context_specific, True, idx, pack_asn1_octet_string(b_value)))

        return pack_asn1_sequence(elements)

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSPasswordCreds":
        """Unpacks the TSPasswordCreds TLV value."""
        creds = unpack_sequence(b_data)

        domain_name = unpack_text_field(creds, 0, "TSPasswordCreds", "domainName") or ""
        username = unpack_text_field(creds, 1, "TSPasswordCreds", "userName") or ""
        password = unpack_text_field(creds, 2, "TSPasswordCreds", "password") or ""

        return TSPasswordCreds(domain_name, username, password)


class TSSmartCardCreds:
    """CredSSP TSSmartCardCreds structure.

    The TSSmartCardCreds structure contains the user's smart card credentials that are delegated to the server.

    The ASN.1 definition for the TSSmartCardCreds structure is defined in `MS-CSSP 2.2.1.2.2 TSSmartCardCreds`_::

        TSSmartCardCreds ::= SEQUENCE {
                pin         [0] OCTET STRING,
                cspData     [1] TSCspDataDetail,
                userHint    [2] OCTET STRING OPTIONAL,
                domainHint  [3] OCTET STRING OPTIONAL
        }

    Args:
        pin: THe user's smart card PIN.
        csp_data: Info about the cryptographic service provider (CSP).
        user_hint: The user's account hint.
        domain_hint: The user's domain name to which the user's account belongs.

    Attributes:
        pin (str): See args.
        csp_data (TSCspDataDetail): See args.
        user_hint (Optional[str]): See args.
        domain_hint (Optional[str]): See args.

    .. _MS-CSSP 2.2.1.2.2 TSSmartCardCreds:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/4251d165-cf01-4513-a5d8-39ee4a98b7a4
    """

    def __init__(
        self,
        pin: str,
        csp_data: "TSCspDataDetail",
        user_hint: typing.Optional[str] = None,
        domain_hint: typing.Optional[str] = None,
    ) -> None:
        self.pin = pin
        self.csp_data = csp_data
        self.user_hint = user_hint
        self.domain_hint = domain_hint

    def pack(self) -> bytes:
        """Packs the TSSmartCardCreds as a byte string."""
        elements = [
            pack_asn1(TagClass.context_specific, True, 0, pack_asn1_octet_string(self.pin.encode("utf-16-le"))),
            pack_asn1(TagClass.context_specific, True, 1, self.csp_data.pack()),
        ]
        for idx, value in [(2, self.user_hint), (3, self.domain_hint)]:
            if value:
                b_value = value.encode("utf-16-le")
                elements.append(pack_asn1(TagClass.context_specific, True, idx, pack_asn1_octet_string(b_value)))

        return pack_asn1_sequence(elements)

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSSmartCardCreds":
        """Unpacks the TSSmartCardCreds TLV value."""
        creds = unpack_sequence(b_data)

        pin = unpack_text_field(creds, 0, "TSSmartCardCreds", "pin") or ""
        csp_data = get_sequence_value(creds, 1, "TSSmartCardCreds", "cspData", TSCspDataDetail.unpack)
        user_hint = unpack_text_field(creds, 2, "TSSmartCardCreds", "userHint", default=None)
        domain_hint = unpack_text_field(creds, 3, "TSSmartCardCreds", "domainHint", default=None)

        return TSSmartCardCreds(pin, csp_data, user_hint, domain_hint)


class TSCspDataDetail:
    """CredSSP TSCspDataDetail structure.

    The TSCspDataDetail structure contains CSP information used during smart card logon.

    The ASN.1 definition for the TSCspDataDetail structure is defined in `MS-CSSP 2.2.1.2.2.1 TSCspDataDetail`_::

        TSCspDataDetail ::= SEQUENCE {
                keySpec       [0] INTEGER,
                cardName      [1] OCTET STRING OPTIONAL,
                readerName    [2] OCTET STRING OPTIONAL,
                containerName [3] OCTET STRING OPTIONAL,
                cspName       [4] OCTET STRING OPTIONAL
        }

    Args:
        key_spec: The specification of the user's smart card.
        card_name: The name of the smart card.
        reader_name: The name of the smart card reader.
        container_name: The name of the certificate container.
        csp_name: The name of the CSP.

    Attributes:
        key_spec (int): See args.
        card_name (Optional[str]): See args.
        reader_name (Optional[str]): See args.
        container_name (Optional[str]): See args.
        csp_name (Optional[str]): See args.

    .. _MS-CSSP 2.2.1.2.2.1 TSCspDataDetail:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/34ee27b3-5791-43bb-9201-076054b58123
    """

    def __init__(
        self,
        key_spec: int,
        card_name: typing.Optional[str] = None,
        reader_name: typing.Optional[str] = None,
        container_name: typing.Optional[str] = None,
        csp_name: typing.Optional[str] = None,
    ) -> None:
        self.key_spec = key_spec
        self.card_name = card_name
        self.reader_name = reader_name
        self.container_name = container_name
        self.csp_name = csp_name

    def pack(self) -> bytes:
        """Packs the TSCspDataDetail as a byte string."""
        elements = [
            pack_asn1(TagClass.context_specific, True, 0, pack_asn1_integer(self.key_spec)),
        ]

        value_map = [
            (1, self.card_name),
            (2, self.reader_name),
            (3, self.container_name),
            (4, self.csp_name),
        ]
        for idx, value in value_map:
            if value:
                b_value = value.encode("utf-16-le")
                elements.append(pack_asn1(TagClass.context_specific, True, idx, pack_asn1_octet_string(b_value)))

        return pack_asn1_sequence(elements)

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSCspDataDetail":
        """Unpacks the TSCspDataDetail TLV value."""
        csp_data = unpack_sequence(b_data)

        key_spec = get_sequence_value(csp_data, 0, "TSCspDataDetail", "keySpec", unpack_asn1_integer)
        card_name = unpack_text_field(csp_data, 1, "TSCspDataDetail", "cardName", default=None)
        reader_name = unpack_text_field(csp_data, 2, "TSCspDataDetail", "readerName", default=None)
        container_name = unpack_text_field(csp_data, 3, "TSCspDataDetail", "containerName", default=None)
        csp_name = unpack_text_field(csp_data, 4, "TSCspDataDetail", "cspName", default=None)

        return TSCspDataDetail(key_spec, card_name, reader_name, container_name, csp_name)


class TSRemoteGuardCreds:
    """CredSSP TSRemoteGuardCreds structure.

    The TSRemoteGuardCreds structure contains a logon credential and supplemental credentials provided by security
    packages. The format of the individual credentials depends on the packages that provided them.

    The ASN.1 definition for the TSRemoteGuardCreds structure is defined in `MS-CSSP 2.2.1.2.3 TSRemoteGuardCreds`_::

        TSRemoteGuardCreds ::= SEQUENCE{
            logonCred           [0] TSRemoteGuardPackageCred,
            supplementalCreds   [1] SEQUENCE OF TSRemoteGuardPackageCred OPTIONAL,
        }

    Args:
        logon_cred: The logon credential for the user.
        supplemental_creds: Optional supplemental credentials for other security packages.

    Attributes:
        logon_cred (TSRemoteGuardPackageCred): See args.
        supplemental_creds (List[TSRemoteGuardPackageCred]): See args.

    .. _MS-CSSP 2.2.1.2.3 TSRemoteGuardCreds:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/7ef8229c-44ea-4c1b-867f-00369b882b38
    """

    def __init__(
        self,
        logon_cred: "TSRemoteGuardPackageCred",
        supplemental_creds: typing.Optional[
            typing.Union["TSRemoteGuardPackageCred", typing.List["TSRemoteGuardPackageCred"]]
        ] = None,
    ) -> None:
        self.logon_cred = logon_cred

        if supplemental_creds is not None and not isinstance(supplemental_creds, list):
            supplemental_creds = [supplemental_creds]
        self.supplemental_creds = supplemental_creds

    def pack(self) -> bytes:
        """Packs the TSRemoteGuardCreds as a byte string."""
        elements = [pack_asn1(TagClass.context_specific, True, 0, self.logon_cred.pack())]
        if self.supplemental_creds is not None:
            supplemental_creds = [cred.pack() for cred in self.supplemental_creds]
            elements.append(pack_asn1(TagClass.context_specific, True, 1, pack_asn1_sequence(supplemental_creds)))

        return pack_asn1_sequence(elements)

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSRemoteGuardCreds":
        """Unpacks the TSRemoteGuardCreds TLV value."""
        cred = unpack_sequence(b_data)
        logon_cred = get_sequence_value(cred, 0, "TSRemoteGuardCreds", "logonCred", TSRemoteGuardPackageCred.unpack)

        raw_supplemental_creds = get_sequence_value(cred, 1, "TSRemoteGuardCreds", "supplementalCreds")
        if raw_supplemental_creds:
            supplemental_creds = []
            remaining_bytes = raw_supplemental_creds.b_data
            while remaining_bytes:
                supplemental_creds.append(TSRemoteGuardPackageCred.unpack(remaining_bytes))
                remaining_bytes = unpack_asn1(remaining_bytes)[1]

        else:
            supplemental_creds = None

        return TSRemoteGuardCreds(logon_cred, supplemental_creds)


class TSRemoteGuardPackageCred:
    """CredSSP TSRemoteGuardPackageCred structure.

    The TSRemoteGuardPackageCred structure contains credentials for a specific security package.

    The ASN.1 definition for the TSRemoteGuardPackageCred structure is defined in
    `MS-CSSP 2.2.1.2.3.1 TSRemoteGuardPackageCred`_::

        TSRemoteGuardPackageCred ::= SEQUENCE{
            packageName [0] OCTET STRING,
            credBuffer  [1] OCTET STRING,
        }

    Args:
        package_name: The name of the packages for which these credentials are intended.
        cred_buffer: The credentials in a format specified by the CredSSP server.

    Attributes:
        package_name (str): See args.
        cred_buffer (bytes): See args.

    .. _MS-CSSP 2.2.1.2.3.1 TSRemoteGuardPackageCred:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/173eee44-1a2c-463f-b909-c15db01e68d7
    """

    def __init__(self, package_name: str, cred_buffer: bytes) -> None:
        self.package_name = package_name
        self.cred_buffer = cred_buffer

    def pack(self) -> bytes:
        """Packs the TSRemoteGuardPackageCred as a byte string."""
        b_package_name = self.package_name.encode("utf-16-le")
        return pack_asn1_sequence(
            [
                pack_asn1(TagClass.context_specific, True, 0, pack_asn1_octet_string(b_package_name)),
                pack_asn1(TagClass.context_specific, True, 1, pack_asn1_octet_string(self.cred_buffer)),
            ]
        )

    @staticmethod
    def unpack(b_data: typing.Union[ASN1Value, bytes]) -> "TSRemoteGuardPackageCred":
        """Unpacks the TSRemoteGuardPackageCred TLV value."""
        package_cred = unpack_sequence(b_data)
        package_name = unpack_text_field(package_cred, 0, "TSRemoteGuardPackageCred", "packageName") or ""
        cred_buffer = get_sequence_value(
            package_cred, 1, "TSRemoteGuardPackageCred", "credBuffer", unpack_asn1_octet_string
        )

        return TSRemoteGuardPackageCred(package_name, cred_buffer)
