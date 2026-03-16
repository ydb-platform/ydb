import abc
import enum
import secrets
import struct
from dataclasses import dataclass
from hashlib import sha256, sha384, sha512
from typing import Dict, Optional, Tuple, Union

from asn1crypto import core
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from pyhanko.pdf_utils import generic, misc

from ._legacy import (
    compute_o_value_legacy,
    compute_o_value_legacy_prep,
    compute_u_value_r2,
    compute_u_value_r34,
    legacy_normalise_pw,
)
from ._util import aes_cbc_decrypt, aes_cbc_encrypt, rc4_encrypt
from .api import (
    AuthResult,
    AuthStatus,
    CryptFilter,
    CryptFilterBuilder,
    CryptFilterConfiguration,
    IdentityCryptFilter,
    PdfKeyNotAvailableError,
    SecurityHandler,
    SecurityHandlerVersion,
)
from .cred_ser import SerialisableCredential, SerialisedCredential
from .filter_mixins import (
    AESCryptFilterMixin,
    AESGCMCryptFilterMixin,
    RC4CryptFilterMixin,
)
from .permissions import StandardPermissions


@dataclass
class _R6KeyEntry:
    hash_value: bytes
    validation_salt: bytes
    key_salt: bytes

    @classmethod
    def from_bytes(cls, entry: bytes) -> '_R6KeyEntry':
        assert len(entry) == 48
        return _R6KeyEntry(entry[:32], entry[32:40], entry[40:48])


def _r6_normalise_pw(password: Union[str, bytes]) -> bytes:
    if isinstance(password, str):
        # saslprep expects non-empty strings, apparently
        if not password:
            return b''
        from ._saslprep import saslprep

        password = saslprep(password).encode('utf-8')
    return password[:127]


def _r6_password_authenticate(
    pw_bytes: bytes, entry: _R6KeyEntry, u_entry: Optional[bytes] = None
):
    purported_hash = _r6_hash_algo(pw_bytes, entry.validation_salt, u_entry)
    return purported_hash == entry.hash_value


def _r6_derive_file_key(
    pw_bytes: bytes,
    entry: _R6KeyEntry,
    e_entry: bytes,
    u_entry: Optional[bytes] = None,
):
    interm_key = _r6_hash_algo(pw_bytes, entry.key_salt, u_entry)
    assert len(e_entry) == 32
    return aes_cbc_decrypt(
        key=interm_key, data=e_entry, iv=bytes(16), use_padding=False
    )


_EXPECTED_PERMS_8 = {0x54: True, 0x46: False}  # 'T'  # 'F'


def _bytes_mod_3(input_bytes: bytes):
    # 256 is 1 mod 3, so we can just sum 'em
    return sum(b % 3 for b in input_bytes) % 3


def _r6_hash_algo(
    pw_bytes: bytes, current_salt: bytes, u_entry: Optional[bytes] = None
) -> bytes:
    """
    Algorithm 2.B in ISO 32000-2 § 7.6.4.3.4
    """
    # NOTE: Suppress LGTM warning here, we have to do what the spec says
    initial_hash = sha256(pw_bytes)  # lgtm
    assert len(current_salt) == 8
    initial_hash.update(current_salt)
    if u_entry:
        assert len(u_entry) == 48
        initial_hash.update(u_entry)
    k = initial_hash.digest()
    hashes = (sha256, sha384, sha512)
    round_no = last_byte_val = 0
    while round_no < 64 or last_byte_val > round_no - 32:
        k1 = (pw_bytes + k + (u_entry or b'')) * 64
        e = aes_cbc_encrypt(
            key=k[:16], data=k1, iv=k[16:32], use_padding=False
        )[1]
        # compute the first 16 bytes of e, interpreted as an unsigned integer
        # mod 3
        next_hash = hashes[_bytes_mod_3(e[:16])]
        k = next_hash(e).digest()
        last_byte_val = e[len(e) - 1]
        round_no += 1
    return k[:32]


@enum.unique
class StandardSecuritySettingsRevision(misc.VersionEnum):
    """Indicate the standard security handler revision to emulate."""

    RC4_BASIC = 2
    RC4_EXTENDED = 3
    RC4_OR_AES128 = 4
    AES256 = 6
    AES_GCM = 7
    OTHER = None
    """
    Placeholder value for custom security handlers.
    """

    def as_pdf_object(self) -> generic.PdfObject:
        val = self.value
        return (
            generic.NullObject() if val is None else generic.NumberObject(val)
        )

    @classmethod
    def from_number(cls, value) -> 'StandardSecuritySettingsRevision':
        try:
            return StandardSecuritySettingsRevision(value)
        except ValueError:
            return StandardSecuritySettingsRevision.OTHER


class _PasswordCredential(core.Sequence, SerialisableCredential):
    _fields = [
        ('pwd_bytes', core.OctetString),
        ('id1', core.OctetString, {'optional': True}),
    ]

    @classmethod
    def get_name(cls) -> str:
        return 'pwd_bytes'

    def _ser_value(self) -> bytes:
        return self.dump()

    @classmethod
    def _deser_value(cls, data: bytes):
        try:
            return _PasswordCredential.load(data)
        except ValueError:
            raise misc.PdfReadError("Failed to deserialise password credential")


class StandardCryptFilter(CryptFilter, abc.ABC):
    """
    Crypt filter for use with the standard security handler.
    """

    _handler: Optional['StandardSecurityHandler'] = None

    @property
    def _auth_failed(self):
        if isinstance(self._handler, StandardSecurityHandler):
            return self._handler._auth_failed
        raise NotImplementedError

    def _set_security_handler(self, handler):
        if not isinstance(handler, StandardSecurityHandler):
            raise TypeError  # pragma: nocover
        super()._set_security_handler(handler)
        self._shared_key = None

    def derive_shared_encryption_key(self) -> bytes:
        assert self._handler
        return self._handler.get_file_encryption_key()

    def as_pdf_object(self):
        result = super().as_pdf_object()
        # Specifying the length in bytes is wrong per the 2017 spec,
        # but the 2020 revision mandates doing it this way
        result['/Length'] = generic.NumberObject(self.keylen)
        return result


class StandardAESCryptFilter(StandardCryptFilter, AESCryptFilterMixin):
    """
    AES crypt filter for the standard security handler.
    """

    pass


class StandardAESGCMCryptFilter(StandardCryptFilter, AESGCMCryptFilterMixin):
    """
    AES-GCM crypt filter for the standard security handler.
    """

    pass


class StandardRC4CryptFilter(StandardCryptFilter, RC4CryptFilterMixin):
    """
    RC4 crypt filter for the standard security handler.
    """

    pass


STD_CF = generic.NameObject('/StdCF')


def _std_rc4_config(keylen):
    return CryptFilterConfiguration(
        {STD_CF: StandardRC4CryptFilter(keylen=keylen)},
        default_stream_filter=STD_CF,
        default_string_filter=STD_CF,
    )


def _std_aes_config(keylen):
    return CryptFilterConfiguration(
        {STD_CF: StandardAESCryptFilter(keylen=keylen)},
        default_stream_filter=STD_CF,
        default_string_filter=STD_CF,
    )


def _std_gcm_config():
    return CryptFilterConfiguration(
        {STD_CF: StandardAESGCMCryptFilter()},
        default_stream_filter=STD_CF,
        default_string_filter=STD_CF,
    )


def _build_legacy_standard_crypt_filter(
    cfdict: generic.DictionaryObject, _acts_as_default
):
    keylen_bits = cfdict.get('/Length', 40)
    return StandardRC4CryptFilter(keylen=keylen_bits // 8)


@SecurityHandler.register
class StandardSecurityHandler(SecurityHandler):
    """
    Implementation of the standard (password-based) security handler.

    You shouldn't have to instantiate :class:`.StandardSecurityHandler` objects
    yourself. For encrypting new documents, use :meth:`build_from_pw`
    or :meth:`build_from_pw_legacy`.

    For decrypting existing documents, pyHanko will take care of instantiating
    security handlers through :meth:`.SecurityHandler.build`.
    """

    _known_crypt_filters: Dict[generic.NameObject, CryptFilterBuilder] = {
        generic.NameObject('/V2'): _build_legacy_standard_crypt_filter,
        generic.NameObject('/AESV2'): lambda _, __: StandardAESCryptFilter(
            keylen=16
        ),
        generic.NameObject('/AESV3'): lambda _, __: StandardAESCryptFilter(
            keylen=32
        ),
        generic.NameObject('/AESV4'): lambda _, __: StandardAESGCMCryptFilter(),
        generic.NameObject('/Identity'): lambda _, __: IdentityCryptFilter(),
    }

    @classmethod
    def get_name(cls) -> str:
        return generic.NameObject('/Standard')

    @classmethod
    def build_from_pw_legacy(
        cls,
        rev: StandardSecuritySettingsRevision,
        id1,
        desired_owner_pass,
        desired_user_pass=None,
        keylen_bytes=16,
        use_aes128=True,
        perms: StandardPermissions = StandardPermissions.allow_everything(),
        crypt_filter_config=None,
        encrypt_metadata=True,
        **kwargs,
    ):
        """
        Initialise a legacy password-based security handler, to attach to a
        :class:`~.pyhanko.pdf_utils.writer.PdfFileWriter`.
        Any remaining keyword arguments will be passed to the constructor.

        .. danger::
            The functionality implemented by this handler is deprecated in the
            PDF standard. We only provide it for testing purposes, and to
            interface with legacy systems.

        :param rev:
            Security handler revision to use, see
            :class:`.StandardSecuritySettingsRevision`.
        :param id1:
            The first part of the document ID.
        :param desired_owner_pass:
            Desired owner password.
        :param desired_user_pass:
            Desired user password.
        :param keylen_bytes:
            Length of the key (in bytes).
        :param use_aes128:
            Use AES-128 instead of RC4 (default: ``True``).
        :param perms:
            Permission bits to set
        :param crypt_filter_config:
            Custom crypt filter configuration. PyHanko will supply a reasonable
            default if none is specified.
        :return:
            A :class:`StandardSecurityHandler` instance.
        """
        desired_owner_pass = legacy_normalise_pw(desired_owner_pass)
        desired_user_pass = (
            legacy_normalise_pw(desired_user_pass)
            if desired_user_pass is not None
            else desired_owner_pass
        )
        if rev > StandardSecuritySettingsRevision.RC4_OR_AES128:
            raise ValueError(
                f"{rev} is not supported by this bootstrapping method."
            )
        if rev == StandardSecuritySettingsRevision.RC4_BASIC:
            keylen_bytes = 5
        elif (
            use_aes128 and rev == StandardSecuritySettingsRevision.RC4_OR_AES128
        ):
            keylen_bytes = 16
        o_entry = compute_o_value_legacy(
            desired_owner_pass, desired_user_pass, rev.value, keylen_bytes
        )

        # force perms to a 4-byte format
        if rev == StandardSecuritySettingsRevision.RC4_BASIC:
            # some permissions are not available for these security handlers
            # the default is 'allow'
            perms = (
                perms
                | StandardPermissions.ALLOW_FORM_FILLING
                | StandardPermissions.ALLOW_ASSISTIVE_TECHNOLOGY
                | StandardPermissions.ALLOW_REASSEMBLY
                | StandardPermissions.ALLOW_HIGH_QUALITY_PRINTING
            )
            u_entry, key = compute_u_value_r2(
                desired_user_pass, o_entry, perms, id1
            )
        else:
            u_entry, key = compute_u_value_r34(
                desired_user_pass,
                rev.value,
                keylen_bytes,
                o_entry,
                perms,
                id1,
                encrypt_metadata,
            )

        if rev == StandardSecuritySettingsRevision.RC4_OR_AES128:
            version = SecurityHandlerVersion.RC4_OR_AES128
        elif rev == StandardSecuritySettingsRevision.RC4_BASIC:
            version = SecurityHandlerVersion.RC4_40
        else:
            version = SecurityHandlerVersion.RC4_LONGER_KEYS

        if (
            rev == StandardSecuritySettingsRevision.RC4_OR_AES128
            and crypt_filter_config is None
        ):
            if use_aes128:
                crypt_filter_config = _std_aes_config(keylen=16)
            else:
                crypt_filter_config = _std_rc4_config(keylen=keylen_bytes)

        sh = cls(
            version=version,
            revision=rev,
            legacy_keylen=keylen_bytes,
            perm_flags=perms,
            odata=o_entry,
            udata=u_entry,
            crypt_filter_config=crypt_filter_config,
            encrypt_metadata=encrypt_metadata,
            **kwargs,
        )
        sh._shared_key = key
        sh._credential = _PasswordCredential(
            {'pwd_bytes': desired_owner_pass, 'id1': id1}
        )
        return sh

    @classmethod
    def build_from_pw(
        cls,
        desired_owner_pass,
        desired_user_pass=None,
        perms: StandardPermissions = StandardPermissions.allow_everything(),
        encrypt_metadata=True,
        pdf_mac: bool = True,
        use_gcm: bool = False,
        **kwargs,
    ):
        """
        Initialise a password-based security handler backed by AES-256,
        to attach to a :class:`~.pyhanko.pdf_utils.writer.PdfFileWriter`.
        This handler will use the new PDF 2.0 encryption scheme.

        Any remaining keyword arguments will be passed to the constructor.

        :param desired_owner_pass:
            Desired owner password.
        :param desired_user_pass:
            Desired user password.
        :param perms:
            Desired usage permissions.
        :param encrypt_metadata:
            Whether to set up the security handler for encrypting metadata
            as well.
        :param pdf_mac:
            Include an ISO/TS 32004 MAC.
        :param use_gcm:
            Use AES-GCM (ISO/TS 32003) to encrypt strings and streams.

            .. danger::
                Due to the way PDF encryption works, the authentication
                guarantees of AES-GCM only apply to the content of individual
                strings and streams. The PDF file structure itself is not
                authenticated. Document-level integrity protection is provided
                by the ``pdf_mac=True`` option.

            .. warning::
                This option is disabled by default because support for
                ISO/TS 32003 is not available in mainstream PDF
                software yet. This default may change in the future.
        :return:
            A :class:`StandardSecurityHandler` instance.
        """
        owner_pw_bytes = _r6_normalise_pw(desired_owner_pass)
        user_pw_bytes = (
            _r6_normalise_pw(desired_user_pass)
            if desired_user_pass is not None
            else owner_pw_bytes
        )
        encryption_key = secrets.token_bytes(32)
        u_validation_salt = secrets.token_bytes(8)
        u_key_salt = secrets.token_bytes(8)
        u_hash = _r6_hash_algo(user_pw_bytes, u_validation_salt)
        u_entry = u_hash + u_validation_salt + u_key_salt
        u_interm_key = _r6_hash_algo(user_pw_bytes, u_key_salt)
        _, ue_seed = aes_cbc_encrypt(
            u_interm_key, encryption_key, bytes(16), use_padding=False
        )
        assert len(ue_seed) == 32

        o_validation_salt = secrets.token_bytes(8)
        o_key_salt = secrets.token_bytes(8)
        o_hash = _r6_hash_algo(owner_pw_bytes, o_validation_salt, u_entry)
        o_entry = o_hash + o_validation_salt + o_key_salt
        o_interm_key = _r6_hash_algo(owner_pw_bytes, o_key_salt, u_entry)
        _, oe_seed = aes_cbc_encrypt(
            o_interm_key, encryption_key, bytes(16), use_padding=False
        )
        assert len(oe_seed) == 32

        if pdf_mac:
            # clear bit 13 (1-indexed)
            perms &= ~StandardPermissions.TOLERATE_MISSING_PDF_MAC
        perms_bytes = perms.as_bytes()[::-1]
        extd_perms_bytes = (
            perms_bytes
            + (b'\xff' * 4)
            + (b'T' if encrypt_metadata else b'F')
            + b'adb'
            + secrets.token_bytes(4)
        )

        # need to encrypt one 16 byte block in ECB mode
        #  [I _really_ don't like the way this part of the spec works, but
        #   we have to sacrifice our principles on the altar of backwards
        #   compatibility.]
        cipher = Cipher(algorithms.AES(encryption_key), modes.ECB())
        encryptor = cipher.encryptor()
        encrypted_perms = (
            encryptor.update(extd_perms_bytes) + encryptor.finalize()
        )  # lgtm

        if pdf_mac:
            kdf_salt = secrets.token_bytes(32)
        else:
            kdf_salt = None

        if use_gcm:
            version = SecurityHandlerVersion.AES_GCM
            revision = StandardSecuritySettingsRevision.AES_GCM
        else:
            version = SecurityHandlerVersion.AES256
            revision = StandardSecuritySettingsRevision.AES256

        sh = cls(
            version=version,
            revision=revision,
            legacy_keylen=32,
            perm_flags=perms,
            odata=o_entry,
            udata=u_entry,
            oeseed=oe_seed,
            ueseed=ue_seed,
            encrypted_perms=encrypted_perms,
            encrypt_metadata=encrypt_metadata,
            kdf_salt=kdf_salt,
            **kwargs,
        )
        sh._shared_key = encryption_key
        sh._credential = _PasswordCredential({'pwd_bytes': owner_pw_bytes})
        return sh

    @staticmethod
    def _check_r6_values(udata, odata, oeseed, ueseed, encrypted_perms, rev=6):
        if not (len(udata) == len(odata) == 48):
            raise misc.PdfError(
                "/U and /O entries must be 48 bytes long in a "
                f"rev. {rev} security handler"
            )
        if not oeseed or not ueseed or not (len(oeseed) == len(ueseed) == 32):
            raise misc.PdfError(
                "/UE and /OE must be present and be 32 bytes long in a "
                f"rev. {rev} security handler"
            )
        if not encrypted_perms or len(encrypted_perms) != 16:
            raise misc.PdfError(
                "/Perms must be present and be 16 bytes long in a "
                f"rev. {rev} security handler"
            )

    def __init__(
        self,
        version: SecurityHandlerVersion,
        revision: StandardSecuritySettingsRevision,
        legacy_keylen,  # in bytes, not bits
        perm_flags: StandardPermissions,
        odata,
        udata,
        oeseed=None,
        ueseed=None,
        encrypted_perms=None,
        encrypt_metadata=True,
        crypt_filter_config: Optional[CryptFilterConfiguration] = None,
        compat_entries=True,
        kdf_salt: Optional[bytes] = None,
    ):
        if crypt_filter_config is None:
            if version == SecurityHandlerVersion.RC4_40:
                crypt_filter_config = _std_rc4_config(5)
            elif version == SecurityHandlerVersion.RC4_LONGER_KEYS:
                crypt_filter_config = _std_rc4_config(legacy_keylen)
            elif version == SecurityHandlerVersion.AES_GCM:
                crypt_filter_config = _std_gcm_config()
            elif (
                version >= SecurityHandlerVersion.AES256
                and crypt_filter_config is None
            ):
                # there's a reasonable default config that we can fall back
                # to here
                crypt_filter_config = _std_aes_config(32)
            else:
                raise misc.PdfError(
                    "Could not impute a reasonable crypt filter config"
                )
        super().__init__(
            version,
            legacy_keylen,
            crypt_filter_config,
            encrypt_metadata=encrypt_metadata,
            compat_entries=compat_entries,
            kdf_salt=kdf_salt,
        )
        self.revision = revision
        self.perms = perm_flags
        self._mac_required = not (
            self.perms & StandardPermissions.TOLERATE_MISSING_PDF_MAC
        )
        if revision >= StandardSecuritySettingsRevision.AES256:
            self.__class__._check_r6_values(
                udata, odata, oeseed, ueseed, encrypted_perms
            )
            self.oeseed = oeseed
            self.ueseed = ueseed
            self.encrypted_perms = encrypted_perms
        else:
            if not (len(udata) == len(odata) == 32):
                raise misc.PdfError(
                    "/U and /O entries must be 32 bytes long in a "
                    "legacy security handler"
                )
            self.oeseed = self.ueseed = self.encrypted_perms = None
        self.odata = odata
        self.udata = udata
        self._shared_key: Optional[bytes] = None
        self._auth_failed = False

    @classmethod
    def gather_encryption_metadata(
        cls, encrypt_dict: generic.DictionaryObject
    ) -> dict:
        """
        Gather and preprocess the "easy" metadata values in an encryption
        dictionary, and turn them into constructor kwargs.

        This function processes ``/Length``, ``/P``, ``/Perms``, ``/O``, ``/U``,
        ``/OE``, ``/UE`` and ``/EncryptMetadata``.
        """

        keylen_bits = encrypt_dict.get('/Length', 40)
        if (keylen_bits % 8) != 0:
            raise misc.PdfError("Key length must be a multiple of 8")
        keylen = keylen_bits // 8
        try:
            odata = encrypt_dict['/O']
            udata = encrypt_dict['/U']
        except KeyError:
            raise misc.PdfError("/O and /U entries must be present")

        def _get_bytes(x: generic.PdfObject) -> bytes:
            if not isinstance(
                x, (generic.TextStringObject, generic.ByteStringObject)
            ):
                raise misc.PdfReadError(f"Expected string, but got {type(x)}")
            return x.original_bytes

        def _parse_permissions(x: generic.PdfObject) -> StandardPermissions:
            if isinstance(x, generic.NumberObject):
                return StandardPermissions.from_sint32(x)
            else:
                raise misc.PdfReadError(
                    f"Cannot parse {x} as a permission indicator"
                )

        return dict(
            legacy_keylen=keylen,
            perm_flags=encrypt_dict.get_and_apply(
                '/P',
                _parse_permissions,
                default=StandardPermissions.allow_everything(),
            ),
            odata=odata.original_bytes[:48],
            udata=udata.original_bytes[:48],
            oeseed=encrypt_dict.get_and_apply('/OE', _get_bytes),
            ueseed=encrypt_dict.get_and_apply('/UE', _get_bytes),
            encrypted_perms=encrypt_dict.get_and_apply('/Perms', _get_bytes),
            encrypt_metadata=encrypt_dict.get_and_apply(
                '/EncryptMetadata', bool, default=True
            ),
            kdf_salt=encrypt_dict.get_and_apply(
                '/KDFSalt',
                lambda x: (
                    x.original_bytes
                    if isinstance(
                        x, (generic.TextStringObject, generic.ByteStringObject)
                    )
                    else None
                ),
            ),
        )

    @classmethod
    def instantiate_from_pdf_object(
        cls, encrypt_dict: generic.DictionaryObject
    ):
        v = SecurityHandlerVersion.from_number(encrypt_dict['/V'])
        r = StandardSecuritySettingsRevision.from_number(encrypt_dict['/R'])
        return StandardSecurityHandler(
            version=v,
            revision=r,
            crypt_filter_config=cls.process_crypt_filters(encrypt_dict),
            **cls.gather_encryption_metadata(encrypt_dict),
        )

    @property
    def pdf_mac_enabled(self) -> bool:
        return super().pdf_mac_enabled or self._mac_required

    def as_pdf_object(self):
        result = generic.DictionaryObject()
        result['/Filter'] = generic.NameObject('/Standard')
        result['/O'] = generic.ByteStringObject(self.odata)
        result['/U'] = generic.ByteStringObject(self.udata)
        result['/P'] = generic.NumberObject(self.perms.as_sint32())
        if self._kdf_salt:
            result['/KDFSalt'] = generic.ByteStringObject(self._kdf_salt)
        # this shouldn't be necessary for V5 handlers, but Adobe Reader
        # requires it anyway ...sigh...
        if (
            self._compat_entries
            or self.version == SecurityHandlerVersion.RC4_LONGER_KEYS
        ):
            result['/Length'] = generic.NumberObject(self.keylen * 8)
        result['/V'] = self.version.as_pdf_object()
        result['/R'] = self.revision.as_pdf_object()
        if self.version > SecurityHandlerVersion.RC4_LONGER_KEYS:
            result['/EncryptMetadata'] = generic.BooleanObject(
                self.encrypt_metadata
            )
            result.update(self.crypt_filter_config.as_pdf_object())
        if self.revision >= StandardSecuritySettingsRevision.AES256:
            result['/OE'] = generic.ByteStringObject(self.oeseed)
            result['/UE'] = generic.ByteStringObject(self.ueseed)
            result['/Perms'] = generic.ByteStringObject(self.encrypted_perms)
        return result

    def _auth_user_password_legacy(self, id1: bytes, password):
        rev = self.revision
        user_token = self.udata
        if rev == StandardSecuritySettingsRevision.RC4_BASIC:
            user_tok_supplied, key = compute_u_value_r2(
                password, self.odata, self.perms, id1
            )
        else:
            user_tok_supplied, key = compute_u_value_r34(
                password,
                rev.value,
                self.keylen,
                self.odata,
                self.perms,
                id1,
                self.encrypt_metadata,
            )
            user_tok_supplied = user_tok_supplied[:16]
            user_token = user_token[:16]

        return user_tok_supplied == user_token, key

    def _authenticate_legacy(self, id1: bytes, password):
        cred = _PasswordCredential({'pwd_bytes': password, 'id1': id1})

        # check the owner password first
        rev = self.revision
        key = compute_o_value_legacy_prep(password, rev.value, self.keylen)
        if rev == StandardSecuritySettingsRevision.RC4_BASIC:
            prp_userpass = rc4_encrypt(key, self.odata)
        else:
            val = self.odata
            for i in range(19, -1, -1):
                new_key = bytes(b ^ i for b in key)
                val = rc4_encrypt(new_key, val)
            prp_userpass = val
        owner_password, key = self._auth_user_password_legacy(id1, prp_userpass)
        if owner_password:
            self._credential = cred
            return AuthStatus.OWNER, key

        # next, check the user password
        user_password, key = self._auth_user_password_legacy(id1, password)
        if user_password:
            self._credential = cred
            return AuthStatus.USER, key
        return AuthStatus.FAILED, None

    def authenticate(
        self, credential, id1: Optional[bytes] = None
    ) -> AuthResult:
        """
        Authenticate a user to this security handler.

        :param credential:
            The credential to use (a password in this case).
        :param id1:
            First part of the document ID. This is mandatory for legacy
            encryption handlers, but meaningless otherwise.
        :return:
            An :class:`AuthResult` object indicating the level of access
            obtained.
        """
        if isinstance(credential, SerialisedCredential):
            credential = SerialisableCredential.deserialise(credential)
        if not isinstance(credential, (_PasswordCredential, str, bytes)):
            raise misc.PdfReadError(
                f"Standard authentication credential must be a "
                f"string, byte string or _PasswordCredential, "
                f"not {type(credential)}."
            )
        if isinstance(credential, _PasswordCredential):
            id1 = credential['id1'].native
            credential = credential['pwd_bytes'].native

        res: AuthStatus
        rev = self.revision
        if rev >= StandardSecuritySettingsRevision.AES256:
            res, key = self._authenticate_r6(credential)
        else:
            if id1 is None:
                raise misc.PdfReadError(
                    "id1 must be specified for legacy encryption"
                )
            credential = legacy_normalise_pw(credential)
            res, key = self._authenticate_legacy(id1, credential)
        if key is not None:
            self._shared_key = key
        else:
            self._auth_failed = True
        return AuthResult(status=res, permission_flags=self.perms)

    # Algorithm 2.A in ISO 32000-2 § 7.6.4.3.3
    def _authenticate_r6(self, password) -> Tuple[AuthStatus, Optional[bytes]]:
        pw_bytes = _r6_normalise_pw(password)
        o_entry_split = _R6KeyEntry.from_bytes(self.odata)
        u_entry_split = _R6KeyEntry.from_bytes(self.udata)

        if _r6_password_authenticate(pw_bytes, o_entry_split, self.udata):
            result = AuthStatus.OWNER
            key = _r6_derive_file_key(
                pw_bytes, o_entry_split, self.oeseed, self.udata
            )
        elif _r6_password_authenticate(pw_bytes, u_entry_split):
            result = AuthStatus.USER
            key = _r6_derive_file_key(pw_bytes, u_entry_split, self.ueseed)
        else:
            return AuthStatus.FAILED, None

        # need to encrypt one 16 byte block in ECB mode
        #  [I _really_ don't like the way this part of the spec works, but
        #   we have to sacrifice our principles on the altar of backwards
        #   compatibility.]
        cipher = Cipher(algorithms.AES(key), modes.ECB())
        decryptor = cipher.decryptor()
        decrypted_p_entry = (
            decryptor.update(self.encrypted_perms) + decryptor.finalize()
        )  # lgtm

        # known plaintext mandated in the standard ...sigh...
        perms_ok = decrypted_p_entry[9:12] == b'adb'
        # endianness reversal, also mask off all but the upper 3 bytes
        perms_ok &= self.perms == StandardPermissions.from_uint(
            struct.unpack('<I', decrypted_p_entry[:4])[0]
        )
        try:
            # check encrypt_metadata flag
            decr_metadata_flag = _EXPECTED_PERMS_8[decrypted_p_entry[8]]
            perms_ok &= decr_metadata_flag == self.encrypt_metadata
        except KeyError:
            perms_ok = False

        if not perms_ok:
            raise misc.PdfError(
                "File decryption key didn't decrypt permission flags "
                "correctly -- file permissions may have been tampered with."
            )
        self._credential = _PasswordCredential({'pwd_bytes': pw_bytes})
        return result, key

    def get_file_encryption_key(self) -> bytes:
        """
        Retrieve the (global) file encryption key for this security handler.

        :return:
            The file encryption key as a :class:`bytes` object.
        :raise misc.PdfReadError:
            Raised if this security handler was instantiated from an encryption
            dictionary and no credential is available.
        """
        key = self._shared_key
        if key is None:
            raise PdfKeyNotAvailableError(
                "Authentication failed."
                if self._auth_failed
                else "No key available to decrypt, please authenticate first."
            )
        return key


SerialisableCredential.register(_PasswordCredential)
