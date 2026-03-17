import abc
import secrets
import struct
from typing import List, Optional

import cryptography.exceptions
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.crypt._util import (
    aes_cbc_decrypt,
    aes_cbc_encrypt,
    rc4_encrypt,
)
from pyhanko.pdf_utils.crypt.api import CryptFilter, SecurityHandlerVersion

from ..extensions import DeveloperExtension, DevExtensionMultivalued
from ._legacy import legacy_derive_object_key

ISO32003 = DeveloperExtension(
    prefix_name=generic.pdf_name('/ISO_'),
    base_version=generic.pdf_name('/2.0'),
    extension_level=32003,
    extension_revision=':2023',
    url='https://www.iso.org/standard/45876.html',
    compare_by_level=False,
    multivalued=DevExtensionMultivalued.ALWAYS,
)


class RC4CryptFilterMixin(CryptFilter, abc.ABC):
    """
    Mixin for RC4-based crypt filters.

    :param keylen:
        Key length, in bytes. Defaults to 5.
    """

    method = generic.NameObject('/V2')

    def __init__(self, *, keylen=5, **kwargs):
        self._keylen = keylen
        super().__init__(**kwargs)

    @property
    def keylen(self) -> int:
        return self._keylen

    def encrypt(self, key, plaintext: bytes, params=None) -> bytes:
        """
        Encrypt data using RC4.

        :param key:
            Local encryption key.
        :param plaintext:
            Plaintext to encrypt.
        :param params:
            Ignored.
        :return:
            Ciphertext.
        """
        return rc4_encrypt(key, plaintext)

    def decrypt(self, key, ciphertext: bytes, params=None) -> bytes:
        """
        Decrypt data using RC4.

        :param key:
            Local encryption key.
        :param ciphertext:
            Ciphertext to decrypt.
        :param params:
            Ignored.
        :return:
            Plaintext.
        """
        return rc4_encrypt(key, ciphertext)

    def derive_object_key(self, idnum, generation) -> bytes:
        """
        Derive the local key for the given object ID and generation number,
        by calling :func:`.legacy_derive_object_key`.

        :param idnum:
            ID of the object being encrypted.
        :param generation:
            Generation number of the object being encrypted.
        :return:
            The local key.
        """
        return legacy_derive_object_key(self.shared_key, idnum, generation)


class AESCryptFilterMixin(CryptFilter, abc.ABC):
    """Mixin for AES-based crypt filters."""

    def __init__(self, *, keylen: int, **kwargs):
        if keylen not in (16, 32):
            raise NotImplementedError("Only AES-128 and AES-256 are supported")
        self._keylen = keylen
        self._method = (
            generic.NameObject('/AESV2')
            if keylen == 16
            else generic.NameObject('/AESV3')
        )
        super().__init__(**kwargs)

    @property
    def method(self) -> generic.NameObject:
        return self._method

    @property
    def keylen(self) -> int:
        return self._keylen

    def encrypt(self, key, plaintext: bytes, params=None):
        """
        Encrypt data using AES in CBC mode, with PKCS#7 padding.

        :param key:
            The key to use.
        :param plaintext:
            The plaintext to be encrypted.
        :param params:
            Ignored.
        :return:
            The resulting ciphertext, prepended with a 16-byte initialisation
            vector.
        """
        iv, ciphertext = aes_cbc_encrypt(
            key, plaintext, secrets.token_bytes(16)
        )
        return iv + ciphertext

    def decrypt(self, key, ciphertext: bytes, params=None) -> bytes:
        """
        Decrypt data using AES in CBC mode, with PKCS#7 padding.

        :param key:
            The key to use.
        :param ciphertext:
            The ciphertext to be decrypted, prepended with a 16-byte
            initialisation vector.
        :param params:
            Ignored.
        :return:
            The resulting plaintext.
        """
        iv, data = ciphertext[:16], ciphertext[16:]
        return aes_cbc_decrypt(key, data, iv)

    def derive_object_key(self, idnum, generation) -> bytes:
        """
        Derive the local key for the given object ID and generation number.

        If the associated handler is of version
        :attr:`.SecurityHandlerVersion.AES256` or greater, this method
        simply returns the global key as-is.
        If not, the computation is carried out by
        :func:`.legacy_derive_object_key`.

        :param idnum:
            ID of the object being encrypted.
        :param generation:
            Generation number of the object being encrypted.
        :return:
            The local key.
        """
        assert self._handler
        if self._handler.version >= SecurityHandlerVersion.AES256:
            return self.shared_key
        else:
            return legacy_derive_object_key(
                self.shared_key, idnum, generation, use_aes=True
            )


class AESGCMCryptFilterMixin(CryptFilter, abc.ABC):
    """Mixin for AES GCM-based crypt filters (ISO 32003)"""

    method = generic.NameObject('/AESV4')

    def __init__(self: 'AESGCMCryptFilterMixin', **kwargs):
        super().__init__(**kwargs)
        self.__counter: int = 0

    @property
    def keylen(self) -> int:
        return 32

    def _get_nonce(self) -> bytes:
        # nonce is 12 bytes, we use 8 for the counter and 4 random ones
        # (mostly because there's no convenient way to do a 12-byte counter with
        # struct.pack)
        # Crypt filter instances are not shared between documents, so this
        # should be plenty unique enough.
        random_part = secrets.token_bytes(4)
        self.__counter += 1
        counter_part = struct.pack('>Q', self.__counter)
        return random_part + counter_part

    def encrypt(self, key, plaintext: bytes, params=None):
        """
        Encrypt data using AES-GCM.

        :param key:
            The key to use.
        :param plaintext:
            The plaintext to be encrypted.
        :param params:
            Ignored.
        :return:
            The resulting ciphertext and tag, prepended with a 12-byte nonce
        """

        nonce = self._get_nonce()
        ciphertext = AESGCM(key).encrypt(
            nonce=nonce, data=plaintext, associated_data=None
        )
        return nonce + ciphertext

    def decrypt(self, key, ciphertext: bytes, params=None) -> bytes:
        """
        Decrypt data using AES-GCM.

        :param key:
            The key to use.
        :param ciphertext:
            The ciphertext to be decrypted, prepended with a 12-byte
            initialisation vector, and suffixed with the 16-byte authentication
            tag.
        :param params:
            Ignored.
        :return:
            The resulting plaintext.
        """
        nonce, data = ciphertext[:12], ciphertext[12:]
        try:
            plaintext = AESGCM(key).decrypt(
                nonce=nonce, data=data, associated_data=None
            )
        except cryptography.exceptions.InvalidTag:
            raise misc.PdfReadError("Invalid GCM tag")
        return plaintext

    def get_extensions(self) -> Optional[List[DeveloperExtension]]:
        return [ISO32003]
