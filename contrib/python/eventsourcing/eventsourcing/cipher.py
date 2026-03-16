from __future__ import annotations

import os
from base64 import b64decode, b64encode
from typing import TYPE_CHECKING

from Crypto.Cipher import AES
from Crypto.Cipher._mode_gcm import (
    GcmMode,  # pyright: ignore [reportPrivateImportUsage]
)
from Crypto.Cipher.AES import key_size

from eventsourcing.persistence import Cipher

if TYPE_CHECKING:
    from eventsourcing.utils import Environment


class AESCipher(Cipher):
    """Cipher strategy that uses AES cipher (in GCM mode)
    from the Python pycryptodome package.
    """

    CIPHER_KEY = "CIPHER_KEY"
    KEY_SIZES = key_size

    @staticmethod
    def create_key(num_bytes: int) -> str:
        """Creates AES cipher key, with length num_bytes.

        :param num_bytes: An int value, either 16, 24, or 32.

        """
        AESCipher.check_key_size(num_bytes)
        return b64encode(AESCipher.random_bytes(num_bytes)).decode("utf8")

    @staticmethod
    def check_key_size(num_bytes: int) -> None:
        if num_bytes not in AESCipher.KEY_SIZES:
            msg = f"Invalid key size: {num_bytes} not in {AESCipher.KEY_SIZES}"
            raise ValueError(msg)

    @staticmethod
    def random_bytes(num_bytes: int) -> bytes:
        return os.urandom(num_bytes)

    def __init__(self, environment: Environment):
        """Initialises AES cipher with ``cipher_key``.

        :param str cipher_key: 16, 24, or 32 bytes encoded as base64
        """
        cipher_key = environment.get(self.CIPHER_KEY)
        if not cipher_key:
            msg = f"'{self.CIPHER_KEY}' not in env"
            raise OSError(msg)
        key = b64decode(cipher_key.encode("utf8"))
        AESCipher.check_key_size(len(key))
        self.key = key

    def encrypt(self, plaintext: bytes) -> bytes:
        """Return ciphertext for given plaintext."""
        # Construct AES-GCM cipher, with 96-bit nonce.
        nonce = AESCipher.random_bytes(12)
        cipher = self.construct_cipher(nonce)

        # Encrypt and digest.
        result = cipher.encrypt_and_digest(plaintext)
        encrypted = result[0]
        tag = result[1]

        # Return ciphertext.
        return nonce + tag + encrypted
        # return nonce + tag + encrypted

    def construct_cipher(self, nonce: bytes) -> GcmMode:
        cipher = AES.new(
            self.key,
            AES.MODE_GCM,
            nonce,
        )
        assert isinstance(cipher, GcmMode)
        return cipher

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Return plaintext for given ciphertext."""
        # Split out the nonce, tag, and encrypted data.
        nonce = ciphertext[:12]
        if len(nonce) != 12:
            msg = "Damaged cipher text: invalid nonce length"
            raise ValueError(msg)

        tag = ciphertext[12:28]
        if len(tag) != 16:
            msg = "Damaged cipher text: invalid tag length"
            raise ValueError(msg)
        encrypted = ciphertext[28:]

        # Construct AES cipher, with old nonce.
        cipher = self.construct_cipher(nonce)

        # Decrypt and verify.
        try:
            plaintext = cipher.decrypt_and_verify(encrypted, tag)
        except ValueError as e:
            msg = f"Cipher text is damaged: {e}"
            raise ValueError(msg) from None
        return plaintext
