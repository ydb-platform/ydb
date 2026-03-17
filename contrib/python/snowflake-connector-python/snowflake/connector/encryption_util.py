#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import base64
import json
import os
import tempfile
from logging import getLogger
from typing import IO, TYPE_CHECKING

from Crypto.Cipher import AES
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .compat import PKCS5_OFFSET, PKCS5_PAD, PKCS5_UNPAD
from .constants import UTF8, EncryptionMetadata, MaterialDescriptor, kilobyte
from .util_text import random_string

block_size = int(algorithms.AES.block_size / 8)  # in bytes

if TYPE_CHECKING:  # pragma: no cover
    from .storage_client import SnowflakeFileEncryptionMaterial

logger = getLogger(__name__)


def matdesc_to_unicode(matdesc):
    """Convert Material Descriptor to Unicode String."""
    return str(
        json.dumps(
            {
                "queryId": matdesc.query_id,
                "smkId": str(matdesc.smk_id),
                "keySize": str(matdesc.key_size),
            },
            separators=(",", ":"),
        )
    )


class SnowflakeEncryptionUtil:
    @staticmethod
    def get_secure_random(byte_length: int) -> bytes:
        return os.urandom(byte_length)

    @staticmethod
    def encrypt_stream(
        encryption_material: SnowflakeFileEncryptionMaterial,
        src: IO[bytes],
        out: IO[bytes],
        chunk_size: int = 64 * kilobyte,  # block_size * 4 * 1024,
    ) -> EncryptionMetadata:
        """Reads content from src and write the encrypted content into out.

        This function is sensitive to current position of src and out.
        It does not seek to position 0 in neither stream objects before or after the encryption.

        Args:
            encryption_material: The encryption material for file.
            src: The input stream.
            out: The output stream.
            chunk_size: The size of read chunks (Default value = block_size * 4 * 1024

        Returns:
            The encryption metadata.
        """
        logger = getLogger(__name__)
        use_openssl_only = os.getenv("SF_USE_OPENSSL_ONLY", "False") == "True"
        decoded_key = base64.standard_b64decode(
            encryption_material.query_stage_master_key
        )
        key_size = len(decoded_key)
        logger.debug("key_size = %s", key_size)

        # Generate key for data encryption
        iv_data = SnowflakeEncryptionUtil.get_secure_random(block_size)
        file_key = SnowflakeEncryptionUtil.get_secure_random(key_size)
        if not use_openssl_only:
            data_cipher = AES.new(key=file_key, mode=AES.MODE_CBC, IV=iv_data)
        else:
            backend = default_backend()
            cipher = Cipher(
                algorithms.AES(file_key), modes.CBC(iv_data), backend=backend
            )
            encryptor = cipher.encryptor()

        padded = False
        while True:
            chunk = src.read(chunk_size)
            if len(chunk) == 0:
                break
            elif len(chunk) % block_size != 0:
                chunk = PKCS5_PAD(chunk, block_size)
                padded = True
            if not use_openssl_only:
                out.write(data_cipher.encrypt(chunk))
            else:
                out.write(encryptor.update(chunk))
        if not padded:
            if not use_openssl_only:
                out.write(
                    data_cipher.encrypt(block_size * chr(block_size).encode(UTF8))
                )
            else:
                out.write(encryptor.update(block_size * chr(block_size).encode(UTF8)))
        if use_openssl_only:
            out.write(encryptor.finalize())

        # encrypt key with QRMK
        if not use_openssl_only:
            key_cipher = AES.new(key=decoded_key, mode=AES.MODE_ECB)
            enc_kek = key_cipher.encrypt(PKCS5_PAD(file_key, block_size))
        else:
            cipher = Cipher(algorithms.AES(decoded_key), modes.ECB(), backend=backend)
            encryptor = cipher.encryptor()
            enc_kek = (
                encryptor.update(PKCS5_PAD(file_key, block_size)) + encryptor.finalize()
            )

        mat_desc = MaterialDescriptor(
            smk_id=encryption_material.smk_id,
            query_id=encryption_material.query_id,
            key_size=key_size * 8,
        )
        metadata = EncryptionMetadata(
            key=base64.b64encode(enc_kek).decode("utf-8"),
            iv=base64.b64encode(iv_data).decode("utf-8"),
            matdesc=matdesc_to_unicode(mat_desc),
        )
        return metadata

    @staticmethod
    def encrypt_file(
        encryption_material: SnowflakeFileEncryptionMaterial,
        in_filename: str,
        chunk_size: int = 64 * kilobyte,
        tmp_dir: str = None,
    ) -> tuple[EncryptionMetadata, str]:
        """Encrypts a file in a temporary directory.

        Args:
            encryption_material: The encryption material for file.
            in_filename: The input file's name.
            chunk_size: The size of read chunks (Default value = block_size * 4 * 1024).
            tmp_dir: Temporary directory to use, optional (Default value = None).

        Returns:
            The encryption metadata and the encrypted file's location.
        """
        logger = getLogger(__name__)
        temp_output_fd, temp_output_file = tempfile.mkstemp(
            text=False, dir=tmp_dir, prefix=os.path.basename(in_filename) + "#"
        )
        logger.debug(
            "unencrypted file: %s, temp file: %s, tmp_dir: %s",
            in_filename,
            temp_output_file,
            tmp_dir,
        )
        with open(in_filename, "rb") as infile:
            with os.fdopen(temp_output_fd, "wb") as outfile:
                metadata = SnowflakeEncryptionUtil.encrypt_stream(
                    encryption_material, infile, outfile, chunk_size
                )
        return metadata, temp_output_file

    @staticmethod
    def decrypt_stream(
        metadata: EncryptionMetadata,
        encryption_material: SnowflakeFileEncryptionMaterial,
        src: IO[bytes],
        out: IO[bytes],
        chunk_size: int = 64 * kilobyte,  # block_size * 4 * 1024,
    ) -> None:
        """To read from `src` stream then decrypt to `out` stream."""

        use_openssl_only = os.getenv("SF_USE_OPENSSL_ONLY", "False") == "True"
        key_base64 = metadata.key
        iv_base64 = metadata.iv
        decoded_key = base64.standard_b64decode(
            encryption_material.query_stage_master_key
        )
        key_bytes = base64.standard_b64decode(key_base64)
        iv_bytes = base64.standard_b64decode(iv_base64)

        if not use_openssl_only:
            key_cipher = AES.new(key=decoded_key, mode=AES.MODE_ECB)
            file_key = PKCS5_UNPAD(key_cipher.decrypt(key_bytes))
            data_cipher = AES.new(key=file_key, mode=AES.MODE_CBC, IV=iv_bytes)
        else:
            backend = default_backend()
            cipher = Cipher(algorithms.AES(decoded_key), modes.ECB(), backend=backend)
            decryptor = cipher.decryptor()
            file_key = PKCS5_UNPAD(decryptor.update(key_bytes) + decryptor.finalize())
            cipher = Cipher(
                algorithms.AES(file_key), modes.CBC(iv_bytes), backend=backend
            )
            decryptor = cipher.decryptor()

        last_decrypted_chunk = None
        chunk = src.read(chunk_size)
        while len(chunk) != 0:
            if last_decrypted_chunk is not None:
                out.write(last_decrypted_chunk)
            if not use_openssl_only:
                d = data_cipher.decrypt(chunk)
            else:
                d = decryptor.update(chunk)
            last_decrypted_chunk = d
            chunk = src.read(chunk_size)

        if last_decrypted_chunk is not None:
            offset = PKCS5_OFFSET(last_decrypted_chunk)
            out.write(last_decrypted_chunk[:-offset])
        if use_openssl_only:
            out.write(decryptor.finalize())

    @staticmethod
    def decrypt_file(
        metadata: EncryptionMetadata,
        encryption_material: SnowflakeFileEncryptionMaterial,
        in_filename: str,
        chunk_size: int = 64 * kilobyte,
        tmp_dir: str | None = None,
    ) -> str:
        """Decrypts a file and stores the output in the temporary directory.

        Args:
            metadata: The file's metadata input.
            encryption_material: The file's encryption material.
            in_filename: The name of the input file.
            chunk_size: The size of read chunks (Default value = block_size * 4 * 1024).
            tmp_dir: Temporary directory to use, optional (Default value = None).

        Returns:
            The decrypted file's location.
        """
        temp_output_file = f"{os.path.basename(in_filename)}#{random_string()}"
        if tmp_dir:
            temp_output_file = os.path.join(tmp_dir, temp_output_file)

        logger.debug("encrypted file: %s, tmp file: %s", in_filename, temp_output_file)
        with open(in_filename, "rb") as infile:
            with open(temp_output_file, "wb") as outfile:
                SnowflakeEncryptionUtil.decrypt_stream(
                    metadata, encryption_material, infile, outfile, chunk_size
                )
        return temp_output_file
