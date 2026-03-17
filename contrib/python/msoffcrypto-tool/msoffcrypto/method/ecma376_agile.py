from __future__ import annotations

import base64
import functools
import hmac
import io
import logging
import secrets
from hashlib import sha1, sha256, sha384, sha512
from struct import pack, unpack

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from msoffcrypto import exceptions
from msoffcrypto.method.container.ecma376_encrypted import ECMA376Encrypted

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ALGORITHM_HASH = {
    "SHA1": sha1,
    "SHA256": sha256,
    "SHA384": sha384,
    "SHA512": sha512,
}

blkKey_VerifierHashInput = bytearray([0xFE, 0xA7, 0xD2, 0x76, 0x3B, 0x4B, 0x9E, 0x79])
blkKey_encryptedVerifierHashValue = bytearray(
    [0xD7, 0xAA, 0x0F, 0x6D, 0x30, 0x61, 0x34, 0x4E]
)
blkKey_encryptedKeyValue = bytearray([0x14, 0x6E, 0x0B, 0xE7, 0xAB, 0xAC, 0xD0, 0xD6])
blkKey_dataIntegrity1 = bytearray([0x5F, 0xB2, 0xAD, 0x01, 0x0C, 0xB9, 0xE1, 0xF6])
blkKey_dataIntegrity2 = bytearray([0xA0, 0x67, 0x7F, 0x02, 0xB2, 0x2C, 0x84, 0x33])


def _random_buffer(sz):
    return secrets.token_bytes(sz)


def _get_num_blocks(sz, block):
    return (sz + block - 1) // block


def _round_up(sz, block):
    return _get_num_blocks(sz, block) * block


def _resize_buffer(buf, n, c=b"\0"):
    if len(buf) >= n:
        return buf[:n]

    return buf + c * (n - len(buf))


def _normalize_key(key, n):
    return _resize_buffer(key, n, b"\x36")


def _get_hash_func(algorithm):
    return ALGORITHM_HASH.get(algorithm, sha1)


def _decrypt_aes_cbc(data, key, iv):
    aes = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = aes.decryptor()
    decrypted = decryptor.update(data) + decryptor.finalize()
    return decrypted


def _encrypt_aes_cbc(data, key, iv):
    aes = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())

    encryptor = aes.encryptor()
    encrypted = encryptor.update(data) + encryptor.finalize()

    return encrypted


def _encrypt_aes_cbc_padded(data, key, iv, blockSize):
    buf = data

    if len(buf) % blockSize:
        buf = _resize_buffer(buf, _round_up(len(buf), blockSize))

    return _encrypt_aes_cbc(buf, key, iv)


def _get_salt(salt_value=None, salt_size=16):
    if salt_value is not None:
        if len(salt_value) != salt_size:
            raise exceptions.EncryptionError(
                f"Invalid salt value size, should be {salt_size}"
            )

        return salt_value

    return _random_buffer(salt_size)


# Hardcoded to AES256 + SHA512 for OOXML.
class ECMA376AgileCipherParams:
    def __init__(self):
        self.cipherName = "AES"
        self.hashName = "SHA512"
        self.saltSize = 16
        self.blockSize = 16
        self.keyBits = 256
        self.hashSize = 64
        self.saltValue: bytes | None = None


def _enc64(b):
    return base64.b64encode(b).decode("UTF-8")


class ECMA376AgileEncryptionInfo:
    def __init__(self):
        self.spinCount = 100000
        self.keyData = ECMA376AgileCipherParams()
        self.encryptedHmacKey: bytes | None = None
        self.encryptedHmacValue: bytes | None = None

        self.encryptedKey = ECMA376AgileCipherParams()
        self.encryptedVerifierHashInput: bytes | None = None
        self.encryptedVerifierHashValue: bytes | None = None
        self.encryptedKeyValue: bytes | None = None

    def getEncryptionDescriptorHeader(self):
        # https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/87020a34-e73f-4139-99bc-bbdf6cf6fa55
        return pack("<HHI", 4, 4, 0x40)

    def toEncryptionDescriptor(self):
        """
        Returns an XML description of the encryption information.
        """
        return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<encryption xmlns="http://schemas.microsoft.com/office/2006/encryption" xmlns:p="http://schemas.microsoft.com/office/2006/keyEncryptor/password" xmlns:c="http://schemas.microsoft.com/office/2006/keyEncryptor/certificate">
    <keyData saltSize="{self.keyData.saltSize}" blockSize="{self.keyData.blockSize}" keyBits="{self.keyData.keyBits}" hashSize="{self.keyData.hashSize}"
             cipherAlgorithm="{self.keyData.cipherName}" cipherChaining="ChainingModeCBC" hashAlgorithm="{self.keyData.hashName}" saltValue="{_enc64(self.keyData.saltValue)}" />
    <dataIntegrity encryptedHmacKey="{_enc64(self.encryptedHmacKey)}" encryptedHmacValue="{_enc64(self.encryptedHmacValue)}" />
    <keyEncryptors>
        <keyEncryptor uri="http://schemas.microsoft.com/office/2006/keyEncryptor/password">
            <p:encryptedKey spinCount="{self.spinCount}" saltSize="{self.encryptedKey.saltSize}" blockSize="{self.encryptedKey.blockSize}" keyBits="{self.encryptedKey.keyBits}"
                            hashSize="{self.encryptedKey.hashSize}" cipherAlgorithm="{self.encryptedKey.cipherName}" cipherChaining="ChainingModeCBC" hashAlgorithm="{self.encryptedKey.hashName}"
                            saltValue="{_enc64(self.encryptedKey.saltValue)}" encryptedVerifierHashInput="{_enc64(self.encryptedVerifierHashInput)}"
                            encryptedVerifierHashValue="{_enc64(self.encryptedVerifierHashValue)}" encryptedKeyValue="{_enc64(self.encryptedKeyValue)}" />
        </keyEncryptor>
    </keyEncryptors>
</encryption>
"""


def _generate_iv(params: ECMA376AgileCipherParams, blkKey, salt_value):
    if not blkKey:
        return _normalize_key(salt_value, params.blockSize)

    hashCalc = _get_hash_func(params.hashName)

    return _normalize_key(hashCalc(salt_value + blkKey).digest(), params.blockSize)


class ECMA376Agile:
    def __init__(self):
        pass

    @staticmethod
    def _derive_iterated_hash_from_password(
        password, saltValue, hashAlgorithm, spinValue
    ):
        r"""
        Do a partial password-based hash derivation.
        Note the block key is not taken into consideration in this function.
        """
        # TODO: This function is quite expensive and it should only be called once.
        # We need to save the result for later use.
        # This is not covered by the specification, but MS Word does so.

        hashCalc = _get_hash_func(hashAlgorithm)

        # NOTE: Initial round sha512(salt + password)
        h = hashCalc(saltValue + password.encode("UTF-16LE"))

        # NOTE: Iteration of 0 -> spincount-1; hash = sha512(iterator + hash)
        for i in range(0, spinValue, 1):
            h = hashCalc(pack("<I", i) + h.digest())

        return h

    @staticmethod
    def _derive_encryption_key(h, blockKey, hashAlgorithm, keyBits):
        r"""
        Finish the password-based key derivation by hashing last hash + blockKey.
        """
        hashCalc = _get_hash_func(hashAlgorithm)
        h_final = hashCalc(h + blockKey)

        # NOTE: Needed to truncate encryption key to bitsize
        encryption_key = h_final.digest()[: keyBits // 8]

        return encryption_key

    @staticmethod
    def decrypt(key, keyDataSalt, hashAlgorithm, ibuf):
        r"""
        Return decrypted data.

            >>> key = b'@ f\t\xd9\xfa\xad\xf2K\x07j\xeb\xf2\xc45\xb7B\x92\xc8\xb8\xa7\xaa\x81\xbcg\x9b\xe8\x97\x11\xb0*\xc2'
            >>> keyDataSalt = b'\x8f\xc7x"+P\x8d\xdcL\xe6\x8c\xdd\x15<\x16\xb4'
            >>> hashAlgorithm = 'SHA512'
        """
        # NOTE: See https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/9e61da63-8ddb-4c0a-b25d-f85d990f44c8
        SEGMENT_LENGTH = 4096
        hashCalc = _get_hash_func(hashAlgorithm)

        obuf = io.BytesIO()

        # NOTE: See https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/b60c8b35-2db2-4409-8710-59d88a793f83
        ibuf.seek(0)
        totalSize = unpack("<Q", ibuf.read(8))
        totalSize = totalSize[0]
        logger.debug("totalSize: {}".format(totalSize))
        remaining = totalSize
        for i, buf in enumerate(
            iter(functools.partial(ibuf.read, SEGMENT_LENGTH), b"")
        ):
            saltWithBlockKey = keyDataSalt + pack("<I", i)
            iv = hashCalc(saltWithBlockKey).digest()
            iv = iv[:16]
            aes = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
            decryptor = aes.decryptor()
            dec = decryptor.update(buf) + decryptor.finalize()
            # TODO: Check
            if remaining < len(dec):
                dec = dec[:remaining]
            obuf.write(dec)
            remaining -= len(dec)
            # TODO: Check if this is needed
            if remaining <= 0:
                break
        return obuf.getvalue()  # return obuf.getbuffer()

    @staticmethod
    def encrypt(key, ibuf, salt_value=None, spin_count=100000):
        """
        Return an OLE compound file buffer (complete with headers) which contains ibuf encrypted into a single stream.

        When salt_value is not specified (the default), we generate a random one.
        """

        # Encryption ported from C++ (https://github.com/herumi/msoffice, BSD-3)

        info, secret_key = ECMA376Agile.generate_encryption_parameters(
            key, salt_value, spin_count
        )
        encrypted_data = ECMA376Agile.encrypt_payload(
            ibuf, info.encryptedKey, secret_key, info.keyData.saltValue
        )
        encryption_info = ECMA376Agile.get_encryption_information(
            info, encrypted_data, secret_key
        )

        obuf = io.BytesIO()
        ECMA376Encrypted(encrypted_data, encryption_info).write_to(obuf)

        return obuf.getvalue()

    @staticmethod
    def get_encryption_information(
        info: ECMA376AgileEncryptionInfo, encrypted_data, secretKey
    ):
        """
        Return the content of an EncryptionInfo Stream, including the short header, per the specifications at

        https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/87020a34-e73f-4139-99bc-bbdf6cf6fa55
        """
        hmacKey, hmacValue = ECMA376Agile.generate_integrity_parameter(
            encrypted_data, info.keyData, secretKey, info.keyData.saltValue
        )

        info.encryptedHmacKey = hmacKey
        info.encryptedHmacValue = hmacValue

        xml_descriptor = info.toEncryptionDescriptor().encode("UTF-8")
        header_descriptor = info.getEncryptionDescriptorHeader()

        return header_descriptor + xml_descriptor

    @staticmethod
    def generate_encryption_parameters(key, salt_value=None, spin_count=100000):
        """
        Generates encryption parameters used to encrypt a payload.

        Returns the information + a secret key.
        """
        info = ECMA376AgileEncryptionInfo()
        info.spinCount = spin_count

        info.encryptedKey.saltValue = _get_salt(salt_value, info.encryptedKey.saltSize)

        h = ECMA376Agile._derive_iterated_hash_from_password(
            key, info.encryptedKey.saltValue, info.encryptedKey.hashName, info.spinCount
        ).digest()

        key1 = ECMA376Agile._derive_encryption_key(
            h,
            blkKey_VerifierHashInput,
            info.encryptedKey.hashName,
            info.encryptedKey.keyBits,
        )
        key2 = ECMA376Agile._derive_encryption_key(
            h,
            blkKey_encryptedVerifierHashValue,
            info.encryptedKey.hashName,
            info.encryptedKey.keyBits,
        )
        key3 = ECMA376Agile._derive_encryption_key(
            h,
            blkKey_encryptedKeyValue,
            info.encryptedKey.hashName,
            info.encryptedKey.keyBits,
        )

        verifierHashInput = _random_buffer(info.encryptedKey.saltSize)
        verifierHashInput = _resize_buffer(
            verifierHashInput,
            _round_up(len(verifierHashInput), info.encryptedKey.blockSize),
        )

        info.encryptedVerifierHashInput = _encrypt_aes_cbc(
            verifierHashInput, key1, info.encryptedKey.saltValue
        )

        hashedVerifier = _get_hash_func(info.encryptedKey.hashName)(
            verifierHashInput
        ).digest()
        hashedVerifier = _resize_buffer(
            hashedVerifier, _round_up(len(hashedVerifier), info.encryptedKey.blockSize)
        )

        info.encryptedVerifierHashValue = _encrypt_aes_cbc(
            hashedVerifier, key2, info.encryptedKey.saltValue
        )

        secret_key = _random_buffer(info.encryptedKey.saltSize)
        secret_key = _normalize_key(secret_key, info.encryptedKey.keyBits // 8)

        info.encryptedKeyValue = _encrypt_aes_cbc(
            secret_key, key3, info.encryptedKey.saltValue
        )

        info.keyData.saltValue = _get_salt(salt_size=info.keyData.saltSize)

        return info, secret_key

    @staticmethod
    def encrypt_payload(ibuf, params: ECMA376AgileCipherParams, secret_key, salt_value):
        """
        Encrypts a payload using the params and secrets passed in.

        Returns the encrypted data as a byte array.
        """
        # Specifications calls for storing the original (unpadded) size as a 64 bit little-endian
        # number at the start of the buffer. We'll loop while there's data, and come back at the
        # end to update the total size, instead of seeking to the end of ibuf to get the size,
        # just in case ibuf is a streaming buffer...
        total_size = 0
        obuf = io.BytesIO()
        obuf.write(pack("<Q", total_size))

        hashCalc = _get_hash_func(params.hashName)
        SEGMENT_LENGTH = 4096

        i = 0
        while True:
            buf = ibuf.read(SEGMENT_LENGTH)
            if not buf:
                break

            iv = _normalize_key(
                hashCalc(salt_value + pack("<I", i)).digest(), params.saltSize
            )

            # Per the specifications, we need to make sure the last chunk is padded to our
            # block size
            enc = _encrypt_aes_cbc_padded(buf, secret_key, iv, params.blockSize)

            obuf.write(enc)
            total_size += len(buf)

            i += 1

        # Update size in the header
        obuf.seek(0)
        obuf.write(pack("<Q", total_size))

        return obuf.getvalue()

    @staticmethod
    def generate_integrity_parameter(
        encrypted_data, params: ECMA376AgileCipherParams, secret_key, salt_value
    ):
        """
        Returns the encrypted HmacKey and HmacValue.
        """
        salt = _random_buffer(params.hashSize)

        iv1 = _generate_iv(params, blkKey_dataIntegrity1, salt_value)
        iv2 = _generate_iv(params, blkKey_dataIntegrity2, salt_value)

        encryptedHmacKey = _encrypt_aes_cbc(salt, secret_key, iv1)

        msg_hmac = hmac.new(salt, encrypted_data, _get_hash_func(params.hashName))
        hmacValue = msg_hmac.digest()

        encryptedHmacValue = _encrypt_aes_cbc(hmacValue, secret_key, iv2)

        return encryptedHmacKey, encryptedHmacValue

    @staticmethod
    def verify_password(
        password,
        saltValue,
        hashAlgorithm,
        encryptedVerifierHashInput,
        encryptedVerifierHashValue,
        spinValue,
        keyBits,
    ):
        r"""
        Return True if the given password is valid.

            >>> password = 'Password1234_'
            >>> saltValue = b'\xcb\xca\x1c\x99\x93C\xfb\xad\x92\x07V4\x15\x004\xb0'
            >>> hashAlgorithm = 'SHA512'
            >>> encryptedVerifierHashInput = b'9\xee\xa5N&\xe5\x14y\x8c(K\xc7qM8\xac'
            >>> encryptedVerifierHashValue = b'\x147mm\x81s4\xe6\xb0\xffO\xd8"\x1a|g\x8e]\x8axN\x8f\x99\x9fL\x18\x890\xc3jK)\xc5\xb33`' + \
            ... b'[\\\xd4\x03\xb0P\x03\xad\xcf\x18\xcc\xa8\xcb\xab\x8d\xeb\xe3s\xc6V\x04\xa0\xbe\xcf\xae\\\n\xd0'
            >>> spinValue = 100000
            >>> keyBits = 256
            >>> ECMA376Agile.verify_password(password, saltValue, hashAlgorithm, encryptedVerifierHashInput, encryptedVerifierHashValue, spinValue, keyBits)
            True
        """
        # NOTE: See https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/a57cb947-554f-4e5e-b150-3f2978225e92

        h = ECMA376Agile._derive_iterated_hash_from_password(
            password, saltValue, hashAlgorithm, spinValue
        )

        key1 = ECMA376Agile._derive_encryption_key(
            h.digest(), blkKey_VerifierHashInput, hashAlgorithm, keyBits
        )
        key2 = ECMA376Agile._derive_encryption_key(
            h.digest(), blkKey_encryptedVerifierHashValue, hashAlgorithm, keyBits
        )

        hash_input = _decrypt_aes_cbc(encryptedVerifierHashInput, key1, saltValue)
        hashCalc = _get_hash_func(hashAlgorithm)
        acutal_hash = hashCalc(hash_input)
        acutal_hash = acutal_hash.digest()

        expected_hash = _decrypt_aes_cbc(encryptedVerifierHashValue, key2, saltValue)

        return acutal_hash == expected_hash

    @staticmethod
    def verify_integrity(
        secretKey,
        keyDataSalt,
        keyDataHashAlgorithm,
        keyDataBlockSize,
        encryptedHmacKey,
        encryptedHmacValue,
        stream,
    ):
        r"""
        Return True if the HMAC of the data payload is valid.
        """
        # NOTE: See https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/63d9c262-82b9-4fa3-a06d-d087b93e3b00

        hashCalc = _get_hash_func(keyDataHashAlgorithm)

        iv1 = hashCalc(keyDataSalt + blkKey_dataIntegrity1).digest()
        iv1 = iv1[:keyDataBlockSize]
        iv2 = hashCalc(keyDataSalt + blkKey_dataIntegrity2).digest()
        iv2 = iv2[:keyDataBlockSize]

        hmacKey = _decrypt_aes_cbc(encryptedHmacKey, secretKey, iv1)
        hmacValue = _decrypt_aes_cbc(encryptedHmacValue, secretKey, iv2)

        msg_hmac = hmac.new(hmacKey, stream.read(), hashCalc)
        actualHmac = msg_hmac.digest()
        stream.seek(0)

        return hmacValue == actualHmac

    @staticmethod
    def makekey_from_privkey(privkey, encryptedKeyValue):
        privkey = serialization.load_pem_private_key(
            privkey.read(), password=None, backend=default_backend()
        )
        skey = privkey.decrypt(encryptedKeyValue, padding.PKCS1v15())
        return skey

    @staticmethod
    def makekey_from_password(
        password, saltValue, hashAlgorithm, encryptedKeyValue, spinValue, keyBits
    ):
        r"""
        Generate intermediate key from given password.

            >>> password = 'Password1234_'
            >>> saltValue = b'Lr]E\xdca\x0f\x93\x94\x12\xa0M\xa7\x91\x04f'
            >>> hashAlgorithm = 'SHA512'
            >>> encryptedKeyValue = b"\xa1l\xd5\x16Zz\xb9\xd2q\x11>\xd3\x86\xa7\x8c\xf4\x96\x92\xe8\xe5'\xb0\xc5\xfc\x00U\xed\x08\x0b|\xb9K"
            >>> spinValue = 100000
            >>> keyBits = 256
            >>> expected = b'@ f\t\xd9\xfa\xad\xf2K\x07j\xeb\xf2\xc45\xb7B\x92\xc8\xb8\xa7\xaa\x81\xbcg\x9b\xe8\x97\x11\xb0*\xc2'
            >>> ECMA376Agile.makekey_from_password(password, saltValue, hashAlgorithm, encryptedKeyValue, spinValue, keyBits) == expected
            True
        """

        h = ECMA376Agile._derive_iterated_hash_from_password(
            password, saltValue, hashAlgorithm, spinValue
        )
        encryption_key = ECMA376Agile._derive_encryption_key(
            h.digest(), blkKey_encryptedKeyValue, hashAlgorithm, keyBits
        )

        skey = _decrypt_aes_cbc(encryptedKeyValue, encryption_key, saltValue)

        return skey
