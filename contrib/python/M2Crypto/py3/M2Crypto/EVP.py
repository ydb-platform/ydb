from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL EVP API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions Copyright (c) 2004-2007 Open Source Applications Foundation.
Author: Heikki Toivonen
"""

import logging
from M2Crypto import BIO, Err, RSA, EC, m2, util
from typing import Optional, Callable, Union  # noqa

log = logging.getLogger('EVP')


class EVPError(ValueError):
    pass


m2.evp_init(EVPError)


def pbkdf2(
    password: bytes, salt: bytes, iter: int, keylen: int
) -> bytes:
    """
    Derive a key from password using PBKDF2 algorithm specified in RFC 2898.

    :param password: Derive the key from this password.
    :param salt:     Salt.
    :param iter:     Number of iterations to perform.
    :param keylen:   Length of key to produce.
    :return:         Key.
    """
    return m2.pkcs5_pbkdf2_hmac_sha1(password, salt, iter, keylen)


class MessageDigest(object):
    """
    Message Digest
    """

    m2_md_ctx_free = m2.md_ctx_free

    def __init__(self, algo: str) -> None:
        md: Optional[Callable] = getattr(m2, algo, None)
        if md is None:
            # if the digest algorithm isn't found as an attribute of the m2
            # module, try to look up the digest using get_digestbyname()
            self.md = m2.get_digestbyname(algo)
        else:
            self.md = md()
        self.ctx = m2.md_ctx_new()
        m2.digest_init(self.ctx, self.md)

    def __del__(self) -> None:
        if getattr(self, 'ctx', None):
            self.m2_md_ctx_free(self.ctx)

    def update(self, data: bytes) -> int:
        """
        Add data to be digested.

        :return: -1 for Python error, 1 for success, 0 for OpenSSL failure.
        """
        return m2.digest_update(self.ctx, data)

    def final(self):
        return m2.digest_final(self.ctx)

    # Deprecated.
    digest = final


class HMAC(object):

    m2_hmac_ctx_free = m2.hmac_ctx_free

    def __init__(self, key: bytes, algo: str = 'sha1') -> None:
        md = getattr(m2, algo, None)
        if md is None:
            raise ValueError('unknown algorithm', algo)
        self.md = md()
        self.ctx = m2.hmac_ctx_new()
        m2.hmac_init(self.ctx, key, self.md)

    def __del__(self) -> None:
        if getattr(self, 'ctx', None):
            self.m2_hmac_ctx_free(self.ctx)

    def reset(self, key: bytes) -> None:
        m2.hmac_init(self.ctx, key, self.md)

    def update(self, data: bytes) -> None:
        m2.hmac_update(self.ctx, data)

    def final(self) -> bytes:
        return m2.hmac_final(self.ctx)

    digest = final


def hmac(key: bytes, data: bytes, algo: str = 'sha1') -> bytes:
    md = getattr(m2, algo, None)
    if md is None:
        raise ValueError('unknown algorithm', algo)
    return m2.hmac(key, data, md())


class Cipher(object):

    m2_cipher_ctx_free = m2.cipher_ctx_free

    def __init__(
        self,
        alg: str,
        key: bytes,
        iv: bytes,
        op: object,
        key_as_bytes: int = 0,
        d: str = 'md5',
        salt: bytes = b'12345678',
        i: int = 1,
        padding: int = 1,
    ) -> None:
        cipher = getattr(m2, alg, None)
        if cipher is None:
            raise ValueError('unknown cipher', alg)
        self.cipher = cipher()
        if key_as_bytes:
            kmd = getattr(m2, d, None)
            if kmd is None:
                raise ValueError('unknown message digest', d)
            key = m2.bytes_to_key(
                self.cipher, kmd(), key, salt, iv, i
            )
        self.ctx = m2.cipher_ctx_new()
        m2.cipher_init(self.ctx, self.cipher, key, iv, op)
        self.set_padding(padding)
        del key

    def __del__(self) -> None:
        if getattr(self, 'ctx', None):
            self.m2_cipher_ctx_free(self.ctx)

    def update(self, data: bytes) -> bytes:
        return m2.cipher_update(self.ctx, data)

    def final(self) -> bytes:
        return m2.cipher_final(self.ctx)

    def set_padding(self, padding: int = 1) -> int:
        """
        Actually always return 1
        """
        return m2.cipher_set_padding(self.ctx, padding)


class PKey(object):
    """
    Object to hold diverse types of asymmetric keys (also known
    as "key pairs").
    """

    m2_pkey_free = m2.pkey_free
    m2_md_ctx_free = m2.md_ctx_free

    def __init__(
        self,
        pkey: Optional[bytes] = None,
        _pyfree: int = 0,
        md: str = 'sha1',
    ) -> None:
        if pkey is not None:
            self.pkey: bytes = pkey
            self._pyfree = _pyfree
        else:
            self.pkey = m2.pkey_new()
            self._pyfree = 1
        self._set_context(md)

    def __del__(self) -> None:
        if getattr(self, '_pyfree', 0):
            self.m2_pkey_free(self.pkey)
        if getattr(self, 'ctx', None):
            self.m2_md_ctx_free(self.ctx)

    def _ptr(self):
        return self.pkey

    def _set_context(self, md: str) -> None:
        if not md:
            self.md = None
        else:
            mda: Optional[Callable] = getattr(m2, md, None)
            if mda is None:
                raise ValueError('unknown message digest', md)
            self.md = mda()
        self.ctx: Context = m2.md_ctx_new()

    def reset_context(self, md: str = 'sha1') -> None:
        """
        Reset internal message digest context.

        :param md: The message digest algorithm.
        """
        self._set_context(md)

    def sign_init(self) -> None:
        """
        Initialise signing operation with self.
        """
        m2.sign_init(self.ctx, self.md)

    def sign_update(self, data: bytes) -> None:
        """
        Feed data to signing operation.

        :param data: Data to be signed.
        """
        m2.sign_update(self.ctx, data)

    def sign_final(self) -> bytes:
        """
        Return signature.

        :return: The signature.
        """
        return m2.sign_final(self.ctx, self.pkey)

    # Deprecated
    update = sign_update
    final = sign_final

    def verify_init(self) -> None:
        """
        Initialise signature verification operation with self.
        """
        m2.verify_init(self.ctx, self.md)

    def verify_update(self, data: bytes) -> int:
        """
        Feed data to verification operation.

        :param data: Data to be verified.
        :return: -1 on Python error, 1 for success, 0 for OpenSSL error
        """
        return m2.verify_update(self.ctx, data)

    def verify_final(self, sign: bytes) -> int:
        """
        Return result of verification.

        :param sign: Signature to use for verification
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """
        return m2.verify_final(self.ctx, sign, self.pkey)

    def digest_sign_init(self) -> None:
        """
        Initialise digest signing operation with self.
        """
        if self.md is None:
            m2.digest_sign_init(self.ctx, self.pkey)
        else:
            m2.digest_sign_init(
                self.ctx, None, self.md, None, self.pkey
            )

    def digest_sign_update(self, data: bytes) -> None:
        """
        Feed data to digest signing operation.

        :param data: Data to be signed.
        """
        m2.digest_sign_update(self.ctx, data)

    def digest_sign_final(self) -> bytes:
        """
        Return signature.

        :return: The signature.
        """
        return m2.digest_sign_final(self.ctx)

    def digest_sign(self, data: bytes) -> bytes:
        """
        Return signature.

        :return: The signature.
        """

        if m2.OPENSSL_VERSION_NUMBER < 0x10101000:
            raise NotImplemented(
                'This method requires OpenSSL version '
                + '1.1.1 or greater.'
            )

        return m2.digest_sign(self.ctx, data)

    def digest_verify_init(self) -> None:
        """
        Initialise verification operation with self.
        """
        if self.md is None:
            m2.digest_verify_init(self.ctx, self.pkey)
        else:
            m2.digest_verify_init(
                self.ctx, None, self.md, None, self.pkey
            )

    def digest_verify_update(self, data: bytes) -> int:
        """
        Feed data to verification operation.

        :param data: Data to be verified.
        :return: -1 on Python error, 1 for success, 0 for OpenSSL error
        """
        return m2.digest_verify_update(self.ctx, data)

    def digest_verify_final(self, sign: bytes) -> int:
        """
        Feed data to digest verification operation.

        :param sign: Signature to use for verification
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """
        return m2.digest_verify_final(self.ctx, sign)

    def digest_verify(self, sign: bytes, data: bytes) -> int:
        """
        Return result of verification.

        :param sign: Signature to use for verification
        :param data: Data to be verified.
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """

        if m2.OPENSSL_VERSION_NUMBER < 0x10101000:
            raise NotImplemented(
                'This method requires OpenSSL version '
                + '1.1.1 or greater.'
            )

        return m2.digest_verify(self.ctx, sign, data)

    def assign_rsa(self, rsa: RSA.RSA, capture: int = 1) -> int:
        """
        Assign the RSA key pair to self.

        :param rsa: M2Crypto.RSA.RSA object to be assigned to self.

        :param capture: If true (default), this PKey object will own the RSA
                        object, meaning that once the PKey object gets
                        deleted it is no longer safe to use the RSA object.

        :return: Return 1 for success and 0 for failure.
        """
        if capture:
            ret = m2.pkey_assign_rsa(self.pkey, rsa.rsa)
            if ret:
                rsa._pyfree = 0
        else:
            ret = m2.pkey_set1_rsa(self.pkey, rsa.rsa)
        return ret

    def get_rsa(self) -> RSA.RSA_pub:
        """
        Return the underlying RSA key if that is what the EVP
        instance is holding.
        """
        rsa_ptr = m2.pkey_get1_rsa(self.pkey)

        rsa = RSA.RSA_pub(rsa_ptr, 1)
        return rsa

    def assign_ec(self, ec: "EC.EC", capture: int = 1) -> int:
        """
        Assign the EC key pair to self.

        :param ec: M2Crypto.EC.EC object to be assigned to self.

        :param capture: If true (default), this PKey object will own the EC
                        object, meaning that once the PKey object gets
                        deleted it is no longer safe to use the EC object.

        :return: Return 1 for success and 0 for failure.
        """
        if capture:
            ret = m2.pkey_assign_ec(self.pkey, ec.ec)
            if ret:
                ec._pyfree = 0
        else:
            ret = m2.pkey_set1_ec(self.pkey, ec.ec)
        return ret

    def get_ec(self) -> "EC.EC_pub":
        """
        Return the underlying EC key if that is what the EVP
        instance is holding.
        """
        ec_ptr = m2.pkey_get1_ec(self.pkey)

        ec = EC.EC_pub(ec_ptr)
        return ec

    def save_key(
        self,
        file: Union[str, bytes],
        cipher: Optional[str] = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> int:
        """
        Save the key pair to a file in PEM format.

        :param file: Name of file to save key to.

        :param cipher: Symmetric cipher to protect the key. The default
                       cipher is 'aes_128_cbc'. If cipher is None, then
                       the key is saved in the clear.

        :param callback: A Python callable object that is invoked
                         to acquire a passphrase with which to protect
                         the key. The default is
                         util.passphrase_callback.
        """
        with BIO.openfile(file, 'wb') as bio:
            return self.save_key_bio(bio, cipher, callback)

    def save_key_bio(
        self,
        bio: BIO.BIO,
        cipher: Optional[str] = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> int:
        """
        Save the key pair to the M2Crypto.BIO object 'bio' in PEM format.

        :param bio: M2Crypto.BIO object to save key to.

        :param cipher: Symmetric cipher to protect the key. The default
                       cipher is 'aes_128_cbc'. If cipher is None, then
                       the key is saved in the clear.

        :param callback: A Python callable object that is invoked
                         to acquire a passphrase with which to protect
                         the key. The default is
                         util.passphrase_callback.
        """
        if cipher is None:
            return m2.pkey_write_pem_no_cipher(
                self.pkey, bio._ptr(), callback
            )
        else:
            proto = getattr(m2, cipher, None)
            if proto is None:
                raise ValueError('no such cipher %s' % cipher)
            return m2.pkey_write_pem(
                self.pkey, bio._ptr(), proto(), callback
            )

    def as_pem(
        self,
        cipher: Optional[str] = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> bytes:
        """
        Return key in PEM format in a string.

        :param cipher: Symmetric cipher to protect the key. The default
                       cipher is ``'aes_128_cbc'``. If cipher is None,
                       then the key is saved in the clear.

        :param callback: A Python callable object that is invoked
                         to acquire a passphrase with which to protect
                         the key. The default is
                         util.passphrase_callback.
        """
        bio = BIO.MemoryBuffer()
        self.save_key_bio(bio, cipher, callback)
        return bio.read_all()

    def as_der(self) -> bytes:
        """
        Return key in DER format in a string
        """
        buf = m2.pkey_as_der(self.pkey)
        bio = BIO.MemoryBuffer(buf)
        return bio.read_all()

    def size(self) -> int:
        """
        Return the size of the key in bytes.
        """
        return m2.pkey_size(self.pkey)

    def get_modulus(self) -> Optional[bytes]:
        """
        Return the modulus in hex format.
        """
        return m2.pkey_get_modulus(self.pkey)


def load_key(
    file: Union[str, bytes],
    callback: Callable = util.passphrase_callback,
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from file.

    :param file: Name of file containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    with BIO.openfile(file, 'r') as bio:
        cptr = m2.pkey_read_pem(bio.bio, callback)

    return PKey(cptr, 1)


def load_key_pubkey(
    file: Union[str, bytes],
    callback: Callable = util.passphrase_callback,
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from a public key as a file.

    :param file: Name of file containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """

    with BIO.openfile(file, 'r') as bio:
        cptr = m2.pkey_read_pem_pubkey(bio._ptr(), callback)
        if cptr is None:
            raise EVPError(Err.get_error())
    return PKey(cptr, 1)


def load_key_bio(
    bio: BIO.BIO, callback: Callable = util.passphrase_callback
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from an M2Crypto.BIO object.

    :param bio: M2Crypto.BIO object containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    cptr = m2.pkey_read_pem(bio._ptr(), callback)
    return PKey(cptr, 1)


def load_key_bio_pubkey(
    bio: BIO.BIO, callback: Callable = util.passphrase_callback
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from a public key as a M2Crypto.BIO object.

    :param bio: M2Crypto.BIO object containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    cptr = m2.pkey_read_pem_pubkey(bio._ptr(), callback)
    if cptr is None:
        raise EVPError(Err.get_error())
    return PKey(cptr, 1)


def load_key_string(
    string: Union[str, bytes],
    callback: Callable = util.passphrase_callback,
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from a string.

    :param string: String containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    bio = BIO.MemoryBuffer(string)
    return load_key_bio(bio, callback)


def load_key_string_pubkey(
    string: Union[str, bytes],
    callback: Callable = util.passphrase_callback,
) -> PKey:
    """
    Load an M2Crypto.EVP.PKey from a public key as a string.

    :param string: String containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    bio = BIO.MemoryBuffer(string)
    return load_key_bio_pubkey(bio, callback)
