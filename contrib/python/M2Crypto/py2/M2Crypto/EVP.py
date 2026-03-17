from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL EVP API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions Copyright (c) 2004-2007 Open Source Applications Foundation.
Author: Heikki Toivonen
"""

import logging
from M2Crypto import BIO, Err, RSA, EC, m2, util
from typing import AnyStr, Optional, Callable  # noqa

log = logging.getLogger('EVP')


class EVPError(ValueError):
    pass


m2.evp_init(EVPError)


def pbkdf2(password, salt, iter, keylen):
    # type: (bytes, bytes, int, int) -> bytes
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

    def __init__(self, algo):
        # type: (str) -> None
        md = getattr(m2, algo, None)  # type: Optional[Callable]
        if md is None:
            # if the digest algorithm isn't found as an attribute of the m2
            # module, try to look up the digest using get_digestbyname()
            self.md = m2.get_digestbyname(algo)
        else:
            self.md = md()
        self.ctx = m2.md_ctx_new()
        m2.digest_init(self.ctx, self.md)

    def __del__(self):
        # type: () -> None
        if getattr(self, 'ctx', None):
            self.m2_md_ctx_free(self.ctx)

    def update(self, data):
        # type: (bytes) -> int
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

    def __init__(self, key, algo='sha1'):
        # type: (bytes, str) -> None
        md = getattr(m2, algo, None)
        if md is None:
            raise ValueError('unknown algorithm', algo)
        self.md = md()
        self.ctx = m2.hmac_ctx_new()
        m2.hmac_init(self.ctx, key, self.md)

    def __del__(self):
        # type: () -> None
        if getattr(self, 'ctx', None):
            self.m2_hmac_ctx_free(self.ctx)

    def reset(self, key):
        # type: (bytes) -> None
        m2.hmac_init(self.ctx, key, self.md)

    def update(self, data):
        # type: (bytes) -> None
        m2.hmac_update(self.ctx, data)

    def final(self):
        # type: () -> bytes
        return m2.hmac_final(self.ctx)

    digest = final


def hmac(key, data, algo='sha1'):
    # type: (bytes, bytes, str) -> bytes
    md = getattr(m2, algo, None)
    if md is None:
        raise ValueError('unknown algorithm', algo)
    return m2.hmac(key, data, md())


class Cipher(object):

    m2_cipher_ctx_free = m2.cipher_ctx_free

    def __init__(self, alg, key, iv, op, key_as_bytes=0, d='md5',
                 salt=b'12345678', i=1, padding=1):
        # type: (str, bytes, bytes, object, int, str, bytes, int, int) -> None
        cipher = getattr(m2, alg, None)
        if cipher is None:
            raise ValueError('unknown cipher', alg)
        self.cipher = cipher()
        if key_as_bytes:
            kmd = getattr(m2, d, None)
            if kmd is None:
                raise ValueError('unknown message digest', d)
            key = m2.bytes_to_key(self.cipher, kmd(), key, salt, iv, i)
        self.ctx = m2.cipher_ctx_new()
        m2.cipher_init(self.ctx, self.cipher, key, iv, op)
        self.set_padding(padding)
        del key

    def __del__(self):
        # type: () -> None
        if getattr(self, 'ctx', None):
            self.m2_cipher_ctx_free(self.ctx)

    def update(self, data):
        # type: (bytes) -> bytes
        return m2.cipher_update(self.ctx, data)

    def final(self):
        # type: () -> bytes
        return m2.cipher_final(self.ctx)

    def set_padding(self, padding=1):
        # type: (int) -> int
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

    def __init__(self, pkey=None, _pyfree=0, md='sha1'):
        # type: (Optional[bytes], int, str) -> None
        if pkey is not None:
            self.pkey = pkey  # type: bytes
            self._pyfree = _pyfree
        else:
            self.pkey = m2.pkey_new()
            self._pyfree = 1
        self._set_context(md)

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_pkey_free(self.pkey)
        if getattr(self, 'ctx', None):
            self.m2_md_ctx_free(self.ctx)

    def _ptr(self):
        return self.pkey

    def _set_context(self, md):
        # type: (str) -> None
        if not md:
            self.md = None
        else:
            mda = getattr(m2, md, None)  # type: Optional[Callable]
            if mda is None:
                raise ValueError('unknown message digest', md)
            self.md = mda()
        self.ctx = m2.md_ctx_new()  ## type: Context

    def reset_context(self, md='sha1'):
        # type: (str) -> None
        """
        Reset internal message digest context.

        :param md: The message digest algorithm.
        """
        self._set_context(md)

    def sign_init(self):
        # type: () -> None
        """
        Initialise signing operation with self.
        """
        m2.sign_init(self.ctx, self.md)

    def sign_update(self, data):
        # type: (bytes) -> None
        """
        Feed data to signing operation.

        :param data: Data to be signed.
        """
        m2.sign_update(self.ctx, data)

    def sign_final(self):
        # type: () -> bytes
        """
        Return signature.

        :return: The signature.
        """
        return m2.sign_final(self.ctx, self.pkey)

    # Deprecated
    update = sign_update
    final = sign_final

    def verify_init(self):
        # type: () -> None
        """
        Initialise signature verification operation with self.
        """
        m2.verify_init(self.ctx, self.md)

    def verify_update(self, data):
        # type: (bytes) -> int
        """
        Feed data to verification operation.

        :param data: Data to be verified.
        :return: -1 on Python error, 1 for success, 0 for OpenSSL error
        """
        return m2.verify_update(self.ctx, data)

    def verify_final(self, sign):
        # type: (bytes) -> int
        """
        Return result of verification.

        :param sign: Signature to use for verification
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """
        return m2.verify_final(self.ctx, sign, self.pkey)

    def digest_sign_init(self):
        # type: () -> None
        """
        Initialise digest signing operation with self.
        """
        if self.md is None:
            m2.digest_sign_init(self.ctx, self.pkey)
        else:
            m2.digest_sign_init(self.ctx, None, self.md, None, self.pkey)

    def digest_sign_update(self, data):
        # type: (bytes) -> None
        """
        Feed data to digest signing operation.

        :param data: Data to be signed.
        """
        m2.digest_sign_update(self.ctx, data)

    def digest_sign_final(self):
        # type: () -> bytes
        """
        Return signature.

        :return: The signature.
        """
        return m2.digest_sign_final(self.ctx)

    def digest_sign(self, data):
        # type: () -> bytes
        """
        Return signature.

        :return: The signature.
        """

        if m2.OPENSSL_VERSION_NUMBER < 0x10101000:
            raise NotImplemented('This method requires OpenSSL version ' +
                    '1.1.1 or greater.')

        return m2.digest_sign(self.ctx, data)

    def digest_verify_init(self):
        # type: () -> None
        """
        Initialise verification operation with self.
        """
        if self.md is None:
            m2.digest_verify_init(self.ctx, self.pkey)
        else:
            m2.digest_verify_init(self.ctx, None, self.md, None, self.pkey)

    def digest_verify_update(self, data):
        # type: (bytes) -> int
        """
        Feed data to verification operation.

        :param data: Data to be verified.
        :return: -1 on Python error, 1 for success, 0 for OpenSSL error
        """
        return m2.digest_verify_update(self.ctx, data)

    def digest_verify_final(self, sign):
        # type: (bytes) -> int
        """
        Feed data to digest verification operation.

        :param sign: Signature to use for verification
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """
        return m2.digest_verify_final(self.ctx, sign)

    def digest_verify(self, sign, data):
        # type: (bytes) -> int
        """
        Return result of verification.

        :param sign: Signature to use for verification
        :param data: Data to be verified.
        :return: Result of verification: 1 for success, 0 for failure, -1 on
                 other error.
        """

        if m2.OPENSSL_VERSION_NUMBER < 0x10101000:
            raise NotImplemented('This method requires OpenSSL version ' +
                    '1.1.1 or greater.')

        return m2.digest_verify(self.ctx, sign, data)

    def assign_rsa(self, rsa, capture=1):
        # type: (RSA.RSA, int) -> int
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

    def get_rsa(self):
        # type: () -> RSA.RSA_pub
        """
        Return the underlying RSA key if that is what the EVP
        instance is holding.
        """
        rsa_ptr = m2.pkey_get1_rsa(self.pkey)

        rsa = RSA.RSA_pub(rsa_ptr, 1)
        return rsa

    def assign_ec(self, ec, capture=1):
        # type: (EC.EC, int) -> int
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

    def get_ec(self):
        # type: () -> EC.EC_pub
        """
        Return the underlying EC key if that is what the EVP
        instance is holding.
        """
        ec_ptr = m2.pkey_get1_ec(self.pkey)

        ec = EC.EC_pub(ec_ptr)
        return ec

    def save_key(self, file, cipher='aes_128_cbc',
                 callback=util.passphrase_callback):
        # type: (AnyStr, Optional[str], Callable) -> int
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

    def save_key_bio(self, bio, cipher='aes_128_cbc',
                     callback=util.passphrase_callback):
        # type: (BIO.BIO, Optional[str], Callable) -> int
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
            return m2.pkey_write_pem_no_cipher(self.pkey, bio._ptr(), callback)
        else:
            proto = getattr(m2, cipher, None)
            if proto is None:
                raise ValueError('no such cipher %s' % cipher)
            return m2.pkey_write_pem(self.pkey, bio._ptr(), proto(), callback)

    def as_pem(self, cipher='aes_128_cbc', callback=util.passphrase_callback):
        # type: (Optional[str], Callable) -> bytes
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

    def as_der(self):
        # type: () -> bytes
        """
        Return key in DER format in a string
        """
        buf = m2.pkey_as_der(self.pkey)
        bio = BIO.MemoryBuffer(buf)
        return bio.read_all()

    def size(self):
        # type: () -> int
        """
        Return the size of the key in bytes.
        """
        return m2.pkey_size(self.pkey)

    def get_modulus(self):
        # type: () -> Optional[bytes]
        """
        Return the modulus in hex format.
        """
        return m2.pkey_get_modulus(self.pkey)


def load_key(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> PKey
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


def load_key_pubkey(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> PKey
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


def load_key_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> PKey
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


def load_key_bio_pubkey(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> PKey
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


def load_key_string(string, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> PKey
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


def load_key_string_pubkey(string, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> PKey
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
