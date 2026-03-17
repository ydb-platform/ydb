from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL RSA API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

import sys

from M2Crypto import BIO, Err, m2, util
from typing import Any, AnyStr, Callable, IO, Optional, Tuple  # noqa


class RSAError(Exception):
    pass


m2.rsa_init(RSAError)

no_padding = m2.no_padding
pkcs1_padding = m2.pkcs1_padding
if hasattr(m2, 'sslv23_padding'):
    sslv23_padding = m2.sslv23_padding
pkcs1_oaep_padding = m2.pkcs1_oaep_padding


class RSA(object):
    """
    RSA Key Pair.
    """

    m2_rsa_free = m2.rsa_free

    def __init__(self, rsa, _pyfree=0):
        # type: (bytes, int) -> None
        """
        :param rsa: binary representation of OpenSSL RSA type
        """
        assert m2.rsa_type_check(rsa), "'rsa' type error"
        self.rsa = rsa
        self._pyfree = _pyfree

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_rsa_free(self.rsa)

    def __len__(self):
        # type: () -> int
        return int(m2.rsa_size(self.rsa) << 3)

    def __getattr__(self, name):
        # type: (str) -> bytes
        if name == 'e':
            return m2.rsa_get_e(self.rsa)
        elif name == 'n':
            return m2.rsa_get_n(self.rsa)
        else:
            raise AttributeError

    def pub(self):
        # type: () -> Tuple[bytes, bytes]
        assert self.check_key(), 'key is not initialised'
        return m2.rsa_get_e(self.rsa), m2.rsa_get_n(self.rsa)

    def public_encrypt(self, data, padding):
        # type: (bytes, int) -> bytes
        assert self.check_key(), 'key is not initialised'
        return m2.rsa_public_encrypt(self.rsa, data, padding)

    def public_decrypt(self, data, padding):
        # type: (bytes, int) -> bytes
        assert self.check_key(), 'key is not initialised'
        return m2.rsa_public_decrypt(self.rsa, data, padding)

    def private_encrypt(self, data, padding):
        # type: (bytes, int) -> bytes
        assert self.check_key(), 'key is not initialised'
        return m2.rsa_private_encrypt(self.rsa, data, padding)

    def private_decrypt(self, data, padding):
        # type: (bytes, int) -> bytes
        assert self.check_key(), 'key is not initialised'
        return m2.rsa_private_decrypt(self.rsa, data, padding)

    def save_key_bio(self, bio, cipher='aes_128_cbc',
                     callback=util.passphrase_callback):
        # type: (BIO.BIO, Optional[str], Callable) -> int
        """
        Save the key pair to an M2Crypto.BIO.BIO object in PEM format.

        :param bio: M2Crypto.BIO.BIO object to save key to.

        :param cipher: Symmetric cipher to protect the key. The default
                       cipher is 'aes_128_cbc'. If cipher is None, then
                       the key is saved in the clear.

        :param callback: A Python callable object that is invoked
                         to acquire a passphrase with which to protect
                         the key.  The default is
                         util.passphrase_callback.
        """
        if cipher is None:
            return m2.rsa_write_key_no_cipher(self.rsa, bio._ptr(), callback)
        else:
            ciph = getattr(m2, cipher, None)
            if ciph is None:
                raise RSAError('not such cipher %s' % cipher)
            else:
                ciph = ciph()
            return m2.rsa_write_key(self.rsa, bio._ptr(), ciph, callback)

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
                         the key.  The default is
                         util.passphrase_callback.
        """
        with BIO.openfile(file, 'wb') as bio:
            return self.save_key_bio(bio, cipher, callback)

    save_pem = save_key

    def as_pem(self, cipher='aes_128_cbc', callback=util.passphrase_callback):
        # type: (Optional[str], Callable) -> bytes
        """
        Returns the key(pair) as a string in PEM format.
        """
        bio = BIO.MemoryBuffer()
        self.save_key_bio(bio, cipher, callback)
        return bio.read()

    def save_key_der_bio(self, bio):
        # type: (BIO.BIO) -> int
        """
        Save the key pair to an M2Crypto.BIO.BIO object in DER format.

        :param bio: M2Crypto.BIO.BIO object to save key to.
        """
        return m2.rsa_write_key_der(self.rsa, bio._ptr())

    def save_key_der(self, file):
        # type: (AnyStr) -> int
        """
        Save the key pair to a file in DER format.

        :param file: Filename to save key to
        """
        with BIO.openfile(file, 'wb') as bio:
            return self.save_key_der_bio(bio)

    def save_pub_key_bio(self, bio):
        # type: (BIO.BIO) -> int
        """
        Save the public key to an M2Crypto.BIO.BIO object in PEM format.

        :param bio: M2Crypto.BIO.BIO object to save key to.
        """
        return m2.rsa_write_pub_key(self.rsa, bio._ptr())

    def save_pub_key(self, file):
        # type: (AnyStr) -> int
        """
        Save the public key to a file in PEM format.

        :param file: Name of file to save key to.
        """
        with BIO.openfile(file, 'wb') as bio:
            return m2.rsa_write_pub_key(self.rsa, bio._ptr())

    def check_key(self):
        # type: () -> int
        """
        Validate RSA keys.

        It checks that p and q are in fact prime, and that n = p*q.

        :return: returns 1 if rsa is a valid RSA key, and 0 otherwise.
                 -1 is returned if an error occurs while checking the key.
                 If the key is invalid or an error occurred, the reason
                 code can be obtained using ERR_get_error(3).
        """
        return m2.rsa_check_key(self.rsa)

    def sign_rsassa_pss(self, digest, algo='sha1', salt_length=20):
        # type: (bytes, str, int) -> bytes
        """
        Signs a digest with the private key using RSASSA-PSS

        :param digest: A digest created by using the digest method

        :param salt_length: The length of the salt to use

        :param algo: The hash algorithm to use
                     Legal values like 'sha1','sha224', 'sha256',
                     'ripemd160', and 'md5'.

        :return: a string which is the signature
        """
        hash = getattr(m2, algo, None)

        if hash is None:
            raise RSAError('not such hash algorithm %s' % algo)

        signature = m2.rsa_padding_add_pkcs1_pss(self.rsa, digest, hash(), salt_length)

        return self.private_encrypt(signature, m2.no_padding)

    def verify_rsassa_pss(self, data, signature, algo='sha1', salt_length=20):
        # type: (bytes, bytes, str, int) -> int
        """
        Verifies the signature RSASSA-PSS

        :param data: Data that has been signed

        :param signature: The signature signed with RSASSA-PSS

        :param salt_length: The length of the salt that was used

        :param algo: The hash algorithm to use
                     Legal values are for example 'sha1','sha224',
                     'sha256', 'ripemd160', and 'md5'.

        :return: 1 or 0, depending on whether the signature was
                 verified or not.
        """
        hash = getattr(m2, algo, None)

        if hash is None:
            raise RSAError('not such hash algorithm %s' % algo)

        plain_signature = self.public_decrypt(signature, m2.no_padding)

        return m2.rsa_verify_pkcs1_pss(self.rsa, data, plain_signature, hash(), salt_length)

    def sign(self, digest, algo='sha1'):
        # type: (bytes, str) -> bytes
        """
        Signs a digest with the private key

        :param digest: A digest created by using the digest method

        :param algo: The method that created the digest.
                     Legal values like 'sha1','sha224', 'sha256',
                     'ripemd160', and 'md5'.

        :return: a string which is the signature
        """
        digest_type = getattr(m2, 'NID_' + algo, None)
        if digest_type is None:
            raise ValueError('unknown algorithm', algo)

        return m2.rsa_sign(self.rsa, digest, digest_type)

    def verify(self, data, signature, algo='sha1'):
        # type: (bytes, bytes, str) -> int
        """
        Verifies the signature with the public key

        :param data: Data that has been signed

        :param signature: The signature signed with the private key

        :param algo: The method use to create digest from the data
                     before it was signed.  Legal values like
                     'sha1','sha224', 'sha256', 'ripemd160', and 'md5'.

        :return: 1 or 0, depending on whether the signature was
                 verified or not.
        """
        digest_type = getattr(m2, 'NID_' + algo, None)
        if digest_type is None:
            raise ValueError('unknown algorithm', algo)

        return m2.rsa_verify(self.rsa, data, signature, digest_type)


class RSA_pub(RSA):

    """
    Object interface to an RSA public key.
    """

    def __setattr__(self, name, value):
        # type: (str, bytes) -> None
        if name in ['e', 'n']:
            raise RSAError('use factory function new_pub_key() to set (e, n)')
        else:
            self.__dict__[name] = value

    def private_encrypt(self, *argv):
        # type: (*Any) -> None
        raise RSAError('RSA_pub object has no private key')

    def private_decrypt(self, *argv):
        # type: (*Any) -> None
        raise RSAError('RSA_pub object has no private key')

    def save_key(self, file, *args, **kw):
        # type: (AnyStr, *Any, **Any) -> int
        """
        Save public key to file.
        """
        return self.save_pub_key(file)

    def save_key_bio(self, bio, *args, **kw):
        # type: (BIO.BIO, *Any, **Any) -> int
        """
        Save public key to BIO.
        """
        return self.save_pub_key_bio(bio)

    # save_key_der

    # save_key_der_bio

    def check_key(self):
        # type: () -> int
        return m2.rsa_check_pub_key(self.rsa)


def rsa_error():
    # type: () -> None
    raise RSAError(Err.get_error_message())


def keygen_callback(p, n, out=sys.stdout):
    # type: (int, Any, IO[str]) -> None
    """
    Default callback for gen_key().
    """
    ch = ['.', '+', '*', '\n']
    out.write(ch[p])
    out.flush()


def gen_key(bits, e, callback=keygen_callback):
    # type: (int, int, Callable) -> RSA
    """
    Generate an RSA key pair.

    :param bits: Key length, in bits.

    :param e: The RSA public exponent.

    :param callback: A Python callable object that is invoked
                     during key generation; its usual purpose is to
                     provide visual feedback. The default callback is
                     keygen_callback.

    :return: M2Crypto.RSA.RSA object.
    """
    return RSA(m2.rsa_generate_key(bits, e, callback), 1)


def load_key(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> RSA
    """
    Load an RSA key pair from file.

    :param file: Name of file containing RSA public key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to unlock the
                     key.  The default is util.passphrase_callback.

    :return: M2Crypto.RSA.RSA object.
    """
    with BIO.openfile(file) as bio:
        return load_key_bio(bio, callback)


def load_key_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> RSA
    """
    Load an RSA key pair from an M2Crypto.BIO.BIO object.

    :param bio: M2Crypto.BIO.BIO object containing RSA key pair in PEM
                format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to unlock the
                     key.  The default is util.passphrase_callback.

    :return: M2Crypto.RSA.RSA object.
    """
    rsa = m2.rsa_read_key(bio._ptr(), callback)
    if rsa is None:
        rsa_error()
    return RSA(rsa, 1)


def load_key_string(string, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> RSA
    """
    Load an RSA key pair from a string.

    :param string: String containing RSA key pair in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to unlock the
                     key. The default is util.passphrase_callback.

    :return: M2Crypto.RSA.RSA object.
    """
    bio = BIO.MemoryBuffer(string)
    return load_key_bio(bio, callback)


def load_pub_key(file):
    # type: (AnyStr) -> RSA_pub
    """
    Load an RSA public key from file.

    :param file: Name of file containing RSA public key in PEM format.

    :return: M2Crypto.RSA.RSA_pub object.
    """
    with BIO.openfile(file) as bio:
        return load_pub_key_bio(bio)


def load_pub_key_bio(bio):
    # type: (BIO.BIO) -> RSA_pub
    """
    Load an RSA public key from an M2Crypto.BIO.BIO object.

    :param bio: M2Crypto.BIO.BIO object containing RSA public key in PEM
                format.

    :return: M2Crypto.RSA.RSA_pub object.
    """
    rsa = m2.rsa_read_pub_key(bio._ptr())
    if rsa is None:
        rsa_error()
    return RSA_pub(rsa, 1)


def new_pub_key(e_n):
    # type: (Tuple[bytes, bytes]) -> RSA_pub
    """
    Instantiate an RSA_pub object from an (e, n) tuple.

    :param e: The RSA public exponent; it is a string in OpenSSL's MPINT
              format - 4-byte big-endian bit-count followed by the
              appropriate number of bits.

    :param n: The RSA composite of primes; it is a string in OpenSSL's
              MPINT format - 4-byte big-endian bit-count followed by the
              appropriate number of bits.

    :return: M2Crypto.RSA.RSA_pub object.
    """
    (e, n) = e_n
    rsa = m2.rsa_new()
    m2.rsa_set_en(rsa, e, n)
    return RSA_pub(rsa, 1)
