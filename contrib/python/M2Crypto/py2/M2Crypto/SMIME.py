from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL S/MIME API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

from M2Crypto import BIO, EVP, Err, X509, m2, util
from typing import AnyStr, Callable, Optional  # noqa

PKCS7_TEXT = m2.PKCS7_TEXT  # type: int
PKCS7_NOCERTS = m2.PKCS7_NOCERTS  # type: int
PKCS7_NOSIGS = m2.PKCS7_NOSIGS  # type: int
PKCS7_NOCHAIN = m2.PKCS7_NOCHAIN  # type: int
PKCS7_NOINTERN = m2.PKCS7_NOINTERN  # type: int
PKCS7_NOVERIFY = m2.PKCS7_NOVERIFY  # type: int
PKCS7_DETACHED = m2.PKCS7_DETACHED  # type: int
PKCS7_BINARY = m2.PKCS7_BINARY  # type: int
PKCS7_NOATTR = m2.PKCS7_NOATTR  # type: int

PKCS7_SIGNED = m2.PKCS7_SIGNED  # type: int
PKCS7_ENVELOPED = m2.PKCS7_ENVELOPED  # type: int
PKCS7_SIGNED_ENVELOPED = m2.PKCS7_SIGNED_ENVELOPED  # Deprecated
PKCS7_DATA = m2.PKCS7_DATA  # type: int


class PKCS7_Error(Exception):
    pass


m2.pkcs7_init(PKCS7_Error)


class PKCS7(object):

    m2_pkcs7_free = m2.pkcs7_free

    def __init__(self, pkcs7=None, _pyfree=0):
        # type: (Optional[bytes], int) -> None
        """PKCS7 object.

        :param pkcs7: binary representation of
               the OpenSSL type PKCS7
        """
        if pkcs7 is not None:
            self.pkcs7 = pkcs7
            self._pyfree = _pyfree
        else:
            self.pkcs7 = m2.pkcs7_new()
            self._pyfree = 1

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_pkcs7_free(self.pkcs7)

    def _ptr(self):
        return self.pkcs7

    def type(self, text_name=0):
        # type: (int) -> int
        if text_name:
            return m2.pkcs7_type_sn(self.pkcs7)
        else:
            return m2.pkcs7_type_nid(self.pkcs7)

    def write(self, bio):
        # type: (BIO.BIO) -> int
        return m2.pkcs7_write_bio(self.pkcs7, bio._ptr())

    def write_der(self, bio):
        # type: (BIO.BIO) -> int
        return m2.pkcs7_write_bio_der(self.pkcs7, bio._ptr())

    def get0_signers(self, certs, flags=0):
        # type: (X509.X509_Stack, int) -> X509.X509_Stack
        return X509.X509_Stack(m2.pkcs7_get0_signers(self.pkcs7,
                                                     certs.stack, flags), 1)


def load_pkcs7(p7file):
    # type: (AnyStr) -> PKCS7
    with BIO.openfile(p7file, 'r') as bio:
        p7_ptr = m2.pkcs7_read_bio(bio.bio)

    return PKCS7(p7_ptr, 1)


def load_pkcs7_der(p7file):
    # type: (AnyStr) -> PKCS7
    with BIO.openfile(p7file, 'rb') as bio:
        p7_ptr = m2.pkcs7_read_bio_der(bio.bio)

    return PKCS7(p7_ptr, 1)


def load_pkcs7_bio(p7_bio):
    # type: (BIO.BIO) -> PKCS7
    p7_ptr = m2.pkcs7_read_bio(p7_bio._ptr())
    return PKCS7(p7_ptr, 1)


def load_pkcs7_bio_der(p7_bio):
    # type: (BIO.BIO) -> PKCS7
    p7_ptr = m2.pkcs7_read_bio_der(p7_bio._ptr())
    return PKCS7(p7_ptr, 1)


def smime_load_pkcs7(p7file):
    # type: (AnyStr) -> PKCS7
    bio = m2.bio_new_file(p7file, 'r')

    try:
        p7_ptr, bio_ptr = m2.smime_read_pkcs7(bio)
    finally:
        m2.bio_free(bio)

    if bio_ptr is None:
        return PKCS7(p7_ptr, 1), None
    else:
        return PKCS7(p7_ptr, 1), BIO.BIO(bio_ptr, 1)


def smime_load_pkcs7_bio(p7_bio):
    # type: (BIO.BIO) -> PKCS7
    p7_ptr, bio_ptr = m2.smime_read_pkcs7(p7_bio._ptr())
    if p7_ptr is None:
        raise SMIME_Error(Err.get_error())
    if bio_ptr is None:
        return PKCS7(p7_ptr, 1), None
    else:
        return PKCS7(p7_ptr, 1), BIO.BIO(bio_ptr, 1)


class Cipher(object):
    """Object interface to EVP_CIPHER without all the frills of
    M2Crypto.EVP.Cipher.
    """

    def __init__(self, algo):
        # type: (str) -> None
        cipher = getattr(m2, algo, None)
        if cipher is None:
            raise ValueError('unknown cipher', algo)
        self.cipher = cipher()

    def _ptr(self):
        return self.cipher


class SMIME_Error(Exception):
    pass

m2.smime_init(SMIME_Error)


# FIXME class has no __init__ method
class SMIME(object):
    def load_key(self, keyfile, certfile=None,
                 callback=util.passphrase_callback):
        # type: (AnyStr, Optional[AnyStr], Callable) -> None
        if certfile is None:
            certfile = keyfile
        self.pkey = EVP.load_key(keyfile, callback)
        self.x509 = X509.load_cert(certfile)

    def load_key_bio(self, keybio, certbio=None,
                     callback=util.passphrase_callback):
        # type: (BIO.BIO, Optional[BIO.BIO], Callable) -> None
        if certbio is None:
            certbio = keybio
        self.pkey = EVP.load_key_bio(keybio, callback)
        self.x509 = X509.load_cert_bio(certbio)

    def set_x509_stack(self, stack):
        # type: (X509.X509_Stack) -> None
        assert isinstance(stack, X509.X509_Stack)
        self.x509_stack = stack

    def set_x509_store(self, store):
        # type: (X509.X509_Store) -> None
        assert isinstance(store, X509.X509_Store)
        self.x509_store = store

    def set_cipher(self, cipher):
        # type: (Cipher) -> None
        assert isinstance(cipher, Cipher)
        self.cipher = cipher

    def unset_key(self):
        # type: () -> None
        del self.pkey
        del self.x509

    def unset_x509_stack(self):
        # type: () -> None
        del self.x509_stack

    def unset_x509_store(self):
        # type: () -> None
        del self.x509_store

    def unset_cipher(self):
        # type: () -> None
        del self.cipher

    def encrypt(self, data_bio, flags=0):
        # type: (BIO.BIO, int) -> PKCS7
        if not hasattr(self, 'cipher'):
            raise SMIME_Error('no cipher: use set_cipher()')
        if not hasattr(self, 'x509_stack'):
            raise SMIME_Error('no recipient certs: use set_x509_stack()')

        pkcs7 = m2.pkcs7_encrypt(self.x509_stack._ptr(), data_bio._ptr(),
                                 self.cipher._ptr(), flags)

        return PKCS7(pkcs7, 1)

    def decrypt(self, pkcs7, flags=0):
        # type: (PKCS7, int) -> Optional[bytes]
        if not hasattr(self, 'pkey'):
            raise SMIME_Error('no private key: use load_key()')
        if not hasattr(self, 'x509'):
            raise SMIME_Error('no certificate: load_key() used incorrectly?')
        blob = m2.pkcs7_decrypt(pkcs7._ptr(), self.pkey._ptr(),
                                self.x509._ptr(), flags)
        return blob

    def sign(self, data_bio, flags=0, algo='sha1'):
        # type: (BIO.BIO, int, Optional[str]) -> PKCS7
        if not hasattr(self, 'pkey'):
            raise SMIME_Error('no private key: use load_key()')

        hash = getattr(m2, algo, None)

        if hash is None:
            raise SMIME_Error('no such hash algorithm %s' % algo)

        if hasattr(self, 'x509_stack'):
            pkcs7 = m2.pkcs7_sign1(self.x509._ptr(), self.pkey._ptr(),
                                   self.x509_stack._ptr(),
                                   data_bio._ptr(), hash(), flags)
            return PKCS7(pkcs7, 1)
        else:
            pkcs7 = m2.pkcs7_sign0(self.x509._ptr(), self.pkey._ptr(),
                                   data_bio._ptr(), hash(), flags)
            return PKCS7(pkcs7, 1)

    def verify(self, pkcs7, data_bio=None, flags=0):
        # type: (PKCS7, BIO.BIO, int) -> Optional[bytes]
        if not hasattr(self, 'x509_stack'):
            raise SMIME_Error('no signer certs: use set_x509_stack()')
        if not hasattr(self, 'x509_store'):
            raise SMIME_Error('no x509 cert store: use set_x509_store()')
        assert isinstance(pkcs7, PKCS7), 'pkcs7 not an instance of PKCS7'
        p7 = pkcs7._ptr()
        if data_bio is None:
            blob = m2.pkcs7_verify0(p7, self.x509_stack._ptr(),
                                    self.x509_store._ptr(), flags)
        else:
            blob = m2.pkcs7_verify1(p7, self.x509_stack._ptr(),
                                    self.x509_store._ptr(),
                                    data_bio._ptr(), flags)
        return blob

    def write(self, out_bio, pkcs7, data_bio=None, flags=0):
        # type: (BIO.BIO, PKCS7, Optional[BIO.BIO], int) -> int
        assert isinstance(pkcs7, PKCS7)
        if data_bio is None:
            return m2.smime_write_pkcs7(out_bio._ptr(), pkcs7._ptr(), flags)
        else:
            return m2.smime_write_pkcs7_multi(out_bio._ptr(), pkcs7._ptr(),
                                              data_bio._ptr(), flags)


def text_crlf(text):
    # type: (bytes) -> bytes
    bio_in = BIO.MemoryBuffer(text)
    bio_out = BIO.MemoryBuffer()
    if m2.smime_crlf_copy(bio_in._ptr(), bio_out._ptr()):
        return bio_out.read()
    else:
        raise SMIME_Error(Err.get_error())


def text_crlf_bio(bio_in):
    # type: (BIO.BIO) -> BIO.BIO
    bio_out = BIO.MemoryBuffer()
    if m2.smime_crlf_copy(bio_in._ptr(), bio_out._ptr()):
        return bio_out
    else:
        raise SMIME_Error(Err.get_error())
