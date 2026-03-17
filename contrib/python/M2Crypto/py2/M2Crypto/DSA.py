from __future__ import absolute_import, print_function

"""
    M2Crypto wrapper for OpenSSL DSA API.

    Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved.

    Portions created by Open Source Applications Foundation (OSAF) are
    Copyright (C) 2004 OSAF. All Rights Reserved.
"""

from M2Crypto import BIO, m2, util
from typing import Any, AnyStr, Callable, Tuple  # noqa


class DSAError(Exception):
    pass


m2.dsa_init(DSAError)


class DSA(object):

    """
    This class is a context supporting DSA key and parameter
    values, signing and verifying.

    Simple example::

        from M2Crypto import EVP, DSA, util

        message = 'Kilroy was here!'
        md = EVP.MessageDigest('sha1')
        md.update(message)
        digest = md.final()

        dsa = DSA.gen_params(1024)
        dsa.gen_key()
        r, s = dsa.sign(digest)
        good = dsa.verify(digest, r, s)
        if good:
            print('  ** success **')
        else:
            print('  ** verification failed **')
    """

    m2_dsa_free = m2.dsa_free

    def __init__(self, dsa, _pyfree=0):
        # type: (bytes, int) -> None
        """
        Use one of the factory functions to create an instance.
        :param dsa: binary representation of OpenSSL DSA type
        """
        assert m2.dsa_type_check(dsa), "'dsa' type error"
        self.dsa = dsa
        self._pyfree = _pyfree

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_dsa_free(self.dsa)

    def __len__(self):
        # type: () -> int
        """
        Return the key length.

        :return:  the DSA key length in bits
        """
        assert m2.dsa_type_check(self.dsa), "'dsa' type error"
        return m2.dsa_keylen(self.dsa)

    def __getattr__(self, name):
        # type: (str) -> bytes
        """
        Return specified DSA parameters and key values.

        :param name: name of variable to be returned.  Must be
                     one of 'p', 'q', 'g', 'pub', 'priv'.
        :return:     value of specified variable (a "byte string")
        """
        if name in ['p', 'q', 'g', 'pub', 'priv']:
            method = getattr(m2, 'dsa_get_%s' % (name,))
            assert m2.dsa_type_check(self.dsa), "'dsa' type error"
            return method(self.dsa)
        else:
            raise AttributeError

    def __setattr__(self, name, value):
        # type: (str, bytes) -> None
        if name in ['p', 'q', 'g']:
            raise DSAError('set (p, q, g) via set_params()')
        elif name in ['pub', 'priv']:
            raise DSAError('generate (pub, priv) via gen_key()')
        else:
            self.__dict__[name] = value

    def set_params(self, p, q, g):
        # type: (bytes, bytes, bytes) -> None
        """
        Set new parameters.

        :param p: MPI binary representation ... format that consists of
                  the number's length in bytes represented as a 4-byte
                  big-endian number, and the number itself in big-endian
                  format, where the most significant bit signals
                  a negative number (the representation of numbers with
                  the MSB set is prefixed with null byte).
        :param q: ditto
        :param g: ditto

        @warning: This does not change the private key, so it may be
                  unsafe to use this method. It is better to use
                  gen_params function to create a new DSA object.
        """
        m2.dsa_set_pqg(self.dsa, p, q, g)

    def gen_key(self):
        # type: () -> None
        """
        Generate a key pair.
        """
        assert m2.dsa_type_check(self.dsa), "'dsa' type error"
        m2.dsa_gen_key(self.dsa)

    def save_params(self, filename):
        # type: (AnyStr) -> int
        """
        Save the DSA parameters to a file.

        :param filename: Save the DSA parameters to this file.
        :return:         1 (true) if successful
        """
        with BIO.openfile(filename, 'wb') as bio:
            ret = m2.dsa_write_params_bio(self.dsa, bio._ptr())

        return ret

    def save_params_bio(self, bio):
        # type: (BIO.BIO) -> int
        """
        Save DSA parameters to a BIO object.

        :param bio: Save DSA parameters to this object.
        :return:    1 (true) if successful
        """
        return m2.dsa_write_params_bio(self.dsa, bio._ptr())

    def save_key(self, filename, cipher='aes_128_cbc',
                 callback=util.passphrase_callback):
        # type: (AnyStr, str, Callable) -> int
        """
        Save the DSA key pair to a file.

        :param filename: Save the DSA key pair to this file.
        :param cipher:   name of symmetric key algorithm and mode
                         to encrypt the private key.
        :return:         1 (true) if successful
        """
        with BIO.openfile(filename, 'wb') as bio:
            ret = self.save_key_bio(bio, cipher, callback)

        return ret

    def save_key_bio(self, bio, cipher='aes_128_cbc',
                     callback=util.passphrase_callback):
        # type: (BIO.BIO, str, Callable) -> int
        """
        Save DSA key pair to a BIO object.

        :param bio:    Save DSA parameters to this object.
        :param cipher: name of symmetric key algorithm and mode
                       to encrypt the private key.
        :return:       1 (true) if successful
        """
        if cipher is None:
            return m2.dsa_write_key_bio_no_cipher(self.dsa,
                                                  bio._ptr(), callback)
        else:
            ciph = getattr(m2, cipher, None)
            if ciph is None:
                raise DSAError('no such cipher: %s' % cipher)
            else:
                ciph = ciph()
            return m2.dsa_write_key_bio(self.dsa, bio._ptr(), ciph, callback)

    def save_pub_key(self, filename):
        # type: (AnyStr) -> int
        """
        Save the DSA public key (with parameters) to a file.

        :param filename: Save DSA public key (with parameters)
                         to this file.
        :return:         1 (true) if successful
        """
        with BIO.openfile(filename, 'wb') as bio:
            ret = self.save_pub_key_bio(bio)

        return ret

    def save_pub_key_bio(self, bio):
        # type: (BIO.BIO) -> int
        """
        Save DSA public key (with parameters) to a BIO object.

        :param bio: Save DSA public key (with parameters)
                    to this object.
        :return:  1 (true) if successful
        """
        return m2.dsa_write_pub_key_bio(self.dsa, bio._ptr())

    def sign(self, digest):
        # type: (bytes) -> Tuple[bytes, bytes]
        """
        Sign the digest.

        :param digest: SHA-1 hash of message (same as output
                       from MessageDigest, a "byte string")
        :return:       DSA signature, a tuple of two values, r and s,
                       both "byte strings".
        """
        assert self.check_key(), 'key is not initialised'
        return m2.dsa_sign(self.dsa, digest)

    def verify(self, digest, r, s):
        # type: (bytes, bytes, bytes) -> int
        """
        Verify a newly calculated digest against the signature
        values r and s.

        :param digest: SHA-1 hash of message (same as output
                       from MessageDigest, a "byte string")
        :param r:      r value of the signature, a "byte string"
        :param s:      s value of the signature, a "byte string"
        :return:       1 (true) if verify succeeded, 0 if failed
        """
        assert self.check_key(), 'key is not initialised'
        return m2.dsa_verify(self.dsa, digest, r, s)

    def sign_asn1(self, digest):
        assert self.check_key(), 'key is not initialised'
        return m2.dsa_sign_asn1(self.dsa, digest)

    def verify_asn1(self, digest, blob):
        assert self.check_key(), 'key is not initialised'
        return m2.dsa_verify_asn1(self.dsa, digest, blob)

    def check_key(self):
        """
        Check to be sure the DSA object has a valid private key.

        :return:  1 (true) if a valid private key
        """
        assert m2.dsa_type_check(self.dsa), "'dsa' type error"
        return m2.dsa_check_key(self.dsa)


class DSA_pub(DSA):

    """
    This class is a DSA context that only supports a public key
    and verification.  It does NOT support a private key or
    signing.

    """

    def sign(self, *argv):
        # type: (*Any) -> None
        raise DSAError('DSA_pub object has no private key')

    sign_asn1 = sign

    def check_key(self):
        # type: () -> int
        """
        :return: does DSA_pub contain a pub key?
        """
        return m2.dsa_check_pub_key(self.dsa)

    save_key = DSA.save_pub_key

    save_key_bio = DSA.save_pub_key_bio

# --------------------------------------------------------------
# factories and other functions


def gen_params(bits, callback=util.genparam_callback):
    # type: (int, Callable) -> DSA
    """
    Factory function that generates DSA parameters and
    instantiates a DSA object from the output.

    :param bits: The length of the prime to be generated. If
                 'bits' < 512, it is set to 512.
    :param callback: A Python callback object that will be
                 invoked during parameter generation; it usual
                 purpose is to provide visual feedback.
    :return:  instance of DSA.
    """
    dsa = m2.dsa_generate_parameters(bits, callback)
    return DSA(dsa, 1)


def set_params(p, q, g):
    # type: (bytes, bytes, bytes) -> DSA
    """
    Factory function that instantiates a DSA object with DSA
    parameters.

    :param p: value of p, a "byte string"
    :param q: value of q, a "byte string"
    :param g: value of g, a "byte string"
    :return:  instance of DSA.
    """
    dsa = m2.dsa_new()
    m2.dsa_set_pqg(dsa, p, q, g)
    return DSA(dsa, 1)


def load_params(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> DSA
    """
    Factory function that instantiates a DSA object with DSA
    parameters from a file.

    :param file:     Names the file (a path) that contains the PEM
                     representation of the DSA parameters.
    :param callback: A Python callback object that will be
                     invoked if the DSA parameters file is
                     passphrase-protected.
    :return:         instance of DSA.
    """
    with BIO.openfile(file) as bio:
        ret = load_params_bio(bio, callback)

    return ret


def load_params_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> DSA
    """
    Factory function that instantiates a DSA object with DSA
    parameters from a M2Crypto.BIO object.

    :param bio:      Contains the PEM representation of the DSA
                     parameters.
    :param callback: A Python callback object that will be
                     invoked if the DSA parameters file is
                     passphrase-protected.
    :return:         instance of DSA.
    """
    dsa = m2.dsa_read_params(bio._ptr(), callback)
    return DSA(dsa, 1)


def load_key(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> DSA
    """
    Factory function that instantiates a DSA object from a
    PEM encoded DSA key pair.

    :param file:     Names the file (a path) that contains the PEM
                     representation of the DSA key pair.
    :param callback: A Python callback object that will be
                     invoked if the DSA key pair is
                     passphrase-protected.
    :return:         instance of DSA.
    """
    with BIO.openfile(file) as bio:
        ret = load_key_bio(bio, callback)

    return ret


def load_key_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> DSA
    """
    Factory function that instantiates a DSA object from a
    PEM encoded DSA key pair.

    :param bio:      Contains the PEM representation of the DSA
                     key pair.
    :param callback: A Python callback object that will be
                     invoked if the DSA key pair is
                     passphrase-protected.
    :return:         instance of DSA.
    """
    dsa = m2.dsa_read_key(bio._ptr(), callback)
    return DSA(dsa, 1)


def pub_key_from_params(p, q, g, pub):
    # type: (bytes, bytes, bytes, bytes) -> DSA_pub
    """
    Factory function that instantiates a DSA_pub object using
    the parameters and public key specified.

    :param p: value of p
    :param q: value of q
    :param g: value of g
    :param pub: value of the public key
    :return:  instance of DSA_pub.
    """
    dsa = m2.dsa_new()
    m2.dsa_set_pqg(dsa, p, q, g)
    m2.dsa_set_pub(dsa, pub)
    return DSA_pub(dsa, 1)


def load_pub_key(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> DSA_pub
    """
    Factory function that instantiates a DSA_pub object using
    a DSA public key contained in PEM file.  The PEM file
    must contain the parameters in addition to the public key.

    :param file:     Names the file (a path) that contains the PEM
                     representation of the DSA public key.
    :param callback: A Python callback object that will be
                     invoked should the DSA public key be
                     passphrase-protected.
    :return:         instance of DSA_pub.
    """
    with BIO.openfile(file) as bio:
        ret = load_pub_key_bio(bio, callback)

    return ret


def load_pub_key_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> DSA_pub
    """
    Factory function that instantiates a DSA_pub object using
    a DSA public key contained in PEM format.  The PEM
    must contain the parameters in addition to the public key.

    :param bio:      Contains the PEM representation of the DSA
                     public key (with params).
    :param callback: A Python callback object that will be
                     invoked should the DSA public key be
                     passphrase-protected.
    :return:         instance of DSA_pub.
    """
    dsapub = m2.dsa_read_pub_key(bio._ptr(), callback)
    return DSA_pub(dsapub, 1)
