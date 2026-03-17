from __future__ import absolute_import

"""
M2Crypto wrapper for OpenSSL ECDH/ECDSA API.

@requires: OpenSSL 0.9.8 or newer

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved.

Portions copyright (c) 2005-2006 Vrije Universiteit Amsterdam.
All rights reserved."""

from M2Crypto import BIO, Err, m2, util
from typing import AnyStr, Callable, Dict, Optional, Tuple, Union  # noqa

EC_Key = bytes


class ECError(Exception):
    pass


m2.ec_init(ECError)

# Curve identifier constants
NID_secp112r1 = m2.NID_secp112r1  # type: int
NID_secp112r2 = m2.NID_secp112r2  # type: int
NID_secp128r1 = m2.NID_secp128r1  # type: int
NID_secp128r2 = m2.NID_secp128r2  # type: int
NID_secp160k1 = m2.NID_secp160k1  # type: int
NID_secp160r1 = m2.NID_secp160r1  # type: int
NID_secp160r2 = m2.NID_secp160r2  # type: int
NID_secp192k1 = m2.NID_secp192k1  # type: int
NID_secp224k1 = m2.NID_secp224k1  # type: int
NID_secp224r1 = m2.NID_secp224r1  # type: int
NID_secp256k1 = m2.NID_secp256k1  # type: int
NID_secp384r1 = m2.NID_secp384r1  # type: int
NID_secp521r1 = m2.NID_secp521r1  # type: int
NID_sect113r1 = m2.NID_sect113r1  # type: int
NID_sect113r2 = m2.NID_sect113r2  # type: int
NID_sect131r1 = m2.NID_sect131r1  # type: int
NID_sect131r2 = m2.NID_sect131r2  # type: int
NID_sect163k1 = m2.NID_sect163k1  # type: int
NID_sect163r1 = m2.NID_sect163r1  # type: int
NID_sect163r2 = m2.NID_sect163r2  # type: int
NID_sect193r1 = m2.NID_sect193r1  # type: int
NID_sect193r2 = m2.NID_sect193r2  # type: int
# default for secg.org TLS test server
NID_sect233k1 = m2.NID_sect233k1  # type: int
NID_sect233r1 = m2.NID_sect233r1  # type: int
NID_sect239k1 = m2.NID_sect239k1  # type: int
NID_sect283k1 = m2.NID_sect283k1  # type: int
NID_sect283r1 = m2.NID_sect283r1  # type: int
NID_sect409k1 = m2.NID_sect409k1  # type: int
NID_sect409r1 = m2.NID_sect409r1  # type: int
NID_sect571k1 = m2.NID_sect571k1  # type: int
NID_sect571r1 = m2.NID_sect571r1  # type: int

NID_prime192v1 = m2.NID_X9_62_prime192v1  # type: int
NID_prime192v2 = m2.NID_X9_62_prime192v2  # type: int
NID_prime192v3 = m2.NID_X9_62_prime192v3  # type: int
NID_prime239v1 = m2.NID_X9_62_prime239v1  # type: int
NID_prime239v2 = m2.NID_X9_62_prime239v2  # type: int
NID_prime239v3 = m2.NID_X9_62_prime239v3  # type: int
NID_prime256v1 = m2.NID_X9_62_prime256v1  # type: int
NID_c2pnb163v1 = m2.NID_X9_62_c2pnb163v1  # type: int
NID_c2pnb163v2 = m2.NID_X9_62_c2pnb163v2  # type: int
NID_c2pnb163v3 = m2.NID_X9_62_c2pnb163v3  # type: int
NID_c2pnb176v1 = m2.NID_X9_62_c2pnb176v1  # type: int
NID_c2tnb191v1 = m2.NID_X9_62_c2tnb191v1  # type: int
NID_c2tnb191v2 = m2.NID_X9_62_c2tnb191v2  # type: int
NID_c2tnb191v3 = m2.NID_X9_62_c2tnb191v3  # type: int
NID_c2pnb208w1 = m2.NID_X9_62_c2pnb208w1  # type: int
NID_c2tnb239v1 = m2.NID_X9_62_c2tnb239v1  # type: int
NID_c2tnb239v2 = m2.NID_X9_62_c2tnb239v2  # type: int
NID_c2tnb239v3 = m2.NID_X9_62_c2tnb239v3  # type: int
NID_c2pnb272w1 = m2.NID_X9_62_c2pnb272w1  # type: int
NID_c2pnb304w1 = m2.NID_X9_62_c2pnb304w1  # type: int
NID_c2tnb359v1 = m2.NID_X9_62_c2tnb359v1  # type: int
NID_c2pnb368w1 = m2.NID_X9_62_c2pnb368w1  # type: int
NID_c2tnb431r1 = m2.NID_X9_62_c2tnb431r1  # type: int

# To preserve compatibility with older names
NID_X9_62_prime192v1 = NID_prime192v1  # type: int
NID_X9_62_prime192v2 = NID_prime192v2  # type: int
NID_X9_62_prime192v3 = NID_prime192v3  # type: int
NID_X9_62_prime239v1 = NID_prime239v1  # type: int
NID_X9_62_prime239v2 = NID_prime239v2  # type: int
NID_X9_62_prime239v3 = NID_prime239v3  # type: int
NID_X9_62_prime256v1 = NID_prime256v1  # type: int
NID_X9_62_c2pnb163v1 = NID_c2pnb163v1  # type: int
NID_X9_62_c2pnb163v2 = NID_c2pnb163v2  # type: int
NID_X9_62_c2pnb163v3 = NID_c2pnb163v3  # type: int
NID_X9_62_c2pnb176v1 = NID_c2pnb176v1  # type: int
NID_X9_62_c2tnb191v1 = NID_c2tnb191v1  # type: int
NID_X9_62_c2tnb191v2 = NID_c2tnb191v2  # type: int
NID_X9_62_c2tnb191v3 = NID_c2tnb191v3  # type: int
NID_X9_62_c2pnb208w1 = NID_c2pnb208w1  # type: int
NID_X9_62_c2tnb239v1 = NID_c2tnb239v1  # type: int
NID_X9_62_c2tnb239v2 = NID_c2tnb239v2  # type: int
NID_X9_62_c2tnb239v3 = NID_c2tnb239v3  # type: int
NID_X9_62_c2pnb272w1 = NID_c2pnb272w1  # type: int
NID_X9_62_c2pnb304w1 = NID_c2pnb304w1  # type: int
NID_X9_62_c2tnb359v1 = NID_c2tnb359v1  # type: int
NID_X9_62_c2pnb368w1 = NID_c2pnb368w1  # type: int
NID_X9_62_c2tnb431r1 = NID_c2tnb431r1  # type: int

NID_wap_wsg_idm_ecid_wtls1 = m2.NID_wap_wsg_idm_ecid_wtls1  # type: int
NID_wap_wsg_idm_ecid_wtls3 = m2.NID_wap_wsg_idm_ecid_wtls3  # type: int
NID_wap_wsg_idm_ecid_wtls4 = m2.NID_wap_wsg_idm_ecid_wtls4  # type: int
NID_wap_wsg_idm_ecid_wtls5 = m2.NID_wap_wsg_idm_ecid_wtls5  # type: int
NID_wap_wsg_idm_ecid_wtls6 = m2.NID_wap_wsg_idm_ecid_wtls6  # type: int
NID_wap_wsg_idm_ecid_wtls7 = m2.NID_wap_wsg_idm_ecid_wtls7  # type: int
NID_wap_wsg_idm_ecid_wtls8 = m2.NID_wap_wsg_idm_ecid_wtls8  # type: int
NID_wap_wsg_idm_ecid_wtls9 = m2.NID_wap_wsg_idm_ecid_wtls9  # type: int
NID_wap_wsg_idm_ecid_wtls10 = m2.NID_wap_wsg_idm_ecid_wtls10  # type: int
NID_wap_wsg_idm_ecid_wtls11 = m2.NID_wap_wsg_idm_ecid_wtls11  # type: int
NID_wap_wsg_idm_ecid_wtls12 = m2.NID_wap_wsg_idm_ecid_wtls12  # type: int

# The following two curves, according to OpenSSL, have a
# "Questionable extension field!" and are not supported by
# the OpenSSL inverse function.  ECError: no inverse.
# As such they cannot be used for signing.  They might,
# however, be usable for encryption but that has not
# been tested.  Until thir usefulness can be established,
# they are not supported at this time.
# NID_ipsec3 = m2.NID_ipsec3
# NID_ipsec4 = m2.NID_ipsec4


class EC(object):

    """
    Object interface to a EC key pair.
    """

    m2_ec_key_free = m2.ec_key_free

    def __init__(self, ec, _pyfree=0):
        # type: (EC, int) -> None
        assert m2.ec_key_type_check(ec), "'ec' type error"
        self.ec = ec
        self._pyfree = _pyfree

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_ec_key_free(self.ec)

    def __len__(self):
        # type: () -> int
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        return m2.ec_key_keylen(self.ec)

    def gen_key(self):
        # type: () -> int
        """
        Generates the key pair from its parameters. Use::

            keypair = EC.gen_params(curve)
            keypair.gen_key()

        to create an EC key pair.
        """
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        m2.ec_key_gen_key(self.ec)

    def pub(self):
        # type: () -> EC_pub
        # Don't let python free
        return EC_pub(self.ec, 0)

    def sign_dsa(self, digest):
        # type: (bytes) -> Tuple[bytes, bytes]
        """
        Sign the given digest using ECDSA. Returns a tuple (r,s), the two
        ECDSA signature parameters.
        """
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_sign(self.ec, digest)

    def verify_dsa(self, digest, r, s):
        # type: (bytes, bytes, bytes) -> int
        """
        Verify the given digest using ECDSA. r and s are the ECDSA
        signature parameters.
        """
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_verify(self.ec, digest, r, s)

    def sign_dsa_asn1(self, digest):
        # type: (bytes) -> bytes
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_sign_asn1(self.ec, digest)

    def verify_dsa_asn1(self, digest, blob):
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_verify_asn1(self.ec, digest, blob)

    def compute_dh_key(self, pub_key):
        # type: (EC) -> Optional[bytes]
        """
        Compute the ECDH shared key of this key pair and the given public
        key object. They must both use the same curve. Returns the
        shared key in binary as a buffer object. No Key Derivation Function is
        applied.
        """
        assert self.check_key(), 'key is not initialised'
        return m2.ecdh_compute_key(self.ec, pub_key.ec)

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
                         the key. The default is
                         util.passphrase_callback.
        """
        if cipher is None:
            return m2.ec_key_write_bio_no_cipher(self.ec, bio._ptr(), callback)
        else:
            ciph = getattr(m2, cipher, None)
            if ciph is None:
                raise ValueError('not such cipher %s' % cipher)
            return m2.ec_key_write_bio(self.ec, bio._ptr(), ciph(), callback)

    def save_key(self, file, cipher='aes_128_cbc',
                 callback=util.passphrase_callback):
        # type: (AnyStr, Optional[str], Callable) -> int
        """
        Save the key pair to a file in PEM format.

        :param file: Name of filename to save key to.

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

    def save_pub_key_bio(self, bio):
        # type: (BIO.BIO) -> int
        """
        Save the public key to an M2Crypto.BIO.BIO object in PEM format.

        :param bio: M2Crypto.BIO.BIO object to save key to.
        """
        return m2.ec_key_write_pubkey(self.ec, bio._ptr())

    def save_pub_key(self, file):
        # type: (AnyStr) -> int
        """
        Save the public key to a filename in PEM format.

        :param file: Name of filename to save key to.
        """
        with BIO.openfile(file, 'wb') as bio:
            return m2.ec_key_write_pubkey(self.ec, bio._ptr())

    def as_pem(self, cipher='aes_128_cbc', callback=util.passphrase_callback):
        """
        Returns the key(pair) as a string in PEM format.
        If no password is passed and the cipher is set
        it exits with error
        """
        with BIO.MemoryBuffer() as bio:
            self.save_key_bio(bio, cipher, callback)
            return bio.read()

    def _check_key_type(self):
        # type: () -> int
        return m2.ec_key_type_check(self.ec)

    def check_key(self):
        # type: () -> int
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        return m2.ec_key_check_key(self.ec)


class EC_pub(EC):

    """
    Object interface to an EC public key.
    ((don't like this implementation inheritance))
    """
    def __init__(self, ec, _pyfree=0):
        # type: (EC, int) -> None
        EC.__init__(self, ec, _pyfree)
        self.der = None  # type: Optional[bytes]

    def get_der(self):
        # type: () -> bytes
        """
        Returns the public key in DER format as a buffer object.
        """
        assert self.check_key(), 'key is not initialised'
        if self.der is None:
            self.der = m2.ec_key_get_public_der(self.ec)
        return self.der

    def get_key(self):
        # type: () -> bytes
        """
        Returns the public key as a byte string.
        """
        assert self.check_key(), 'key is not initialised'
        return m2.ec_key_get_public_key(self.ec)

    def as_pem(self):
        """
        Returns the key(pair) as a string in PEM format.
        If no password is passed and the cipher is set
        it exits with error
        """
        with BIO.MemoryBuffer() as bio:
            self.save_key_bio(bio)
            return bio.read()

    save_key = EC.save_pub_key

    save_key_bio = EC.save_pub_key_bio


def gen_params(curve):
    # type: (int) -> EC
    """
    Factory function that generates EC parameters and
    instantiates a EC object from the output.

    :param curve: This is the OpenSSL nid of the curve to use.
    """
    assert curve in [x['NID'] for x in m2.ec_get_builtin_curves()], \
        'Elliptic curve %s is not available on this system.' % \
        m2.obj_nid2sn(curve)
    return EC(m2.ec_key_new_by_curve_name(curve), 1)


def load_key(file, callback=util.passphrase_callback):
    # type: (AnyStr, Callable) -> EC
    """
    Factory function that instantiates a EC object.

    :param file: Names the filename that contains the PEM representation
                 of the EC key pair.

    :param callback: Python callback object that will be invoked
                     if the EC key pair is passphrase-protected.
    """
    with BIO.openfile(file) as bio:
        return load_key_bio(bio, callback)


def load_key_string(string, callback=util.passphrase_callback):
    # type: (str, Callable) -> EC
    """
    Load an EC key pair from a string.

    :param string: String containing EC key pair in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to unlock the
                     key. The default is util.passphrase_callback.

    :return: M2Crypto.EC.EC object.
    """
    with BIO.MemoryBuffer(string) as bio:
        return load_key_bio(bio, callback)


def load_key_bio(bio, callback=util.passphrase_callback):
    # type: (BIO.BIO, Callable) -> EC
    """
    Factory function that instantiates a EC object.

    :param bio: M2Crypto.BIO object that contains the PEM
                representation of the EC key pair.

    :param callback: Python callback object that will be invoked
                     if the EC key pair is passphrase-protected.
    """
    return EC(m2.ec_key_read_bio(bio._ptr(), callback), 1)


def load_pub_key(file):
    # type: (AnyStr) -> EC_pub
    """
    Load an EC public key from filename.

    :param file: Name of filename containing EC public key in PEM
                 format.

    :return: M2Crypto.EC.EC_pub object.
    """
    with BIO.openfile(file) as bio:
        return load_pub_key_bio(bio)


def load_key_string_pubkey(string, callback=util.passphrase_callback):
    # type: (str, Callable) -> EC.PKey
    """
    Load an M2Crypto.EC.PKey from a public key as a string.

    :param string: String containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EC.PKey object.
    """
    from M2Crypto.EVP import load_key_bio_pubkey
    with BIO.MemoryBuffer(string) as bio:
        return load_key_bio_pubkey(bio, callback)


def load_pub_key_bio(bio):
    # type: (BIO.BIO) -> EC_pub
    """
    Load an EC public key from an M2Crypto.BIO.BIO object.

    :param bio: M2Crypto.BIO.BIO object containing EC public key in PEM
                format.

    :return: M2Crypto.EC.EC_pub object.
    """
    ec = m2.ec_key_read_pubkey(bio._ptr())
    if ec is None:
        ec_error()
    return EC_pub(ec, 1)


def ec_error():
    # type: () -> ECError
    raise ECError(Err.get_error_message())


def pub_key_from_der(der):
    # type: (bytes) -> EC_pub
    """
    Create EC_pub from DER.
    """
    return EC_pub(m2.ec_key_from_pubkey_der(der), 1)


def pub_key_from_params(curve, bytes):
    # type: (bytes, bytes) -> EC_pub
    """
    Create EC_pub from curve name and octet string.
    """
    return EC_pub(m2.ec_key_from_pubkey_params(curve, bytes), 1)


def get_builtin_curves():
    # type: () -> Tuple[Dict[str, Union[int, str]]]
    return m2.ec_get_builtin_curves()
