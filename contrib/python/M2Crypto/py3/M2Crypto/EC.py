from __future__ import absolute_import

"""
M2Crypto wrapper for OpenSSL ECDH/ECDSA API.

@requires: OpenSSL 0.9.8 or newer

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved.

Portions copyright (c) 2005-2006 Vrije Universiteit Amsterdam.
All rights reserved."""

from typing import Callable, Dict, Optional, Tuple, Union  # noqa
from M2Crypto import BIO, Err, EVP, m2, types as C, util

EC_Key = bytes


class ECError(Exception):
    pass


m2.ec_init(ECError)

# Curve identifier constants
NID_secp112r1: int = m2.NID_secp112r1
NID_secp112r2: int = m2.NID_secp112r2
NID_secp128r1: int = m2.NID_secp128r1
NID_secp128r2: int = m2.NID_secp128r2
NID_secp160k1: int = m2.NID_secp160k1
NID_secp160r1: int = m2.NID_secp160r1
NID_secp160r2: int = m2.NID_secp160r2
NID_secp192k1: int = m2.NID_secp192k1
NID_secp224k1: int = m2.NID_secp224k1
NID_secp224r1: int = m2.NID_secp224r1
NID_secp256k1: int = m2.NID_secp256k1
NID_secp384r1: int = m2.NID_secp384r1
NID_secp521r1: int = m2.NID_secp521r1
NID_sect113r1: int = m2.NID_sect113r1
NID_sect113r2: int = m2.NID_sect113r2
NID_sect131r1: int = m2.NID_sect131r1
NID_sect131r2: int = m2.NID_sect131r2
NID_sect163k1: int = m2.NID_sect163k1
NID_sect163r1: int = m2.NID_sect163r1
NID_sect163r2: int = m2.NID_sect163r2
NID_sect193r1: int = m2.NID_sect193r1
NID_sect193r2: int = m2.NID_sect193r2
# default for secg.org TLS test server
NID_sect233k1: int = m2.NID_sect233k1
NID_sect233r1: int = m2.NID_sect233r1
NID_sect239k1: int = m2.NID_sect239k1
NID_sect283k1: int = m2.NID_sect283k1
NID_sect283r1: int = m2.NID_sect283r1
NID_sect409k1: int = m2.NID_sect409k1
NID_sect409r1: int = m2.NID_sect409r1
NID_sect571k1: int = m2.NID_sect571k1
NID_sect571r1: int = m2.NID_sect571r1

NID_prime192v1: int = m2.NID_X9_62_prime192v1
NID_prime192v2: int = m2.NID_X9_62_prime192v2
NID_prime192v3: int = m2.NID_X9_62_prime192v3
NID_prime239v1: int = m2.NID_X9_62_prime239v1
NID_prime239v2: int = m2.NID_X9_62_prime239v2
NID_prime239v3: int = m2.NID_X9_62_prime239v3
NID_prime256v1: int = m2.NID_X9_62_prime256v1
NID_c2pnb163v1: int = m2.NID_X9_62_c2pnb163v1
NID_c2pnb163v2: int = m2.NID_X9_62_c2pnb163v2
NID_c2pnb163v3: int = m2.NID_X9_62_c2pnb163v3
NID_c2pnb176v1: int = m2.NID_X9_62_c2pnb176v1
NID_c2tnb191v1: int = m2.NID_X9_62_c2tnb191v1
NID_c2tnb191v2: int = m2.NID_X9_62_c2tnb191v2
NID_c2tnb191v3: int = m2.NID_X9_62_c2tnb191v3
NID_c2pnb208w1: int = m2.NID_X9_62_c2pnb208w1
NID_c2tnb239v1: int = m2.NID_X9_62_c2tnb239v1
NID_c2tnb239v2: int = m2.NID_X9_62_c2tnb239v2
NID_c2tnb239v3: int = m2.NID_X9_62_c2tnb239v3
NID_c2pnb272w1: int = m2.NID_X9_62_c2pnb272w1
NID_c2pnb304w1: int = m2.NID_X9_62_c2pnb304w1
NID_c2tnb359v1: int = m2.NID_X9_62_c2tnb359v1
NID_c2pnb368w1: int = m2.NID_X9_62_c2pnb368w1
NID_c2tnb431r1: int = m2.NID_X9_62_c2tnb431r1

# To preserve compatibility with older names
NID_X9_62_prime192v1: int = NID_prime192v1
NID_X9_62_prime192v2: int = NID_prime192v2
NID_X9_62_prime192v3: int = NID_prime192v3
NID_X9_62_prime239v1: int = NID_prime239v1
NID_X9_62_prime239v2: int = NID_prime239v2
NID_X9_62_prime239v3: int = NID_prime239v3
NID_X9_62_prime256v1: int = NID_prime256v1
NID_X9_62_c2pnb163v1: int = NID_c2pnb163v1
NID_X9_62_c2pnb163v2: int = NID_c2pnb163v2
NID_X9_62_c2pnb163v3: int = NID_c2pnb163v3
NID_X9_62_c2pnb176v1: int = NID_c2pnb176v1
NID_X9_62_c2tnb191v1: int = NID_c2tnb191v1
NID_X9_62_c2tnb191v2: int = NID_c2tnb191v2
NID_X9_62_c2tnb191v3: int = NID_c2tnb191v3
NID_X9_62_c2pnb208w1: int = NID_c2pnb208w1
NID_X9_62_c2tnb239v1: int = NID_c2tnb239v1
NID_X9_62_c2tnb239v2: int = NID_c2tnb239v2
NID_X9_62_c2tnb239v3: int = NID_c2tnb239v3
NID_X9_62_c2pnb272w1: int = NID_c2pnb272w1
NID_X9_62_c2pnb304w1: int = NID_c2pnb304w1
NID_X9_62_c2tnb359v1: int = NID_c2tnb359v1
NID_X9_62_c2pnb368w1: int = NID_c2pnb368w1
NID_X9_62_c2tnb431r1: int = NID_c2tnb431r1

NID_wap_wsg_idm_ecid_wtls1: int = m2.NID_wap_wsg_idm_ecid_wtls1
NID_wap_wsg_idm_ecid_wtls3: int = m2.NID_wap_wsg_idm_ecid_wtls3
NID_wap_wsg_idm_ecid_wtls4: int = m2.NID_wap_wsg_idm_ecid_wtls4
NID_wap_wsg_idm_ecid_wtls5: int = m2.NID_wap_wsg_idm_ecid_wtls5
NID_wap_wsg_idm_ecid_wtls6: int = m2.NID_wap_wsg_idm_ecid_wtls6
NID_wap_wsg_idm_ecid_wtls7: int = m2.NID_wap_wsg_idm_ecid_wtls7
NID_wap_wsg_idm_ecid_wtls8: int = m2.NID_wap_wsg_idm_ecid_wtls8
NID_wap_wsg_idm_ecid_wtls9: int = m2.NID_wap_wsg_idm_ecid_wtls9
NID_wap_wsg_idm_ecid_wtls10: int = m2.NID_wap_wsg_idm_ecid_wtls10
NID_wap_wsg_idm_ecid_wtls11: int = m2.NID_wap_wsg_idm_ecid_wtls11
NID_wap_wsg_idm_ecid_wtls12: int = m2.NID_wap_wsg_idm_ecid_wtls12

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

    def __init__(self, ec: C.EC, _pyfree: int = 0) -> None:
        assert m2.ec_key_type_check(ec), "'ec' type error"
        self.ec = ec
        self._pyfree = _pyfree

    def __del__(self) -> None:
        if getattr(self, '_pyfree', 0):
            self.m2_ec_key_free(self.ec)

    def __len__(self) -> int:
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        return m2.ec_key_keylen(self.ec)

    def gen_key(self) -> int:
        """
        Generates the key pair from its parameters. Use::

            keypair = EC.gen_params(curve)
            keypair.gen_key()

        to create an EC key pair.
        """
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        m2.ec_key_gen_key(self.ec)

    def pub(self) -> "EC_pub":
        # Don't let python free
        return EC_pub(self.ec, 0)

    def sign_dsa(self, digest: bytes) -> Tuple[bytes, bytes]:
        """
        Sign the given digest using ECDSA. Returns a tuple (r,s), the two
        ECDSA signature parameters.
        """
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_sign(self.ec, digest)

    def verify_dsa(self, digest: bytes, r: bytes, s: bytes) -> int:
        """
        Verify the given digest using ECDSA. r and s are the ECDSA
        signature parameters.
        """
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_verify(self.ec, digest, r, s)

    def sign_dsa_asn1(self, digest: bytes) -> bytes:
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_sign_asn1(self.ec, digest)

    def verify_dsa_asn1(self, digest: bytes, blob: bytes) -> int:
        assert self._check_key_type(), "'ec' type error"
        return m2.ecdsa_verify_asn1(self.ec, digest, blob)

    def compute_dh_key(self, pub_key: "EC") -> Optional[bytes]:
        """
        Compute the ECDH shared key of this key pair and the given public
        key object. They must both use the same curve. Returns the
        shared key in binary as a buffer object. No Key Derivation Function is
        applied.
        """
        assert self.check_key(), 'key is not initialised'
        return m2.ecdh_compute_key(self.ec, pub_key.ec)

    def save_key_bio(
        self,
        bio: BIO.BIO,
        cipher: Optional[str] = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> int:
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
            return m2.ec_key_write_bio_no_cipher(
                self.ec, bio._ptr(), callback
            )
        else:
            ciph = getattr(m2, cipher, None)
            if ciph is None:
                raise ValueError('not such cipher %s' % cipher)
            return m2.ec_key_write_bio(
                self.ec, bio._ptr(), ciph(), callback
            )

    def save_key(
        self,
        file: Union[str, bytes],
        cipher: Optional[str] = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> int:
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

    def save_pub_key_bio(self, bio: BIO.BIO) -> int:
        """
        Save the public key to an M2Crypto.BIO.BIO object in PEM format.

        :param bio: M2Crypto.BIO.BIO object to save key to.
        """
        return m2.ec_key_write_pubkey(self.ec, bio._ptr())

    def save_pub_key(self, file: Union[str, bytes]) -> int:
        """
        Save the public key to a filename in PEM format.

        :param file: Name of filename to save key to.
        """
        with BIO.openfile(file, 'wb') as bio:
            return m2.ec_key_write_pubkey(self.ec, bio._ptr())

    def as_pem(
        self,
        cipher: str = 'aes_128_cbc',
        callback: Callable = util.passphrase_callback,
    ) -> bytes:
        """
        Returns the key(pair) as a string in PEM format.
        If no password is passed and the cipher is set
        it exits with error
        """
        with BIO.MemoryBuffer() as bio:
            self.save_key_bio(bio, cipher, callback)
            return bio.read()

    def _check_key_type(self) -> int:
        return m2.ec_key_type_check(self.ec)

    def check_key(self) -> int:
        assert m2.ec_key_type_check(self.ec), "'ec' type error"
        return m2.ec_key_check_key(self.ec)


class EC_pub(EC):
    """
    Object interface to an EC public key.
    ((don't like this implementation inheritance))
    """

    def __init__(self, ec: C.EC, _pyfree: int = 0) -> None:
        EC.__init__(self, ec, _pyfree)
        self.der: Optional[bytes] = None

    def get_der(self) -> bytes:
        """
        Returns the public key in DER format as a buffer object.
        """
        assert self.check_key(), 'key is not initialised'
        if self.der is None:
            self.der = m2.ec_key_get_public_der(self.ec)
        return self.der

    def get_key(self) -> bytes:
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


def gen_params(curve: int) -> EC:
    """
    Factory function that generates EC parameters and
    instantiates a EC object from the output.

    :param curve: This is the OpenSSL nid of the curve to use.
    """
    assert curve in [x['NID'] for x in m2.ec_get_builtin_curves()], (
        'Elliptic curve %s is not available on this system.'
        % m2.obj_nid2sn(curve)
    )
    return EC(m2.ec_key_new_by_curve_name(curve), 1)


def load_key(
    file: Union[str, bytes],
    callback: Callable = util.passphrase_callback,
) -> EC:
    """
    Factory function that instantiates a EC object.

    :param file: Names the filename that contains the PEM representation
                 of the EC key pair.

    :param callback: Python callback object that will be invoked
                     if the EC key pair is passphrase-protected.
    """
    with BIO.openfile(file) as bio:
        return load_key_bio(bio, callback)


def load_key_string(
    string: str, callback: Callable = util.passphrase_callback
) -> EC:
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


def load_key_bio(
    bio: BIO.BIO, callback: Callable = util.passphrase_callback
) -> EC:
    """
    Factory function that instantiates a EC object.

    :param bio: M2Crypto.BIO object that contains the PEM
                representation of the EC key pair.

    :param callback: Python callback object that will be invoked
                     if the EC key pair is passphrase-protected.
    """
    key = m2.ec_key_read_bio(bio._ptr(), callback)
    if key is None:
        raise IOError(
            "Cannot read EC key pair from PEM file {}.".format(
                bio.fname
            )
        )
    return EC(key, 1)


def load_pub_key(file: Union[str, bytes]) -> EC_pub:
    """
    Load an EC public key from filename.

    :param file: Name of filename containing EC public key in PEM
                 format.

    :return: M2Crypto.EC.EC_pub object.
    """
    with BIO.openfile(file) as bio:
        return load_pub_key_bio(bio)


def load_key_string_pubkey(
    string: str, callback: Callable = util.passphrase_callback
) -> "EVP.PKey":
    """
    Load an M2Crypto.EVP.PKey from a public key as a string.

    :param string: String containing the key in PEM format.

    :param callback: A Python callable object that is invoked
                     to acquire a passphrase with which to protect the
                     key.

    :return: M2Crypto.EVP.PKey object.
    """
    from M2Crypto.EVP import load_key_bio_pubkey

    with BIO.MemoryBuffer(string) as bio:
        return load_key_bio_pubkey(bio, callback)


def load_pub_key_bio(bio: BIO.BIO) -> EC_pub:
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


def ec_error() -> EC_pub:
    raise ECError(Err.get_error_message())


def pub_key_from_der(der: bytes) -> EC_pub:
    """
    Create EC_pub from DER.
    """
    return EC_pub(m2.ec_key_from_pubkey_der(der), 1)


def pub_key_from_params(curve: bytes, bytes: bytes) -> EC_pub:
    """
    Create EC_pub from curve name and octet string.
    """
    return EC_pub(m2.ec_key_from_pubkey_params(curve, bytes), 1)


def get_builtin_curves() -> Tuple[Dict[str, Union[int, str]]]:
    return m2.ec_get_builtin_curves()
