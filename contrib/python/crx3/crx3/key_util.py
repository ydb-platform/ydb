# -*- coding: utf-8 -*-

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

RSA_F4 = 65537
RSA_KEY_SIZE = 2048


def create_private_key():
    """
    Create RSA private key, length 2048.
    Refer: https://source.chromium.org/chromium/chromium/src/+/main:crypto/rsa_private_key.cc
    :return: Generated RSA private key object
    """
    return rsa.generate_private_key(public_exponent=RSA_F4, key_size=RSA_KEY_SIZE, backend=default_backend())


def get_private_key_data(private_key):
    """
    Get private key data in PEM format.
    Refer: https://source.chromium.org/chromium/chromium/src/+/main:crypto/rsa_private_key.h
    :param private_key: RSAPrivateKey object
    :return: private key bytes
    """
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def load_private_key_from_pem(pem_data):
    """
    Load private key from pem format bytes.
    :param pem_data: pem format bytes
    :return: RSAPrivateKey object
    """
    return serialization.load_pem_private_key(pem_data, password=None, backend=default_backend())


def load_public_key_from_pem(pem_data):
    """
    Load public key from pem format bytes.
    :param pem_data: pem format bytes
    :return: RSAPublicKey object
    """
    return serialization.load_pem_public_key(pem_data, backend=default_backend())


def load_public_key_from_der(der_data):
    """
    Load public key from der format bytes.
    :param der_data: der format bytes
    :return: RSAPublicKey object
    """
    return serialization.load_der_public_key(der_data, backend=default_backend())


def extract_public_key_from_private_key(private_key):
    """
    Extract public key from private key.
    :param private_key: RSAPrivateKey object
    :return: RSAPublicKey object
    """
    return private_key.public_key()


def get_public_key_data(public_key):
    """
    Get public key data in DER format.
    :param public_key: RSAPublicKey object
    :return: public key bytes
    """
    return public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
