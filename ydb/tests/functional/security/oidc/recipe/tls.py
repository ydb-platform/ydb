"""Ephemeral TLS certificates for the Keycloak integration fixture."""

import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def _new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _certificate(subject, issuer, public_key):
    now = datetime.datetime.now(datetime.timezone.utc)
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
    )


def prepare(directory):
    """Generate a private CA and localhost TLS identity."""
    directory = Path(directory)
    ca_key = _new_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'OIDC test CA')])
    ca = (
        _certificate(ca_name, ca_name, ca_key.public_key())
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    tls_key = _new_key()
    tls_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    tls = (
        _certificate(tls_name, ca_name, tls_key.public_key())
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName('localhost'),
                    # The SDK HTTP client uses OpenSSL's DNS-name verifier
                    # even for an IP literal, so cover both SAN forms.
                    x509.DNSName('127.0.0.1'),
                    x509.IPAddress(ipaddress.ip_address('127.0.0.1')),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    for name, cert in [('ca.pem', ca), ('server.pem', tls)]:
        (directory / name).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    for name, key in [('server.key', tls_key)]:
        path = directory / name
        path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        path.chmod(0o600)
