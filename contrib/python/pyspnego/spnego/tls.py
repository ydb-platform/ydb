# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import dataclasses
import datetime
import platform
import ssl
import typing

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa


@dataclasses.dataclass
class CredSSPTLSContext:
    """A TLS context generated for CredSSP.

    This is the SSLContext object used by both an initiator and acceptor of
    CredSSP authentication. It allows the caller to finely control the SSL/TLS
    options used during the CredSSP authentication phase, e.g. selecting
    min/max protocol versions, specific cipher suites, etc.

    The public_key attribute is used for acceptor CredSSP contexts as the
    DER encoded public key loaded in the SSLContext. Here is an example of
    generating and loading a X509 certificate for an acceptor context:

        ctx = spnego.tls.default_tls_context()
        cert_pem, key_Pem, pub_key = spnego.tls.generate_tls_certificate()

        # Cannot use tempfile.NamedTemporaryFile due to sharing violations on
        # Windows. Use a tempdir as a workaround.
        temp_dir = tempfile.mkdtemp()
        try:
            cert_path = os.path.join(tmpe_dir, 'ca.pem')
            with open(cert_path, mode'wb') as fd:
                fd.write(cert_pem)
                fd.write(key_pem)

            ctx.context.load_cert_chain(cert_path)
            ctx.public_key = pub_key

        finally:
            shutil.rmtree(temp_dir)

    This context is then passed in through the `credssp_tls_context` kwarg of
    :meth:`spnego.client` or :meth:`spnego.server`.

    Attributes:
        context (ssl.SSLContext): The TLS context generated for CredSSP.
        public_key (Optional[bytes]): When generating the TLS context for an
            acceptor this is the public key bytes for the generated cert in the
            TLS context.
    """

    context: ssl.SSLContext
    public_key: typing.Optional[bytes] = None


def default_tls_context(
    usage: str = "initiate",
) -> CredSSPTLSContext:
    """CredSSP TLSContext with sane defaults.

    Creates the TLS context used to generate the SSL object for CredSSP
    authentication. By default the TLS context will set the minimum protocol to
    TLSv1.2. Certificate verification is also disabled for both the initiator
    and acceptor as per the `MS-CSSP Events and Sequencing Rules`_ in step 1.
    This can be used as a base context where the caller applies further changes
    based on their requirements such as cert validation and so forth.

    This context is then passed in through the `credssp_tls_context` kwarg of
    :meth:`spnego.client` or :meth:`spnego.server`.

    Args:
        usage: Either `initiate` for a client context or `accept` for a server context.

    Returns:
        TLSContext: The TLS context that can be used with CredSSP auth.

    .. _MS-CSSP Events and Sequencing Rules:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-cssp/385a7489-d46b-464c-b224-f7340e308a5c
    """
    if usage == "initiate":
        # TLS_CLIENT enables CA/CN checking but CredSSP does not use this by default in the docs. The caller can
        # re-enable this if needed.
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    # Required to interop with SChannel which does not support compression, TLS padding, and empty fragments
    # SSL_OP_NO_COMPRESSION | SSL_OP_TLS_BLOCK_PADDING_BUG | SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS
    ctx.options |= ssl.OP_NO_COMPRESSION | 0x00000200 | 0x00000800

    # The minimum_version field requires OpenSSL 1.1.0g or newer, fallback to the deprecated method of setting the
    # OP_NO_* options.
    # FUTURE: Remove once Python 3.10 is minimum which requires 1.1.1 or newer
    tls_version = getattr(ssl, "TLSVersion", None)
    if hasattr(ctx, "minimum_version") and tls_version:
        setattr(ctx, "minimum_version", tls_version.TLSv1_2)

    else:
        ctx.options |= (
            ssl.Options.OP_NO_SSLv2 | ssl.Options.OP_NO_SSLv3 | ssl.Options.OP_NO_TLSv1 | ssl.Options.OP_NO_TLSv1_1
        )

    return CredSSPTLSContext(context=ctx)


def generate_tls_certificate() -> typing.Tuple[bytes, bytes, bytes]:
    """Generates X509 cert and key for CredSSP acceptor.

    Generates a TLS X509 certificate and key that can be used by a CredSSP
    acceptor for authentication. This certificate is modelled after the one
    that the WSMan CredSSP service uses on Windows.

    Returns:
        Tuple[bytes, bytes, bytes]: The X509 PEM encoded certificate,
        PEM encoded key, and DER encoded public key.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    # socket.getfqdn() can block for a few seconds if DNS is not set up properly.
    # The name also has a max length of 64 characters.
    cn_name = f"CREDSSP-{platform.node()}"[:64]
    name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, cn_name)])

    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )
    cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_key = cert.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.PKCS1)

    return cert_pem, key_pem, public_key


def get_certificate_public_key(
    data: bytes,
) -> bytes:
    """Public key bytes of an X.509 Certificate.

    Gets the public key bytes used by CredSSP of the provided X.509
    certificate. Use this for the `public_key` attribute of
    class:`CredSSPTLSContext` for an acceptor context when providing your own
    certificate.

    Args:
        data: The DER encoded bytes of the X.509 certificate.

    Returns:
        bytes: The public key bytes of the certificate.
    """
    cert = x509.load_der_x509_certificate(data, default_backend())
    public_key = cert.public_key()

    return public_key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.PKCS1)
