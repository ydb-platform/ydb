import datetime

import _openssl
import _rust
import _rust_runtime_test

from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.bindings import _rust as bindings
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.x509.oid import NameOID
from temporalio.runtime import MetricBuffer, Runtime, TelemetryConfig


def test_native_modules():
    assert bindings.x509 is _rust.x509
    assert _openssl.lib.OpenSSL_version_num() > 0


def test_encrypt_and_decrypt():
    cipher = Fernet(Fernet.generate_key())
    assert cipher.decrypt(cipher.encrypt(b"Rust crates built from source")) == b"Rust crates built from source"


def test_sign_and_load_certificate():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Arcadia Rust build")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(datetime.datetime(2020, 1, 1))
        .not_valid_after(datetime.datetime(2030, 1, 1))
        .sign(key, hashes.SHA256())
    )
    encoded = certificate.public_bytes(serialization.Encoding.DER)
    loaded = x509.load_der_x509_certificate(encoded)
    key.public_key().verify(
        loaded.signature, loaded.tbs_certificate_bytes, padding.PKCS1v15(), loaded.signature_hash_algorithm
    )
    encoded_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    decoded_key = serialization.load_pem_private_key(encoded_key, password=None)
    assert decoded_key.private_numbers() == key.private_numbers()


def test_shared_runtime_with_librsvg():
    assert _rust_runtime_test.svg_size() == (17.0, 23.0)


def test_temporal_runtime_with_cryptography():
    metrics = MetricBuffer(1000)
    runtime = Runtime(
        telemetry=TelemetryConfig(logging=None, metrics=metrics),
        worker_heartbeat_interval=None,
    )
    counter = runtime.metric_meter.create_counter("native_rust_counter")
    counter.add(7)
    updates = [update for update in metrics.retrieve_updates() if update.metric.name == "native_rust_counter"]
    assert [update.value for update in updates] == [7]
    cipher = Fernet(Fernet.generate_key())
    assert cipher.decrypt(cipher.encrypt(b"shared Rust runtime")) == b"shared Rust runtime"
