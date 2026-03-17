###############################################################################
# Copyright (c) 2018, Lawrence Livermore National Security, LLC
# Produced at the Lawrence Livermore National Laboratory
# Written by Thomas Mendoza mendoza33@llnl.gov
# LLNL-CODE-754897
# All rights reserved
#
# This file is part of Certipy: https://github.com/LLNL/certipy
#
# SPDX-License-Identifier: BSD-3-Clause
###############################################################################

import os
import pytest
import socket
import ssl
from urllib.request import urlopen, URLError
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from flask import Flask
from tempfile import TemporaryDirectory
from threading import Thread
from werkzeug.serving import make_server

from pytest import fixture

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from ipaddress import ip_address

from certipy.certipy import (
    TLSFileType,
    TLSFile,
    TLSFileBundle,
    CertStore,
    open_tls_file,
    Certipy,
)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def make_flask_app():
    app = Flask(__name__)

    @app.route("/")
    def working():
        return "working"
    
    return app


@contextmanager
def tls_server(certfile: str, keyfile: str, host: str = "localhost", port: int = 0):
    if port == 0:
        port = find_free_port()

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile, keyfile)
    server = make_server(
        host, port, make_flask_app(), ssl_context=ssl_context, threaded=True
    )
    t = Thread(target=server.serve_forever)
    t.start()
    try:
        yield server
    finally:
        server.shutdown()


@fixture
def fake_cert_file(tmp_path):
    sub_dir = tmp_path / "certipy"
    sub_dir.mkdir()

    filename = sub_dir / "foo.crt"
    filename.touch()
    return filename


@fixture(scope="module")
def signed_key_pair():
    pkey = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    subject = issuer = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(pkey.public_key())
        .serial_number(1)
        .not_valid_before(datetime.now(timezone.utc))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=365 * 2))
        .sign(pkey, hashes.SHA256())
    )
    return (pkey, cert)


@fixture(scope="module")
def record():
    return {
        "serial": 1,
        "parent_ca": "",
        "signees": None,
        "files": {
            "key": "out/foo.key",
            "cert": "out/foo.crt",
            "ca": "out/ca.crt",
        },
    }


def test_tls_context_manager(fake_cert_file):
    def simple_perms(f):
        return oct(os.stat(f).st_mode & 0o777)

    # read
    with pytest.raises(OSError):
        with open_tls_file("foo.test", "r"):
            pass

    with open_tls_file(fake_cert_file, "r"):
        pass

    # write
    containing_dir = os.path.dirname(fake_cert_file)
    # public certificate
    with open_tls_file(fake_cert_file, "w", private=False):
        assert simple_perms(containing_dir) == "0o755"

    assert simple_perms(fake_cert_file) == "0o644"

    # private certificate
    with open_tls_file(fake_cert_file, "w"):
        assert simple_perms(containing_dir) == "0o755"

    assert simple_perms(fake_cert_file) == "0o600"


def test_tls_file(signed_key_pair, fake_cert_file):
    key, cert = signed_key_pair

    def read_write_key(file_type):
        tlsfile = TLSFile(fake_cert_file, file_type=file_type)
        # test persist to disk
        x509 = cert if file_type is TLSFileType.CERT else key
        tlsfile.save(x509)
        with open(fake_cert_file, "r") as f:
            assert f.read() is not None
        # test load from disk
        loaded_tlsfile = TLSFile(fake_cert_file, file_type=file_type)
        loaded_tlsfile.x509 = tlsfile.load()
        assert str(loaded_tlsfile) == str(tlsfile)

    # public key
    read_write_key(TLSFileType.CERT)

    # private key
    read_write_key(TLSFileType.KEY)


def test_tls_file_bundle(signed_key_pair, record):
    key, cert = signed_key_pair
    # from record
    bundle = TLSFileBundle("foo").from_record(record)
    assert bundle.key and bundle.cert and bundle.ca

    # to record
    exported_record = bundle.to_record()
    f_types = {key for key in exported_record["files"].keys()}
    assert len(f_types) == 3
    assert f_types == {"key", "cert", "ca"}


def test_certipy_store(signed_key_pair, record):
    key, cert = signed_key_pair
    key_str = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    cert_str = cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    with TemporaryDirectory() as td:
        common_name = "foo"
        store = CertStore(containing_dir=td)
        # add files
        x509s = {
            "key": key,
            "cert": cert,
            "ca": None,
        }
        store.add_files(common_name, x509s)

        # check the TLSFiles
        bundle = store.get_files(common_name)
        bundle.key.load()
        bundle.cert.load()
        assert key_str == str(bundle.key)
        assert cert_str == str(bundle.cert)

        # save the store records to a file
        store.save()

        # read the records back in
        store.load()

        # check the record for those files
        main_record = store.get_record(common_name)
        non_empty_paths = [f for f in main_record["files"].values() if f]
        assert len(non_empty_paths) == 2

        # add another record with no physical files
        signee_common_name = "bar"
        store.add_record(signee_common_name, record=record)

        # 'sign' cert
        store.add_sign_link(common_name, signee_common_name)
        signee_record = store.get_record(signee_common_name)
        assert len(main_record["signees"]) == 1
        assert signee_record["parent_ca"] == common_name


def test_certipy():
    # FIXME: unfortunately similar names...either separate tests or rename
    with TemporaryDirectory() as td:
        # create a CA
        ca_name = "foo"
        certipy = Certipy(store_dir=td)
        ca_record = certipy.create_ca(ca_name, pathlen=None)

        non_empty_paths = [f for f in ca_record["files"].values() if f]
        assert len(non_empty_paths) == 2

        # check that the paths are backed by actual files
        ca_bundle = certipy.store.get_files(ca_name)
        assert ca_bundle.key.load() is not None
        assert ca_bundle.cert.load() is not None
        assert "PRIVATE" in str(ca_bundle.key)
        assert "CERTIFICATE" in str(ca_bundle.cert)

        # create a cert and sign it with that CA
        cert_name = "bar"
        alt_names = ["DNS:bar.example.com", "IP:10.10.10.10"]
        cert_record = certipy.create_signed_pair(
            cert_name, ca_name, alt_names=alt_names
        )

        non_empty_paths = [f for f in cert_record["files"].values() if f]
        assert len(non_empty_paths) == 3
        assert cert_record["files"]["ca"] == ca_record["files"]["cert"]

        cert_bundle = certipy.store.get_files(cert_name)
        cert_bundle.cert.load()

        subject_alt = cert_bundle.cert.get_extension_value(x509.SubjectAlternativeName)
        assert subject_alt is not None
        assert subject_alt.get_values_for_type(x509.IPAddress) == [
            ip_address("10.10.10.10")
        ]
        assert subject_alt.get_values_for_type(x509.DNSName) == ["bar.example.com"]

        # add a second CA
        ca_name1 = "baz"
        certipy.create_ca(ca_name1)

        # create a bundle from all known certs
        bundle_file_name = "bundle.crt"
        bundle_file = certipy.create_ca_bundle(bundle_file_name)
        ca_bundle1 = certipy.store.get_files(ca_name1)
        ca_bundle1.cert.load()
        assert bundle_file is not None
        with open(bundle_file, "r") as fh:
            all_certs = fh.read()

            # should contain both CA certs
            assert str(ca_bundle.cert) in all_certs
            assert str(ca_bundle1.cert) in all_certs

        # bundle of CA certs for only a single name this time
        bundle_file = certipy.create_ca_bundle_for_names(bundle_file_name, ["bar"])
        assert bundle_file is not None
        with open(bundle_file, "r") as fh:
            all_certs = fh.read()
            assert str(ca_bundle.cert) in all_certs
            assert str(ca_bundle1.cert) not in all_certs

        # delete certs
        deleted_record = certipy.store.remove_files("bar", delete_dir=True)
        assert not os.path.exists(deleted_record["files"]["cert"])
        assert not os.path.exists(deleted_record["files"]["key"])

        # the CA cert should still be around, we have to delete that explicitly
        assert os.path.exists(deleted_record["files"]["ca"])

        # create an intermediate CA
        begin_ca_signee_num = len(ca_record["signees"] or {})
        intermediate_ca = "bat"
        certipy.create_ca(intermediate_ca, ca_name=ca_name, pathlen=1)
        end_ca_signee_num = len(ca_record["signees"])
        intermediate_ca_bundle = certipy.store.get_files(intermediate_ca)
        basic_constraints = intermediate_ca_bundle.cert.get_extension_value(
            "basicConstraints"
        )

        assert end_ca_signee_num > begin_ca_signee_num
        assert intermediate_ca_bundle.record["parent_ca"] == ca_name
        assert intermediate_ca_bundle.is_ca()
        assert basic_constraints.path_length == 1


def test_certipy_trust_graph():
    trust_graph = {
        "foo": ["foo", "bar"],
        "bar": ["foo"],
        "baz": ["bar"],
    }

    def distinct_components(graph):
        """Return a set of components from the provided graph."""
        components = set(graph.keys())
        for trusts in graph.values():
            components |= set(trusts)
        return components

    with TemporaryDirectory() as td:
        certipy = Certipy(store_dir=td)
        # after this, all components in the graph should exist in certipy
        trust_files = certipy.trust_from_graph(trust_graph)

        bundles = {}
        all_components = distinct_components(trust_graph)

        for component in all_components:
            bundles[component] = certipy.store.get_files(component)

        # components should only trust others listed explicitly in the graph
        for component, trusts in trust_graph.items():
            trust_file = trust_files[component]
            not_trusts = all_components - set(trusts)
            with open(trust_file) as fh:
                trust_bundle = fh.read()
                for trusted_comp in trusts:
                    bundle = bundles[trusted_comp]
                    assert str(bundle.cert) in trust_bundle
                for untrusted_comp in not_trusts:
                    bundle = bundles[untrusted_comp]
                    assert str(bundle.cert) not in trust_bundle


def test_certs():
    with TemporaryDirectory() as td:
        # Setup
        ca_name = "foo"
        certipy = Certipy(store_dir=td)
        ca_record = certipy.create_ca(ca_name, pathlen=-1)

        cert_name = "bar"
        alt_names = ["DNS:localhost", "IP:127.0.0.1"]
        cert_record = certipy.create_signed_pair(
            cert_name, ca_name, alt_names=alt_names
        )

        with tls_server(
            cert_record["files"]["cert"], cert_record["files"]["key"]
        ) as server:
            # Execute/Verify
            url = f"https://{server.host}:{server.port}"
            # Fails without specifying a CA for verification
            with pytest.raises(URLError, match="SSL"):
                with urlopen(url):
                    pass

            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_record["files"]["cert"])
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_default_certs()
            ssl_context.load_cert_chain(ca_record["files"]["cert"], ca_record["files"]["key"])

            # Succeeds when supplying the CA cert
            with urlopen(url, context=ssl_context):
                pass
