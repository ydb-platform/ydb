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
import json
import shutil
import warnings
from enum import Enum
from functools import partial
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography import x509
from ipaddress import ip_address


class KeyType(Enum):
    """Enum for available key types"""

    rsa = "rsa"
    # not supported (widely deprecated)
    # dsa = 'dsa'
    # not supported (yet)
    # ecdsa = 'ecdsa'


class TLSFileType(Enum):
    KEY = "key"
    CERT = "cert"
    CA = "ca"


def _make_oid_map(enum_cls):
    """Make a mapping of name: OIDClass from an Enum

    for easy lookup of e.g. 'subjectAltName': x509.ExtensionOID.SubjectAlternativeName
    """
    mapping = {}
    for name in dir(enum_cls):
        if name.startswith("_"):
            continue
        value = getattr(enum_cls, name)
        if isinstance(value, x509.ObjectIdentifier):
            mapping[value._name] = value
    return mapping


# mapping of CN to NameOID.COUNTRY_NAME
_name_oid_map = _make_oid_map(x509.oid.NameOID)
# short names
_name_oid_map["C"] = x509.oid.NameOID.COUNTRY_NAME
_name_oid_map["ST"] = x509.oid.NameOID.STATE_OR_PROVINCE_NAME
_name_oid_map["L"] = x509.oid.NameOID.LOCALITY_NAME
_name_oid_map["O"] = x509.oid.NameOID.ORGANIZATION_NAME
_name_oid_map["OU"] = x509.oid.NameOID.ORGANIZATIONAL_UNIT_NAME
_name_oid_map["CN"] = x509.oid.NameOID.COMMON_NAME

_ext_oid_map = _make_oid_map(x509.oid.ExtensionOID)


def _altname(name):
    """Construct a subjectAltName field from an OpenSSL-style string

    turns IP:1.2.3.4 into x509.IPAddress('1.2.3.4')
    """
    key, _, value = name.partition(":")
    # are these case sensitive? I don't find a spec,
    # in practice only IP and DNS are used.
    if key == "IP":
        return x509.IPAddress(ip_address(value))
    elif key == "DNS":
        return x509.DNSName(value)
    elif key == "email":
        return x509.RFC822Name(value)
    elif key == "URI":
        return x509.UniformResourceIdentifier(value)
    elif key == "RID":
        return x509.RegisteredID(value)
    elif key == "dirName":
        return x509.DirectoryName(value)
    else:
        raise ValueError(f"Unrecognized subjectAltName prefix {key!r} in {name!r}")


class CertNotFoundError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class CertExistsError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class CertificateAuthorityInUseError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


@contextmanager
def open_tls_file(file_path, mode, private=True):
    """Context to ensure correct file permissions for certs and directories

    Ensures:
        - A containing directory with appropriate permissions
        - Correct file permissions based on what the file is (0o600 for keys
        and 0o644 for certs)
    """

    containing_dir = os.path.dirname(file_path)
    fh = None
    try:
        if "w" in mode:
            os.chmod(containing_dir, mode=0o755)
        fh = open(file_path, mode)
    except OSError:
        if "w" in mode:
            os.makedirs(containing_dir, mode=0o755, exist_ok=True)
            os.chmod(containing_dir, mode=0o755)
            fh = open(file_path, "w")
        else:
            raise
    yield fh
    mode = 0o600 if private else 0o644
    os.chmod(file_path, mode=mode)
    fh.close()


class TLSFile:
    """Describes basic information about files used for TLS"""

    def __init__(
        self,
        file_path,
        encoding=serialization.Encoding.PEM,
        file_type=TLSFileType.CERT,
        x509=None,
    ):
        if isinstance(encoding, int):
            warnings.warn(
                "OpenSSL.crypto.TYPE_* encoding arguments are deprecated. Use cryptography.hazmat.primitives.serialization.Encoding enum or string 'PEM'",
                DeprecationWarning,
                stacklevel=2,
            )
            # match values in OpenSSL.crypto
            if encoding == 1:
                # PEM
                encoding = serialization.Encoding.PEM
            elif encoding == 2:
                # ASN / DER
                encoding = serialization.Encoding.DER

        self.file_path = file_path
        self.containing_dir = os.path.dirname(self.file_path)
        self.encoding = serialization.Encoding(encoding)
        self.file_type = file_type
        self.x509 = x509

    def __str__(self):
        data = ""
        if not self.x509:
            self.load()

        if self.file_type is TLSFileType.KEY:
            data = self.x509.private_bytes(
                encoding=self.encoding,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            data = self.x509.public_bytes(self.encoding)

        return data.decode("utf-8")

    def get_extension_value(self, ext_name):
        if self.is_private():
            return
        if not self.x509:
            self.load()

        if isinstance(ext_name, str):
            # string name, lookup OID
            ext_oid = _ext_oid_map[ext_name]
        elif hasattr(ext_name, "oid"):
            # given ExtensionType, get OID
            ext_oid = ext_name.oid
        else:
            # otherwise, assume given OID
            ext_oid = ext_name

        try:
            return self.x509.extensions.get_extension_for_oid(ext_oid).value
        except x509.ExtensionNotFound:
            return None

    def is_ca(self):
        if self.is_private():
            return False

        if not self.x509:
            self.load()

        try:
            basic_constraints = self.x509.get_extension_for_class(x509.BasicConstraints)
        except x509.ExtensionNotFound:
            return False
        else:
            return basic_constraints.ca

    def is_private(self):
        """Is this a private key"""

        return True if self.file_type is TLSFileType.KEY else False

    def load(self):
        """Load from a file and return an x509 object"""

        private = self.is_private()
        if private:
            if self.encoding == serialization.Encoding.DER:
                load = serialization.load_der_private_key
            else:
                load = serialization.load_pem_private_key
            load = partial(load, password=None)
        else:
            if self.encoding == serialization.Encoding.DER:
                load = x509.load_der_x509_certificate
            else:
                load = x509.load_pem_x509_certificate
        with open_tls_file(self.file_path, "rb", private=private) as fh:
            self.x509 = load(fh.read())
        return self.x509

    def save(self, x509):
        """Persist this x509 object to disk"""

        self.x509 = x509
        with open_tls_file(self.file_path, "w", private=self.is_private()) as fh:
            fh.write(str(self))


class TLSFileBundle:
    """Maintains information that is shared by a set of TLSFiles"""

    def __init__(
        self,
        common_name,
        files=None,
        x509s=None,
        serial=0,
        is_ca=False,
        parent_ca="",
        signees=None,
    ):
        self.record = {}
        self.record["serial"] = serial
        self.record["is_ca"] = is_ca
        self.record["parent_ca"] = parent_ca
        self.record["signees"] = signees
        for t in TLSFileType:
            setattr(self, t.value, None)

        files = files or {}
        x509s = x509s or {}
        self._setup_tls_files(files)
        self.save_x509s(x509s)

    def _setup_tls_files(self, files):
        """Initiates TLSFIle objects with the paths given to this bundle"""

        for file_type in TLSFileType:
            if file_type.value in files:
                file_path = files[file_type.value]
                setattr(self, file_type.value, TLSFile(file_path, file_type=file_type))

    def save_x509s(self, x509s):
        """Saves the x509 objects to the paths known by this bundle"""

        for file_type in TLSFileType:
            if file_type.value in x509s:
                x509 = x509s[file_type.value]
                if file_type is not TLSFileType.CA:
                    # persist this key or cert to disk
                    tlsfile = getattr(self, file_type.value)
                    if tlsfile:
                        tlsfile.save(x509)

    def load_all(self):
        """Utility to load bring all files into memory"""

        for t in TLSFileType:
            self[t.value].load()
        return self

    def is_ca(self):
        """Is this bundle for a CA certificate"""
        return self.record["is_ca"]

    def to_record(self):
        """Create a CertStore record from this TLSFileBundle"""

        tf_list = [getattr(self, k, None) for k in [_.value for _ in TLSFileType]]
        # If a cert isn't defined in this bundle, remove it
        tf_list = filter(lambda x: x, tf_list)
        files = {tf.file_type.value: tf.file_path for tf in tf_list}
        self.record["files"] = files
        return self.record

    def from_record(self, record):
        """Build a bundle from a CertStore record"""

        self.record = record
        self._setup_tls_files(self.record["files"])
        return self


class CertStore:
    """Maintains records of certificates created by Certipy

    Minimally, each record keyed by common name needs:
        - file
            - path
            - type
        - serial number
        - parent CA
        - signees
    Common names, for the sake of simplicity, are assumed to be unique.
    If a pair of certs need to be valid for the same IP/DNS address (ex:
    localhost), that information can be specified in the Subject Alternative
    Name field.
    """

    def __init__(
        self, containing_dir="out", store_file="certipy.json", remove_existing=False
    ):
        self.store = {}
        self.containing_dir = containing_dir
        self.store_file_path = os.path.join(containing_dir, store_file)
        try:
            if remove_existing:
                shutil.rmtree(containing_dir)
            os.stat(containing_dir)
            self.load()
        except FileNotFoundError:
            os.makedirs(containing_dir, mode=0o755, exist_ok=True)
        finally:
            os.chmod(containing_dir, mode=0o755)

    def save(self):
        """Write the store dict to a file specified by store_file_path"""

        with open(self.store_file_path, "w") as fh:
            fh.write(json.dumps(self.store, indent=4))

    def load(self):
        """Read the store dict from file"""

        with open(self.store_file_path, "r") as fh:
            self.store = json.loads(fh.read())

    def get_record(self, common_name):
        """Return the record associated with this common name

        In most cases, all that's really needed to use an existing cert are
        the file paths to the files that make up that cert. This method
        returns just that and doesn't bother loading the associated files.
        """

        try:
            record = self.store[common_name]
            return record
        except KeyError as e:
            raise CertNotFoundError(
                "Unable to find record of {name}".format(name=common_name), errors=e
            )

    def get_files(self, common_name):
        """Return a bundle of TLS files associated with a common name"""

        record = self.get_record(common_name)
        return TLSFileBundle(common_name).from_record(record)

    def add_record(
        self,
        common_name,
        serial=0,
        parent_ca="",
        signees=None,
        files=None,
        record=None,
        is_ca=False,
        overwrite=False,
    ):
        """Manually create a record of certs

        Generally, Certipy can be left to manage certificate locations and
        storage, but it is occasionally useful to keep track of a set of
        certs that were created externally (for example, let's encrypt)
        """

        if not overwrite:
            try:
                self.get_record(common_name)
                raise CertExistsError(
                    "Certificate {name} already exists!"
                    " Set overwrite=True to force add.".format(name=common_name)
                )
            except CertNotFoundError:
                pass

        record = record or {
            "serial": serial,
            "is_ca": is_ca,
            "parent_ca": parent_ca,
            "signees": signees,
            "files": files,
        }
        self.store[common_name] = record
        self.save()

    def add_files(
        self,
        common_name,
        x509s,
        files=None,
        parent_ca="",
        is_ca=False,
        signees=None,
        serial=0,
        overwrite=False,
    ):
        """Add a set files comprising a certificate to Certipy

        Used with all the defaults, Certipy will manage creation of file paths
        to be used to store these files to disk and automatically calls save
        on all TLSFiles that it creates (and where it makes sense to).
        """

        if common_name in self.store and not overwrite:
            raise CertExistsError(
                "Certificate {name} already exists!"
                " Set overwrite=True to force add.".format(name=common_name)
            )
        elif common_name in self.store and overwrite:
            record = self.get_record(common_name)
            serial = int(record["serial"])
            record["serial"] = serial + 1
            TLSFileBundle(common_name).from_record(record).save_x509s(x509s)
        else:
            file_base_tmpl = "{prefix}/{cn}/{cn}"
            file_base = file_base_tmpl.format(
                prefix=self.containing_dir, cn=common_name
            )
            try:
                ca_record = self.get_record(parent_ca)
                ca_file = ca_record["files"]["cert"]
            except CertNotFoundError:
                ca_file = ""
            files = files or {
                "key": file_base + ".key",
                "cert": file_base + ".crt",
                "ca": ca_file,
            }
            bundle = TLSFileBundle(
                common_name,
                files=files,
                x509s=x509s,
                is_ca=is_ca,
                serial=serial,
                parent_ca=parent_ca,
                signees=signees,
            )
            self.store[common_name] = bundle.to_record()
        self.save()

    def add_sign_link(self, ca_name, signee_name):
        """Adds to the CA signees and a parent ref to the signee"""

        ca_record = self.get_record(ca_name)
        signee_record = self.get_record(signee_name)
        signees = ca_record["signees"] or {}
        signees = Counter(signees)
        if signee_name not in signees:
            signees[signee_name] = 1
            ca_record["signees"] = signees
            signee_record["parent_ca"] = ca_name
        self.save()

    def remove_sign_link(self, ca_name, signee_name):
        """Removes signee_name to the signee list for ca_name"""

        ca_record = self.get_record(ca_name)
        signee_record = self.get_record(signee_name)
        signees = ca_record["signees"] or {}
        signees = Counter(signees)
        if signee_name in signees:
            signees[signee_name] = 0
            ca_record["signees"] = signees
            signee_record["parent_ca"] = ""
        self.save()

    def update_record(self, common_name, **fields):
        """Update fields in an existing record"""

        record = self.get_record(common_name)
        if fields is not None:
            for field, value in fields:
                record[field] = value
        self.save()
        return record

    def remove_record(self, common_name):
        """Delete the record associated with this common name"""

        bundle = self.get_files(common_name)
        num_signees = len(Counter(bundle.record["signees"]))
        if bundle.is_ca() and num_signees > 0:
            raise CertificateAuthorityInUseError(
                "Authority {name} has signed {x} certificates".format(
                    name=common_name, x=num_signees
                )
            )
        try:
            ca_name = bundle.record["parent_ca"]
            self.get_record(ca_name)
            self.remove_sign_link(ca_name, common_name)
        except CertNotFoundError:
            pass
        record_copy = dict(self.store[common_name])
        del self.store[common_name]
        self.save()
        return record_copy

    def remove_files(self, common_name, delete_dir=False):
        """Delete files and record associated with this common name"""

        record = self.remove_record(common_name)
        if delete_dir:
            delete_dirs = []
            if "files" in record:
                key_containing_dir = os.path.dirname(record["files"]["key"])
                delete_dirs.append(key_containing_dir)
                cert_containing_dir = os.path.dirname(record["files"]["cert"])
                if key_containing_dir != cert_containing_dir:
                    delete_dirs.append(cert_containing_dir)
                for d in delete_dirs:
                    shutil.rmtree(d)
        return record


class Certipy:
    def __init__(
        self, store_dir="out", store_file="certipy.json", remove_existing=False
    ):
        self.store = CertStore(
            containing_dir=store_dir,
            store_file=store_file,
            remove_existing=remove_existing,
        )

    def create_key_pair(self, cert_type, bits):
        """
        Create a public/private key pair.

        Arguments: type - Key type, must be one of KeyType (currently only 'rsa')
                   bits - Number of bits to use in the key
        Returns:   The cryptography private_key keypair object
        """
        if isinstance(cert_type, int):
            warnings.warn(
                "Certipy support for PyOpenSSL is deprecated. Use `cert_type='rsa'",
                DeprecationWarning,
                stacklevel=2,
            )
            if cert_type == 6:
                cert_type = KeyType.rsa
            elif cert_type == 116:
                raise ValueError("DSA keys are no longer supported. Use 'rsa'.")
        # this will raise on unrecognized values
        key_type = KeyType(cert_type)
        if key_type == KeyType.rsa:
            key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=bits,
            )
        else:
            raise ValueError(f"Only cert_type='rsa' is supported, not {cert_type!r}")
        return key

    def create_request(self, pkey, digest="sha256", **name):
        """
        Create a certificate request.

        Arguments: pkey   - The key to associate with the request
                   digest - Digestion method to use for signing, default is
                            sha256
                   **name - The name of the subject of the request, possible
                            arguments are:
                              C     - Country name
                              ST    - State or province name
                              L     - Locality name
                              O     - Organization name
                              OU    - Organizational unit name
                              CN    - Common name
                              emailAddress - E-mail address


        Returns:   The certificate request in an X509Req object
        """
        csr = x509.CertificateSigningRequestBuilder()

        if name is not None:
            name_attrs = [
                x509.NameAttribute(_name_oid_map[key], value)
                for key, value in name.items()
            ]
            csr = csr.subject_name(x509.Name(name_attrs))

        algorithm = getattr(hashes, digest.upper())()
        return csr.sign(pkey, algorithm)

    def sign(
        self,
        req,
        issuer_cert_key,
        validity_period,
        digest="sha256",
        extensions=None,
        serial=0,
    ):
        """
        Generate a certificate given a certificate request.

        Arguments: req         - Certificate request to use
                   issuer_cert - The certificate of the issuer
                   issuer_key  - The private key of the issuer
                   not_before  - Timestamp (relative to now) when the
                                 certificate starts being valid
                   not_after   - Timestamp (relative to now) when the
                                 certificate stops being valid
                   digest      - Digest method to use for signing,
                                 default is sha256
        Returns:   The signed certificate in an X509 object
        """

        issuer_cert, issuer_key = issuer_cert_key
        not_before, not_after = validity_period
        now = datetime.now(timezone.utc)
        if not isinstance(not_before, datetime):
            # backward-compatibility: integer seconds from now
            not_before = now + timedelta(seconds=not_before)
        if not isinstance(not_after, datetime):
            not_after = now + timedelta(seconds=not_after)

        cert_builder = (
            x509.CertificateBuilder()
            .subject_name(req.subject)
            .serial_number(serial or x509.random_serial_number())
            .not_valid_before(not_before)
            .not_valid_after(not_after)
            .issuer_name(issuer_cert.subject)
            .public_key(req.public_key())
        )

        if extensions:
            for ext, critical in extensions:
                cert_builder = cert_builder.add_extension(ext, critical=critical)

        algorithm = getattr(hashes, digest.upper())()
        cert = cert_builder.sign(issuer_key, algorithm=algorithm)
        return cert

    def create_ca_bundle_for_names(self, bundle_name, names):
        """Create a CA bundle to trust only certs defined in names"""

        records = [rec for name, rec in self.store.store.items() if name in names]
        return self.create_bundle(bundle_name, names=[r["parent_ca"] for r in records])

    def create_ca_bundle(self, bundle_name, ca_names=None):
        """
        Create a bundle of CA public certs for trust distribution

        Deprecated: 0.1.2
        Arguments: ca_names    - The names of CAs to include in the bundle
                   bundle_name - The name of the bundle file to output
        Returns:   Path to the bundle file
        """

        return self.create_bundle(bundle_name, names=ca_names)

    def create_bundle(self, bundle_name, names=None, ca_only=True):
        """Create a bundle of public certs for trust distribution

        This will create a bundle of both CAs and/or regular certificates.
        Arguments: names       - The names of certs to include in the bundle
                   bundle_name - The name of the bundle file to output
        Returns:   Path to the bundle file
        """

        if not names:
            if ca_only:
                names = []
                for name, record in self.store.store.items():
                    if record["is_ca"]:
                        names.append(name)
            else:
                names = self.store.store.keys()

        out_file_path = os.path.join(self.store.containing_dir, bundle_name)
        with open(out_file_path, "w") as fh:
            for name in names:
                bundle = self.store.get_files(name)
                bundle.cert.load()
                fh.write(str(bundle.cert))
        return out_file_path

    def trust_from_graph(self, graph):
        """Create a set of trust bundles from a relationship graph.

        Components in this sense are defined by unique CAs. This method assists
        in setting up complicated trust between various components that need
        to do TLS auth.
        Arguments: graph - dict component:list(components)
        Returns:   dict component:trust bundle file path
        """

        # Ensure there are CAs backing all graph components
        def distinct_components(graph):
            """Return a set of components from the provided graph."""
            components = set(graph.keys())
            for trusts in graph.values():
                components |= set(trusts)
            return components

        # Default to creating a CA (incapable of signing intermediaries) to
        # identify a component not known to Certipy
        for component in distinct_components(graph):
            try:
                self.store.get_record(component)
            except CertNotFoundError:
                self.create_ca(component)

        # Build bundles from the graph
        trust_files = {}
        for component, trusts in graph.items():
            file_name = component + "_trust.crt"
            trust_files[component] = self.create_bundle(
                file_name, names=trusts, ca_only=False
            )

        return trust_files

    def create_ca(
        self,
        name,
        ca_name="",
        cert_type=KeyType.rsa,
        bits=2048,
        alt_names=None,
        years=5,
        serial=0,
        pathlen=0,
        overwrite=False,
    ):
        """
        Create a certificate authority

        Arguments: name     - The name of the CA
                   cert_type - The type of the cert. Always 'rsa'.
                   bits     - The number of bits to use
                   alt_names - An array of alternative names in the format:
                               IP:address, DNS:address
        Returns:   KeyCertPair for the new CA
        """

        cakey = self.create_key_pair(cert_type, bits)
        req = self.create_request(cakey, CN=name)
        signing_key = cakey
        signing_cert = req

        if pathlen is not None and pathlen < 0:
            warnings.warn(
                "negative pathlen is deprecated. Use pathlen=None",
                DeprecationWarning,
                stacklevel=2,
            )
            pathlen = None

        parent_ca = ""
        if ca_name:
            ca_bundle = self.store.get_files(ca_name)
            signing_key = ca_bundle.key.load()
            signing_cert = ca_bundle.cert.load()
            parent_ca = ca_bundle.cert.file_path

        extensions = [
            (x509.BasicConstraints(True, pathlen), True),
            (
                x509.KeyUsage(
                    key_cert_sign=True,
                    crl_sign=True,
                    digital_signature=False,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                True,
            ),
            (
                x509.ExtendedKeyUsage(
                    [
                        x509.ExtendedKeyUsageOID.SERVER_AUTH,
                        x509.ExtendedKeyUsageOID.CLIENT_AUTH,
                    ]
                ),
                True,
            ),
            (x509.SubjectKeyIdentifier.from_public_key(cakey.public_key()), False),
            (
                x509.AuthorityKeyIdentifier.from_issuer_public_key(
                    signing_cert.public_key()
                ),
                False,
            ),
        ]

        if alt_names:
            extensions.append(
                (
                    x509.SubjectAlternativeName([_altname(name) for name in alt_names]),
                    False,
                )
            )

        # TODO: start time before today for clock skew?
        cacert = self.sign(
            req,
            (signing_cert, signing_key),
            (0, 60 * 60 * 24 * 365 * years),
            extensions=extensions,
            serial=serial,
        )

        x509s = {"key": cakey, "cert": cacert, "ca": cacert}
        self.store.add_files(
            name,
            x509s,
            overwrite=overwrite,
            parent_ca=parent_ca,
            is_ca=True,
            serial=serial,
        )
        if ca_name:
            self.store.add_sign_link(ca_name, name)
        return self.store.get_record(name)

    def create_signed_pair(
        self,
        name,
        ca_name,
        cert_type=KeyType.rsa,
        bits=2048,
        years=5,
        alt_names=None,
        serial=0,
        overwrite=False,
    ):
        """
        Create a key-cert pair

        Arguments: name     - The name of the key-cert pair
                   ca_name   - The name of the CA to sign this cert
                   cert_type - The type of the cert. Always 'rsa'
                   bits     - The number of bits to use
                   alt_names - An array of alternative names in the format:
                               IP:address, DNS:address
        Returns:   KeyCertPair for the new signed pair
        """

        key = self.create_key_pair(cert_type, bits)
        req = self.create_request(key, CN=name)
        extensions = [
            (
                x509.ExtendedKeyUsage(
                    [
                        x509.ExtendedKeyUsageOID.SERVER_AUTH,
                        x509.ExtendedKeyUsageOID.CLIENT_AUTH,
                    ]
                ),
                True,
            ),
        ]

        if alt_names:
            extensions.append(
                (
                    x509.SubjectAlternativeName([_altname(name) for name in alt_names]),
                    False,
                )
            )

        ca_bundle = self.store.get_files(ca_name)
        cacert = ca_bundle.cert.load()
        cakey = ca_bundle.key.load()

        extensions.append(
            (x509.AuthorityKeyIdentifier.from_issuer_public_key(cacert.public_key()), False)
        )

        now = datetime.now(timezone.utc)
        eol = now + timedelta(days=years * 365)
        cert = self.sign(
            req, (cacert, cakey), (now, eol), extensions=extensions, serial=serial
        )

        x509s = {"key": key, "cert": cert, "ca": None}
        self.store.add_files(
            name, x509s, parent_ca=ca_name, overwrite=overwrite, serial=serial
        )

        # Relate these certs as being parent and child
        self.store.add_sign_link(ca_name, name)
        return self.store.get_record(name)
