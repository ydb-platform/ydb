# Copyright (c) 2016 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import binascii

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding

from pylxd.models import _model as model


class Certificate(model.Model):
    """A LXD certificate."""

    certificate = model.Attribute()
    fingerprint = model.Attribute()
    type = model.Attribute()
    name = model.Attribute()

    @classmethod
    def get(cls, client, fingerprint):
        """Get a certificate by fingerprint."""
        response = client.api.certificates[fingerprint].get()

        return cls(client, **response.json()['metadata'])

    @classmethod
    def all(cls, client):
        """Get all certificates."""
        response = client.api.certificates.get()

        certs = []
        for cert in response.json()['metadata']:
            fingerprint = cert.split('/')[-1]
            certs.append(cls(client, fingerprint=fingerprint))
        return certs

    @classmethod
    def create(cls, client, password, cert_data):
        """Create a new certificate."""
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        base64_cert = cert.public_bytes(Encoding.PEM).decode('utf-8')
        # STRIP OUT CERT META "-----BEGIN CERTIFICATE-----"
        base64_cert = '\n'.join(base64_cert.split('\n')[1:-2])
        data = {
            'type': 'client',
            'certificate': base64_cert,
            'password': password,
        }
        client.api.certificates.post(json=data)

        # XXX: rockstar (08 Jun 2016) - Please see the open lxd bug here:
        # https://github.com/lxc/lxd/issues/2092
        fingerprint = binascii.hexlify(
            cert.fingerprint(hashes.SHA256())).decode('utf-8')
        return cls.get(client, fingerprint)

    @property
    def api(self):
        return self.client.api.certificates[self.fingerprint]
