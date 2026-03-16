from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.concatkdf import ConcatKDFHash
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization

from fido2.utils import sha256, bytes2int
from fido2.webauthn import AuthenticatorData


class U2FDevice(object):
    _priv_key_bytes = bytes.fromhex(
        "308184020100301006072a8648ce3d020106052b8104000a046d306b02010104201672f5ceb963e07d475f5db9a975b7ad42ac3bf71ccddfb6539cc651a1156a6ba144034200045a4be44eb94eebff3ed665dde31deb74a060fabd214c5f5713aa043efa805dac8f45e0e17afe2eafbd1802c413c1e485fd854af9f06ef20938398f6795f12e0e"  # noqa E501
    )

    def __init__(self, credential_id, app_id):
        assert isinstance(credential_id, bytes)
        assert isinstance(app_id, bytes)
        # Note: do not use in production, no garantees is provided this is
        # cryptographically safe to use.
        priv_key_params = ConcatKDFHash(
            algorithm=hashes.SHA256(),
            length=32,
            otherinfo=credential_id + app_id,
            backend=default_backend(),
        ).derive(self._priv_key_bytes)
        self.app_id = app_id

        self.priv_key = ec.derive_private_key(
            bytes2int(priv_key_params), ec.SECP256R1(), default_backend()
        )
        self.pub_key = self.priv_key.public_key()
        self.public_key_bytes = self.pub_key.public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )

        self.credential_id = self.key_handle = credential_id

    def sign(self, client_data):
        authenticator_data = AuthenticatorData.create(
            sha256(self.app_id), flags=AuthenticatorData.FLAG.UP, counter=0
        )

        signature = self.priv_key.sign(
            authenticator_data + client_data.hash, ec.ECDSA(hashes.SHA256())
        )

        return authenticator_data, signature
