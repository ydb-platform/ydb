# Copyright (c) 2019-2023 by Ron Frederick <ronf@timeheart.net> and others.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License v2.0 which accompanies this
# distribution and is available at:
#
#     http://www.eclipse.org/legal/epl-2.0/
#
# This program may also be made available under the following secondary
# licenses when the conditions for such availability set forth in the
# Eclipse Public License v2.0 are satisfied:
#
#    GNU General Public License, Version 2.0, or any later versions of
#    that license
#
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
#
# Contributors:
#     Ron Frederick - initial implementation, API, and documentation

"""A shim around PyCA for Edwards-curve keys and key exchange"""

from typing import Dict, Optional, Union, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends.openssl import backend
from cryptography.hazmat.primitives.asymmetric import ed25519, ed448
from cryptography.hazmat.primitives.asymmetric import x25519, x448
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.primitives.serialization import NoEncryption

from .misc import CryptoKey, PyCAKey


_EdPrivateKey = Union[ed25519.Ed25519PrivateKey, ed448.Ed448PrivateKey]
_EdPublicKey = Union[ed25519.Ed25519PublicKey, ed448.Ed448PublicKey]


ed25519_available = backend.ed25519_supported()
ed448_available = backend.ed448_supported()
curve25519_available = backend.x25519_supported()
curve448_available = backend.x448_supported()


class _EdDSAKey(CryptoKey):
    """Base class for shim around PyCA for EdDSA keys"""

    def __init__(self, pyca_key: PyCAKey, pub: bytes,
                 priv: Optional[bytes] = None):
        super().__init__(pyca_key)

        self._pub = pub
        self._priv = priv

    @property
    def public_value(self) -> bytes:
        """Return the public value encoded as a byte string"""

        return self._pub

    @property
    def private_value(self) -> Optional[bytes]:
        """Return the private value encoded as a byte string"""

        return self._priv


class EdDSAPrivateKey(_EdDSAKey):
    """A shim around PyCA for EdDSA private keys"""

    _priv_classes: Dict[bytes, object] = {}

    if ed25519_available: # pragma: no branch
        _priv_classes[b'ed25519'] = ed25519.Ed25519PrivateKey

    if ed448_available: # pragma: no branch
        _priv_classes[b'ed448'] = ed448.Ed448PrivateKey

    @classmethod
    def construct(cls, curve_id: bytes, priv: bytes) -> 'EdDSAPrivateKey':
        """Construct an EdDSA private key"""

        priv_cls = cast('_EdPrivateKey', cls._priv_classes[curve_id])
        priv_key = priv_cls.from_private_bytes(priv)
        pub_key = priv_key.public_key()
        pub = pub_key.public_bytes(Encoding.Raw, PublicFormat.Raw)

        return cls(priv_key, pub, priv)

    @classmethod
    def generate(cls, curve_id: bytes) -> 'EdDSAPrivateKey':
        """Generate a new EdDSA private key"""

        priv_cls = cast('_EdPrivateKey', cls._priv_classes[curve_id])
        priv_key = priv_cls.generate()
        priv = priv_key.private_bytes(Encoding.Raw, PrivateFormat.Raw,
                                      NoEncryption())

        pub_key = priv_key.public_key()
        pub = pub_key.public_bytes(Encoding.Raw, PublicFormat.Raw)

        return cls(priv_key, pub, priv)

    def sign(self, data: bytes, hash_name: str = '') -> bytes:
        """Sign a block of data"""

        # pylint: disable=unused-argument

        priv_key = cast('_EdPrivateKey', self.pyca_key)
        return priv_key.sign(data)


class EdDSAPublicKey(_EdDSAKey):
    """A shim around PyCA for EdDSA public keys"""

    _pub_classes: Dict[bytes, object] = {
        b'ed25519': ed25519.Ed25519PublicKey,
        b'ed448': ed448.Ed448PublicKey
    }

    @classmethod
    def construct(cls, curve_id: bytes, pub: bytes) -> 'EdDSAPublicKey':
        """Construct an EdDSA public key"""

        pub_cls = cast('_EdPublicKey', cls._pub_classes[curve_id])
        pub_key = pub_cls.from_public_bytes(pub)

        return cls(pub_key, pub)

    def verify(self, data: bytes, sig: bytes, hash_name: str = '') -> bool:
        """Verify the signature on a block of data"""

        # pylint: disable=unused-argument

        try:
            pub_key = cast('_EdPublicKey', self.pyca_key)
            pub_key.verify(sig, data)
            return True
        except InvalidSignature:
            return False


class Curve25519DH:
    """Curve25519 Diffie Hellman implementation based on PyCA"""

    def __init__(self) -> None:
        self._priv_key = x25519.X25519PrivateKey.generate()

    def get_public(self) -> bytes:
        """Return the public key to send in the handshake"""

        return self._priv_key.public_key().public_bytes(Encoding.Raw,
                                                        PublicFormat.Raw)

    def get_shared_bytes(self, peer_public: bytes) -> bytes:
        """Return the shared key from the peer's public key as bytes"""

        peer_key = x25519.X25519PublicKey.from_public_bytes(peer_public)
        return self._priv_key.exchange(peer_key)

    def get_shared(self, peer_public: bytes) -> int:
        """Return the shared key from the peer's public key"""

        return int.from_bytes(self.get_shared_bytes(peer_public), 'big')


class Curve448DH:
    """Curve448 Diffie Hellman implementation based on PyCA"""

    def __init__(self) -> None:
        self._priv_key = x448.X448PrivateKey.generate()

    def get_public(self) -> bytes:
        """Return the public key to send in the handshake"""

        return self._priv_key.public_key().public_bytes(Encoding.Raw,
                                                        PublicFormat.Raw)

    def get_shared(self, peer_public: bytes) -> int:
        """Return the shared key from the peer's public key"""

        peer_key = x448.X448PublicKey.from_public_bytes(peer_public)
        shared = self._priv_key.exchange(peer_key)
        return int.from_bytes(shared, 'big')
