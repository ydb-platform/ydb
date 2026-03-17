from __future__ import unicode_literals

from binascii import unhexlify

from .utils import hex_from, int_from_hex, hex_from_b64, value_encode, b64_from
from .exceptions import SRPException


if False:  # pragma: no cover
    from .context import SRPContext


class SRPSessionBase(object):
    """Base session class for server and client."""

    role = None

    def __init__(self, srp_context, private=None):
        """
        :param SRPContext srp_context:
        :param str|unicode private:
        """
        self._context = srp_context

        self._salt = None  # type: bytes
        self._common_secret = None  # type: int
        self._key = None  # type: bytes
        self._key_proof = None  # type: bytes
        self._key_proof_hash = None  # type: bytes

        self._server_public = None  # type: int
        self._client_public = None  # type: int

        self._this_private = None  # type: int

        if private:
            self._this_private = int_from_hex(private)  # type: int

    @property
    def _this_public(self):
        return getattr(self, '_%s_public' % self.role)

    def _other_public(self, val):
        other = ('server' if self.role == 'client' else 'client')
        setattr(self, '_%s_public' % other, val)

    _other_public = property(None, _other_public)

    @property
    def private(self):
        return hex_from(self._this_private)

    @property
    def private_b64(self):
        return b64_from(self._this_private)

    @property
    def public(self):
        return hex_from(self._this_public)

    @property
    def public_b64(self):
        return b64_from(self._this_public)

    @property
    def key(self):
        return hex_from(self._key)

    @property
    def key_b64(self):
        return b64_from(self._key)

    @property
    def key_proof(self):
        return hex_from(self._key_proof)

    @property
    def key_proof_b64(self):
        return b64_from(self._key_proof)

    @property
    def key_proof_hash(self):
        return hex_from(self._key_proof_hash)

    @property
    def key_proof_hash_b64(self):
        return b64_from(self._key_proof_hash)

    @classmethod
    def _value_decode(cls, value, base64=False):
        """Decodes value into hex optionally from base64"""
        return hex_from_b64(value) if base64 else value

    def process(self, other_public, salt, base64=False):
        salt = self._value_decode(salt, base64)
        other_public = self._value_decode(other_public, base64)

        self.init_base(salt)
        self.init_common_secret(other_public)
        self.init_session_key()
        self.init_session_key_proof()

        key = value_encode(self._key, base64)
        key_proof = value_encode(self._key_proof, base64)
        key_proof_hash = value_encode(self._key_proof_hash, base64)

        return key, key_proof, key_proof_hash

    def init_base(self, salt):
        salt = unhexlify(salt)
        self._salt = salt

    def init_session_key(self):
        """"""

    def verify_proof(self, key_prove, base64=False):
        """"""

    def init_common_secret(self, other_public):
        other_public = int_from_hex(other_public)

        if other_public % self._context._prime == 0:  # A % N is zero | B % N is zero
            raise SRPException('Wrong public provided for %s.' % self.__class__.__name__)

        self._other_public = other_public

        self._common_secret = self._context.get_common_secret(self._server_public, self._client_public)

    def init_session_key_proof(self):
        proof = self._context.get_common_session_key_proof(
            self._key, self._salt, self._server_public, self._client_public)
        self._key_proof = proof

        self._key_proof_hash = self._context.get_common_session_key_proof_hash(self._key, proof, self._client_public)
