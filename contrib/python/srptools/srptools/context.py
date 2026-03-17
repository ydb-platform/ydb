from __future__ import unicode_literals
from random import SystemRandom as random

from six import integer_types, PY3

from .utils import int_from_hex, int_to_bytes, hex_from, value_encode, b64_from
from .constants import PRIME_1024, PRIME_1024_GEN, HASH_SHA_1
from .exceptions import SRPException


class SRPContext(object):
    """

    * The SRP Authentication and Key Exchange System
        https://tools.ietf.org/html/rfc2945

    * Using the Secure Remote Password (SRP) Protocol for TLS Authentication
        https://tools.ietf.org/html/rfc5054

    """
    def __init__(
            self, username, password=None, prime=None, generator=None, hash_func=None, multiplier=None,
            bits_random=1024, bits_salt=64):
        """

        :param str|unicode username: User name
        :param str|unicode password: User _password
        :param str|unicode|None prime: Prime hex string . Default: PRIME_1024
        :param str|unicode|None generator: Generator hex string. Default: PRIME_1024_GEN
        :param str|unicode hash_func: Function to calculate hash. Default: HASH_SHA_1
        :param str|unicode multiplier: Multiplier hex string. If not given will be calculated
            automatically using _prime and _gen.
        :param int bits_random: Random value bits. Default: 1024
        :param int bits_salt: Salt value bits. Default: 64
        """
        self._hash_func = hash_func or HASH_SHA_1  # H
        self._user = username  # I
        self._password = password  # p

        self._gen = int_from_hex(generator or PRIME_1024_GEN)  # g
        self._prime = int_from_hex(prime or PRIME_1024)  # N
        self._mult = (  # k = H(N | PAD(g))
            int_from_hex(multiplier) if multiplier else self.hash(self._prime, self.pad(self._gen)))

        self._bits_salt = bits_salt
        self._bits_random = bits_random

    @property
    def generator(self):
        return hex_from(self._gen)

    @property
    def generator_b64(self):
        return b64_from(self._gen)

    @property
    def prime(self):
        return hex_from(self._prime)

    @property
    def prime_b64(self):
        return b64_from(self._prime)

    def pad(self, val):
        """
        :param val:
        :rtype: bytes
        """
        padding = len(int_to_bytes(self._prime))
        padded = int_to_bytes(val).rjust(padding, b'\x00')
        return padded

    def hash(self, *args, **kwargs):
        """
        :param args:
        :param kwargs:
            joiner - string to join values (args)
            as_bytes - bool to return hash bytes instead of default int
        :rtype: int|bytes
        """
        joiner = kwargs.get('joiner', '').encode('utf-8')
        as_bytes = kwargs.get('as_bytes', False)

        def conv(arg):
            if isinstance(arg, integer_types):
                arg = int_to_bytes(arg)

            if PY3:
                if isinstance(arg, str):
                    arg = arg.encode('utf-8')
                return arg

            return str(arg)

        digest = joiner.join(map(conv, args))

        hash_obj = self._hash_func(digest)

        if as_bytes:
            return hash_obj.digest()

        return int_from_hex(hash_obj.hexdigest())

    def generate_random(self, bits_len=None):
        """Generates a random value.

        :param int bits_len:
        :rtype: int
        """
        bits_len = bits_len or self._bits_random
        return random().getrandbits(bits_len)

    def generate_salt(self):
        """s = random

        :rtype: int
        """
        return self.generate_random(self._bits_salt)

    def get_common_secret(self, server_public, client_public):
        """u = H(PAD(A) | PAD(B))

        :param int server_public:
        :param int client_public:
        :rtype: int
        """
        return self.hash(self.pad(client_public), self.pad(server_public))

    def get_client_premaster_secret(self, password_hash, server_public, client_private, common_secret):
        """S = (B - (k * g^x)) ^ (a + (u * x)) % N

        :param int server_public:
        :param int password_hash:
        :param int client_private:
        :param int common_secret:
        :rtype: int
        """
        password_verifier = self.get_common_password_verifier(password_hash)
        return pow(
            (server_public - (self._mult * password_verifier)),
            (client_private + (common_secret * password_hash)), self._prime)

    def get_common_session_key(self, premaster_secret):
        """K = H(S)

        :param int premaster_secret:
        :rtype: bytes
        """
        return self.hash(premaster_secret, as_bytes=True)

    def get_server_premaster_secret(self, password_verifier, server_private, client_public, common_secret):
        """S = (A * v^u) ^ b % N

        :param int password_verifier:
        :param int server_private:
        :param int client_public:
        :param int common_secret:
        :rtype: int
        """
        return pow((client_public * pow(password_verifier, common_secret, self._prime)), server_private, self._prime)

    def generate_client_private(self):
        """a = random()

        :rtype: int
        """
        return self.generate_random()

    def generate_server_private(self):
        """b = random()

        :rtype: int
        """
        return self.generate_random()

    def get_client_public(self, client_private):
        """A = g^a % N

        :param int client_private:
        :rtype: int
        """
        return pow(self._gen, client_private, self._prime)

    def get_server_public(self, password_verifier, server_private):
        """B = (k*v + g^b) % N

        :param int password_verifier:
        :param int server_private:
        :rtype: int
        """
        return ((self._mult * password_verifier) + pow(self._gen, server_private, self._prime)) % self._prime

    def get_common_password_hash(self, salt):
        """x = H(s | H(I | ":" | P))

        :param int salt:
        :rtype: int
        """
        password = self._password
        if password is None:
            raise SRPException('User password should be in context for this scenario.')

        return self.hash(salt, self.hash(self._user, password, joiner=':', as_bytes=True))

    def get_common_password_verifier(self, password_hash):
        """v = g^x % N

        :param int password_hash:
        :rtype: int
        """
        return pow(self._gen, password_hash, self._prime)

    def get_common_session_key_proof(self, session_key, salt, server_public, client_public):
        """M = H(H(N) XOR H(g) | H(U) | s | A | B | K)

        :param bytes session_key:
        :param int salt:
        :param int server_public:
        :param int client_public:
        :rtype: bytes
        """
        h = self.hash
        prove = h(
            h(self._prime) ^ h(self._gen),
            h(self._user),
            salt,
            client_public,
            server_public,
            session_key,
            as_bytes=True
        )
        return prove

    def get_common_session_key_proof_hash(self, session_key, session_key_proof, client_public):
        """H(A | M | K)

        :param bytes session_key:
        :param bytes session_key_proof:
        :param int client_public:
        :rtype: bytes
        """
        return self.hash(client_public, session_key_proof, session_key, as_bytes=True)

    def get_user_data_triplet(self, base64=False):
        """( <_user>, <_password verifier>, <salt> )

        :param base64:
        :rtype: tuple
        """
        salt = self.generate_salt()
        verifier = self.get_common_password_verifier(self.get_common_password_hash(salt))

        verifier = value_encode(verifier, base64)
        salt = value_encode(salt, base64)

        return self._user, verifier, salt
