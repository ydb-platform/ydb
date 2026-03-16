"""
Monocypher library Python bindings.

Monocypher is an easy to use, easy to deploy, auditable crypto library
written in portable C.
"""

from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
import secrets
import warnings


# also edit setup.py
__version__ = '4.0.2.6'   # also change setup.py
__title__ = 'pymonocypher'
__description__ = 'Python ctypes bindings to the Monocypher library'
__url__ = 'https://github.com/jetperch/pymonocypher'
__author__ = 'Jetperch LLC'
__author_email__ = 'joulescope-dev@jetperch.com'
__license__ = 'BSD 2-clause'
__copyright__ = 'Copyright 2018-2025 Jetperch LLC'


cdef extern from "monocypher.h":

    cpdef int crypto_verify16(const uint8_t a[16], const uint8_t b[16])

    cpdef int crypto_verify32(const uint8_t a[32], const uint8_t b[32])

    cpdef int crypto_verify64(const uint8_t a[64], const uint8_t b[64])

    void crypto_wipe(uint8_t* secret, size_t size)

    void crypto_aead_lock(uint8_t* cipher_text, uint8_t mac[16], const uint8_t key[32], const uint8_t nonce[24], const uint8_t* ad, size_t ad_size, const uint8_t* plain_text, size_t text_size)

    int crypto_aead_unlock(uint8_t* plain_text, const uint8_t mac[16], const uint8_t key[32], const uint8_t nonce[24], const uint8_t* ad, size_t ad_size, const uint8_t* cipher_text, size_t text_size)

    ctypedef struct crypto_aead_ctx:
        uint64_t counter
        uint8_t key[32]
        uint8_t nonce[8]

    void crypto_aead_init_x(crypto_aead_ctx* ctx, const uint8_t key[32], const uint8_t nonce[24])

    void crypto_aead_init_djb(crypto_aead_ctx* ctx, const uint8_t key[32], const uint8_t nonce[8])

    void crypto_aead_init_ietf(crypto_aead_ctx* ctx, const uint8_t key[32], const uint8_t nonce[12])

    void crypto_aead_write(crypto_aead_ctx* ctx, uint8_t* cipher_text, uint8_t mac[16], const uint8_t* ad, size_t ad_size, const uint8_t* plain_text, size_t text_size)

    int crypto_aead_read(crypto_aead_ctx* ctx, uint8_t* plain_text, const uint8_t mac[16], const uint8_t* ad, size_t ad_size, const uint8_t* cipher_text, size_t text_size)

    void crypto_blake2b(uint8_t* hash, size_t hash_size, const uint8_t* message, size_t message_size)

    void crypto_blake2b_keyed(uint8_t* hash, size_t hash_size, const uint8_t* key, size_t key_size, const uint8_t* message, size_t message_size)

    ctypedef struct crypto_blake2b_ctx:
        uint64_t hash[8]
        uint64_t input_offset[2]
        uint64_t input[16]
        size_t input_idx
        size_t hash_size

    void crypto_blake2b_init(crypto_blake2b_ctx* ctx, size_t hash_size)

    void crypto_blake2b_keyed_init(crypto_blake2b_ctx* ctx, size_t hash_size, const uint8_t* key, size_t key_size)

    void crypto_blake2b_update(crypto_blake2b_ctx* ctx, const uint8_t* message, size_t message_size)

    void crypto_blake2b_final(crypto_blake2b_ctx* ctx, uint8_t* hash)

    ctypedef struct crypto_argon2_config:
        uint32_t algorithm
        uint32_t nb_blocks
        uint32_t nb_passes
        uint32_t nb_lanes

    ctypedef struct crypto_argon2_inputs:
        const uint8_t* pass_ "pass"
        const uint8_t* salt
        uint32_t pass_size
        uint32_t salt_size

    ctypedef struct crypto_argon2_extras:
        const uint8_t* key
        const uint8_t* ad
        uint32_t key_size
        uint32_t ad_size

    const crypto_argon2_extras crypto_argon2_no_extras

    void crypto_argon2(uint8_t* hash, uint32_t hash_size, void* work_area, crypto_argon2_config config, crypto_argon2_inputs inputs, crypto_argon2_extras extras)

    void crypto_x25519_public_key(uint8_t public_key[32], const uint8_t secret_key[32])

    void crypto_x25519(uint8_t raw_shared_secret[32], const uint8_t your_secret_key[32], const uint8_t their_public_key[32])

    void crypto_x25519_to_eddsa(uint8_t eddsa[32], const uint8_t x25519[32])

    void crypto_x25519_inverse(uint8_t blind_salt[32], const uint8_t private_key[32], const uint8_t curve_point[32])

    void crypto_x25519_dirty_small(uint8_t pk[32], const uint8_t sk[32])

    void crypto_x25519_dirty_fast(uint8_t pk[32], const uint8_t sk[32])

    void crypto_eddsa_key_pair(uint8_t secret_key[64], uint8_t public_key[32], uint8_t seed[32])

    void crypto_eddsa_sign(uint8_t signature[64], const uint8_t secret_key[64], const uint8_t* message, size_t message_size)

    int crypto_eddsa_check(const uint8_t signature[64], const uint8_t public_key[32], const uint8_t* message, size_t message_size)

    void crypto_eddsa_to_x25519(uint8_t x25519[32], const uint8_t eddsa[32])

    void crypto_eddsa_trim_scalar(uint8_t out[32], const uint8_t in_[32])

    void crypto_eddsa_reduce(uint8_t reduced[32], const uint8_t expanded[64])

    void crypto_eddsa_mul_add(uint8_t r[32], const uint8_t a[32], const uint8_t b[32], const uint8_t c[32])

    void crypto_eddsa_scalarbase(uint8_t point[32], const uint8_t scalar[32])

    int crypto_eddsa_check_equation(const uint8_t signature[64], const uint8_t public_key[32], const uint8_t h_ram[32])

    void crypto_chacha20_h(uint8_t out[32], const uint8_t key[32], const uint8_t in_[16])

    uint64_t crypto_chacha20_djb(uint8_t* cipher_text, const uint8_t* plain_text, size_t text_size, const uint8_t key[32], const uint8_t nonce[8], uint64_t ctr)

    uint32_t crypto_chacha20_ietf(uint8_t* cipher_text, const uint8_t* plain_text, size_t text_size, const uint8_t key[32], const uint8_t nonce[12], uint32_t ctr)

    uint64_t crypto_chacha20_x(uint8_t* cipher_text, const uint8_t* plain_text, size_t text_size, const uint8_t key[32], const uint8_t nonce[24], uint64_t ctr)

    void crypto_poly1305(uint8_t mac[16], const uint8_t* message, size_t message_size, const uint8_t key[32])

    ctypedef struct crypto_poly1305_ctx:
        uint8_t c[16]
        size_t c_idx
        uint32_t r[4]
        uint32_t pad[4]
        uint32_t h[5]

    void crypto_poly1305_init(crypto_poly1305_ctx* ctx, const uint8_t key[32])

    void crypto_poly1305_update(crypto_poly1305_ctx* ctx, const uint8_t* message, size_t message_size)

    void crypto_poly1305_final(crypto_poly1305_ctx* ctx, uint8_t mac[16])

    void crypto_elligator_map(uint8_t curve[32], const uint8_t hidden[32])

    int crypto_elligator_rev(uint8_t hidden[32], const uint8_t curve[32], uint8_t tweak)

    void crypto_elligator_key_pair(uint8_t hidden[32], uint8_t secret_key[32], uint8_t seed[32])


def wipe(data):
    """Wipe a bytes object from memory.

    :param data: The bytes object to clear.

    WARNING: this violates the Python memory model and may result in corrupted
    data.  Ensure that the data to wipe is the only active reference!
    """
    crypto_wipe(data, len(data))


def lock(key, nonce, message, associated_data=None):
    """Perform authenticated encryption.

    :param key: The 32-byte shared session key.
    :param nonce: The 24-byte number, used only once with any given session
        key.
    :param message: The secret message to encrypt.
    :param associated_data: The additional data to authenticate which
        is NOT encrypted.
    :return: the tuple of (MAC, ciphertext).  MAC is the 16-byte message
        authentication code.  ciphertext is the encrypted message.
    """
    mac = bytes(16)
    crypto_text = bytes(len(message))
    associated_data = b'' if associated_data is None else associated_data
    crypto_aead_lock(crypto_text, mac, key, nonce, associated_data, len(associated_data), message, len(message))
    return mac, crypto_text


def unlock(key, nonce, mac, message, associated_data=None):
    """Perform authenticated decryption.

    :param key: The 32-byte shared session key.
    :param nonce: The 24-byte number, used only once with any given session
        key.
    :param mac: The 16-byte message authentication code produced by :func:`lock`.
    :param message: The ciphertext encrypted message to decrypt produced by :func:`lock`.
    :param associated_data: The additional data to authenticate which
        is NOT encrypted.
    :return: The secret message or None on authentication failure.
    """
    plain_text = bytearray(len(message))
    associated_data = b'' if associated_data is None else associated_data
    rv = crypto_aead_unlock(plain_text, mac, key, nonce, associated_data, len(associated_data), message, len(message))
    if 0 != rv:
        return None
    return plain_text


cdef class IncrementalAuthenticatedEncryption:
    cdef crypto_aead_ctx _ctx

    """Instantiate the incremental authenticated encryption handler.

    :param key: The 32-byte shared session key.
    :param nonce: The 24-byte number, used only once with any given session key.
    """
    def __init__(self, key, nonce):
        if len(key) != 32:
            raise ValueError(f'Invalid key length {len(key)} != 32')

        if len(nonce) != 24:
            raise ValueError(f'Invalid nonce length {len(key)} != 24')

        crypto_aead_init_x(&self._ctx, key, nonce)

    def lock(self, message, associated_data=None):
        """Perform authenticated encryption.

        :param message: The secret message to encrypt.
        :param associated_data: The additional data to authenticate which
            is NOT encrypted.
        :return: the tuple of (MAC, ciphertext).  MAC is the 16-byte message
            authentication code.  ciphertext is the encrypted message.
        """
        mac = bytes(16)
        crypto_text = bytes(len(message))
        associated_data = b'' if associated_data is None else associated_data
        crypto_aead_write(&self._ctx, crypto_text, mac, associated_data, len(associated_data), message, len(message))
        return mac, crypto_text

    def unlock(self, mac, message, associated_data=None):
        """Perform authenticated decryption.

        :param mac: The 16-byte message authentication code produced by :func:`lock`.
        :param message: The ciphertext encrypted message to decrypt produced by :func:`lock`.
        :param associated_data: The additional data to authenticate which
            is NOT encrypted.
        :return: The secret message or None on authentication failure.
        """
        if len(mac) != 16:
            raise ValueError(f'Invalid mac length {len(mac)} != 16')

        plain_text = bytearray(len(message))
        associated_data = b'' if associated_data is None else associated_data
        rv = crypto_aead_read(&self._ctx, plain_text, mac, associated_data, len(associated_data), message, len(message))
        if 0 != rv:
            return None
        return plain_text


def chacha20(key, nonce, message):
    """Encrypt/Decrypt a message with ChaCha20.

    :param key: The 32-byte shared secret key.
    :param nonce: The 24-byte or 8-byte nonce.
    :param message: The message to encrypt or decrypt.
    :return: The message XOR'ed with the ChaCha20 stream.
    """
    result = bytes(len(message))
    if 24 == len(nonce):
        crypto_chacha20_x(result, message, len(message), key, nonce, 0)
    elif 8 == len(nonce):
        crypto_chacha20_djb(result, message, len(message), key, nonce, 0)
        pass
    else:
        raise ValueError('invalid nonce length')
    return result


def blake2b(msg, key=None):
    key = b'' if key is None else key
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    hash = bytes(64)
    crypto_blake2b_keyed(hash, len(hash), key, len(key), msg, len(msg))
    return hash


cdef class Blake2b:
    cdef crypto_blake2b_ctx _ctx
    cdef int _hash_size

    """Incrementally compute the Blake2b hash.

    :param key: The optional 32-byte key.
    :param hash_size: The resulting hash size.  None (default) is 64.
    """
    def __init__(self, key=None, hash_size=None):
        key = b'' if key is None else key
        self._hash_size = 64 if hash_size is None else hash_size
        crypto_blake2b_keyed_init(&self._ctx, self._hash_size, key, len(key))

    def update(self, message):
        """Add new data to the hash.

        :param message: Additional data to hash.
        """
        crypto_blake2b_update(&self._ctx, message, len(message))

    def finalize(self):
        """Finalize and return the computed hash.

        :return: The hash.
        """
        hash = bytes(self._hash_size)
        crypto_blake2b_final(&self._ctx, hash)
        return hash


cdef uint32_t _validate_u32(variable_name, value):
    if value > 0xffff_ffff:
        raise ValueError(f'{variable_name} too long: {value}')
    return <uint32_t> value


def argon2i_32(nb_blocks, nb_iterations, password, salt, key=None, ad=None, _wipe=True) -> bytes:
    key = b'' if key is None else key
    ad = b'' if ad is None else ad

    cdef crypto_argon2_config config;
    config.algorithm = 1
    config.nb_blocks = nb_blocks
    config.nb_passes = nb_iterations
    config.nb_lanes = 1

    cdef crypto_argon2_inputs inputs;
    inputs.pass_ = password
    inputs.pass_size = _validate_u32('password', len(password))
    inputs.salt = salt
    inputs.salt_size = _validate_u32('salt', len(salt))

    cdef crypto_argon2_extras extras;
    extras.key = key
    extras.key_size = _validate_u32('key', len(key))
    extras.ad = ad
    extras.ad_size = _validate_u32('ad', len(ad))

    hash = bytes(32)
    work_area = malloc((<size_t> nb_blocks) * (<size_t> 1024))
    try:
        crypto_argon2(hash, <uint32_t> len(hash), work_area, config, inputs, extras)
    finally:
        free(work_area)
    if _wipe:
        crypto_wipe(password, len(password))
    return hash


def compute_key_exchange_public_key(secret_key: bytes) -> bytes:
    """Generate the public key for key exchange from the secret key.

    :param secret_key: The 32-byte secret key.
    :return: The 32-byte public key for :func:`key_exchange`.
    """
    public_key = bytes(32)
    crypto_x25519_public_key(public_key, secret_key)
    return public_key


def key_exchange(your_secret_key: bytes, their_public_key: bytes) -> bytes:
    """Compute a shared secret based upon public-key crytography.

    :param your_secret_key: Your private, secret 32-byte key.
    :param their_public_key: Their public 32-byte key.
    :return: A 32-byte shared secret that can will match what is
        computed using their_secret_key and your_public_key.
    """
    p = bytes(32)
    crypto_x25519(p, your_secret_key, their_public_key)
    return p


def compute_signing_public_key(secret_key: bytes) -> bytes:
    """Generate the public key from the secret key.

    :param secret_key: The 64-byte secret key from generate_signing_key_pair.
    :return: The 32-byte public key.
    """
    if len(secret_key) == 32:
        warnings.warn('Provide the full 64-byte key from generate_signing_key_pair()',
                      DeprecationWarning, stacklevel=2)
        secret = bytes(64)
        public = bytes(32)
        crypto_eddsa_key_pair(secret, public, bytes(secret_key))
        secret_key = secret
    if len(secret_key) != 64:
        raise ValueError('secret key length invalid')
    return secret_key[32:]


def signature_sign(secret_key: bytes, message: bytes) -> bytes:
    """Cryptographically sign a message.

    :param secret_key: Your 64-byte secret key.
    :param message: The message to sign.
    :return: The 64-byte signature of message.

    For a quick description of the signing process, see the bottom of
    https://pynacl.readthedocs.io/en/stable/signing/.
    """
    if len(secret_key) == 32:
        secret = bytes(64)
        public = bytes(32)
        crypto_eddsa_key_pair(secret, public, bytes(secret_key))
        secret_key = secret
    elif len(secret_key) != 64:
        raise ValueError('invalid secret key length')
    sig = bytes(64)
    crypto_eddsa_sign(sig, secret_key, message, len(message))
    return sig


def signature_check(signature, public_key, message) -> bool:
    """Verify the signature.

    :param signature: The 64-byte signature generated by :func:`signature_sign`.
    :param public_key: The public key matching the secret_key provided to
        :func:`signature_sign` that generated the signature.
    :param message: The message to check.
    :return: True if the message verifies correctly.  False if the message
        fails verification.
    """
    return 0 == crypto_eddsa_check(signature, public_key, message, len(message))


def generate_key(length=None, method=None) -> bytes:
    """Generate a random key.

    :param length: The key length.  None (default) is equivalent to 32.
    :param method: The random number generation method which is one of:
        * 'os': Use the platform's random number generator directly
        * 'chacha20': Apply the ChaCha20 cipher to the platform's random
          number generator to increase entropy (does not improve randomness).
        * None: (default) equivalent to 'chacha20'.
    :return: A key that is as secure as the random number generator of
        your platform.  See the Python secrets module for details on the
        randomness (https://docs.python.org/3/library/secrets.html).
    :see: generate_signing_key_pair, generate_key_exchange_key_pair, elligator_key_pair

    You should probably use the *_key_pair function for your
    specific crypto usage.
    """
    length = 32 if length is None else int(length)
    if method in ['chacha20', None, '', 'default']:
        # Do not entirely trust the platform's random number generator
        key = secrets.token_bytes(32)
        nonce = secrets.token_bytes(24)
        message = secrets.token_bytes(length)
        key = chacha20(key, nonce, message)
    elif method in ['os', 'secrets']:
        key = secrets.token_bytes(length)
    else:
        raise ValueError('unsupported method: %s' % method)
    return key


def generate_signing_key_pair() -> tuple[bytes, bytes]:
    """Generate a new keypair for signing using default settings.

    :return: (secret, public)

    To print a key, use the following code snippet:

        import binascii
        print(binascii.hexlify(key))
    """
    secret = bytes(64)
    public = bytes(32)
    crypto_eddsa_key_pair(secret, public, generate_key())
    return secret, public


def generate_key_exchange_key_pair() -> tuple[bytes, bytes]:
    """Generate a new keypair for key exchange using default settings.

    :return: (secret, public)
    """
    secret = generate_key()
    public = compute_key_exchange_public_key(secret)
    return secret, public


## Elligator bindings


def elligator_map(hidden: bytes) -> bytes:
    """Computes the point corresponding to a representative.

    :param hidden: The 32 byte hidden key.
    :return: The 32-byte little-endian encoded public key.
        Since positive representatives fits in 254 bits,
        the two most significant bits are ignored.
    """
    curve = bytes(32)
    if len(hidden) != 32:
        raise ValueError(f'Invalid hidden length {len(hidden)} != 32')
    crypto_elligator_map(curve, hidden)
    return curve


def elligator_rev(curve: bytes, tweak=None) -> bytes:
    """Computes the representative of a point.

    :param curve: The 32-byte little-endian encoded public key.
    :param tweak: The optional random byte.  If None (default),
        then automatically generate a random byte.
    :return: The 32-byte secret key
    :raise ValueError: If the combination of curve point and tweak
        is unsuitable for hiding.  Choose another curve point
        and try again.
    """
    hidden = bytes(32)
    if len(curve) != 32:
        raise ValueError(f'Invalid curve length {len(curve)} != 32')
    if tweak is None:
        tweak = secrets.randbits(8)
    rv = crypto_elligator_rev(hidden, curve, tweak)
    if rv:
        raise ValueError('curve point is unsuitable for hiding')
    return hidden


def elligator_key_pair(seed: bytes = None) -> tuple[bytes, bytes]:
    """Generate a key pair.

    :param seed: The 32-byte seed that is used to derive the key pair.
        None (default) generate a cryptographically secure random seed.
    :return: The tuple of hidden and secret_key.
        * hidden: The 32-byte little ending encoding of a point on the curve
          which is effectively indistinguishable from random.
        * secret_key: The generated 32-byte little endian secret key.
    """
    hidden = bytes(32)
    secret_key = bytes(32)
    if seed is None:
        seed = secrets.token_bytes(32)
    elif len(seed) != 32:
        raise ValueError(f'Invalid seed length {len(seed)} != 32')
    crypto_elligator_key_pair(hidden, secret_key, seed)
    return hidden, secret_key
