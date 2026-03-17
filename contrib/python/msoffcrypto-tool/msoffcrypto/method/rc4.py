import functools
import io
import logging
from hashlib import md5
from struct import pack

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher

try:
    # NOTE: Avoid DeprecationWarning since cryptography>=43.0
    # TODO: .algorithm differs from the official documentation
    from cryptography.hazmat.decrepit.ciphers.algorithms import ARC4
except ImportError:
    from cryptography.hazmat.primitives.ciphers.algorithms import ARC4

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _makekey(password, salt, block):
    r"""
    Return a intermediate key.

        >>> password = 'password1'
        >>> salt = b'\xe8w,\x1d\x91\xc5j7\x96Ga\xb2\x80\x182\x17'
        >>> block = 0
        >>> expected = b' \xbf2\xdd\xf5@\x85\x8cQ7D\xaf\x0f$\xe0<'
        >>> _makekey(password, salt, block) == expected
        True
    """
    # https://msdn.microsoft.com/en-us/library/dd920360(v=office.12).aspx
    password = password.encode("UTF-16LE")
    h0 = md5(password).digest()
    truncatedHash = h0[:5]
    intermediateBuffer = (truncatedHash + salt) * 16
    h1 = md5(intermediateBuffer).digest()
    truncatedHash = h1[:5]
    blockbytes = pack("<I", block)
    hfinal = md5(truncatedHash + blockbytes).digest()
    key = hfinal[: 128 // 8]
    return key


class DocumentRC4:
    def __init__(self):
        pass

    @staticmethod
    def verifypw(password, salt, encryptedVerifier, encryptedVerifierHash):
        r"""
        Return True if the given password is valid.

            >>> password = 'password1'
            >>> salt = b'\xe8w,\x1d\x91\xc5j7\x96Ga\xb2\x80\x182\x17'
            >>> encryptedVerifier = b'\xc9\xe9\x97\xd4T\x97=1\x0b\xb1\xbap\x14&\x83~'
            >>> encryptedVerifierHash = b'\xb1\xde\x17\x8f\x07\xe9\x89\xc4M\xae^L\xf9j\xc4\x07'
            >>> DocumentRC4.verifypw(password, salt, encryptedVerifier, encryptedVerifierHash)
            True
        """
        # https://msdn.microsoft.com/en-us/library/dd952648(v=office.12).aspx
        block = 0
        key = _makekey(password, salt, block)
        cipher = Cipher(ARC4(key), mode=None, backend=default_backend())
        decryptor = cipher.decryptor()
        verifier = decryptor.update(encryptedVerifier)
        verfiferHash = decryptor.update(encryptedVerifierHash)
        hash = md5(verifier).digest()
        logging.debug([verfiferHash, hash])
        return hash == verfiferHash

    @staticmethod
    def decrypt(password, salt, ibuf, blocksize=0x200):
        r"""
        Return decrypted data.
        """
        obuf = io.BytesIO()

        block = 0
        key = _makekey(password, salt, block)

        for c, buf in enumerate(iter(functools.partial(ibuf.read, blocksize), b"")):
            cipher = Cipher(ARC4(key), mode=None, backend=default_backend())
            decryptor = cipher.decryptor()

            dec = decryptor.update(buf) + decryptor.finalize()
            obuf.write(dec)

            # From wvDecrypt:
            # at this stage we need to rekey the rc4 algorithm
            # Dieter Spaar <spaar@mirider.augusta.de> figured out
            # this rekeying, big kudos to him
            block += 1
            key = _makekey(password, salt, block)

        obuf.seek(0)
        return obuf
