import functools
import io
import logging
from hashlib import sha1
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


def _makekey(password, salt, keyLength, block, algIdHash=0x00008004):
    r"""
    Return a intermediate key.
    """
    # https://msdn.microsoft.com/en-us/library/dd920677(v=office.12).aspx
    password = password.encode("UTF-16LE")
    h0 = sha1(salt + password).digest()
    blockbytes = pack("<I", block)
    hfinal = sha1(h0 + blockbytes).digest()
    if keyLength == 40:
        key = hfinal[:5] + b"\x00" * 11
    else:
        key = hfinal[: keyLength // 8]
    return key


class DocumentRC4CryptoAPI:
    def __init__(self):
        pass

    @staticmethod
    def verifypw(
        password,
        salt,
        keySize,
        encryptedVerifier,
        encryptedVerifierHash,
        algId=0x00006801,
        block=0,
    ):
        r"""
        Return True if the given password is valid.
        """
        # TODO: For consistency with others, rename method to verify_password or the like
        # https://msdn.microsoft.com/en-us/library/dd953617(v=office.12).aspx
        key = _makekey(password, salt, keySize, block)
        cipher = Cipher(ARC4(key), mode=None, backend=default_backend())
        decryptor = cipher.decryptor()
        verifier = decryptor.update(encryptedVerifier)
        verfiferHash = decryptor.update(encryptedVerifierHash)
        hash = sha1(verifier).digest()
        logging.debug([verfiferHash, hash])
        return hash == verfiferHash

    @staticmethod
    def decrypt(password, salt, keySize, ibuf, blocksize=0x200, block=0):
        r"""
        Return decrypted data.
        """
        obuf = io.BytesIO()

        key = _makekey(password, salt, keySize, block)

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
            key = _makekey(password, salt, keySize, block)

        obuf.seek(0)
        return obuf
