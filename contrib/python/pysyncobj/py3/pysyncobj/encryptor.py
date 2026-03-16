import base64
try:
    import cryptography
    from cryptography.fernet import Fernet
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except:
    HAS_CRYPTO = False

SALT = b'\x15%q\xe6\xbb\x02\xa6\xf8\x13q\x90\xcf6+\x1e\xeb'

def getEncryptor(password):
    if not isinstance(password, bytes):
        password = bytes(password.encode())
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=SALT,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    return Fernet(key)
