import base64

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend
from django.utils.encoding import force_bytes

backend = default_backend()
info = b'django-fernet-fields'
# We need reproducible key derivation, so we can't use a random salt
salt = b'django-fernet-fields-hkdf-salt'


def derive_fernet_key(input_key):
    """Derive a 32-bit b64-encoded Fernet key from arbitrary input key."""
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=info,
        backend=backend,
    )
    return base64.urlsafe_b64encode(hkdf.derive(force_bytes(input_key)))
