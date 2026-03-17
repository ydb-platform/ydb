"""
.. versionchanged:: 0.13.0
    Refactor ``crypt`` module into package.
.. versionchanged:: 0.3.0
    Added support for PDF 2.0 encryption standards and crypt filters.

Utilities for PDF encryption. This module covers all methods outlined in the
standard:

* Legacy RC4-based encryption (based on PyPDF2 code).
* AES-128 encryption with legacy key derivation (partly based on PyPDF2 code).
* PDF 2.0 AES-256 encryption.
* Public key encryption backed by any of the above.

Following the language in the standard, encryption operations are backed by
subclasses of the :class:`SecurityHandler` class, which provides a more or less
generic API.

.. danger::
    The members of this package are all considered internal API, and are
    therefore subject to change without notice.

.. danger::
    One should also be aware that the legacy encryption scheme implemented
    here is (very) weak, and we only support it for compatibility reasons.
    Under no circumstances should it still be used to encrypt new files.


About crypt filters
-------------------

Crypt filters are objects that handle encryption and decryption of streams and
strings, either for all of them, or for a specific subset (e.g. streams
representing embedded files). In the context of the PDF standard, crypt filters
are a notion that only makes sense for security handlers of version 4 and up.
In pyHanko, however, *all* encryption and decryption operations pass through
crypt filters, and the serialisation/deserialisation logic in
:class:`SecurityHandler` and its subclasses transparently deals with staying
backwards compatible with earlier revisions.

Internally, pyHanko loosely distinguishes between implicit and explicit
uses of crypt filters:

* Explicit crypt filters are used by directly referring to them from the
  ``/Filter`` entry of a stream dictionary. These are invoked in the usual
  stream decoding process.
* Implicit crypt filters are set by the ``/StmF`` and ``/StrF`` entries
  in the security handler's crypt filter configuration, and are invoked by the
  object reading/writing procedures as necessary. These filters are invisble
  to the stream encoding/decoding process: the
  :attr:`~.generic.StreamObject.encoded_data` attribute of
  an "implicitly encrypted" stream will therefore contain decrypted data ready
  to be decoded in the usual way.

As long as you don't require access to encoded object data and/or raw encrypted
object data, this distiction should be irrelevant to you as an API user.
"""

from .api import (
    IDENTITY,
    AuthResult,
    AuthStatus,
    CryptFilter,
    CryptFilterBuilder,
    CryptFilterConfiguration,
    IdentityCryptFilter,
    PdfKeyNotAvailableError,
    SecurityHandler,
    SecurityHandlerVersion,
    build_crypt_filter,
)
from .cred_ser import SerialisableCredential, SerialisedCredential
from .filter_mixins import AESCryptFilterMixin, RC4CryptFilterMixin
from .pubkey import (
    DEF_EMBEDDED_FILE,
    DEFAULT_CRYPT_FILTER,
    EnvelopeKeyDecrypter,
    PubKeyAdbeSubFilter,
    PubKeyAESCryptFilter,
    PubKeyCryptFilter,
    PubKeyRC4CryptFilter,
    PubKeySecurityHandler,
    SimpleEnvelopeKeyDecrypter,
)
from .standard import (
    STD_CF,
    StandardAESCryptFilter,
    StandardCryptFilter,
    StandardRC4CryptFilter,
    StandardSecurityHandler,
    StandardSecuritySettingsRevision,
    _PasswordCredential,
)

__all__ = [
    'SecurityHandler',
    'StandardSecurityHandler',
    'PubKeySecurityHandler',
    'AuthResult',
    'AuthStatus',
    'SecurityHandlerVersion',
    'StandardSecuritySettingsRevision',
    'PubKeyAdbeSubFilter',
    'CryptFilterConfiguration',
    'CryptFilter',
    'StandardCryptFilter',
    'PubKeyCryptFilter',
    'IdentityCryptFilter',
    'RC4CryptFilterMixin',
    'AESCryptFilterMixin',
    'StandardAESCryptFilter',
    'StandardRC4CryptFilter',
    'PubKeyAESCryptFilter',
    'PubKeyRC4CryptFilter',
    'EnvelopeKeyDecrypter',
    'SimpleEnvelopeKeyDecrypter',
    'SerialisedCredential',
    'SerialisableCredential',
    'STD_CF',
    'DEFAULT_CRYPT_FILTER',
    'DEF_EMBEDDED_FILE',
    'IDENTITY',
    'CryptFilterBuilder',
    'build_crypt_filter',
    'PdfKeyNotAvailableError',
]
