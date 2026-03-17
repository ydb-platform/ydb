"""
This package houses the part of pyHanko that produces digital signatures.
It contains modules for creating ``SignedData`` CMS objects, embedding them
in PDF files, and for handling PDF-specific document signing needs.
"""

# reexport this for backwards compatibility
from ...keys import load_certs_from_pemder
from .constants import (
    DEFAULT_MD,
    DEFAULT_SIG_SUBFILTER,
    DEFAULT_SIGNER_KEY_USAGE,
    DEFAULT_SIGNING_STAMP_STYLE,
)
from .functions import async_sign_pdf, embed_payload_with_cms, sign_pdf
from .pdf_byterange import (
    DocumentTimestamp,
    PdfByteRangeDigest,
    PdfSignedData,
    SignatureObject,
)
from .pdf_cms import ExternalSigner, Signer, SimpleSigner
from .pdf_signer import PdfSignatureMetadata, PdfSigner, PdfTimeStamper

__all__ = [
    'PdfSignatureMetadata',
    'Signer',
    'SimpleSigner',
    'ExternalSigner',
    'PdfSigner',
    'PdfTimeStamper',
    'PdfByteRangeDigest',
    'PdfSignedData',
    'SignatureObject',
    'DocumentTimestamp',
    'sign_pdf',
    'async_sign_pdf',
    'DEFAULT_MD',
    'DEFAULT_SIGNING_STAMP_STYLE',
    'DEFAULT_SIG_SUBFILTER',
    'DEFAULT_SIGNER_KEY_USAGE',
]
