"""
This module defines constants & defaults used by pyHanko when creating digital
signatures.
"""

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.extensions import (
    DeveloperExtension,
    DevExtensionMultivalued,
)
from pyhanko.sign.fields import SigSeedSubFilter
from pyhanko.stamp import STAMP_ART_CONTENT, TextStampStyle

__all__ = [
    'DEFAULT_MD',
    'DEFAULT_SIG_SUBFILTER',
    'DEFAULT_SIGNER_KEY_USAGE',
    'SIG_DETAILS_DEFAULT_TEMPLATE',
    'DEFAULT_SIGNING_STAMP_STYLE',
    'ESIC_EXTENSION_1',
    'ISO32001',
    'ISO32002',
]


DEFAULT_SIG_SUBFILTER = SigSeedSubFilter.ADOBE_PKCS7_DETACHED
"""
Default SubFilter to use for a PDF signature.
"""

# TODO I've encountered TSAs that will spew invalid timestamps when presented
#  with a sha512 req (Adobe Reader agrees).
#  Should get to the bottom of that. In the meantime, default to sha256
DEFAULT_MD = 'sha256'
"""
Default message digest algorithm used when computing digests for use in
signatures.
"""

DEFAULT_SIGNER_KEY_USAGE = {"non_repudiation"}
"""
Default key usage bits required for the signer's certificate.
"""


SIG_DETAILS_DEFAULT_TEMPLATE = (
    'Digitally signed by %(signer)s.\n' 'Timestamp: %(ts)s.'
)
"""
Default template string for signature appearances.
"""

DEFAULT_SIGNING_STAMP_STYLE = TextStampStyle(
    stamp_text=SIG_DETAILS_DEFAULT_TEMPLATE, background=STAMP_ART_CONTENT
)
"""
Default stamp style used for visible signatures.
"""


ESIC_EXTENSION_1 = DeveloperExtension(
    prefix_name=generic.pdf_name('/ESIC'),
    base_version=generic.pdf_name('/1.7'),
    extension_level=1,
    compare_by_level=True,
    multivalued=DevExtensionMultivalued.NEVER,
)
"""
ESIC extension for PDF 1.7. Used to declare usage of PAdES structures.
"""


ISO32001 = DeveloperExtension(
    prefix_name=generic.pdf_name('/ISO_'),
    base_version=generic.pdf_name('/2.0'),
    extension_level=32001,
    extension_revision=':2022',
    url='https://www.iso.org/standard/45874.html',
    compare_by_level=False,
    multivalued=DevExtensionMultivalued.ALWAYS,
)
"""
ISO extension to PDF 2.0 to include SHA-3 and SHAKE256 support.
This extension is defined in ISO/TS 32001.

Declared automatically whenever either of these is used in the signing or
document digesting process.
"""


ISO32002 = DeveloperExtension(
    prefix_name=generic.pdf_name('/ISO_'),
    base_version=generic.pdf_name('/2.0'),
    extension_level=32002,
    extension_revision=':2022',
    url='https://www.iso.org/standard/45875.html',
    compare_by_level=False,
    multivalued=DevExtensionMultivalued.ALWAYS,
)
"""
ISO extension to PDF 2.0 to include EdDSA support and clarify supported curves
for ECDSA. This extension is defined in ISO/TS 32002.

Declared automatically whenever Ed25519 or Ed448 are used, and
when ECDSA is used with one of the curves listed in ISO/TS 32002.
"""

ISO32002_CURVE_NAMES = {
    'secp256r1',
    'secp384r1',
    'secp521r1',
    'brainpoolp256r1',
    'brainpoolp384r1',
    'brainpoolp512r1',
}
"""
Names used in ``asn1crypto`` for curves included in ISO/TS 32002.
"""
