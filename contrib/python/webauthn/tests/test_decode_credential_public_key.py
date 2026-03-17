from unittest import TestCase

from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.cose import COSEKTY, COSEAlgorithmIdentifier
from webauthn.helpers.decode_credential_public_key import (
    DecodedEC2PublicKey,
    DecodedRSAPublicKey,
    decode_credential_public_key,
)


class TestDecodeCredentialPublicKey(TestCase):
    def test_decodes_ec2_public_key(self) -> None:
        decoded = decode_credential_public_key(
            base64url_to_bytes(
                "pQECAyYgASFYIDDHBDxTqWP4yZZnAa524L6uPuwhireUwRD5sXY6U2gxIlggxuwbECbDdNfTTegnc174oYdusZiMmJgct0yI_ulrJGI"
            )
        )

        assert isinstance(decoded, DecodedEC2PublicKey)
        assert decoded.kty == COSEKTY.EC2
        assert decoded.alg == COSEAlgorithmIdentifier.ECDSA_SHA_256
        assert decoded.crv == 1
        assert (
            decoded.x
            and bytes_to_base64url(decoded.x) == "MMcEPFOpY_jJlmcBrnbgvq4-7CGKt5TBEPmxdjpTaDE"
        )
        assert (
            decoded.y
            and bytes_to_base64url(decoded.y) == "xuwbECbDdNfTTegnc174oYdusZiMmJgct0yI_ulrJGI"
        )

    def test_decode_rsa_public_key(self) -> None:
        decoded = decode_credential_public_key(
            base64url_to_bytes(
                "pAEDAzkBACBZAQDxfpXrj0ba_AH30JJ_-W7BHSOPugOD8aEDdNBKc1gjB9AmV3FPl2aL0fwiOMKtM_byI24qXb2FzcyjC7HUVkHRtzkAQnahXckI4wY_01koaY6iwXuIE3Ya0Zjs2iZyz6u4G_abGnWdObqa_kHxc3CHR7Xy5MDkAkKyX6TqU0tgHZcEhDd_Lb5ONJDwg4wvKlZBtZYElfMuZ6lonoRZ7qR_81rGkDZyFaxp6RlyvzEbo4ijeIaHQylqCz-oFm03ifZMOfRHYuF4uTjJDRH-g4BW1f3rdi7DTHk1hJnIw1IyL_VFIQ9NifkAguYjNCySCUNpYli2eMrPhAu5dYJFFjINIUMBAAE"
            )
        )

        assert isinstance(decoded, DecodedRSAPublicKey)
        assert decoded.kty == COSEKTY.RSA
        assert decoded.alg == COSEAlgorithmIdentifier.RSASSA_PKCS1_v1_5_SHA_256
        assert decoded.e and bytes_to_base64url(decoded.e) == "AQAB"
        assert (
            decoded.n
            and bytes_to_base64url(decoded.n)
            == "8X6V649G2vwB99CSf_luwR0jj7oDg_GhA3TQSnNYIwfQJldxT5dmi9H8IjjCrTP28iNuKl29hc3Mowux1FZB0bc5AEJ2oV3JCOMGP9NZKGmOosF7iBN2GtGY7Nomcs-ruBv2mxp1nTm6mv5B8XNwh0e18uTA5AJCsl-k6lNLYB2XBIQ3fy2-TjSQ8IOMLypWQbWWBJXzLmepaJ6EWe6kf_NaxpA2chWsaekZcr8xG6OIo3iGh0Mpags_qBZtN4n2TDn0R2LheLk4yQ0R_oOAVtX963Yuw0x5NYSZyMNSMi_1RSEPTYn5AILmIzQskglDaWJYtnjKz4QLuXWCRRYyDQ"
        )

    def test_decode_uncompressed_ec2_public_key(self) -> None:
        decoded = decode_credential_public_key(
            base64url_to_bytes(
                "BBaxKZueVyr5ICDfosygxwRflSdPUcNheZhThXCeTFTNo0EM9dj0V+xJ1JwpE2XZ/8NRIt5KVvr71Zl0rB8BWOs="
            )
        )

        assert isinstance(decoded, DecodedEC2PublicKey)
        assert decoded.kty == COSEKTY.EC2
        assert decoded.alg == COSEAlgorithmIdentifier.ECDSA_SHA_256
        assert decoded.crv == 1
        assert (
            decoded.x
            and bytes_to_base64url(decoded.x) == "FrEpm55XKvkgIN-izKDHBF-VJ09Rw2F5mFOFcJ5MVM0"
        )
        assert (
            decoded.y
            and bytes_to_base64url(decoded.y) == "o0EM9dj0V-xJ1JwpE2XZ_8NRIt5KVvr71Zl0rB8BWOs"
        )
