from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.cose import COSEAlgorithmIdentifier
from webauthn.helpers.parse_attestation_object import parse_attestation_object
from webauthn.helpers.structs import AttestationFormat


class TestParseAttestationObject(TestCase):
    """
    To generate values to pass into `parse_attestation_object()`:

    1. Perform an attestation in the browser until you receive a value back from
       `navigator.credentials.create()`
    2. Base64URL-encode the value of `response.attestationObject` to make it easy to
       include in tests below
    """

    def test_can_parse_good_none_attestation_object(self) -> None:
        attestation_object = base64url_to_bytes(
            "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVjESZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2NFAAAAKwAAAAAAAAAAAAAAAAAAAAAAQBGidLzwQ3gFpfrQexcVIM1EKISJJUorv8HP1BQ-Km2G7Lw5VyGWRVG7pibkOG3OJ2LYG9tZY57jUrSjOqfEJgilAQIDJiABIVggBlNr3gyj0tq3QJrYd7t74BncruqqAVPCDlMfaU-7_kIiWCCXa5Oxqay8XoVwsTYzLSDp-ULUIAalnTyiQJrrrRWB_A"
        )

        output = parse_attestation_object(attestation_object)

        assert output.fmt == AttestationFormat.NONE
        # attestation statement
        att_stmt = output.att_stmt
        assert att_stmt.sig is None
        assert att_stmt.x5c is None
        assert att_stmt.response is None
        assert att_stmt.alg is None
        assert att_stmt.ver is None
        assert att_stmt.cert_info is None
        assert att_stmt.pub_area is None
        # authenticator data
        auth_data = output.auth_data
        assert auth_data.rp_id_hash == base64url_to_bytes(
            "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2M"
        )
        assert auth_data.flags.up
        assert auth_data.flags.uv
        assert auth_data.flags.at
        assert not auth_data.flags.ed
        assert auth_data.sign_count == 43
        # attested credential data
        assert auth_data.attested_credential_data
        attested_cred_data = auth_data.attested_credential_data
        assert attested_cred_data.aaguid == base64url_to_bytes("AAAAAAAAAAAAAAAAAAAAAA")
        assert attested_cred_data.credential_id == base64url_to_bytes(
            "EaJ0vPBDeAWl-tB7FxUgzUQohIklSiu_wc_UFD4qbYbsvDlXIZZFUbumJuQ4bc4nYtgb21ljnuNStKM6p8QmCA"
        )
        assert attested_cred_data.credential_public_key == base64url_to_bytes(
            "pQECAyYgASFYIAZTa94Mo9Lat0Ca2He7e-AZ3K7qqgFTwg5TH2lPu_5CIlggl2uTsamsvF6FcLE2My0g6flC1CAGpZ08okCa660Vgfw"
        )
        # extensions
        assert not auth_data.extensions

    def test_can_parse_good_packed_attestation_object(self) -> None:
        attestation_object = base64url_to_bytes(
            "o2NmbXRmcGFja2VkZ2F0dFN0bXSjY2FsZyZjc2lnWEcwRQIgRpuZ6hdaLAgWgCFTIo4BGSTBAxwwqk4u3s1-JAzv_H4CIQCZnfoic34aOwlac1A09eflEtb0V1kO7yGhHOw5P5wVWmN4NWOBWQLBMIICvTCCAaWgAwIBAgIEKudiYzANBgkqhkiG9w0BAQsFADAuMSwwKgYDVQQDEyNZdWJpY28gVTJGIFJvb3QgQ0EgU2VyaWFsIDQ1NzIwMDYzMTAgFw0xNDA4MDEwMDAwMDBaGA8yMDUwMDkwNDAwMDAwMFowbjELMAkGA1UEBhMCU0UxEjAQBgNVBAoMCVl1YmljbyBBQjEiMCAGA1UECwwZQXV0aGVudGljYXRvciBBdHRlc3RhdGlvbjEnMCUGA1UEAwweWXViaWNvIFUyRiBFRSBTZXJpYWwgNzE5ODA3MDc1MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEKgOGXmBD2Z4R_xCqJVRXhL8Jr45rHjsyFykhb1USGozZENOZ3cdovf5Ke8fj2rxi5tJGn_VnW4_6iQzKdIaeP6NsMGowIgYJKwYBBAGCxAoCBBUxLjMuNi4xLjQuMS40MTQ4Mi4xLjEwEwYLKwYBBAGC5RwCAQEEBAMCBDAwIQYLKwYBBAGC5RwBAQQEEgQQbUS6m_bsLkm5MAyP6SDLczAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBCwUAA4IBAQByV9A83MPhFWmEkNb4DvlbUwcjc9nmRzJjKxHc3HeK7GvVkm0H4XucVDB4jeMvTke0WHb_jFUiApvpOHh5VyMx5ydwFoKKcRs5x0_WwSWL0eTZ5WbVcHkDR9pSNcA_D_5AsUKOBcbpF5nkdVRxaQHuuIuwV4k1iK2IqtMNcU8vL6w21U261xCcWwJ6sMq4zzVO8QCKCQhsoIaWrwz828GDmPzfAjFsJiLJXuYivdHACkeJ5KHMt0mjVLpfJ2BCML7_rgbmvwL7wBW80VHfNdcKmKjkLcpEiPzwcQQhiN_qHV90t-p4iyr5xRSpurlP5zic2hlRkLKxMH2_kRjhqSn4aGF1dGhEYXRhWMRJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0UAAAAqbUS6m_bsLkm5MAyP6SDLcwBAFsWBrFcw8yRjxV8z18Egh91o1AScNRYkIuUoY6wIlIhslDpP7eydKi1q5s9g1ugDP9mqBlPDDFPRbH6YLwHbtqUBAgMmIAEhWCAq3y0RWh8nLzanBZQwTA7yAbUy9KEDAM0b3N9Elrb0VCJYIJrX7ygtpyInb5mXBE7g9YEow6xWrJ400HhL2r4q5tzV",
        )

        output = parse_attestation_object(attestation_object)

        assert output.fmt == AttestationFormat.PACKED
        # attestation statement
        att_stmt = output.att_stmt
        assert att_stmt.sig and att_stmt.sig == base64url_to_bytes(
            "MEUCIEabmeoXWiwIFoAhUyKOARkkwQMcMKpOLt7NfiQM7_x-AiEAmZ36InN-GjsJWnNQNPXn5RLW9FdZDu8hoRzsOT-cFVo"
        )
        assert att_stmt.x5c and len(att_stmt.x5c) == 1
        assert att_stmt.response is None
        assert att_stmt.alg == COSEAlgorithmIdentifier.ECDSA_SHA_256
        assert att_stmt.ver is None
        assert att_stmt.cert_info is None
        assert att_stmt.pub_area is None
        # authenticator data
        auth_data = output.auth_data
        assert auth_data.rp_id_hash == base64url_to_bytes(
            "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2M"
        )
        assert auth_data.flags.up
        assert auth_data.flags.uv
        assert auth_data.flags.at
        assert not auth_data.flags.ed
        assert auth_data.sign_count == 42
        # attested credential data
        assert auth_data.attested_credential_data
        attested_cred_data = auth_data.attested_credential_data
        assert attested_cred_data.aaguid == base64url_to_bytes("bUS6m_bsLkm5MAyP6SDLcw")
        assert attested_cred_data.credential_id == base64url_to_bytes(
            "FsWBrFcw8yRjxV8z18Egh91o1AScNRYkIuUoY6wIlIhslDpP7eydKi1q5s9g1ugDP9mqBlPDDFPRbH6YLwHbtg"
        )
        assert attested_cred_data.credential_public_key == base64url_to_bytes(
            "pQECAyYgASFYICrfLRFaHycvNqcFlDBMDvIBtTL0oQMAzRvc30SWtvRUIlggmtfvKC2nIidvmZcETuD1gSjDrFasnjTQeEvavirm3NU"
        )
        # extensions
        assert auth_data.extensions is None
