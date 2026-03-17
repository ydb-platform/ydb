from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.structs import AttestationFormat
from webauthn import verify_registration_response


class TestVerifyRegistrationResponsePacked(TestCase):
    def test_verify_attestation_from_yubikey_firefox(self) -> None:
        credential = """{
            "id": "syGQPDZRUYdb4m3rdWeyPaIMYlbmydGp1TP_33vE_lqJ3PHNyTd0iKsnKr5WjnCcBzcesZrDEfB_RBLFzU3k4w",
            "rawId": "syGQPDZRUYdb4m3rdWeyPaIMYlbmydGp1TP_33vE_lqJ3PHNyTd0iKsnKr5WjnCcBzcesZrDEfB_RBLFzU3k4w",
            "response": {
                "attestationObject": "o2NmbXRmcGFja2VkZ2F0dFN0bXSjY2FsZyZjc2lnWEcwRQIhAOfrFlQpbavT6dJeTDJSCDzYSYPjBDHli2-syT2c1IiKAiAx5gQ2z5cHjdQX-jEHTb7JcjfQoVSW8fXszF5ihSgeOGN4NWOBWQLBMIICvTCCAaWgAwIBAgIEKudiYzANBgkqhkiG9w0BAQsFADAuMSwwKgYDVQQDEyNZdWJpY28gVTJGIFJvb3QgQ0EgU2VyaWFsIDQ1NzIwMDYzMTAgFw0xNDA4MDEwMDAwMDBaGA8yMDUwMDkwNDAwMDAwMFowbjELMAkGA1UEBhMCU0UxEjAQBgNVBAoMCVl1YmljbyBBQjEiMCAGA1UECwwZQXV0aGVudGljYXRvciBBdHRlc3RhdGlvbjEnMCUGA1UEAwweWXViaWNvIFUyRiBFRSBTZXJpYWwgNzE5ODA3MDc1MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEKgOGXmBD2Z4R_xCqJVRXhL8Jr45rHjsyFykhb1USGozZENOZ3cdovf5Ke8fj2rxi5tJGn_VnW4_6iQzKdIaeP6NsMGowIgYJKwYBBAGCxAoCBBUxLjMuNi4xLjQuMS40MTQ4Mi4xLjEwEwYLKwYBBAGC5RwCAQEEBAMCBDAwIQYLKwYBBAGC5RwBAQQEEgQQbUS6m_bsLkm5MAyP6SDLczAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBCwUAA4IBAQByV9A83MPhFWmEkNb4DvlbUwcjc9nmRzJjKxHc3HeK7GvVkm0H4XucVDB4jeMvTke0WHb_jFUiApvpOHh5VyMx5ydwFoKKcRs5x0_WwSWL0eTZ5WbVcHkDR9pSNcA_D_5AsUKOBcbpF5nkdVRxaQHuuIuwV4k1iK2IqtMNcU8vL6w21U261xCcWwJ6sMq4zzVO8QCKCQhsoIaWrwz828GDmPzfAjFsJiLJXuYivdHACkeJ5KHMt0mjVLpfJ2BCML7_rgbmvwL7wBW80VHfNdcKmKjkLcpEiPzwcQQhiN_qHV90t-p4iyr5xRSpurlP5zic2hlRkLKxMH2_kRjhqSn4aGF1dGhEYXRhWMRJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0UAAAA0bUS6m_bsLkm5MAyP6SDLcwBAsyGQPDZRUYdb4m3rdWeyPaIMYlbmydGp1TP_33vE_lqJ3PHNyTd0iKsnKr5WjnCcBzcesZrDEfB_RBLFzU3k46UBAgMmIAEhWCBAX_i3O3DvBnkGq_uLNk_PeAX5WwO_MIxBp0mhX6Lw7yJYIOW-1-Fch829McWvRUYAHTWZTx5IycKSGECL1UzUaK_8",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiOExCQ2lPWTNxMWNCWkhGQVd0UzRBWlpDaHpHcGh5NjdsSzdJNzB6S2k0eUM3cGdyUTJQY2g3bkFqTGsxd3E5Z3Jlc2hJQXNXMkFqaWJoWGpqSTBUbVEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9"
            },
            "type": "public-key",
            "clientExtensionResults": {},
            "transports": [
                "nfc",
                "usb"
            ]
        }"""
        challenge = base64url_to_bytes(
            "8LBCiOY3q1cBZHFAWtS4AZZChzGphy67lK7I70zKi4yC7pgrQ2Pch7nAjLk1wq9greshIAsW2AjibhXjjI0TmQ"
        )
        rp_id = "localhost"
        expected_origin = "http://localhost:5000"

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.PACKED
        assert verification.credential_id == base64url_to_bytes(
            "syGQPDZRUYdb4m3rdWeyPaIMYlbmydGp1TP_33vE_lqJ3PHNyTd0iKsnKr5WjnCcBzcesZrDEfB_RBLFzU3k4w"
        )

    def test_verify_attestation_with_okp_public_key(self) -> None:
        credential = """{
            "id": "WlHiMqH6UhUs-d43z-aGlE3nsXuEOQpa9P9pwpqb4tmvtBMBfGvAV2wUrqBCDENjkkxd6kIRzZQKcluyOFlyW_vXVZSAEgod1xj-1QmFpuwyBVnlkQGefRbmUjbEt5iE4q3tdjy65EWIekO0SNjCQx3LxIJMzi25fgUkI9Y-gg0",
            "rawId": "WlHiMqH6UhUs-d43z-aGlE3nsXuEOQpa9P9pwpqb4tmvtBMBfGvAV2wUrqBCDENjkkxd6kIRzZQKcluyOFlyW_vXVZSAEgod1xj-1QmFpuwyBVnlkQGefRbmUjbEt5iE4q3tdjy65EWIekO0SNjCQx3LxIJMzi25fgUkI9Y-gg0",
            "response": {
                "attestationObject": "o2NmbXRmcGFja2VkZ2F0dFN0bXSjY2FsZyZjc2lnWEcwRQIgB3c0BvOsyJut14wj4XVWxXliicWZMZLDNF621Zz7h_8CIQCIOqRyWOazVhqlBrD-HL-83KWAvGZRgvW-4A9SE8BLFWN4NWOBWQLBMIICvTCCAaWgAwIBAgIEK_F8eDANBgkqhkiG9w0BAQsFADAuMSwwKgYDVQQDEyNZdWJpY28gVTJGIFJvb3QgQ0EgU2VyaWFsIDQ1NzIwMDYzMTAgFw0xNDA4MDEwMDAwMDBaGA8yMDUwMDkwNDAwMDAwMFowbjELMAkGA1UEBhMCU0UxEjAQBgNVBAoMCVl1YmljbyBBQjEiMCAGA1UECwwZQXV0aGVudGljYXRvciBBdHRlc3RhdGlvbjEnMCUGA1UEAwweWXViaWNvIFUyRiBFRSBTZXJpYWwgNzM3MjQ2MzI4MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEdMLHhCPIcS6bSPJZWGb8cECuTN8H13fVha8Ek5nt-pI8vrSflxb59Vp4bDQlH8jzXj3oW1ZwUDjHC6EnGWB5i6NsMGowIgYJKwYBBAGCxAoCBBUxLjMuNi4xLjQuMS40MTQ4Mi4xLjcwEwYLKwYBBAGC5RwCAQEEBAMCAiQwIQYLKwYBBAGC5RwBAQQEEgQQxe9V_62aS5-1gK3rr-Am0DAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBCwUAA4IBAQCLbpN2nXhNbunZANJxAn_Cd-S4JuZsObnUiLnLLS0FPWa01TY8F7oJ8bE-aFa4kTe6NQQfi8-yiZrQ8N-JL4f7gNdQPSrH-r3iFd4SvroDe1jaJO4J9LeiFjmRdcVa-5cqNF4G1fPCofvw9W4lKnObuPakr0x_icdVq1MXhYdUtQk6Zr5mBnc4FhN9qi7DXqLHD5G7ZFUmGwfIcD2-0m1f1mwQS8yRD5-_aDCf3vutwddoi3crtivzyromwbKklR4qHunJ75LGZLZA8pJ_mXnUQ6TTsgRqPvPXgQPbSyGMf2z_DIPbQqCD_Bmc4dj9o6LozheBdDtcZCAjSPTAd_uiaGF1dGhEYXRhWOFJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0EAAAACxe9V_62aS5-1gK3rr-Am0ACAWlHiMqH6UhUs-d43z-aGlE3nsXuEOQpa9P9pwpqb4tmvtBMBfGvAV2wUrqBCDENjkkxd6kIRzZQKcluyOFlyW_vXVZSAEgod1xj-1QmFpuwyBVnlkQGefRbmUjbEt5iE4q3tdjy65EWIekO0SNjCQx3LxIJMzi25fgUkI9Y-gg2kAQEDJyAGIVggnB_oUZDQU0esRlNPmjEO96aMDTgs34D8Dv31tAwhUZo",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiN0pVQmpXWkZkRm96dWx4YjcxRHZIa2gzUDZXS1VHNEVsVW83d0VrS2ljMkpFVEpNQUlLQ2Y3ckJFOVlrc0k1b05qRHpRNkhxaDZFNzNPeTZTUGNNbnciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9"
            },
            "type": "public-key",
            "clientExtensionResults": {},
            "transports": [
                "usb"
            ]
        }"""
        challenge = base64url_to_bytes(
            "7JUBjWZFdFozulxb71DvHkh3P6WKUG4ElUo7wEkKic2JETJMAIKCf7rBE9YksI5oNjDzQ6Hqh6E73Oy6SPcMnw"
        )
        rp_id = "localhost"
        expected_origin = "http://localhost:5000"

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.PACKED
        assert verification.credential_id == base64url_to_bytes(
            "WlHiMqH6UhUs-d43z-aGlE3nsXuEOQpa9P9pwpqb4tmvtBMBfGvAV2wUrqBCDENjkkxd6kIRzZQKcluyOFlyW_vXVZSAEgod1xj-1QmFpuwyBVnlkQGefRbmUjbEt5iE4q3tdjy65EWIekO0SNjCQx3LxIJMzi25fgUkI9Y-gg0"
        )
