from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.structs import AttestationFormat
from webauthn import verify_registration_response
from webauthn.helpers.exceptions import InvalidRegistrationResponse


class TestVerifyRegistrationResponseFIDOU2F(TestCase):
    def test_verify_attestation_from_yubikey_firefox(self) -> None:
        credential = """{
            "id": "lrjqbPdLbWXTJ2sFIreka9aWd2ED-SDx_VAgBAh4XmCJgjCjudEjoi42pGQd-_Bi6nNPQ3T7-xOEgty2I3m7cw",
            "rawId": "lrjqbPdLbWXTJ2sFIreka9aWd2ED-SDx_VAgBAh4XmCJgjCjudEjoi42pGQd-_Bi6nNPQ3T7-xOEgty2I3m7cw",
            "response": {
                "attestationObject": "o2NmbXRoZmlkby11MmZnYXR0U3RtdKJjc2lnWEcwRQIhAIJO_x-FicMO0XDFwnVfXWyF-Dp7GW7oL43QfZyUjKU0AiAxQBUFu17l9rmKmqAWnHL6F1dDaIu54WYc9Mi_jtdNL2N4NWOBWQLBMIICvTCCAaWgAwIBAgIEKudiYzANBgkqhkiG9w0BAQsFADAuMSwwKgYDVQQDEyNZdWJpY28gVTJGIFJvb3QgQ0EgU2VyaWFsIDQ1NzIwMDYzMTAgFw0xNDA4MDEwMDAwMDBaGA8yMDUwMDkwNDAwMDAwMFowbjELMAkGA1UEBhMCU0UxEjAQBgNVBAoMCVl1YmljbyBBQjEiMCAGA1UECwwZQXV0aGVudGljYXRvciBBdHRlc3RhdGlvbjEnMCUGA1UEAwweWXViaWNvIFUyRiBFRSBTZXJpYWwgNzE5ODA3MDc1MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEKgOGXmBD2Z4R_xCqJVRXhL8Jr45rHjsyFykhb1USGozZENOZ3cdovf5Ke8fj2rxi5tJGn_VnW4_6iQzKdIaeP6NsMGowIgYJKwYBBAGCxAoCBBUxLjMuNi4xLjQuMS40MTQ4Mi4xLjEwEwYLKwYBBAGC5RwCAQEEBAMCBDAwIQYLKwYBBAGC5RwBAQQEEgQQbUS6m_bsLkm5MAyP6SDLczAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBCwUAA4IBAQByV9A83MPhFWmEkNb4DvlbUwcjc9nmRzJjKxHc3HeK7GvVkm0H4XucVDB4jeMvTke0WHb_jFUiApvpOHh5VyMx5ydwFoKKcRs5x0_WwSWL0eTZ5WbVcHkDR9pSNcA_D_5AsUKOBcbpF5nkdVRxaQHuuIuwV4k1iK2IqtMNcU8vL6w21U261xCcWwJ6sMq4zzVO8QCKCQhsoIaWrwz828GDmPzfAjFsJiLJXuYivdHACkeJ5KHMt0mjVLpfJ2BCML7_rgbmvwL7wBW80VHfNdcKmKjkLcpEiPzwcQQhiN_qHV90t-p4iyr5xRSpurlP5zic2hlRkLKxMH2_kRjhqSn4aGF1dGhEYXRhWMRJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0EAAAAAAAAAAAAAAAAAAAAAAAAAAABAlrjqbPdLbWXTJ2sFIreka9aWd2ED-SDx_VAgBAh4XmCJgjCjudEjoi42pGQd-_Bi6nNPQ3T7-xOEgty2I3m7c6UBAgMmIAEhWCDpQmUwrGnn3mk-T27A6awwL4HJG62Plj8Hz-lL0KltuiJYILwFIu48iEB8Ov6mEAqtCxn3mQOoaGIG4eidiWyLRg05",
                "clientDataJSON": "eyJjaGFsbGVuZ2UiOiJaSlZObG1Pclh3d2dRa2QxZ0RNbHk4QklpSU1WNElRRERzQ3I2S1BiQ0lIY09aNXdhS0htLU1adHVZNzQ4U0NxZy1OWlZwUnJZRnlGcmMzNmdRb0Z0dyIsImNsaWVudEV4dGVuc2lvbnMiOnt9LCJoYXNoQWxnb3JpdGhtIjoiU0hBLTI1NiIsIm9yaWdpbiI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAwMCIsInR5cGUiOiJ3ZWJhdXRobi5jcmVhdGUifQ"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "ZJVNlmOrXwwgQkd1gDMly8BIiIMV4IQDDsCr6KPbCIHcOZ5waKHm-MZtuY748SCqg-NZVpRrYFyFrc36gQoFtw"
        )
        rp_id = "localhost"
        expected_origin = "http://localhost:5000"

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.FIDO_U2F
        assert verification.credential_id == base64url_to_bytes(
            "lrjqbPdLbWXTJ2sFIreka9aWd2ED-SDx_VAgBAh4XmCJgjCjudEjoi42pGQd-_Bi6nNPQ3T7-xOEgty2I3m7cw"
        )

    def test_verify_attestation_from_fido_conformance(self) -> None:
        credential = """{
            "id": "2i53XtAuBVv2ztu9hdTkG_I4_zc-MhmYOjM2HWDCIlk",
            "rawId": "2i53XtAuBVv2ztu9hdTkG_I4_zc-MhmYOjM2HWDCIlk",
            "response": {
                "clientDataJSON": "eyJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjaGFsbGVuZ2UiOiJjMFd4b3JXd0VUb094bGJiNU1zRFpQV0tkTmlVaHlPSkw4cy1NSWNqSS1RNVNGSk9ub1dlX3EyWmhnWUtIeWt2NFBHdzNMLV9wbGdOQm5LZ3d3M2RBdyIsInR5cGUiOiJ3ZWJhdXRobi5jcmVhdGUifQ",
                "attestationObject": "o2NmbXRoZmlkby11MmZnYXR0U3RtdKJjc2lnWEcwRQIgYCbOYmJrEQy_oJqbBpgT_V8DhOUA-Kt3578zKalWyPkCIQDkyAm3YO98SF3AFi5Px1LChUDMLfPzL_p7um8sqxel0GN4NWOBWQQvMIIEKzCCAhOgAwIBAgIBATANBgkqhkiG9w0BAQUFADCBoTEYMBYGA1UEAwwPRklETzIgVEVTVCBST09UMTEwLwYJKoZIhvcNAQkBFiJjb25mb3JtYW5jZS10b29sc0BmaWRvYWxsaWFuY2Uub3JnMRYwFAYDVQQKDA1GSURPIEFsbGlhbmNlMQwwCgYDVQQLDANDV0cxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJNWTESMBAGA1UEBwwJV2FrZWZpZWxkMB4XDTE4MDMxNjE0MzUyN1oXDTI4MDMxMzE0MzUyN1owgawxIzAhBgNVBAMMGkZJRE8yIEJBVENIIEtFWSBwcmltZTI1NnYxMTEwLwYJKoZIhvcNAQkBFiJjb25mb3JtYW5jZS10b29sc0BmaWRvYWxsaWFuY2Uub3JnMRYwFAYDVQQKDA1GSURPIEFsbGlhbmNlMQwwCgYDVQQLDANDV0cxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJNWTESMBAGA1UEBwwJV2FrZWZpZWxkMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAETzpeXqtsH7yul_bfZEmWdix773IAQCp2xvIw9lVvF6qZm1l_xL9Qiq-OnvDNAT9aub0nkUvwgEN4y8yxG4m1RqMsMCowCQYDVR0TBAIwADAdBgNVHQ4EFgQUVk33wPjGVbahH2xNGfO_QeL9AXkwDQYJKoZIhvcNAQEFBQADggIBAI-_jI31FB-8J2XxzBXMuI4Yg-vAtq07ABHJqnQpUmt8lpOzmvJ0COKcwtq_7bpsgSVBJ26zhnyWcm1q8V0ZbxUvN2kH8N7nteIGn-CJOJkHDII-IbiH4-TUQCJjuCB52duUWL0fGVw2R13J6V-K7U5r0OWBzmtmwwiRVTggVbjDpbx2oqGAwzupG3RmBFDX1M92s3tgywnLr-e6NZal5yZdS8VblJGjswDZbdY-Qobo2DCN6vxvn5TVkukAHiArjpBBpAmuQfKa52vqSCYRpTCm57fQUZ1c1n29OsvDw1x9ckyH8j_9Xgk0AG-MlQ9Rdg3hCb7LkSPvC_zYDeS2Cj_yFw6OWahnnIRwO6t4UtLuRAkLrjP1T7nk0zu1whwj7YEwtva45niWWh6rdyg_SZlfsph3o_MZN5DwKaSrUaEO6b-numELH5GWjjiPgfgPKkIof-D40xaKUFBpNJzorQkAZCJWuHvXRpBZWFVh_UhNlGhX0mhz2yFlBrujYa9BgvIkdJ8Keok6qfAn-r5EEFXcSI8vGY7OEF01QKXVpu8-FW0uSxtQ991AcFD6KjvR51l7e61visUgduhZRIq9bYzeCIxnK5Jhm3o_NJE2bOp2NmVwVe4kjuJX87wo3Ba41bXgwIpdiLWyWJhSHPmJI_1ibRTZ5XO92xbPPSnnkXrFaGF1dGhEYXRhWKRJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0EAAAACAAAAAAAAAAAAAAAAAAAAAAAg2i53XtAuBVv2ztu9hdTkG_I4_zc-MhmYOjM2HWDCIlmlAQIDJiABIVggFgD81bv3uFCOrjw3DfHTIuscQkA7gikvjGdE6ltNPjoiWCA_7nP-fPzwp30E4kLjh4nyMyHQ2tfPJR8lA0h7SQ1dfA"
            },
            "type": "public-key",
            "transports": null
        }"""
        challenge = base64url_to_bytes(
            "c0WxorWwEToOxlbb5MsDZPWKdNiUhyOJL8s-MIcjI-Q5SFJOnoWe_q2ZhgYKHykv4PGw3L-_plgNBnKgww3dAw"
        )
        rp_id = "localhost"
        expected_origin = "http://localhost:5000"

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.FIDO_U2F
        assert verification.credential_id == base64url_to_bytes(
            "2i53XtAuBVv2ztu9hdTkG_I4_zc-MhmYOjM2HWDCIlk"
        )

    def test_verify_attestation_with_unsupported_token_binding_status(self) -> None:
        # Credential contains `clientDataJSON: { tokenBinding: { status: "not-supported" } }`
        credential = """{
            "id": "JeC3qgQjIVysq88GxhGUYyDl4oZeW8mLWd7luJWQvnrm-wxGZ5mzf2bBCaUDq7D2qr4aQezvzfoFIF880ciAsQ",
            "rawId": "JeC3qgQjIVysq88GxhGUYyDl4oZeW8mLWd7luJWQvnrm-wxGZ5mzf2bBCaUDq7D2qr4aQezvzfoFIF880ciAsQ",
            "response": {
                "clientDataJSON": "eyJjaGFsbGVuZ2UiOiJhR0ZHQ2JMMFZrUGYxRmNDVm5TQVpwNFpVSlRjZkY5diIsIm9yaWdpbiI6Imh0dHBzOi8vYXBpLWR1bzEuZHVvLnRlc3QiLCJ0b2tlbkJpbmRpbmciOnsic3RhdHVzIjoibm90LXN1cHBvcnRlZCJ9LCJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIn0",
                "attestationObject": "o2NmbXRoZmlkby11MmZnYXR0U3RtdKJjc2lnWEgwRgIhAJB-eYnY106PNDhEVM8QLuZnVCxBX0Khp9vzdj-kjw1CAiEA0PlEdDtH4GKG3eY_1YC4sIZR2ZPg-9SsLgODDWUDYnRjeDVjgVkCUzCCAk8wggE3oAMCAQICBDxoKU0wDQYJKoZIhvcNAQELBQAwLjEsMCoGA1UEAxMjWXViaWNvIFUyRiBSb290IENBIFNlcmlhbCA0NTcyMDA2MzEwIBcNMTQwODAxMDAwMDAwWhgPMjA1MDA5MDQwMDAwMDBaMDExLzAtBgNVBAMMJll1YmljbyBVMkYgRUUgU2VyaWFsIDIzOTI1NzM0ODExMTE3OTAxMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEvd9nk9t3lMNQMXHtLE1FStlzZnUaSLql2fm1ajoggXlrTt8rzXuSehSTEPvEaEdv_FeSqX22L6Aoa8ajIAIOY6M7MDkwIgYJKwYBBAGCxAoCBBUxLjMuNi4xLjQuMS40MTQ4Mi4xLjUwEwYLKwYBBAGC5RwCAQEEBAMCBSAwDQYJKoZIhvcNAQELBQADggEBAKrADVEJfuwVpIazebzEg0D4Z9OXLs5qZ_ukcONgxkRZ8K04QtP_CB5x6olTlxsj-SXArQDCRzEYUgbws6kZKfuRt2a1P-EzUiqDWLjRILSr-3_o7yR7ZP_GpiFKwdm-czb94POoGD-TS1IYdfXj94mAr5cKWx4EKjh210uovu_pLdLjc8xkQciUrXzZpPR9rT2k_q9HkZhHU-NaCJzky-PTyDbq0KKnzqVhWtfkSBCGw3ezZkTS-5lrvOKbIa24lfeTgu7FST5OwTPCFn8HcfWZMXMSD_KNU-iBqJdAwTLPPDRoLLvPTl29weCAIh-HUpmBQd0UltcPOrA_LFvAf61oYXV0aERhdGFYxEvYNAXQphWPDADgZiwL4n_m5OggKcxKctshspIdTVKXQQAAAAAAAAAAAAAAAAAAAAAAAAAAAEAl4LeqBCMhXKyrzwbGEZRjIOXihl5byYtZ3uW4lZC-eub7DEZnmbN_ZsEJpQOrsPaqvhpB7O_N-gUgXzzRyICxpQECAyYgASFYIJH525xGEk1oYXXxejoHvYcBrIwDbwcC_Kptwg6m5hArIlggvPFJ43EBPjkPK7aCIkiA3mtk_b6mkmzR8C_Fgjq3-xQ"
            },
            "type": "public-key"
        }"""
        challenge = base64url_to_bytes(
            "aGFGCbL0VkPf1FcCVnSAZp4ZUJTcfF9v",
        )
        rp_id = "duo.test"
        expected_origin = "https://api-duo1.duo.test"

        with self.assertRaisesRegex(
            InvalidRegistrationResponse, "Unexpected token_binding status"
        ):
            verify_registration_response(
                credential=credential,
                expected_challenge=challenge,
                expected_origin=expected_origin,
                expected_rp_id=rp_id,
            )

    def test_verify_attestation_with_unsupported_token_binding(self) -> None:
        # Credential contains `clientDataJSON: { tokenBinding: "unused" }`
        credential = """{
            "id": "jXFBv0gxr-DvGP58Oz3qfxMydiZM2RFlRoItoHyeAhdLNbmR7aPkzPVSKWO9VOZ4A2EEUQz8nsLtsP5EOqeNiQ",
            "rawId": "jXFBv0gxr-DvGP58Oz3qfxMydiZM2RFlRoItoHyeAhdLNbmR7aPkzPVSKWO9VOZ4A2EEUQz8nsLtsP5EOqeNiQ",
            "response": {
                "clientDataJSON": "eyJjaGFsbGVuZ2UiOiJCMjBMWlFBNnZtb2hXMnB2Q1RpM3lsVU5HZXQ4NTZwbyIsImhhc2hBbGdvcml0aG0iOiJTSEEtMjU2Iiwib3JpZ2luIjoiaHR0cHM6Ly9hcGktZHVvMS5kdW8udGVzdCIsInRva2VuQmluZGluZyI6InVudXNlZCIsInR5cGUiOiJ3ZWJhdXRobi5jcmVhdGUifQ",
                "attestationObject": "o2NmbXRoZmlkby11MmZnYXR0U3RtdKJjc2lnWEYwRAIgZNXeJymCt3MaODbi40dqksHQmfjvOajH4Th3qHK4nboCIEemlbhtg6g549fk5f6xU901qYeowKQ4lDOxwBisVo-RY3g1Y4FZAlMwggJPMIIBN6ADAgECAgQ8aClNMA0GCSqGSIb3DQEBCwUAMC4xLDAqBgNVBAMTI1l1YmljbyBVMkYgUm9vdCBDQSBTZXJpYWwgNDU3MjAwNjMxMCAXDTE0MDgwMTAwMDAwMFoYDzIwNTAwOTA0MDAwMDAwWjAxMS8wLQYDVQQDDCZZdWJpY28gVTJGIEVFIFNlcmlhbCAyMzkyNTczNDgxMTExNzkwMTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABL3fZ5Pbd5TDUDFx7SxNRUrZc2Z1Gki6pdn5tWo6IIF5a07fK817knoUkxD7xGhHb_xXkql9ti-gKGvGoyACDmOjOzA5MCIGCSsGAQQBgsQKAgQVMS4zLjYuMS40LjEuNDE0ODIuMS41MBMGCysGAQQBguUcAgEBBAQDAgUgMA0GCSqGSIb3DQEBCwUAA4IBAQCqwA1RCX7sFaSGs3m8xINA-GfTly7Oamf7pHDjYMZEWfCtOELT_wgeceqJU5cbI_klwK0AwkcxGFIG8LOpGSn7kbdmtT_hM1Iqg1i40SC0q_t_6O8ke2T_xqYhSsHZvnM2_eDzqBg_k0tSGHX14_eJgK-XClseBCo4dtdLqL7v6S3S43PMZEHIlK182aT0fa09pP6vR5GYR1PjWgic5Mvj08g26tCip86lYVrX5EgQhsN3s2ZE0vuZa7zimyGtuJX3k4LuxUk-TsEzwhZ_B3H1mTFzEg_yjVPogaiXQMEyzzw0aCy7z05dvcHggCIfh1KZgUHdFJbXDzqwPyxbwH-taGF1dGhEYXRhWMRL2DQF0KYVjwwA4GYsC-J_5uToICnMSnLbIbKSHU1Sl0EAAAAAAAAAAAAAAAAAAAAAAAAAAABAjXFBv0gxr-DvGP58Oz3qfxMydiZM2RFlRoItoHyeAhdLNbmR7aPkzPVSKWO9VOZ4A2EEUQz8nsLtsP5EOqeNiaUBAgMmIAEhWCD2BOHjcogovRtFtYbJpxctnRdgL-GL_Dwh0Pje7wUJ7yJYIACvXapVQbM5vEurQkI4096yU5dXv6CA-PftVTiTj-LT"
            },
            "type": "public-key"
        }"""
        challenge = base64url_to_bytes(
            "B20LZQA6vmohW2pvCTi3ylUNGet856po",
        )
        rp_id = "duo.test"
        expected_origin = "https://api-duo1.duo.test"

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.FIDO_U2F
        assert verification.credential_id == base64url_to_bytes(
            "jXFBv0gxr-DvGP58Oz3qfxMydiZM2RFlRoItoHyeAhdLNbmR7aPkzPVSKWO9VOZ4A2EEUQz8nsLtsP5EOqeNiQ",
        )
