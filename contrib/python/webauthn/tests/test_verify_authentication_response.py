from unittest import TestCase

from webauthn import verify_authentication_response
from webauthn.helpers import base64url_to_bytes, parse_authentication_credential_json
from webauthn.helpers.exceptions import InvalidAuthenticationResponse


class TestVerifyAuthenticationResponse(TestCase):
    def test_verify_authentication_response_with_EC2_public_key(self):
        credential = """{
            "id": "EDx9FfAbp4obx6oll2oC4-CZuDidRVV4gZhxC529ytlnqHyqCStDUwfNdm1SNHAe3X5KvueWQdAX3x9R1a2b9Q",
            "rawId": "EDx9FfAbp4obx6oll2oC4-CZuDidRVV4gZhxC529ytlnqHyqCStDUwfNdm1SNHAe3X5KvueWQdAX3x9R1a2b9Q",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MBAAAATg",
                "clientDataJSON": "eyJjaGFsbGVuZ2UiOiJ4aTMwR1BHQUZZUnhWRHBZMXNNMTBEYUx6VlFHNjZudi1fN1JVYXpIMHZJMll2RzhMWWdERW52TjVmWlpOVnV2RUR1TWk5dGUzVkxxYjQyTjBma0xHQSIsImNsaWVudEV4dGVuc2lvbnMiOnt9LCJoYXNoQWxnb3JpdGhtIjoiU0hBLTI1NiIsIm9yaWdpbiI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAwMCIsInR5cGUiOiJ3ZWJhdXRobi5nZXQifQ",
                "signature": "MEUCIGisVZOBapCWbnJJvjelIzwpixxIwkjCCb5aCHafQu68AiEA88v-2pJNNApPFwAKFiNuf82-2hBxYW5kGwVweeoxCwo"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "xi30GPGAFYRxVDpY1sM10DaLzVQG66nv-_7RUazH0vI2YvG8LYgDEnvN5fZZNVuvEDuMi9te3VLqb42N0fkLGA"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pQECAyYgASFYIIeDTe-gN8A-zQclHoRnGFWN8ehM1b7yAsa8I8KIvmplIlgg4nFGT5px8o6gpPZZhO01wdy9crDSA_Ngtkx0vGpvPHI"
        )
        sign_count = 77

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
        )

        assert verification.credential_id == base64url_to_bytes(
            "EDx9FfAbp4obx6oll2oC4-CZuDidRVV4gZhxC529ytlnqHyqCStDUwfNdm1SNHAe3X5KvueWQdAX3x9R1a2b9Q"
        )
        assert verification.new_sign_count == 78
        assert verification.credential_backed_up == False
        assert verification.credential_device_type == "single_device"
        assert not verification.user_verified

    def test_verify_authentication_response_with_RSA_public_key(self):
        credential = """{
            "id": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "rawId": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFAAAAAQ",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiaVBtQWkxUHAxWEw2b0FncTNQV1p0WlBuWmExekZVRG9HYmFRMF9LdlZHMWxGMnMzUnRfM280dVN6Y2N5MHRtY1RJcFRUVDRCVTFULUk0bWFhdm5kalEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "iOHKX3erU5_OYP_r_9HLZ-CexCE4bQRrxM8WmuoKTDdhAnZSeTP0sjECjvjfeS8MJzN1ArmvV0H0C3yy_FdRFfcpUPZzdZ7bBcmPh1XPdxRwY747OrIzcTLTFQUPdn1U-izCZtP_78VGw9pCpdMsv4CUzZdJbEcRtQuRS03qUjqDaovoJhOqEBmxJn9Wu8tBi_Qx7A33RbYjlfyLm_EDqimzDZhyietyop6XUcpKarKqVH0M6mMrM5zTjp8xf3W7odFCadXEJg-ERZqFM0-9Uup6kJNLbr6C5J4NDYmSm3HCSA6lp2iEiMPKU8Ii7QZ61kybXLxsX4w4Dm3fOLjmDw",
                "userHandle": "T1RWa1l6VXdPRFV0WW1NNVlTMDBOVEkxTFRnd056Z3RabVZpWVdZNFpEVm1ZMk5p"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "iPmAi1Pp1XL6oAgq3PWZtZPnZa1zFUDoGbaQ0_KvVG1lF2s3Rt_3o4uSzccy0tmcTIpTTT4BU1T-I4maavndjQ"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pAEDAzkBACBZAQDfV20epzvQP-HtcdDpX-cGzdOxy73WQEvsU7Dnr9UWJophEfpngouvgnRLXaEUn_d8HGkp_HIx8rrpkx4BVs6X_B6ZjhLlezjIdJbLbVeb92BaEsmNn1HW2N9Xj2QM8cH-yx28_vCjf82ahQ9gyAr552Bn96G22n8jqFRQKdVpO-f-bvpvaP3IQ9F5LCX7CUaxptgbog1SFO6FI6ob5SlVVB00lVXsaYg8cIDZxCkkENkGiFPgwEaZ7995SCbiyCpUJbMqToLMgojPkAhWeyktu7TlK6UBWdJMHc3FPAIs0lH_2_2hKS-mGI1uZAFVAfW1X-mzKL0czUm2P1UlUox7IUMBAAE"
        )
        sign_count = 0

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
            require_user_verification=True,
        )

        assert verification.new_sign_count == 1
        assert verification.user_verified

    def test_raises_exception_on_incorrect_public_key(self):
        credential = """{
            "id": "FviUBZA3FGMxEm3A1K2T8MhuEBLp4qQsV9ScAKYrpdw2kbGnqx24tF4ev6PEHEYC3g8z6HMJh7dYHe3Uuq7_8Q",
            "rawId": "FviUBZA3FGMxEm3A1K2T8MhuEBLp4qQsV9ScAKYrpdw2kbGnqx24tF4ev6PEHEYC3g8z6HMJh7dYHe3Uuq7_8Q",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFAAAAJA",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoienNmaU1aajE2VFVWQ3JUNXREUllYZFlsVXJKcDd6bl9VTmQ1Tm1Cb2NQYzRJMmRLWmJlRVdwd0JBd0E0czZvSGtWWDZfbHlfamdwNzQzZHlpV0hZWXciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "MEQCIBX9B1LaLaQ0LYJsRv7cOyMS-Do1rJfFJoF9oO1tHMA4AiBRKdNneMKPlN53i8uoTZ5y9Gj4ORZySmiercS38655_g"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "zsfiMZj16TUVCrT5tDRYXdYlUrJp7zn_UNd5NmBocPc4I2dKZbeEWpwBAwA4s6oHkVX6_ly_jgp743dyiWHYYw"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pAEDAzkBACBZAQDfV20epzvQP-HtcdDpX-cGzdOxy73WQEvsU7Dnr9UWJophEfpngouvgnRLXaEUn_d8HGkp_HIx8rrpkx4BVs6X_B6ZjhLlezjIdJbLbVeb92BaEsmNn1HW2N9Xj2QM8cH-yx28_vCjf82ahQ9gyAr552Bn96G22n8jqFRQKdVpO-f-bvpvaP3IQ9F5LCX7CUaxptgbog1SFO6FI6ob5SlVVB00lVXsaYg8cIDZxCkkENkGiFPgwEaZ7995SCbiyCpUJbMqToLMgojPkAhWeyktu7TlK6UBWdJMHc3FPAIs0lH_2_2hKS-mGI1uZAFVAfW1X-mzKL0czUm2P1UlUox7IUMBAAE"
        )
        sign_count = 35

        with self.assertRaisesRegex(
            InvalidAuthenticationResponse,
            "Could not verify authentication signature",
        ):
            verify_authentication_response(
                credential=credential,
                expected_challenge=challenge,
                expected_rp_id=expected_rp_id,
                expected_origin=expected_origin,
                credential_public_key=credential_public_key,
                credential_current_sign_count=sign_count,
                require_user_verification=True,
            )

    def test_raises_exception_on_uv_required_but_false(self):
        credential = """{
            "id": "4-5MZF69j3n2B6Z99dUN0fNrAQmrjELJIebWVw8aKfw1EQKg28Tx40R_kw-1pcrfSgJFKm3mCtAtBgSRWgDMng",
            "rawId": "4-5MZF69j3n2B6Z99dUN0fNrAQmrjELJIebWVw8aKfw1EQKg28Tx40R_kw-1pcrfSgJFKm3mCtAtBgSRWgDMng",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MBAAAAIQ",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoidW1HZW1YSklQQlhQeGtEOEhqYW51djlCRG9yOFo3TzNhUGR0T2dNQ2RXNFBBZnFEWDQzRUZsaHJzRjBQVzkwZGY1enJnYnQ3WVZNUkFhMjd0Q2RIenciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "MEUCIGp5ADnU_SFvT4J_bKvQJ4Pc1GmANhbYq5GioOLjyUrxAiEA6Kk5qAZb8MLY-jyTiJLr_R9Fke02UHkxsRB0dnZt2X8"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "umGemXJIPBXPxkD8Hjanuv9BDor8Z7O3aPdtOgMCdW4PAfqDX43EFlhrsF0PW90df5zrgbt7YVMRAa27tCdHzw"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pQECAyYgASFYIOQ5TKpXJR2cV76Wgfge9BkLkEhLxVjhFjM1jKHYOcqpIlggaiNy1blt3OU8Hsmg041HUYP7eajgL7fk3nSuTEjYCwU"
        )
        sign_count = 32

        with self.assertRaisesRegex(
            InvalidAuthenticationResponse,
            "User verification is required but user was not verified",
        ):
            verify_authentication_response(
                credential=credential,
                expected_challenge=challenge,
                expected_rp_id=expected_rp_id,
                expected_origin=expected_origin,
                credential_public_key=credential_public_key,
                credential_current_sign_count=sign_count,
                require_user_verification=True,
            )

    def test_verify_authentication_response_with_OKP_public_key(self):
        credential = """{
            "id": "fq9Nj0nS24B5y6Pkw_h3-9GEAEA3-0LBPxE2zvTdLjDqtSeCSNYFe9VMRueSpAZxT3YDc6L1lWXdQNwI-sVNYrefEcRR1Nsb_0jpHE955WEtFud2xxZg3MvoLMxHLet63i5tajd1fHtP7I-00D6cehM8ZWlLp2T3s9lfZgVIFcA",
            "rawId": "fq9Nj0nS24B5y6Pkw_h3-9GEAEA3-0LBPxE2zvTdLjDqtSeCSNYFe9VMRueSpAZxT3YDc6L1lWXdQNwI-sVNYrefEcRR1Nsb_0jpHE955WEtFud2xxZg3MvoLMxHLet63i5tajd1fHtP7I-00D6cehM8ZWlLp2T3s9lfZgVIFcA",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MBAAAABw",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiZVo0ZWVBM080ank1Rkl6cURhU0o2SkROR3UwYkJjNXpJMURqUV9rTHNvMVdOcWtHNms1bUNZZjFkdFFoVlVpQldaV2xaa3pSNU1GZWVXQ3BKUlVOWHciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "RRWV8mYDRvK7YdQgdtZD4pJ2dh1D_IWZ_D6jsZo6FHJBoenbj0CVT5nA20vUzlRhN4R6dOEUHmUwP1F8eRBhBg"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        challenge = base64url_to_bytes(
            "eZ4eeA3O4jy5FIzqDaSJ6JDNGu0bBc5zI1DjQ_kLso1WNqkG6k5mCYf1dtQhVUiBWZWlZkzR5MFeeWCpJRUNXw"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pAEBAycgBiFYIMz6_SUFLiDid2Yhlq0YboyJ-CDrIrNpkPUGmJp4D3Dp"
        )
        sign_count = 3

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
        )

        assert verification.new_sign_count == 7

    def test_supports_multiple_expected_origins(self) -> None:
        credential = """{
            "id": "AXmOjWWZH67pgl5_gAbKVBqoL2dyHHGEWZLspIsCwULG0hZ3HyuGgvkaRcSOLq9W72XtegcvFYXIdlafrilbtVnx2Q14gNbfSQQP2sgNEAif4MjHtGpeVB0BfFawCs85Y3XY_j4sxthVnyTY_Q",
            "rawId": "AXmOjWWZH67pgl5_gAbKVBqoL2dyHHGEWZLspIsCwULG0hZ3HyuGgvkaRcSOLq9W72XtegcvFYXIdlafrilbtVnx2Q14gNbfSQQP2sgNEAif4MjHtGpeVB0BfFawCs85Y3XY_j4sxthVnyTY_Q",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFYN-Mog",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiNnpyU1JYOEN4d1BTWEVBclh5WEwydHBiNnJCN1N0YXIwckxWSWo1cnZmNzRtWktGNWlyNzE1WG1nejV0QV9HeUhleE40b1hmclE4ODlBclZDTGFSZEEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "MEUCIQDBqeI274exaKWGQz37g7yo1--TVcZSCcYVftZ1AnEJkQIgNw-nlx-_U9rVfFfER8oX6BlYZTuPFyGaL_wCDY23s0E",
                "userHandle": "TldNMFlqYzNOVFF0WW1NNE5DMDBaakprTFRrME9EVXROR05rTnpreVkyTTROVEUz"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""

        challenge = base64url_to_bytes(
            "6zrSRX8CxwPSXEArXyXL2tpb6rB7Star0rLVIj5rvf74mZKF5ir715Xmgz5tA_GyHexN4oXfrQ889ArVCLaRdA"
        )
        expected_rp_id = "localhost"
        expected_origin = ["https://foo.bar", "http://localhost:5000"]
        credential_public_key = base64url_to_bytes(
            "pQECAyYgASFYIFm1Py-FzzFOuwXbRbTr95SiDxuB1BkZsEEJxFhquzqkIlggL1U1T713Jo_2muzhXvpbwRNdoAs8CYK6PflvY1MBdCI"
        )
        sign_count = 1625263263

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
        )

        assert verification.credential_id == base64url_to_bytes(
            "AXmOjWWZH67pgl5_gAbKVBqoL2dyHHGEWZLspIsCwULG0hZ3HyuGgvkaRcSOLq9W72XtegcvFYXIdlafrilbtVnx2Q14gNbfSQQP2sgNEAif4MjHtGpeVB0BfFawCs85Y3XY_j4sxthVnyTY_Q"
        )

    def test_supports_already_parsed_credential(self) -> None:
        parsed_credential = parse_authentication_credential_json(
            """{
            "id": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "rawId": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFAAAAAQ",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiaVBtQWkxUHAxWEw2b0FncTNQV1p0WlBuWmExekZVRG9HYmFRMF9LdlZHMWxGMnMzUnRfM280dVN6Y2N5MHRtY1RJcFRUVDRCVTFULUk0bWFhdm5kalEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "iOHKX3erU5_OYP_r_9HLZ-CexCE4bQRrxM8WmuoKTDdhAnZSeTP0sjECjvjfeS8MJzN1ArmvV0H0C3yy_FdRFfcpUPZzdZ7bBcmPh1XPdxRwY747OrIzcTLTFQUPdn1U-izCZtP_78VGw9pCpdMsv4CUzZdJbEcRtQuRS03qUjqDaovoJhOqEBmxJn9Wu8tBi_Qx7A33RbYjlfyLm_EDqimzDZhyietyop6XUcpKarKqVH0M6mMrM5zTjp8xf3W7odFCadXEJg-ERZqFM0-9Uup6kJNLbr6C5J4NDYmSm3HCSA6lp2iEiMPKU8Ii7QZ61kybXLxsX4w4Dm3fOLjmDw",
                "userHandle": "T1RWa1l6VXdPRFV0WW1NNVlTMDBOVEkxTFRnd056Z3RabVZpWVdZNFpEVm1ZMk5p"
            },
            "type": "public-key",
            "clientExtensionResults": {}
        }"""
        )
        challenge = base64url_to_bytes(
            "iPmAi1Pp1XL6oAgq3PWZtZPnZa1zFUDoGbaQ0_KvVG1lF2s3Rt_3o4uSzccy0tmcTIpTTT4BU1T-I4maavndjQ"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pAEDAzkBACBZAQDfV20epzvQP-HtcdDpX-cGzdOxy73WQEvsU7Dnr9UWJophEfpngouvgnRLXaEUn_d8HGkp_HIx8rrpkx4BVs6X_B6ZjhLlezjIdJbLbVeb92BaEsmNn1HW2N9Xj2QM8cH-yx28_vCjf82ahQ9gyAr552Bn96G22n8jqFRQKdVpO-f-bvpvaP3IQ9F5LCX7CUaxptgbog1SFO6FI6ob5SlVVB00lVXsaYg8cIDZxCkkENkGiFPgwEaZ7995SCbiyCpUJbMqToLMgojPkAhWeyktu7TlK6UBWdJMHc3FPAIs0lH_2_2hKS-mGI1uZAFVAfW1X-mzKL0czUm2P1UlUox7IUMBAAE"
        )
        sign_count = 0

        verification = verify_authentication_response(
            credential=parsed_credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
            require_user_verification=True,
        )

        assert verification.new_sign_count == 1

    def test_supports_dict_credential(self) -> None:
        credential = {
            "id": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "rawId": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
            "response": {
                "authenticatorData": "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFAAAAAQ",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiaVBtQWkxUHAxWEw2b0FncTNQV1p0WlBuWmExekZVRG9HYmFRMF9LdlZHMWxGMnMzUnRfM280dVN6Y2N5MHRtY1RJcFRUVDRCVTFULUk0bWFhdm5kalEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                "signature": "iOHKX3erU5_OYP_r_9HLZ-CexCE4bQRrxM8WmuoKTDdhAnZSeTP0sjECjvjfeS8MJzN1ArmvV0H0C3yy_FdRFfcpUPZzdZ7bBcmPh1XPdxRwY747OrIzcTLTFQUPdn1U-izCZtP_78VGw9pCpdMsv4CUzZdJbEcRtQuRS03qUjqDaovoJhOqEBmxJn9Wu8tBi_Qx7A33RbYjlfyLm_EDqimzDZhyietyop6XUcpKarKqVH0M6mMrM5zTjp8xf3W7odFCadXEJg-ERZqFM0-9Uup6kJNLbr6C5J4NDYmSm3HCSA6lp2iEiMPKU8Ii7QZ61kybXLxsX4w4Dm3fOLjmDw",
                "userHandle": "T1RWa1l6VXdPRFV0WW1NNVlTMDBOVEkxTFRnd056Z3RabVZpWVdZNFpEVm1ZMk5p",
            },
            "type": "public-key",
            "clientExtensionResults": {},
        }
        challenge = base64url_to_bytes(
            "iPmAi1Pp1XL6oAgq3PWZtZPnZa1zFUDoGbaQ0_KvVG1lF2s3Rt_3o4uSzccy0tmcTIpTTT4BU1T-I4maavndjQ"
        )
        expected_rp_id = "localhost"
        expected_origin = "http://localhost:5000"
        credential_public_key = base64url_to_bytes(
            "pAEDAzkBACBZAQDfV20epzvQP-HtcdDpX-cGzdOxy73WQEvsU7Dnr9UWJophEfpngouvgnRLXaEUn_d8HGkp_HIx8rrpkx4BVs6X_B6ZjhLlezjIdJbLbVeb92BaEsmNn1HW2N9Xj2QM8cH-yx28_vCjf82ahQ9gyAr552Bn96G22n8jqFRQKdVpO-f-bvpvaP3IQ9F5LCX7CUaxptgbog1SFO6FI6ob5SlVVB00lVXsaYg8cIDZxCkkENkGiFPgwEaZ7995SCbiyCpUJbMqToLMgojPkAhWeyktu7TlK6UBWdJMHc3FPAIs0lH_2_2hKS-mGI1uZAFVAfW1X-mzKL0czUm2P1UlUox7IUMBAAE"
        )
        sign_count = 0

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=expected_rp_id,
            expected_origin=expected_origin,
            credential_public_key=credential_public_key,
            credential_current_sign_count=sign_count,
            require_user_verification=True,
        )

        assert verification.new_sign_count == 1
