from unittest import TestCase
from datetime import datetime
from OpenSSL.crypto import X509Store

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.structs import AttestationFormat
from webauthn import verify_registration_response

from .helpers.x509store import patch_validate_certificate_chain_x509store_getter


class TestVerifyRegistrationResponseAndroidKey(TestCase):
    @patch_validate_certificate_chain_x509store_getter
    def test_verify_attestation_android_key_hardware_authority(
        self,
        patched_x509store: X509Store,
    ) -> None:
        """
        This android-key attestation was generated on a Pixel 8a in January 2025 via an origin
        trial. Google will be sunsetting android-safetynet attestation for android-key attestations
        for device-bound passkeys (i.e. `"residentKey": "discouraged"`) in April 2025

        See here for more info:
        https://android-developers.googleblog.com/2024/09/attestation-format-change-for-android-fido2-api.html
        """
        credential = """{
            "id": "AYNe4CBKc8H30FuAb8uaht6JbEQfbSBnS0SX7B6MFg8ofI92oR5lheRDJCgwY-JqB_QSJtezdhMbf8Wzt_La5N0",
            "rawId": "AYNe4CBKc8H30FuAb8uaht6JbEQfbSBnS0SX7B6MFg8ofI92oR5lheRDJCgwY-JqB_QSJtezdhMbf8Wzt_La5N0",
            "response": {
                "attestationObject": "o2NmbXRrYW5kcm9pZC1rZXlnYXR0U3RtdKNjYWxnJmNzaWdYSDBGAiEAs9Aufj5f5HyLKEFsgfmqyaXfAih-hGuTJqgmxZGijzYCIQDAMddAq1gwH3MtesYR6WE6IAockRz8ilR7CFw_kgdmv2N4NWOFWQLQMIICzDCCAnKgAwIBAgIBATAKBggqhkjOPQQDAjA5MSkwJwYDVQQDEyBkNjAyYTAzYTY3MmQ4NjViYTVhNDg1ZTMzYTIwN2M3MzEMMAoGA1UEChMDVEVFMB4XDTcwMDEwMTAwMDAwMFoXDTQ4MDEwMTAwMDAwMFowHzEdMBsGA1UEAxMUQW5kcm9pZCBLZXlzdG9yZSBLZXkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATXVi3-n-rBsrP3A4Pj9P8e6PNh3eNdC38PaFiCZyMWdUVA6PbE6985PSUDDcnk3Knnpyc66J_HFOu_geuqiWtAo4IBgzCCAX8wDgYDVR0PAQH_BAQDAgeAMIIBawYKKwYBBAHWeQIBEQSCAVswggFXAgIBLAoBAQICASwKAQEEIFZS4txFVJqW-Wr6IlUC-H-twIpgvAITksC-jFBi_V9eBAAwd7-FPQgCBgGUcHc4or-FRWcEZTBjMT0wGwQWY29tLmdvb2dsZS5hbmRyb2lkLmdzZgIBIzAeBBZjb20uZ29vZ2xlLmFuZHJvaWQuZ21zAgQO6jzjMSIEIPD9bFtBDyXLJcO1M0bIly-uMPjudBHfkQSArWstYNuDMIGpoQUxAwIBAqIDAgEDowQCAgEApQUxAwIBBKoDAgEBv4N4AwIBA7-DeQMCAQq_hT4DAgEAv4VATDBKBCCd4l-wK7VTDUQUnRSEN8guJn5VcyJTCqbwOwrC6Skx2gEB_woBAAQg6y0px0ZXc5v2bsVb45w-6IiMbXzp3gyHIWKS1mbz6gu_hUEFAgMCSfC_hUIFAgMDFwW_hU4GAgQBNP35v4VPBgIEATT9-TAKBggqhkjOPQQDAgNIADBFAiEAzNz6wyTo4t5ixo9G4zXPwh4zSB9F854sU_KDGTf0dxYCICaQVSWzWgTZLQYv13MXJJee8S8_luQB3W5lPPzP0exsWQHjMIIB3zCCAYWgAwIBAgIRANYCoDpnLYZbpaSF4zogfHMwCgYIKoZIzj0EAwIwKTETMBEGA1UEChMKR29vZ2xlIExMQzESMBAGA1UEAxMJRHJvaWQgQ0EzMB4XDTI1MDEwNzE3MDg0M1oXDTI1MDIwMjEwMzUyN1owOTEpMCcGA1UEAxMgZDYwMmEwM2E2NzJkODY1YmE1YTQ4NWUzM2EyMDdjNzMxDDAKBgNVBAoTA1RFRTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFPbPYqm91rYvZVCBdFaHRMg0tw7U07JA1EcD9ZP4d0lK2NFM4A0wGKS4jbTR_bu7NTt_YyF388S0PWAJTluqnOjfjB8MB0GA1UdDgQWBBSXyrsZ_A1NnJGRq0sm2G9nm-NC5zAfBgNVHSMEGDAWgBTFUX4F2MtjWykYrAIa8sh9bBL-kjAPBgNVHRMBAf8EBTADAQH_MA4GA1UdDwEB_wQEAwICBDAZBgorBgEEAdZ5AgEeBAuiAQgDZkdvb2dsZTAKBggqhkjOPQQDAgNIADBFAiEAysd6JDoI8X4NEdrRwUwtIAy-hLxSEKUVS2XVWS2CP04CIFNQQzM4TkA_xaZj8KyiS61nb-aOBP35tlA34JCOlv9nWQHcMIIB2DCCAV2gAwIBAgIUAIUK9vrO5iIEbQx0izdwqlWwtk0wCgYIKoZIzj0EAwMwKTETMBEGA1UEChMKR29vZ2xlIExMQzESMBAGA1UEAxMJRHJvaWQgQ0EyMB4XDTI0MTIwOTA2Mjg1M1oXDTI1MDIxNzA2Mjg1MlowKTETMBEGA1UEChMKR29vZ2xlIExMQzESMBAGA1UEAxMJRHJvaWQgQ0EzMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPjbr-yt9xhgcbKLXoN3RK-1FcCjwIpeMPJZjayW0dqNtFflHp2smO0DxN_6x7M7NAGbcC9lM1_E-N6z51ODv-6NjMGEwDgYDVR0PAQH_BAQDAgIEMA8GA1UdEwEB_wQFMAMBAf8wHQYDVR0OBBYEFMVRfgXYy2NbKRisAhryyH1sEv6SMB8GA1UdIwQYMBaAFKYLhqTwyH8ztWE5Ys0956c6QoNIMAoGCCqGSM49BAMDA2kAMGYCMQCuzU0wV_NkOQzgqzyqP66SJN6lilrU-NDVU6qNCnbFsUoZQOm4wBwUw7LqfoUhx7YCMQDFEvqHfc2hwN2J4I9Z4rTHiLlsy6gA33WvECzIZmVMpKcyEiHlm4c9XR0nVkAjQ_5ZA4QwggOAMIIBaKADAgECAgoDiCZnYGWJloYOMA0GCSqGSIb3DQEBCwUAMBsxGTAXBgNVBAUTEGY5MjAwOWU4NTNiNmIwNDUwHhcNMjIwMTI2MjI0OTQ1WhcNMzcwMTIyMjI0OTQ1WjApMRMwEQYDVQQKEwpHb29nbGUgTExDMRIwEAYDVQQDEwlEcm9pZCBDQTIwdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAT72ZtYJ0I2etFhouvtVs0sBzvYsx8thNCZV1wsDPvsMDSTPij-M1wBFD00OUn2bfU5b7K2_t2NkXc2-_V9g--mdb6SoRGmJ_AG9ScY60LKSA7iPT7gZ_5-q0tnEPPZJCqjZjBkMB0GA1UdDgQWBBSmC4ak8Mh_M7VhOWLNPeenOkKDSDAfBgNVHSMEGDAWgBQ2YeEAfIgFCVGLRGxH_xpMyepPEjASBgNVHRMBAf8ECDAGAQH_AgECMA4GA1UdDwEB_wQEAwIBBjANBgkqhkiG9w0BAQsFAAOCAgEArpB2eLbKHNcS6Q3Td3N7ZCgVLN0qA7CboM-Ftu4YYAcHxh-e_sk7T7XOg5S4d9a_DD7mIXgENSBPB_fVqCnBaSDKNJ3nUuC1_9gcT95p4kKJo0tqcsWw8WgKVJhNuZCN7d_ziHLiRRcrKtaj944THzsy7vB-pSai7gTah_RJrDQI91bDUJgld8_p_QAbVnYA8o-msO0sRKxgF1V5QuBwBTfpdkqshqL3nwBm0sofqI_rM-JOQava3-IurHvfkzioiOJ0uFJnBGVjpZFwGwsmyKwzl-3qRKlkHggAOKt3lQQ4GiJnOCm10JrxPa2Za0K6_kyk6YyvvRcFNai5ej3nMKJPg-eeG2nST6N6ePFuaeoNQnD4XkagGFEQYzcqvsdFsmsbUFMghFl7zEVYdscuSgCG939wxW1JgKyG5ce7CI40328w9IuOf8mUS_W3i4jSfxqCJbegyo_SKDpDILnhJUBy0T3fN8mv9AyO0uoJBlvnogIVv2SdpYUt92vyOiGMy3Jx_ZRWjIRa7iIV3VnjLI__pgCrXQLMinZWEWsxVxg25nrk8u32nZd67DJN3k2FufRbsmHZly9CLo0P79lkIEC3rifLqqJeDyHQNaBMUC6BSDZ5RJCtMjSZw2xL5z0X9_zBsKVPkMW61hMhKzVmYNLe1DJQANRP-enru5i1oXlZBSAwggUcMIIDBKADAgECAgkA1Q_yW6Py1rMwDQYJKoZIhvcNAQELBQAwGzEZMBcGA1UEBRMQZjkyMDA5ZTg1M2I2YjA0NTAeFw0xOTExMjIyMDM3NThaFw0zNDExMTgyMDM3NThaMBsxGTAXBgNVBAUTEGY5MjAwOWU4NTNiNmIwNDUwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCvtseCK7GnAewrtC6LzFQWY6vvmC8yx391MQMMl1JLG1_oCfvHKqlFH3Q8vZpvEzV0SqVed_a2rDU17hfCXmOVF92ckuY3SlPL_iWPj_u2_RKTeKIqTKmcRS1HpZ8yAfRBl8oczX52L7L1MVG2_rL__Stv5P5bxr2ew0v-CCOdqvzrjrWo7Ss6zZxeOneQ4bUUQnkxWYWYEa2esqlrvdelfJOpHEH8zSfWf9b2caoLgVJhrThPo3lEhkYE3bPYxPkgoZsWVsLxStbQPFbsBgiZBBwe0aX-bTRAtVa60dChUlicU-VdNwdi8BIu75GGGxsObEyAknSZwOm-wLg-O8H5PHLASWBLvS8TReYsP44m2-wGyUdm88EoI51PQxL62BI4h-Br7PVnWDv4NVqB_uq6-ZqDyN8-KjIq_Gcr8SCxNRWLaCHOrzCbbu53-YgzsBjaoQ5FHwajdNUHgfNZCClmu3eLkwiUJpjnTgvNJGKKAcLMA-UfCz5bSsHk356vn_akkqd8FIOIKIUBW0Is5nuAuIybSOE7YHq1Rccj_4xE-PLTaLn2Ug0xFF6_noYq1x32o7_SRQlZ1lN0DZehLzaLE-9m1dClSm4vXZpv70RoMrxnhEclhh8JPdDm80BdqJZD7w9NabZCAFH9uTBJZz42lQWA0830-9CLxYSDlSYAYwIDAQABo2MwYTAdBgNVHQ4EFgQUNmHhAHyIBQlRi0RsR_8aTMnqTxIwHwYDVR0jBBgwFoAUNmHhAHyIBQlRi0RsR_8aTMnqTxIwDwYDVR0TAQH_BAUwAwEB_zAOBgNVHQ8BAf8EBAMCAgQwDQYJKoZIhvcNAQELBQADggIBAE4xoFzyi6Zdva-hztcJae5cqEEErd7YowbPf23uUDdddF7ZkssCQsznLcnu1RGR_lrVK61907JcCZ4TpJGjzdSHpazOh2YyTErkYzgkaue3ikGKy7mKBcTJ1pbuqrYJ0LoM4aMb6YSQ3z9MDqndyegv-w_LPp692MuVJ4nysUEfrFbIhkJutylgQnNdpQ4RrHFfGBjPn9xOJUo3YzUbaiRAFQhhJjpuMQvhpQ3lx-juiA_dS-WISjcSjRiDC7NHa_QpHoLVxmpklJOeCEgL-8APfYp01D5zc36-XY5OxRUwLUaJaSeA3HU47X6Rdb5hOedNQ604izBQ_9Wp3lJiAAiYwB9jxT3-IiCRCPpPZboWxJzL3gg318WETVS3OYugEi5QWxVckxPP4m5y2H4iqhYW5r2_VH3f-T3ynjWmO0Vf4fwOyVWB8_T3u-O7goOWo3rjFXWCvDdkuXgKI578D3Wh4ubZQc6rrCfd6wHivYQhApvqNNUa7mxgJx1alevQBRWpwAE92Av4fuomC4HDT2iObrE0ivDY6hysMqy52T-iSv8DCoTI8rD1acyVCAsgrDWs4MbY29T2hHcZUZ0yRQFm60vxW4WQRFAa3q9DY4LDSxXjtUyS5htpwr_HJkWJFys8k9vjXOBtCP1cATIsoId7HRJ0OvH61ZQOobwC3YkcaGF1dGhEYXRhWMVJlg3liA6MaHQ0Fw9kdmBbj-SuuaKGMseZXPO6gx2XY0UAAAAAuT_ZYfLmRi-xIoIAIkfeeABBAYNe4CBKc8H30FuAb8uaht6JbEQfbSBnS0SX7B6MFg8ofI92oR5lheRDJCgwY-JqB_QSJtezdhMbf8Wzt_La5N2lAQIDJiABIVgg11Yt_p_qwbKz9wOD4_T_HujzYd3jXQt_D2hYgmcjFnUiWCBFQOj2xOvfOT0lAw3J5Nyp56cnOuifxxTrv4HrqolrQA",
                "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoidDRMV0kwaVlKU1RXUGw5V1hVZE5oZEhBbnJQRExGOWVXQVA5bEhnbUhQOCIsIm9yaWdpbiI6Imh0dHA6Ly9sb2NhbGhvc3Q6ODAwMCIsImNyb3NzT3JpZ2luIjpmYWxzZX0"
            },
            "type": "public-key",
            "clientExtensionResults": {
                "credProps": { "rk": false }
            },
            "authenticatorAttachment": "platform"
        }"""

        challenge = base64url_to_bytes("t4LWI0iYJSTWPl9WXUdNhdHAnrPDLF9eWAP9lHgmHP8")
        rp_id = "localhost"
        expected_origin = "http://localhost:8000"

        # Setting the time to something that satisfies all these:
        # (Leaf) 19700101000000Z <-> 20480101000000Z
        # (Int.) 20250107170843Z <-> 20250202103527Z <- Earliest expiration
        # (Int.) 20241209062853Z <-> 20250217062852Z
        # (Int.) 20220126224945Z <-> 20370122224945Z
        # (Root) 20191122203758Z <-> 20341118203758Z
        patched_x509store.set_time(datetime(2025, 1, 8, 0, 0, 0))

        verification = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_origin=expected_origin,
            expected_rp_id=rp_id,
        )

        assert verification.fmt == AttestationFormat.ANDROID_KEY
        assert verification.credential_id == base64url_to_bytes(
            "AYNe4CBKc8H30FuAb8uaht6JbEQfbSBnS0SX7B6MFg8ofI92oR5lheRDJCgwY-JqB_QSJtezdhMbf8Wzt_La5N0"
        )
