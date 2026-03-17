from unittest import TestCase

from webauthn.helpers.tpm.parse_cert_info import parse_cert_info
from webauthn.helpers.tpm.structs import TPM_ALG, TPM_ST


class TestWebAuthnTPMParseCertInfo(TestCase):
    def test_properly_parses_cert_info_bytes(self) -> None:
        cert_info = b'\xffTCG\x80\x17\x00"\x00\x0bW"f{J5_9"\x15\tL\x01\xd5e\xbcr\xc6\xc9\x03\xbc#\xb5m\xee\xb5yI+j\xe6\xce\x00\x14`\x0bD(A\x99\xf3\xd3\x12I[\x04\x1f\xf4\xe7\xfb)\xc8\x02\x8f\x00\x00\x00\x00\x1a0)Z\x16\xb0}\xb1R\'s\xf8\x01\x97g1K\xfaf`T\x00"\x00\x0b\xe7\x1c"\x90\x07\xdeA\xe1w\xe0\xb3F\xe1\x07\x02\x8c\x16b\xe1\r\x9e\xb8\xae\xe7\xa95\xac\xf6\x1a\xedx\x89\x00"\x00\x0b\x7f\xe8\x84\xdaC\xa7\xc5?\xcept,\xa9\nA\x99\x93\xbc\x1f\x15\xcbs\x7f\xe0\x1a\x96u\xca\xe4\x8f\x86\x81'

        output = parse_cert_info(cert_info)

        assert output.magic == b"\xffTCG"
        assert output.type == TPM_ST.ATTEST_CERTIFY
        assert (
            output.qualified_signer
            == b'\x00\x0bW"f{J5_9"\x15\tL\x01\xd5e\xbcr\xc6\xc9\x03\xbc#\xb5m\xee\xb5yI+j\xe6\xce'
        )
        assert output.extra_data == b"`\x0bD(A\x99\xf3\xd3\x12I[\x04\x1f\xf4\xe7\xfb)\xc8\x02\x8f"
        assert output.firmware_version == b"\x97g1K\xfaf`T"
        # Attested
        assert output.attested.name_alg == TPM_ALG.SHA256
        assert output.attested.name_alg_bytes == b"\x00\x0b"
        assert (
            output.attested.name
            == b'\x00\x0b\xe7\x1c"\x90\x07\xdeA\xe1w\xe0\xb3F\xe1\x07\x02\x8c\x16b\xe1\r\x9e\xb8\xae\xe7\xa95\xac\xf6\x1a\xedx\x89'
        )
        assert (
            output.attested.qualified_name
            == b"\x00\x0b\x7f\xe8\x84\xdaC\xa7\xc5?\xcept,\xa9\nA\x99\x93\xbc\x1f\x15\xcbs\x7f\xe0\x1a\x96u\xca\xe4\x8f\x86\x81"
        )
        # Clock Info
        assert output.clock_info.clock == b"\x00\x00\x00\x00\x1a0)Z"
        assert output.clock_info.reset_count == 380665265
        assert output.clock_info.restart_count == 1378317304
        assert output.clock_info.safe is True
