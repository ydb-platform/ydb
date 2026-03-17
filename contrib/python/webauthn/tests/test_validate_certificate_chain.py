from unittest import TestCase
from datetime import datetime
from OpenSSL.crypto import X509Store

from webauthn.helpers.exceptions import InvalidCertificateChain
from webauthn.helpers.known_root_certs import (
    apple_webauthn_root_ca,
    globalsign_root_ca,
)
from webauthn.helpers.validate_certificate_chain import (
    validate_certificate_chain,
)

from .helpers.x509store import patch_validate_certificate_chain_x509store_getter

apple_x5c_certs = [
    # 2021-08-31 @ 23:02:07Z <-> 2021-09-03 @ 23:02:07Z
    bytes.fromhex(
        "30820243308201c9a0030201020206017ba3992221300a06082a8648ce3d0403023048311c301a06035504030c134170706c6520576562417574686e204341203131133011060355040a0c0a4170706c6520496e632e3113301106035504080c0a43616c69666f726e6961301e170d3231303833313233303230375a170d3231303930333233303230375a3081913149304706035504030c4062313066373138626335646437353838383661316438636662356238623633313732396634643765346261303639616230613939326331633038343738616639311a3018060355040b0c114141412043657274696669636174696f6e31133011060355040a0c0a4170706c6520496e632e3113301106035504080c0a43616c69666f726e69613059301306072a8648ce3d020106082a8648ce3d03010703420004d124b0e9ff8192723c9ee2fa4f8170d373e03286cf880aeec7008a14cdea64724963e05bb8c44a9f980ded12aa8a33795cf81d31e74116ced6f1f4c5eb0c358fa3553053300c0603551d130101ff04023000300e0603551d0f0101ff0404030204f0303306092a864886f76364080204263024a1220420e457e5bc292f1635210248ed2e776ba129c7cc469524a75356836caef2f058a0300a06082a8648ce3d0403020368003065023065c6e7075ddacb50879a8412904759013d0da78726408759a01f1994c1795a69c2c1d11306c2d1bc97be6141627b8677023100ab0b9e7d97ca2b603b1edb6e264c49bf1971380c2afa5d37f8c4ff5a5de6d457a19cb80c02b2edf94b0853e0482f8686"
    ),
    # 2020-03-18 @ 18:38:01Z <-> 2030-03-13 @ 00:00:00Z
    bytes.fromhex(
        "30820234308201baa003020102021056255395c7a7fb40ebe228d8260853b6300a06082a8648ce3d040303304b311f301d06035504030c164170706c6520576562417574686e20526f6f7420434131133011060355040a0c0a4170706c6520496e632e3113301106035504080c0a43616c69666f726e6961301e170d3230303331383138333830315a170d3330303331333030303030305a3048311c301a06035504030c134170706c6520576562417574686e204341203131133011060355040a0c0a4170706c6520496e632e3113301106035504080c0a43616c69666f726e69613076301006072a8648ce3d020106052b8104002203620004832e872f261491810225b9f5fcd6bb6378b5f55f3fcb045bc735993475fd549044df9bfe19211765c69a1dda050b38d45083401a434fb24d112d56c3e1cfbfcb9891fec0696081bef96cbc77c88dddaf46a5aee1dd515b5afaab93be9c0b2691a366306430120603551d130101ff040830060101ff020100301f0603551d2304183016801426d764d9c578c25a67d1a7de6b12d01b63f1c6d7301d0603551d0e04160414ebae82c4ffa1ac5b51d4cf24610500be63bd7788300e0603551d0f0101ff040403020106300a06082a8648ce3d0403030368003065023100dd8b1a3481a5fad9dbb4e7657b841e144c27b75b876a4186c2b1475750337227efe554457ef648950c632e5c483e70c102302c8a6044dc201fcfe59bc34d2930c1487851d960ed6a75f1eb4acabe38cd25b897d0c805bef0c7f78b07a571c6e80e07"
    ),
]


class TestValidateCertificateChain(TestCase):
    def setUp(self):
        # Setting the time to something that satisfies all these:
        # (Leaf) 20210831230207Z <-> 20210903230207Z <- Earliest expiration
        # (Int.) 20200318183801Z <-> 20300313000000Z
        # (Root) 20200318182132Z <-> 20450315000000Z
        self.x509store_time = datetime(2021, 9, 1, 0, 0, 0)

    @patch_validate_certificate_chain_x509store_getter
    def test_validates_certificate_chain(self, patched_x509store: X509Store) -> None:
        patched_x509store.set_time(self.x509store_time)

        try:
            validate_certificate_chain(
                x5c=apple_x5c_certs,
                pem_root_certs_bytes=[apple_webauthn_root_ca],
            )
        except Exception as err:
            print(err)
            self.fail("validate_certificate_chain failed when it should have succeeded")

    @patch_validate_certificate_chain_x509store_getter
    def test_throws_on_bad_root_cert(self, patched_x509store: X509Store) -> None:
        patched_x509store.set_time(self.x509store_time)

        with self.assertRaises(InvalidCertificateChain):
            validate_certificate_chain(
                x5c=apple_x5c_certs,
                # An obviously invalid root cert for these x5c certs
                pem_root_certs_bytes=[globalsign_root_ca],
            )

    def test_passes_on_no_root_certs(self):
        try:
            validate_certificate_chain(
                x5c=apple_x5c_certs,
            )
        except Exception as err:
            print(err)
            self.fail("validate_certificate_chain failed when it should have succeeded")

    def test_passes_on_empty_root_certs_array(self):
        try:
            validate_certificate_chain(
                x5c=apple_x5c_certs,
                pem_root_certs_bytes=[],
            )
        except Exception as err:
            print(err)
            self.fail("validate_certificate_chain failed when it should have succeeded")
