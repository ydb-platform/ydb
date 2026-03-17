# Copyright 2013 Donald Stufft and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import pytest

import nacl.encoding
import nacl.secret


KEY = b"1" * nacl.secret.SecretBox.KEY_SIZE
NONCE = b"1" * nacl.secret.SecretBox.NONCE_SIZE
TEXT = b"The quick brown fox jumps over the lazy dog"
VECTORS = [
    # Encoder, Ciphertext
    (
        nacl.encoding.RawEncoder,
        (b"111111111111111111111111\xfcU\xe2\x9f\xe6E\x92\xd7\x0eFM=x\x83\x8fj"
         b"} v\xd4\xf0\x1a1\xc0\x88Uk\x12\x02\x1cd\xfaOH\x13\xdc\x0e\x0e\xd7A"
         b"\x07\x0b.\x9f\x01\xbf\xe4\xd0s\xf1P\xd3\x0e\xaa\x9d\xb3\xf7\\\x0f"),
    ),
    (
        nacl.encoding.HexEncoder,
        (b"313131313131313131313131313131313131313131313131fc55e29fe64592d70e4"
         b"64d3d78838f6a7d2076d4f01a31c088556b12021c64fa4f4813dc0e0ed741070b2e"
         b"9f01bfe4d073f150d30eaa9db3f75c0f"),
    ),
    (
        nacl.encoding.Base16Encoder,
        (b"313131313131313131313131313131313131313131313131FC55E29FE64592D70E4"
         b"64D3D78838F6A7D2076D4F01A31C088556B12021C64FA4F4813DC0E0ED741070B2E"
         b"9F01BFE4D073F150D30EAA9DB3F75C0F"),
    ),
    (
        nacl.encoding.Base32Encoder,
        (b"GEYTCMJRGEYTCMJRGEYTCMJRGEYTCMJRGEYTCMP4KXRJ7ZSFSLLQ4RSNHV4IHD3KPUQ"
         b"HNVHQDIY4BCCVNMJAEHDE7JHUQE64BYHNOQIHBMXJ6AN74TIHH4KQ2MHKVHNT65OA6"
         b"==="),
    ),
    (
        nacl.encoding.Base64Encoder,
        (b"MTExMTExMTExMTExMTExMTExMTExMTEx/FXin+ZFktcORk09eIOPan0gdtTwGjHAiFV"
         b"rEgIcZPpPSBPcDg7XQQcLLp8Bv+TQc/FQ0w6qnbP3XA8="),
    ),
    (
        nacl.encoding.URLSafeBase64Encoder,
        (b"MTExMTExMTExMTExMTExMTExMTExMTEx_FXin-ZFktcORk09eIOPan0gdtTwGjHAiFV"
         b"rEgIcZPpPSBPcDg7XQQcLLp8Bv-TQc_FQ0w6qnbP3XA8="),
    ),
]


@pytest.mark.parametrize(("encoder", "ciphertext"), VECTORS)
def test_encoders(encoder, ciphertext):
    box = nacl.secret.SecretBox(KEY)

    test_ciphertext = box.encrypt(TEXT, NONCE, encoder=encoder)
    assert test_ciphertext == ciphertext

    test_plaintext = box.decrypt(test_ciphertext, encoder=encoder)
    assert test_plaintext == TEXT
