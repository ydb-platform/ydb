# Copyright 2016 Donald Stufft and individual contributors
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


from binascii import hexlify
from typing import List, Tuple

import pytest

from nacl.exceptions import UnavailableError
from nacl.hash import SIPHASHX_AVAILABLE, siphash24, siphashx24

HASHES = [
    b"\x31\x0e\x0e\xdd\x47\xdb\x6f\x72",
    b"\xfd\x67\xdc\x93\xc5\x39\xf8\x74",
    b"\x5a\x4f\xa9\xd9\x09\x80\x6c\x0d",
    b"\x2d\x7e\xfb\xd7\x96\x66\x67\x85",
    b"\xb7\x87\x71\x27\xe0\x94\x27\xcf",
    b"\x8d\xa6\x99\xcd\x64\x55\x76\x18",
    b"\xce\xe3\xfe\x58\x6e\x46\xc9\xcb",
    b"\x37\xd1\x01\x8b\xf5\x00\x02\xab",
    b"\x62\x24\x93\x9a\x79\xf5\xf5\x93",
    b"\xb0\xe4\xa9\x0b\xdf\x82\x00\x9e",
    b"\xf3\xb9\xdd\x94\xc5\xbb\x5d\x7a",
    b"\xa7\xad\x6b\x22\x46\x2f\xb3\xf4",
    b"\xfb\xe5\x0e\x86\xbc\x8f\x1e\x75",
    b"\x90\x3d\x84\xc0\x27\x56\xea\x14",
    b"\xee\xf2\x7a\x8e\x90\xca\x23\xf7",
    b"\xe5\x45\xbe\x49\x61\xca\x29\xa1",
    b"\xdb\x9b\xc2\x57\x7f\xcc\x2a\x3f",
    b"\x94\x47\xbe\x2c\xf5\xe9\x9a\x69",
    b"\x9c\xd3\x8d\x96\xf0\xb3\xc1\x4b",
    b"\xbd\x61\x79\xa7\x1d\xc9\x6d\xbb",
    b"\x98\xee\xa2\x1a\xf2\x5c\xd6\xbe",
    b"\xc7\x67\x3b\x2e\xb0\xcb\xf2\xd0",
    b"\x88\x3e\xa3\xe3\x95\x67\x53\x93",
    b"\xc8\xce\x5c\xcd\x8c\x03\x0c\xa8",
    b"\x94\xaf\x49\xf6\xc6\x50\xad\xb8",
    b"\xea\xb8\x85\x8a\xde\x92\xe1\xbc",
    b"\xf3\x15\xbb\x5b\xb8\x35\xd8\x17",
    b"\xad\xcf\x6b\x07\x63\x61\x2e\x2f",
    b"\xa5\xc9\x1d\xa7\xac\xaa\x4d\xde",
    b"\x71\x65\x95\x87\x66\x50\xa2\xa6",
    b"\x28\xef\x49\x5c\x53\xa3\x87\xad",
    b"\x42\xc3\x41\xd8\xfa\x92\xd8\x32",
    b"\xce\x7c\xf2\x72\x2f\x51\x27\x71",
    b"\xe3\x78\x59\xf9\x46\x23\xf3\xa7",
    b"\x38\x12\x05\xbb\x1a\xb0\xe0\x12",
    b"\xae\x97\xa1\x0f\xd4\x34\xe0\x15",
    b"\xb4\xa3\x15\x08\xbe\xff\x4d\x31",
    b"\x81\x39\x62\x29\xf0\x90\x79\x02",
    b"\x4d\x0c\xf4\x9e\xe5\xd4\xdc\xca",
    b"\x5c\x73\x33\x6a\x76\xd8\xbf\x9a",
    b"\xd0\xa7\x04\x53\x6b\xa9\x3e\x0e",
    b"\x92\x59\x58\xfc\xd6\x42\x0c\xad",
    b"\xa9\x15\xc2\x9b\xc8\x06\x73\x18",
    b"\x95\x2b\x79\xf3\xbc\x0a\xa6\xd4",
    b"\xf2\x1d\xf2\xe4\x1d\x45\x35\xf9",
    b"\x87\x57\x75\x19\x04\x8f\x53\xa9",
    b"\x10\xa5\x6c\xf5\xdf\xcd\x9a\xdb",
    b"\xeb\x75\x09\x5c\xcd\x98\x6c\xd0",
    b"\x51\xa9\xcb\x9e\xcb\xa3\x12\xe6",
    b"\x96\xaf\xad\xfc\x2c\xe6\x66\xc7",
    b"\x72\xfe\x52\x97\x5a\x43\x64\xee",
    b"\x5a\x16\x45\xb2\x76\xd5\x92\xa1",
    b"\xb2\x74\xcb\x8e\xbf\x87\x87\x0a",
    b"\x6f\x9b\xb4\x20\x3d\xe7\xb3\x81",
    b"\xea\xec\xb2\xa3\x0b\x22\xa8\x7f",
    b"\x99\x24\xa4\x3c\xc1\x31\x57\x24",
    b"\xbd\x83\x8d\x3a\xaf\xbf\x8d\xb7",
    b"\x0b\x1a\x2a\x32\x65\xd5\x1a\xea",
    b"\x13\x50\x79\xa3\x23\x1c\xe6\x60",
    b"\x93\x2b\x28\x46\xe4\xd7\x06\x66",
    b"\xe1\x91\x5f\x5c\xb1\xec\xa4\x6c",
    b"\xf3\x25\x96\x5c\xa1\x6d\x62\x9f",
    b"\x57\x5f\xf2\x8e\x60\x38\x1b\xe5",
    b"\x72\x45\x06\xeb\x4c\x32\x8a\x95",
]

MESG = (
    b"\x00\x01\x02\x03\x04\x05\x06\x07"
    b"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
    b"\x10\x11\x12\x13\x14\x15\x16\x17"
    b"\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
    b"\x20\x21\x22\x23\x24\x25\x26\x27"
    b"\x28\x29\x2a\x2b\x2c\x2d\x2e\x2f"
    b"\x30\x31\x32\x33\x34\x35\x36\x37"
    b"\x38\x39\x3a\x3b\x3c\x3d\x3e"
)

KEY = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

XHASHES = [
    b"a3817f04ba25a8e66df67214c7550293",
    b"da87c1d86b99af44347659119b22fc45",
    b"8177228da4a45dc7fca38bdef60affe4",
    b"9c70b60c5267a94e5f33b6b02985ed51",
    b"f88164c12d9c8faf7d0f6e7c7bcd5579",
    b"1368875980776f8854527a07690e9627",
    b"14eeca338b208613485ea0308fd7a15e",
    b"a1f1ebbed8dbc153c0b84aa61ff08239",
    b"3b62a9ba6258f5610f83e264f31497b4",
    b"264499060ad9baabc47f8b02bb6d71ed",
    b"00110dc378146956c95447d3f3d0fbba",
    b"0151c568386b6677a2b4dc6f81e5dc18",
    b"d626b266905ef35882634df68532c125",
    b"9869e247e9c08b10d029934fc4b952f7",
    b"31fcefac66d7de9c7ec7485fe4494902",
    b"5493e99933b0a8117e08ec0f97cfc3d9",
    b"6ee2a4ca67b054bbfd3315bf85230577",
    b"473d06e8738db89854c066c47ae47740",
    b"a426e5e423bf4885294da481feaef723",
    b"78017731cf65fab074d5208952512eb1",
    b"9e25fc833f2290733e9344a5e83839eb",
    b"568e495abe525a218a2214cd3e071d12",
    b"4a29b54552d16b9a469c10528eff0aae",
    b"c9d184ddd5a9f5e0cf8ce29a9abf691c",
    b"2db479ae78bd50d8882a8a178a6132ad",
    b"8ece5f042d5e447b5051b9eacb8d8f6f",
    b"9c0b53b4b3c307e87eaee08678141f66",
    b"abf248af69a6eae4bfd3eb2f129eeb94",
    b"0664da1668574b88b935f3027358aef4",
    b"aa4b9dc4bf337de90cd4fd3c467c6ab7",
    b"ea5c7f471faf6bde2b1ad7d4686d2287",
    b"2939b0183223fafc1723de4f52c43d35",
    b"7c3956ca5eeafc3e363e9d556546eb68",
    b"77c6077146f01c32b6b69d5f4ea9ffcf",
    b"37a6986cb8847edf0925f0f1309b54de",
    b"a705f0e69da9a8f907241a2e923c8cc8",
    b"3dc47d1f29c448461e9e76ed904f6711",
    b"0d62bf01e6fc0e1a0d3c4751c5d3692b",
    b"8c03468bca7c669ee4fd5e084bbee7b5",
    b"528a5bb93baf2c9c4473cce5d0d22bd9",
    b"df6a301e95c95dad97ae0cc8c6913bd8",
    b"801189902c857f39e73591285e70b6db",
    b"e617346ac9c231bb3650ae34ccca0c5b",
    b"27d93437efb721aa401821dcec5adf89",
    b"89237d9ded9c5e78d8b1c9b166cc7342",
    b"4a6d8091bf5e7d651189fa94a250b14c",
    b"0e33f96055e7ae893ffc0e3dcf492902",
    b"e61c432b720b19d18ec8d84bdc63151b",
    b"f7e5aef549f782cf379055a608269b16",
    b"438d030fd0b7a54fa837f2ad201a6403",
    b"a590d3ee4fbf04e3247e0d27f286423f",
    b"5fe2c1a172fe93c4b15cd37caef9f538",
    b"2c97325cbd06b36eb2133dd08b3a017c",
    b"92c814227a6bca949ff0659f002ad39e",
    b"dce850110bd8328cfbd50841d6911d87",
    b"67f14984c7da791248e32bb5922583da",
    b"1938f2cf72d54ee97e94166fa91d2a36",
    b"74481e9646ed49fe0f6224301604698e",
    b"57fca5de98a9d6d8006438d0583d8a1d",
    b"9fecde1cefdc1cbed4763674d9575359",
    b"e3040c00eb28f15366ca73cbd872e740",
    b"7697009a6a831dfecca91c5993670f7a",
    b"5853542321f567a005d547a4f04759bd",
    b"5150d1772f50834a503e069a973fbd7c",
]


def sip24_vectors() -> List[Tuple[bytes, bytes, bytes]]:
    """Generate test vectors using data from the reference implementation's
    test defined in  https://github.com/veorq/SipHash/blob/master/main.c

    The key, the messages sequence and the expected hashes are all coming
    from that file's definitions.
    """
    vectors = []
    for i, expected in enumerate(HASHES):
        mesg = MESG[0:i]
        vectors.append((mesg, KEY, expected))
    return vectors


def sipx24_vectors() -> List[Tuple[bytes, bytes, bytes]]:
    """Generate test vectors using data from libsodium's tests"""
    vectors = []
    for i, expected in enumerate(XHASHES):
        mesg = MESG[0:i]
        vectors.append((mesg, KEY, expected))
    return vectors


@pytest.mark.parametrize(("inp", "key", "expected"), sip24_vectors())
def test_siphash24(inp: bytes, key: bytes, expected: bytes):
    rs = siphash24(inp, key)
    assert rs == hexlify(expected)


@pytest.mark.skipif(
    not SIPHASHX_AVAILABLE, reason="Requires full build of libsodium"
)
@pytest.mark.parametrize(("inp", "key", "expected"), sipx24_vectors())
def test_siphashx24(inp: bytes, key: bytes, expected: bytes):
    rs = siphashx24(inp, key)
    assert rs == expected


@pytest.mark.parametrize(
    ("inp", "key", "expected"),
    [(b"\00", b"\x00\x01\x02\x03\x04\x05\x06\x07", b"")],
)
def test_siphash24_shortened_key(inp: bytes, key: bytes, expected: bytes):
    with pytest.raises(ValueError):
        siphash24(inp, key)


@pytest.mark.skipif(
    not SIPHASHX_AVAILABLE, reason="Requires full build of libsodium"
)
@pytest.mark.parametrize(
    ("inp", "key", "expected"),
    [(b"\00", b"\x00\x01\x02\x03\x04\x05\x06\x07", b"")],
)
def test_siphashx24_shortened_key(inp: bytes, key: bytes, expected: bytes):
    with pytest.raises(ValueError):
        siphashx24(inp, key)


@pytest.mark.skipif(
    SIPHASHX_AVAILABLE, reason="Requires minimal build of libsodium"
)
def test_siphashx24_unavailable():
    with pytest.raises(UnavailableError):
        siphashx24(b"", b"")
