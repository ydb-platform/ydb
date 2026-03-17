# pylint: disable=missing-module-docstring,missing-function-docstring
import sys

import mmh3

from helper import u32_to_s32


def test_hash() -> None:
    assert mmh3.hash("foo") == -156908512

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    assert mmh3.hash(b"", seed=0) == 0
    assert mmh3.hash(b"", seed=1) == 0x514E28B7
    assert mmh3.hash(b"", seed=0xFFFFFFFF) == u32_to_s32(0x81F16F39)
    assert mmh3.hash(b"\x21\x43\x65\x87", 0) == u32_to_s32(0xF55B516B)
    assert mmh3.hash(b"\x21\x43\x65\x87", 0x5082EDEE) == u32_to_s32(0x2362F9DE)
    assert mmh3.hash(b"\x21\x43\x65", 0) == u32_to_s32(0x7E4A8634)
    assert mmh3.hash(b"\x21\x43", 0) == u32_to_s32(0xA0F7B07A)
    assert mmh3.hash(b"\x21", 0) == u32_to_s32(0x72661CF4)
    assert mmh3.hash(b"\xff\xff\xff\xff", 0) == u32_to_s32(0x76293B50)
    assert mmh3.hash(b"\x00\x00\x00\x00", 0) == u32_to_s32(0x2362F9DE)
    assert mmh3.hash(b"\x00\x00\x00", 0) == u32_to_s32(0x85F0B427)
    assert mmh3.hash(b"\x00\x00", 0) == u32_to_s32(0x30F4C306)
    assert mmh3.hash(b"\x00", 0) == u32_to_s32(0x514E28B7)

    assert mmh3.hash("aaaa", 0x9747B28C) == u32_to_s32(0x5A97808A)
    assert mmh3.hash("aaa", 0x9747B28C) == u32_to_s32(0x283E0130)
    assert mmh3.hash("aa", 0x9747B28C) == u32_to_s32(0x5D211726)
    assert mmh3.hash("a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.hash("abcd", 0x9747B28C) == u32_to_s32(0xF0478627)
    assert mmh3.hash("abc", 0x9747B28C) == u32_to_s32(0xC84A62DD)
    assert mmh3.hash("ab", 0x9747B28C) == u32_to_s32(0x74875592)
    assert mmh3.hash("a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.hash("Hello, world!", 0x9747B28C) == u32_to_s32(0x24884CBA)

    assert mmh3.hash("ππππππππ".encode("utf-8"), 0x9747B28C) == u32_to_s32(0xD58063C1)

    assert mmh3.hash("a" * 256, 0x9747B28C) == u32_to_s32(0x37405BDC)

    assert mmh3.hash("abc", 0) == u32_to_s32(0xB3DD93FA)
    assert mmh3.hash(
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0
    ) == u32_to_s32(0xEE925B90)

    assert mmh3.hash(
        "The quick brown fox jumps over the lazy dog", 0x9747B28C
    ) == u32_to_s32(0x2FA826CD)


def test_hash_unsigned() -> None:
    assert mmh3.hash("foo", signed=False) == 4138058784

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    assert mmh3.hash(b"", seed=0, signed=False) == 0
    assert mmh3.hash(b"", seed=1, signed=False) == 0x514E28B7
    assert mmh3.hash(b"", seed=0xFFFFFFFF, signed=False) == 0x81F16F39
    assert mmh3.hash(b"\x21\x43\x65\x87", 0, signed=False) == 0xF55B516B
    assert mmh3.hash(b"\x21\x43\x65\x87", 0x5082EDEE, signed=False) == 0x2362F9DE
    assert mmh3.hash(b"\x21\x43\x65", 0, signed=False) == 0x7E4A8634
    assert mmh3.hash(b"\x21\x43", 0, signed=False) == 0xA0F7B07A
    assert mmh3.hash(b"\x21", 0, signed=False) == 0x72661CF4
    assert mmh3.hash(b"\xff\xff\xff\xff", 0, signed=False) == 0x76293B50
    assert mmh3.hash(b"\x00\x00\x00\x00", 0, signed=False) == 0x2362F9DE
    assert mmh3.hash(b"\x00\x00\x00", 0, signed=False) == 0x85F0B427
    assert mmh3.hash(b"\x00\x00", 0, signed=False) == 0x30F4C306
    assert mmh3.hash(b"\x00", 0, signed=False) == 0x514E28B7

    assert mmh3.hash("aaaa", 0x9747B28C, signed=False) == 0x5A97808A
    assert mmh3.hash("aaa", 0x9747B28C, signed=False) == 0x283E0130
    assert mmh3.hash("aa", 0x9747B28C, signed=False) == 0x5D211726
    assert mmh3.hash("a", 0x9747B28C, signed=False) == 0x7FA09EA6

    assert mmh3.hash("abcd", 0x9747B28C, signed=False) == 0xF0478627
    assert mmh3.hash("abc", 0x9747B28C, signed=False) == 0xC84A62DD
    assert mmh3.hash("ab", 0x9747B28C, signed=False) == 0x74875592
    assert mmh3.hash("a", 0x9747B28C, signed=False) == 0x7FA09EA6

    assert mmh3.hash("Hello, world!", 0x9747B28C, signed=False) == 0x24884CBA

    assert mmh3.hash("ππππππππ".encode("utf-8"), 0x9747B28C, signed=False) == 0xD58063C1

    assert mmh3.hash("a" * 256, 0x9747B28C, signed=False) == 0x37405BDC

    assert mmh3.hash("abc", 0, signed=False) == 0xB3DD93FA
    assert (
        mmh3.hash(
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0, signed=False
        )
        == 0xEE925B90
    )

    assert (
        mmh3.hash(
            "The quick brown fox jumps over the lazy dog", 0x9747B28C, signed=False
        )
        == 0x2FA826CD
    )

    assert (
        mmh3.hash(
            "The quick brown fox jumps over the lazy dog", 0x9747B28C, signed=False
        )
        == 0x2FA826CD
    )


def test_hash2() -> None:
    assert mmh3.hash("foo") == -156908512

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    assert mmh3.hash(b"", seed=0) == 0
    assert mmh3.hash(b"", seed=1) == 0x514E28B7
    assert mmh3.hash(b"", seed=0xFFFFFFFF) == u32_to_s32(0x81F16F39)
    assert mmh3.hash(b"\x21\x43\x65\x87", 0) == u32_to_s32(0xF55B516B)
    assert mmh3.hash(b"\x21\x43\x65\x87", 0x5082EDEE) == u32_to_s32(0x2362F9DE)
    assert mmh3.hash(b"\x21\x43\x65", 0) == u32_to_s32(0x7E4A8634)
    assert mmh3.hash(b"\x21\x43", 0) == u32_to_s32(0xA0F7B07A)
    assert mmh3.hash(b"\x21", 0) == u32_to_s32(0x72661CF4)
    assert mmh3.hash(b"\xff\xff\xff\xff", 0) == u32_to_s32(0x76293B50)
    assert mmh3.hash(b"\x00\x00\x00\x00", 0) == u32_to_s32(0x2362F9DE)
    assert mmh3.hash(b"\x00\x00\x00", 0) == u32_to_s32(0x85F0B427)
    assert mmh3.hash(b"\x00\x00", 0) == u32_to_s32(0x30F4C306)
    assert mmh3.hash(b"\x00", 0) == u32_to_s32(0x514E28B7)

    assert mmh3.hash("aaaa", 0x9747B28C) == u32_to_s32(0x5A97808A)
    assert mmh3.hash("aaa", 0x9747B28C) == u32_to_s32(0x283E0130)
    assert mmh3.hash("aa", 0x9747B28C) == u32_to_s32(0x5D211726)
    assert mmh3.hash("a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.hash("abcd", 0x9747B28C) == u32_to_s32(0xF0478627)
    assert mmh3.hash("abc", 0x9747B28C) == u32_to_s32(0xC84A62DD)
    assert mmh3.hash("ab", 0x9747B28C) == u32_to_s32(0x74875592)
    assert mmh3.hash("a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.hash("Hello, world!", 0x9747B28C) == u32_to_s32(0x24884CBA)

    assert mmh3.hash("ππππππππ".encode("utf-8"), 0x9747B28C) == u32_to_s32(0xD58063C1)

    assert mmh3.hash("a" * 256, 0x9747B28C) == u32_to_s32(0x37405BDC)

    assert mmh3.hash("abc", 0) == u32_to_s32(0xB3DD93FA)
    assert mmh3.hash(
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0
    ) == u32_to_s32(0xEE925B90)

    assert mmh3.hash(
        "The quick brown fox jumps over the lazy dog", 0x9747B28C
    ) == u32_to_s32(0x2FA826CD)


def test_hash_from_buffer() -> None:
    mview = memoryview("foo".encode("utf8"))
    assert mmh3.hash_from_buffer(mview) == -156908512
    assert mmh3.hash_from_buffer(mview, signed=False) == 4138058784


def test_hash_bytes() -> None:
    assert mmh3.hash_bytes("foo") == b"aE\xf5\x01W\x86q\xe2\x87}\xba+\xe4\x87\xaf~"
    assert (
        mmh3.hash_bytes("foo", 0, True)
        == b"aE\xf5\x01W\x86q\xe2\x87}\xba+\xe4\x87\xaf~"
    )

    # Test vectors from https://github.com/PeterScott/murmur3/blob/master/test.c
    assert mmh3.hash_bytes("Hello, world!", 123, x64arch=False) == (
        0x9E37C886A41621625A1AACD761C9129E
    ).to_bytes(16, "little")
    assert mmh3.hash_bytes("", 123, x64arch=False) == (
        0x26F3E79926F3E79926F3E799FEDC5245
    ).to_bytes(16, "little")


def test_hash64() -> None:
    assert mmh3.hash64("foo") == (-2129773440516405919, 9128664383759220103)
    assert mmh3.hash64("foo", signed=False) == (
        16316970633193145697,
        9128664383759220103,
    )

    assert mmh3.hash64("The quick brown fox jumps over the lazy dog", 0x9747B28C) == (
        8325606756057297185,
        -484854449282476315,
    )
    assert mmh3.hash64(
        "The quick brown fox jumps over the lazy dog", 0x9747B28C, signed=False
    ) == (
        8325606756057297185,
        17961889624427075301,
    )
    assert mmh3.hash64("foo", signed=False, x64arch=True) == (
        16316970633193145697,
        9128664383759220103,
    )

    # Test vectors from https://github.com/PeterScott/murmur3/blob/master/test.c
    assert mmh3.hash64("Hello, world!", 123, signed=False, x64arch=False) == (
        0x5A1AACD761C9129E,
        0x9E37C886A4162162,
    )

    assert mmh3.hash64("", 123, False, False) == (
        0x26F3E799FEDC5245,
        0x26F3E79926F3E799,
    )


def test_hash128() -> None:
    assert mmh3.hash128("foo") == 168394135621993849475852668931176482145
    assert mmh3.hash128("foo", 42) == 215966891540331383248189432718888555506
    assert (
        mmh3.hash128("foo", 42, signed=False) == 215966891540331383248189432718888555506
    )
    assert (
        mmh3.hash128("foo", 42, signed=True) == -124315475380607080215185174712879655950
    )
    # Test vectors from https://github.com/PeterScott/murmur3/blob/master/test.c
    assert (
        mmh3.hash128("Hello, world!", 123, signed=False, x64arch=False)
        == 0x9E37C886A41621625A1AACD761C9129E
    )
    assert mmh3.hash128("", 123, False, False) == 0x26F3E79926F3E79926F3E799FEDC5245


def test_mmh3_32_digest() -> None:
    assert mmh3.mmh3_32_digest(b"") == b"\0\0\0\0"
    assert mmh3.mmh3_32_digest(b"", 0) == b"\0\0\0\0"
    assert mmh3.mmh3_32_digest(b"\x21\x43\x65\x87", 0) == (0xF55B516B).to_bytes(
        4, "little"
    )
    assert mmh3.mmh3_32_digest(b"\x21\x43\x65\x87", u32_to_s32(0x5082EDEE)) == (
        0x2362F9DE
    ).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\x21\x43\x65", 0) == (0x7E4A8634).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\x21\x43", 0) == (0xA0F7B07A).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\x21", 0) == (0x72661CF4).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\xff\xff\xff\xff", 0) == (0x76293B50).to_bytes(
        4, "little"
    )
    assert mmh3.mmh3_32_digest(b"\x00\x00\x00\x00", 0) == (0x2362F9DE).to_bytes(
        4, "little"
    )
    assert mmh3.mmh3_32_digest(b"\x00\x00\x00", 0) == (0x85F0B427).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\x00\x00", 0) == (0x30F4C306).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"\x00", 0) == (0x514E28B7).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(b"aaaa", 0x9747B28C) == (0x5A97808A).to_bytes(
        4, "little"
    )
    assert mmh3.mmh3_32_digest(b"aaa", 0x9747B28C) == (0x283E0130).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"aa", 0x9747B28C) == (0x5D211726).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"a", 0x9747B28C) == (0x7FA09EA6).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(b"abcd", 0x9747B28C) == (0xF0478627).to_bytes(
        4, "little"
    )
    assert mmh3.mmh3_32_digest(b"abc", 0x9747B28C) == (0xC84A62DD).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"ab", 0x9747B28C) == (0x74875592).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(b"a", 0x9747B28C) == (0x7FA09EA6).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(b"Hello, world!", 0x9747B28C) == (0x24884CBA).to_bytes(
        4, "little"
    )

    assert mmh3.mmh3_32_digest("ππππππππ".encode("utf-8"), 0x9747B28C) == (
        0xD58063C1
    ).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(b"a" * 256, 0x9747B28C) == (0x37405BDC).to_bytes(
        4, "little"
    )

    assert mmh3.mmh3_32_digest(b"abc", 0) == (0xB3DD93FA).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(
        b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0
    ) == (0xEE925B90).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(
        b"The quick brown fox jumps over the lazy dog", 0x9747B28C
    ) == (0x2FA826CD).to_bytes(4, "little")

    assert mmh3.mmh3_32_digest(bytearray(b"aaaa"), 0x9747B28C) == (0x5A97808A).to_bytes(
        4, "little"
    )
    v = memoryview(b"aaaa")
    assert mmh3.mmh3_32_digest(v, 0x9747B28C) == (0x5A97808A).to_bytes(4, "little")
    assert mmh3.mmh3_32_digest(v[1:3], 0x9747B28C) == (0x5D211726).to_bytes(4, "little")


def test_mmh3_sintdigest() -> None:
    assert mmh3.mmh3_32_sintdigest(b"foo") == -156908512
    assert mmh3.mmh3_32_sintdigest(bytearray(b"foo")) == -156908512
    assert mmh3.mmh3_32_sintdigest(memoryview(b"foobar")[0:3]) == -156908512

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    assert mmh3.mmh3_32_sintdigest(b"", 0) == 0
    assert mmh3.mmh3_32_sintdigest(b"", 1) == 0x514E28B7
    assert mmh3.mmh3_32_sintdigest(b"", 0xFFFFFFFF) == u32_to_s32(0x81F16F39)
    assert mmh3.mmh3_32_sintdigest(b"\x21\x43\x65\x87", 0) == u32_to_s32(0xF55B516B)
    assert mmh3.mmh3_32_sintdigest(
        b"\x21\x43\x65\x87", u32_to_s32(0x5082EDEE)
    ) == u32_to_s32(0x2362F9DE)
    assert mmh3.mmh3_32_sintdigest(b"\x21\x43\x65", 0) == u32_to_s32(0x7E4A8634)
    assert mmh3.mmh3_32_sintdigest(b"\x21\x43", 0) == u32_to_s32(0xA0F7B07A)
    assert mmh3.mmh3_32_sintdigest(b"\x21", 0) == u32_to_s32(0x72661CF4)
    assert mmh3.mmh3_32_sintdigest(b"\xff\xff\xff\xff", 0) == u32_to_s32(0x76293B50)
    assert mmh3.mmh3_32_sintdigest(b"\x00\x00\x00\x00", 0) == u32_to_s32(0x2362F9DE)
    assert mmh3.mmh3_32_sintdigest(b"\x00\x00\x00", 0) == u32_to_s32(0x85F0B427)
    assert mmh3.mmh3_32_sintdigest(b"\x00\x00", 0) == u32_to_s32(0x30F4C306)
    assert mmh3.mmh3_32_sintdigest(b"\x00", 0) == u32_to_s32(0x514E28B7)

    assert mmh3.mmh3_32_sintdigest(b"aaaa", 0x9747B28C) == u32_to_s32(0x5A97808A)
    assert mmh3.mmh3_32_sintdigest(b"aaa", 0x9747B28C) == u32_to_s32(0x283E0130)
    assert mmh3.mmh3_32_sintdigest(b"aa", 0x9747B28C) == u32_to_s32(0x5D211726)
    assert mmh3.mmh3_32_sintdigest(b"a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.mmh3_32_sintdigest(b"abcd", 0x9747B28C) == u32_to_s32(0xF0478627)
    assert mmh3.mmh3_32_sintdigest(b"abc", 0x9747B28C) == u32_to_s32(0xC84A62DD)
    assert mmh3.mmh3_32_sintdigest(b"ab", 0x9747B28C) == u32_to_s32(0x74875592)
    assert mmh3.mmh3_32_sintdigest(b"a", 0x9747B28C) == u32_to_s32(0x7FA09EA6)

    assert mmh3.mmh3_32_sintdigest(b"Hello, world!", 0x9747B28C) == u32_to_s32(
        0x24884CBA
    )

    assert mmh3.mmh3_32_sintdigest(
        "ππππππππ".encode("utf-8"), 0x9747B28C
    ) == u32_to_s32(0xD58063C1)

    assert mmh3.mmh3_32_sintdigest(b"a" * 256, 0x9747B28C) == u32_to_s32(0x37405BDC)

    assert mmh3.mmh3_32_sintdigest(b"abc", 0) == u32_to_s32(0xB3DD93FA)
    assert mmh3.mmh3_32_sintdigest(
        b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0
    ) == u32_to_s32(0xEE925B90)

    assert mmh3.mmh3_32_sintdigest(
        b"The quick brown fox jumps over the lazy dog", 0x9747B28C
    ) == u32_to_s32(0x2FA826CD)


def test_mmh3_uintdigest() -> None:
    assert mmh3.mmh3_32_uintdigest(b"foo") == 4138058784
    assert mmh3.mmh3_32_uintdigest(bytearray(b"foo")) == 4138058784
    assert mmh3.mmh3_32_uintdigest(memoryview(b"foobar")[0:3]) == 4138058784

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    assert mmh3.mmh3_32_uintdigest(b"") == 0
    assert mmh3.mmh3_32_uintdigest(b"", 0) == 0
    assert mmh3.mmh3_32_uintdigest(b"", 1) == 0x514E28B7
    assert mmh3.mmh3_32_uintdigest(b"", 0xFFFFFFFF) == 0x81F16F39
    assert mmh3.mmh3_32_uintdigest(b"\x21\x43\x65\x87", 0) == 0xF55B516B
    assert mmh3.mmh3_32_uintdigest(b"\x21\x43\x65\x87", 0x5082EDEE) == 0x2362F9DE
    assert mmh3.mmh3_32_uintdigest(b"\x21\x43\x65", 0) == 0x7E4A8634
    assert mmh3.mmh3_32_uintdigest(b"\x21\x43", 0) == 0xA0F7B07A
    assert mmh3.mmh3_32_uintdigest(b"\x21", 0) == 0x72661CF4
    assert mmh3.mmh3_32_uintdigest(b"\xff\xff\xff\xff", 0) == 0x76293B50
    assert mmh3.mmh3_32_uintdigest(b"\x00\x00\x00\x00", 0) == 0x2362F9DE
    assert mmh3.mmh3_32_uintdigest(b"\x00\x00\x00", 0) == 0x85F0B427
    assert mmh3.mmh3_32_uintdigest(b"\x00\x00", 0) == 0x30F4C306
    assert mmh3.mmh3_32_uintdigest(b"\x00", 0) == 0x514E28B7

    assert mmh3.mmh3_32_uintdigest(b"aaaa", 0x9747B28C) == 0x5A97808A
    assert mmh3.mmh3_32_uintdigest(b"aaa", 0x9747B28C) == 0x283E0130
    assert mmh3.mmh3_32_uintdigest(b"aa", 0x9747B28C) == 0x5D211726
    assert mmh3.mmh3_32_uintdigest(b"a", 0x9747B28C) == 0x7FA09EA6

    assert mmh3.mmh3_32_uintdigest(b"abcd", 0x9747B28C) == 0xF0478627
    assert mmh3.mmh3_32_uintdigest(b"abc", 0x9747B28C) == 0xC84A62DD
    assert mmh3.mmh3_32_uintdigest(b"ab", 0x9747B28C) == 0x74875592
    assert mmh3.mmh3_32_uintdigest(b"a", 0x9747B28C) == 0x7FA09EA6

    assert mmh3.mmh3_32_uintdigest(b"Hello, world!", 0x9747B28C) == 0x24884CBA

    assert mmh3.mmh3_32_uintdigest("ππππππππ".encode("utf-8"), 0x9747B28C) == 0xD58063C1

    assert mmh3.mmh3_32_uintdigest(b"a" * 256, 0x9747B28C) == 0x37405BDC

    assert mmh3.mmh3_32_uintdigest(b"abc", 0) == 0xB3DD93FA
    assert (
        mmh3.mmh3_32_uintdigest(
            b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0
        )
        == 0xEE925B90
    )

    assert (
        mmh3.mmh3_32_uintdigest(
            b"The quick brown fox jumps over the lazy dog", 0x9747B28C
        )
        == 0x2FA826CD
    )

    assert (
        mmh3.mmh3_32_uintdigest(
            b"The quick brown fox jumps over the lazy dog", 0x9747B28C
        )
        == 0x2FA826CD
    )


def test_mmh3_x64_128_digest() -> None:
    assert (
        mmh3.mmh3_x64_128_digest(b"foo")
        == b"aE\xf5\x01W\x86q\xe2\x87}\xba+\xe4\x87\xaf~"
    )

    assert (
        mmh3.mmh3_x64_128_digest(
            b"The quick brown fox jumps over the lazy dog", 0x9747B28C
        )
        == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"
    )

    v = bytearray(b"bar boo bar")
    mv = memoryview(v)
    v[4] = ord("f")

    assert (
        mmh3.mmh3_x64_128_digest(mv[4:7])
        == b"aE\xf5\x01W\x86q\xe2\x87}\xba+\xe4\x87\xaf~"
    )


def test_mmh3_x64_128_sintdigest() -> None:
    assert mmh3.mmh3_x64_128_sintdigest(b"") == 0

    assert (
        mmh3.mmh3_x64_128_sintdigest(
            b"The quick brown fox jumps over the lazy dog", 0x9747B28C
        )
        == -8943985938913228316176695348732677855
    )


def test_mmh3_x64_128_uintdigest() -> None:
    assert mmh3.mmh3_x64_128_uintdigest(b"") == 0

    assert (
        mmh3.mmh3_x64_128_uintdigest(b"foo", 42)
        == 215966891540331383248189432718888555506
    )


def test_mmh3_x64_128_stupledigest() -> None:
    assert mmh3.mmh3_x64_128_stupledigest(b"") == (0, 0)

    assert mmh3.mmh3_x64_128_stupledigest(
        memoryview(b"The quick brown fox jumps over the lazy dog"), 0x9747B28C
    ) == (
        8325606756057297185,
        -484854449282476315,
    )


def test_mmh3_x64_128_utupledigest() -> None:
    assert mmh3.mmh3_x64_128_utupledigest(b"") == (0, 0)

    assert mmh3.mmh3_x64_128_utupledigest(memoryview(b"foo")) == (
        16316970633193145697,
        9128664383759220103,
    )


def test_mmh3_x86_128_digest() -> None:
    assert mmh3.mmh3_x86_128_digest(b"", 123) == (
        0x26F3E79926F3E79926F3E799FEDC5245
    ).to_bytes(16, "little")

    assert mmh3.mmh3_x86_128_digest(b"Hello, world!", 123) == (
        0x9E37C886A41621625A1AACD761C9129E
    ).to_bytes(16, "little")

    assert mmh3.mmh3_x86_128_digest(bytearray(b"Hello, world!"), 123) == (
        0x9E37C886A41621625A1AACD761C9129E
    ).to_bytes(16, "little")

    v = bytearray(b"hello, world!!!")
    mv = memoryview(v)
    v[0] = ord("H")

    assert mmh3.mmh3_x86_128_digest(mv[0:13], 123) == (
        0x9E37C886A41621625A1AACD761C9129E
    ).to_bytes(16, "little")


def test_mmh3_x86_128_sintdigest() -> None:
    assert mmh3.mmh3_x64_128_sintdigest(b"") == 0

    assert (
        mmh3.mmh3_x64_128_sintdigest(
            b"The quick brown fox jumps over the lazy dog", 0x9747B28C
        )
        == -8943985938913228316176695348732677855
    )


def test_mmh3_x86_128_uintdigest() -> None:
    assert mmh3.mmh3_x64_128_uintdigest(b"", 0) == 0

    # Test vector from https://github.com/PeterScott/murmur3/blob/master/test.c
    assert (
        mmh3.mmh3_x86_128_uintdigest(b"Hello, world!", 123)
        == 0x9E37C886A41621625A1AACD761C9129E
    )


def test_mmh3_x86_128_stupledigest() -> None:
    assert mmh3.mmh3_x86_128_stupledigest(b"", 0) == (0, 0)

    assert mmh3.mmh3_x86_128_stupledigest(
        memoryview(b"The quick brown fox jumps over the lazy dog"), 0x9747B28C
    ) == (
        5528275682885686622,
        -3623575540584727908,
    )


def test_mmh3_x86_128_utupledigest() -> None:
    assert mmh3.mmh3_x86_128_utupledigest(b"", 0) == (0, 0)

    # Test vector from https://github.com/PeterScott/murmur3/blob/master/test.c
    assert mmh3.mmh3_x86_128_utupledigest(memoryview(b"Hello, world!"), 123) == (
        0x5A1AACD761C9129E,
        0x9E37C886A4162162,
    )


def test_64bit() -> None:
    if sys.maxsize < (1 << 32):  # Skip this test under 32-bit environments
        return
    a = bytes(2**32 + 1)
    assert mmh3.hash(a) == -1710109261
    assert (
        mmh3.hash_bytes(a) == b"\x821\x93\x0c\xe7\xa8\x02\x9d\xe5 \xa6\xf9\xeb8\xd6\x0e"
    )


# from hex string "0xff00de" to integer
def hex_to_int(hex_str: str) -> int:
    return int(hex_str, 16)
