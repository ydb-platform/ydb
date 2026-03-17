# pylint: disable=missing-module-docstring,missing-function-docstring
import mmh3

from helper import u32_to_s32


def test_mmh3_32_digest() -> None:
    hasher = mmh3.mmh3_32()
    hasher.update(b"")
    assert hasher.digest() == b"\x00\x00\x00\x00"

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello, world!")
    assert hasher.digest() == b"\xba\x4c\x88\x24"

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello,")
    hasher.update(b" world!")
    assert hasher.digest() == b"\xba\x4c\x88\x24"

    hasher = mmh3.mmh3_32(b"", 0x9747B28C)
    hasher.update(b"Hello,")
    hasher.update(b" world!")
    assert hasher.digest() == b"\xba\x4c\x88\x24"

    hasher = mmh3.mmh3_32(b"Hello,", 0x9747B28C)
    hasher.update(b" world!")
    assert hasher.digest() == b"\xba\x4c\x88\x24"

    hasher = mmh3.mmh3_32(b"Hello,", seed=0x9747B28C)
    hasher.update(b" world!")
    assert hasher.digest() == b"\xba\x4c\x88\x24"


def test_mmh3_32_sintdigest() -> None:
    hasher = mmh3.mmh3_32()
    hasher.update(b"foo")
    assert hasher.sintdigest() == -156908512

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    hasher = mmh3.mmh3_32()
    hasher.update(b"")
    assert hasher.sintdigest() == 0

    hasher = mmh3.mmh3_32(seed=1)
    hasher.update(b"")
    assert hasher.sintdigest() == 0x514E28B7

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43")
    hasher.update(b"\x65")
    assert hasher.sintdigest() == u32_to_s32(0x7E4A8634)

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43\x65\x87")
    assert hasher.sintdigest() == u32_to_s32(0xF55B516B)

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43")
    hasher.update(b"\x65\x87")
    assert hasher.sintdigest() == u32_to_s32(0xF55B516B)

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello, world!")
    assert hasher.sintdigest() == u32_to_s32(0x24884CBA)

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello,")
    hasher.update(b" world!")
    assert hasher.sintdigest() == u32_to_s32(0x24884CBA)

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"The quick brown fo")
    hasher.update(b"x jumps over the lazy dog")
    assert hasher.sintdigest() == u32_to_s32(0x2FA826CD)


def test_mmh3_32_uintdigest() -> None:
    hasher = mmh3.mmh3_32()
    hasher.update(b"foo")
    assert hasher.uintdigest() == 4138058784

    # Test vectors devised by Ian Boyd
    # https://stackoverflow.com/a/31929528
    hasher = mmh3.mmh3_32()
    hasher.update(b"")
    assert hasher.uintdigest() == 0

    hasher = mmh3.mmh3_32(seed=1)
    hasher.update(b"")
    assert hasher.uintdigest() == 0x514E28B7

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43")
    hasher.update(b"\x65")
    assert hasher.uintdigest() == 0x7E4A8634

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43\x65\x87")
    assert hasher.uintdigest() == 0xF55B516B

    hasher = mmh3.mmh3_32()
    hasher.update(b"\x21\x43")
    hasher.update(b"\x65\x87")
    assert hasher.uintdigest() == 0xF55B516B

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello, world!")
    assert hasher.uintdigest() == 0x24884CBA

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"Hello,")
    hasher.update(b" world!")
    assert hasher.uintdigest() == 0x24884CBA

    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"The quick brown fo")
    hasher.update(b"x jumps over the lazy dog")
    assert hasher.uintdigest() == 0x2FA826CD


def test_mmh3_32_copy() -> None:
    hasher = mmh3.mmh3_32(seed=0x9747B28C)
    hasher.update(b"The quick brown fox")

    hasher2 = hasher.copy()

    hasher.update(b" jumps over the lazy dog")
    assert hasher.uintdigest() == 0x2FA826CD

    hasher2.update(b" jumps over the lazy dog")
    assert hasher2.uintdigest() == 0x2FA826CD


def test_mmh3_x64_128_basic_ops() -> None:
    hasher = mmh3.mmh3_x64_128()
    assert hasher.digest_size == 16
    assert hasher.block_size == 32
    assert hasher.name == "mmh3_x64_128"


def test_mmh3_x64_128_digest() -> None:
    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"foo")
    assert hasher.digest() == b"aE\xf5\x01W\x86q\xe2\x87}\xba+\xe4\x87\xaf~"

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"

    hasher = mmh3.mmh3_x64_128(b"", 0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"

    hasher = mmh3.mmh3_x64_128(b"The quick brown ", seed=0x9747B28C)
    hasher.update(b"fox jumps over the lazy dog")
    assert hasher.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"

    hasher = mmh3.mmh3_x64_128(b"The quick brown ", 0x9747B28C)
    hasher.update(b"fox jumps over the lazy dog")
    assert hasher.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"


def test_mmh3_x64_128_sintdigest() -> None:
    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"")
    assert hasher.sintdigest() == 0

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.sintdigest() == -8943985938913228316176695348732677855

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox j")
    hasher.update(b"umps over the lazy dog")
    assert hasher.sintdigest() == -8943985938913228316176695348732677855


def test_mmh3_x64_128_uintdigest() -> None:
    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"")
    assert hasher.uintdigest() == 0

    hasher = mmh3.mmh3_x64_128(seed=1)
    hasher.update(b"")
    assert hasher.uintdigest() == 108177238965372658051732455265379769525

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"foo")
    assert hasher.uintdigest() == 168394135621993849475852668931176482145

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fo")
    hasher.update(b"o")
    assert hasher.uintdigest() == 168394135621993849475852668931176482145

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fooo")
    assert hasher.uintdigest() == 93757880664175803030724836966881520758

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fooofooo")
    assert hasher.uintdigest() == 211983152696995059280678248292944636041

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fooo")
    hasher.update(b"fooo")
    assert hasher.uintdigest() == 211983152696995059280678248292944636041

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fooofoooo")
    assert hasher.uintdigest() == 338423359992422647011971677127905553798

    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"fooo")
    hasher.update(b"foooo")
    assert hasher.uintdigest() == 338423359992422647011971677127905553798

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.uintdigest() == 331338380982025235147197912083035533601

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"T")
    hasher.update(b"he quick brown fox jumps over the lazy dog")
    assert hasher.uintdigest() == 331338380982025235147197912083035533601

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quic")  # 8 bytes
    hasher.update(b"k brown fox jumps over the lazy dog")
    assert hasher.uintdigest() == 331338380982025235147197912083035533601

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick")
    hasher.update(b" brown fox jumps over the lazy dog")
    assert hasher.uintdigest() == 331338380982025235147197912083035533601


def test_mmh3_x64_128_stupledigest() -> None:
    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"")
    assert hasher.stupledigest() == (0, 0)

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.stupledigest() == (8325606756057297185, -484854449282476315)

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quic")
    hasher.update(b"k brown fox jumps over the lazy dog")
    assert hasher.stupledigest() == (8325606756057297185, -484854449282476315)


def test_mmh3_x64_128_utupledigest() -> None:
    hasher = mmh3.mmh3_x64_128()
    hasher.update(b"")
    assert hasher.utupledigest() == (0, 0)

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.utupledigest() == (8325606756057297185, 17961889624427075301)

    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quic")
    hasher.update(b"k brown fox jumps over the lazy dog")
    assert hasher.utupledigest() == (8325606756057297185, 17961889624427075301)


def test_mmh3_x64_128_copy() -> None:
    hasher = mmh3.mmh3_x64_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox")

    hasher2 = hasher.copy()

    hasher.update(b" jumps over the lazy dog")
    assert hasher.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"

    hasher2.update(b" jumps over the lazy dog")
    assert hasher2.digest() == b"!1c\xd2;\x7f\x8as\xe5\x16\xc0~rsE\xf9"


def test_mmh3_x86_128_basic_ops() -> None:
    hasher = mmh3.mmh3_x86_128()
    assert hasher.digest_size == 16
    assert hasher.block_size == 32
    assert hasher.name == "mmh3_x86_128"


def test_mmh3_x86_128_digest() -> None:
    hasher = mmh3.mmh3_x86_128()
    hasher.update(b"")
    assert (
        hasher.digest()
        == b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    )

    hasher = mmh3.mmh3_x86_128(seed=1)
    hasher.update(b"")
    assert hasher.digest() == b"\xec\xad\xc4\x88\xb9\x01\xd2T\xb9\x01\xd2T\xb9\x01\xd2T"

    hasher = mmh3.mmh3_x86_128()
    hasher.update(b"foo")
    assert hasher.digest() == b"%\x1b|We%\xb6`e%\xb6`e%\xb6`"

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown")  # 15 bytes
    assert (
        hasher.digest() == b"2\xc3\n\xdaW\xc2\xcb\xa9\xc4\xbe\x12\xb9\xdc\x01\xe1\x8e"
    )

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown ")  # 16 bytes
    assert hasher.digest() == b"u\xb6\xf9\x07\xf5|\x93,\x0e\xf5\xf1\xf0k\x98\x83\x19"

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown")  # 15 bytes
    hasher.update(b" fox jumps over the lazy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown ")  # 16 bytes
    hasher.update(b"fox jumps over the lazy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox ju")
    hasher.update(b"mps ove")
    hasher.update(b"r the la")
    hasher.update(b"zy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(b"", 0x9747B28C)
    hasher.update(b"The quick brown fox ju")
    hasher.update(b"mps ove")
    hasher.update(b"r the la")
    hasher.update(b"zy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(b"The quick brown fox ju", seed=0x9747B28C)
    hasher.update(b"mps ove")
    hasher.update(b"r the la")
    hasher.update(b"zy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"

    hasher = mmh3.mmh3_x86_128(b"The quick brown fox ju", 0x9747B28C)
    hasher.update(b"mps ove")
    hasher.update(b"r the la")
    hasher.update(b"zy dog")
    assert hasher.digest() == b"^\xd5\xd4\x8aqa\xb8L\x9c:\xa7\x8e>y\xb6\xcd"


def test_mmh3_x86_128_sintdigest() -> None:
    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.sintdigest() == -66843170628920214366208380873156012706


def test_mmh3_x86_128_uintdigest() -> None:
    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.uintdigest() == 273439196292018249097166226558612198750


def test_mmh3_x86_128_stupledigest() -> None:
    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.stupledigest() == (5528275682885686622, -3623575540584727908)


def test_mmh3_x86_128_utupledigest() -> None:
    hasher = mmh3.mmh3_x86_128(seed=0x9747B28C)
    hasher.update(b"The quick brown fox jumps over the lazy dog")
    assert hasher.utupledigest() == (5528275682885686622, 14823168533124823708)
