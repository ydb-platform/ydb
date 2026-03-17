import pytest

from _argon2_cffi_bindings._ffi_build import _get_target_platform


@pytest.mark.parametrize(
    ("arch_flags", "expected"),
    [
        (" -arch arm64", "arm64"),
        ("abc -arch  arm64  xyz", "arm64"),
        ("abc -arch  aRm64  xyz", "arm64"),
        ("nonsense ", "FOO"),
        ("", "FOO"),
    ],
)
def test_arch(arch_flags, expected):
    """
    _get_target_platform parses ARCHFLAGS and returns the default value if
    it doesn't find anything.
    """
    assert expected == _get_target_platform(arch_flags, "FOO")
