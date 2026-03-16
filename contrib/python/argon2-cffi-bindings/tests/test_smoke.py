# SPDX-License-Identifier: MIT

"""
Since this package doesn't do anything beyond providing bindings, all we can
do is trying to ensure that those bindings are functional.
"""


def test_smoke():
    """
    lib and ffi can be imported and looks OK.
    """
    from _argon2_cffi_bindings import ffi, lib

    assert repr(ffi).startswith("<_cffi_backend.FFI object at")
    assert repr(lib).startswith("<Lib object for")

    assert 19 == lib.ARGON2_VERSION_NUMBER
    assert 42 == lib.argon2_encodedlen(1, 2, 3, 4, 5, lib.Argon2_id)
