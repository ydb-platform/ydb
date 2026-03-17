import pytest


@pytest.mark.parametrize("universal_newline_mode", [True, False])
def test_read_after_eof(universal_newline_mode):
    wfile = open('file', 'w')
    rfile = open('file', 'rU' if universal_newline_mode else 'r')
    assert rfile.read(10) == ''
    wfile.write('123')
    wfile.flush()
    # We are faced with the fact that fread in the implementation of glibc
    # doesn't cache EOF, however, musl follows the standard and do cache it.
    # Python3 is not affected by this issue.
    assert rfile.read(10) == '123'
