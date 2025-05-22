import pytest

from OpenSSL._util import exception_from_error_queue, lib


class TestErrors(object):
    """
    Tests for handling of certain OpenSSL error cases.
    """

    def test_exception_from_error_queue_nonexistent_reason(self):
        """
        :func:`exception_from_error_queue` raises ``ValueError`` when it
        encounters an OpenSSL error code which does not have a reason string.
        """
        lib.ERR_put_error(lib.ERR_LIB_EVP, 0, 1112, b"", 10)
        with pytest.raises(ValueError) as exc:
            exception_from_error_queue(ValueError)
        assert exc.value.args[0][0][2] == ""
