import pytest

from pytest_flask._internal import deprecated


class TestInternal:
    def test_deprecation_decorator(self, appdir):
        @deprecated(reason="testing decorator")
        def deprecated_fun():
            pass

        with pytest.warns(DeprecationWarning) as record:
            deprecated_fun()
        assert len(record) == 1
        assert record[0].message.args[0] == "testing decorator"
