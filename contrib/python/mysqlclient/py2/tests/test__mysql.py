import pytest
from MySQLdb import _mysql


def test_result_type():
    with pytest.raises(TypeError):
        _mysql.result(b"xyz")
