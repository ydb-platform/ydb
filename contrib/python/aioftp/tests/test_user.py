import pytest

import aioftp


def test_user_not_absolute_home():
    with pytest.raises(aioftp.errors.PathIsNotAbsolute):
        aioftp.User(home_path="foo")
