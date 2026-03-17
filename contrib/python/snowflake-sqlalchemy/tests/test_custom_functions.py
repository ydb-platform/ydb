#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import pytest
from sqlalchemy import func

from snowflake.sqlalchemy import snowdialect


def test_flatten_does_not_render_params():
    """This behavior is for backward compatibility.

    In previous version params were not rendered.
    In future this behavior will change.
    """
    flat = func.flatten("[1, 2]", outer=True)
    res = flat.compile(dialect=snowdialect.dialect())

    assert str(res) == "flatten(%(flatten_1)s)"


def test_flatten_emits_warning():
    expected_warning = "For backward compatibility params are not rendered."
    with pytest.warns(DeprecationWarning, match=expected_warning):
        func.flatten().compile(dialect=snowdialect.dialect())
