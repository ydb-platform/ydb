#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import re
import uuid

from snowflake.connector import errors


def test_detecting_duplicate_detail_insertion():
    sfqid = str(uuid.uuid4())
    sqlstate = "24000"
    errno = 123456
    msg = "Some error happened"
    expected_msg = re.compile(rf"{errno} \({sqlstate}\): {sfqid}: {msg}")
    original_ex = errors.ProgrammingError(
        sqlstate=sqlstate,
        sfqid=sfqid,
        errno=errno,
        msg=msg,
    )
    # Test whether regular exception confirms to what we expect to see
    assert expected_msg.fullmatch(original_ex.msg)

    # Test whether exception with flag confirms to what we expect to see
    assert errors.ProgrammingError(
        msg=original_ex.msg,
        done_format_msg=True,
    )
    # Test whether exception with auto detection confirms to what we expect to see
    assert errors.ProgrammingError(
        msg=original_ex.msg,
    )


def test_args():
    assert errors.Error("msg").args == ("msg",)
