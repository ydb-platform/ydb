from __future__ import annotations

from pathlib import Path

import pytest

from nbformat import read
from nbformat.validator import ValidationError


def test_read_invalid_iowrapper(tmpdir):
    ipynb_filepath = tmpdir.join("empty.ipynb")
    Path(ipynb_filepath).write_text("{}", encoding="utf8")

    with pytest.raises(ValidationError) as excinfo, Path(ipynb_filepath).open(
        encoding="utf8"
    ) as fp:
        read(fp, as_version=4)
    assert "cells" in str(excinfo.value)


def test_read_invalid_filepath(tmpdir):
    ipynb_filepath = tmpdir.join("empty.ipynb")
    Path(ipynb_filepath).write_text("{}", encoding="utf8")

    with pytest.raises(ValidationError) as excinfo:
        read(str(ipynb_filepath), as_version=4)
    assert "cells" in str(excinfo.value)


def test_read_invalid_pathlikeobj(tmpdir):
    ipynb_filepath = tmpdir.join("empty.ipynb")
    Path(ipynb_filepath).write_text("{}", encoding="utf8")

    with pytest.raises(ValidationError) as excinfo:
        read(str(ipynb_filepath), as_version=4)
    assert "cells" in str(excinfo.value)


def test_read_invalid_str(tmpdir):
    with pytest.raises(OSError) as excinfo:
        read("not_exist_path", as_version=4)
    #assert "No such file or directory" in str(excinfo.value)


def test_read_invalid_type(tmpdir):
    with pytest.raises(OSError) as excinfo:
        read(123, as_version=4)
