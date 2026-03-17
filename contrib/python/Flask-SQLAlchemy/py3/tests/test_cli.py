from __future__ import annotations

import typing as t

import pytest

from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.cli import add_models_to_shell


@pytest.mark.usefixtures("app_ctx")
def test_shell_context(db: SQLAlchemy, Todo: t.Any) -> None:
    context = add_models_to_shell()
    assert context["db"] is db
    assert context["Todo"] is Todo
