from __future__ import annotations

import typing as t

import pytest
import sqlalchemy as sa
from flask import Flask

from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.track_modifications import before_models_committed
from flask_sqlalchemy.track_modifications import models_committed

pytest.importorskip("blinker")


@pytest.mark.usefixtures("app_ctx")
def test_track_modifications(app: Flask) -> None:
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True
    db = SQLAlchemy(app)

    class Example(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        data = sa.Column(sa.String)

    db.create_all()
    before: list[tuple[t.Any, str]] = []
    after: list[tuple[t.Any, str]] = []

    def before_commit(sender: Flask, changes: list[tuple[t.Any, str]]) -> None:
        nonlocal before
        before = changes

    def after_commit(sender: Flask, changes: list[tuple[t.Any, str]]) -> None:
        nonlocal after
        after = changes

    connect_before = before_models_committed.connected_to(before_commit, app)
    connect_after = models_committed.connected_to(after_commit, app)

    with connect_before, connect_after:
        item = Example()

        db.session.add(item)
        assert not before
        assert not after

        db.session.commit()
        assert len(before) == 1
        assert before[0] == (item, "insert")
        assert before == after

        db.session.remove()
        item = db.session.get(Example, 1)  # type: ignore[assignment]
        item.data = "test"  # type: ignore[assignment]
        db.session.commit()
        assert len(before) == 1
        assert before[0] == (item, "update")
        assert before == after

        db.session.remove()
        item = db.session.get(Example, 1)  # type: ignore[assignment]
        db.session.delete(item)
        db.session.commit()
        assert len(before) == 1
        assert before[0] == (item, "delete")
        assert before == after
