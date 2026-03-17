from __future__ import annotations

import typing as t
from pathlib import Path

import pytest
import sqlalchemy as sa
from flask import Flask
from flask.ctx import AppContext

from flask_sqlalchemy import SQLAlchemy


@pytest.fixture
def app(request: pytest.FixtureRequest, tmp_path: Path) -> Flask:
    app = Flask(request.module.__name__, instance_path=str(tmp_path / "instance"))
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
    app.config["SQLALCHEMY_RECORD_QUERIES"] = False
    return app


@pytest.fixture
def app_ctx(app: Flask) -> t.Generator[AppContext, None, None]:
    with app.app_context() as ctx:
        yield ctx


@pytest.fixture
def db(app: Flask) -> SQLAlchemy:
    return SQLAlchemy(app)


@pytest.fixture
def Todo(app: Flask, db: SQLAlchemy) -> t.Generator[t.Any, None, None]:
    class Todo(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        title = sa.Column(sa.String)

    with app.app_context():
        db.create_all()

    yield Todo

    with app.app_context():
        db.drop_all()
