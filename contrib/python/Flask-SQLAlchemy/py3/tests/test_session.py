from __future__ import annotations

import pytest
import sqlalchemy as sa
from flask import Flask

from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.session import Session


def test_scope(app: Flask, db: SQLAlchemy) -> None:
    with pytest.raises(RuntimeError):
        db.session()

    with app.app_context():
        first = db.session()
        second = db.session()
        assert first is second
        assert isinstance(first, Session)

    with app.app_context():
        third = db.session()
        assert first is not third


def test_custom_scope(app: Flask) -> None:
    count = 0

    def scope() -> int:
        nonlocal count
        count += 1
        return count

    db = SQLAlchemy(app, session_options={"scopefunc": scope})

    with app.app_context():
        first = db.session()
        second = db.session()
        assert first is not second  # a new scope is generated on each call
        first.close()
        second.close()


@pytest.mark.usefixtures("app_ctx")
def test_session_class(app: Flask) -> None:
    class CustomSession(Session):
        pass

    db = SQLAlchemy(app, session_options={"class_": CustomSession})
    assert isinstance(db.session(), CustomSession)


@pytest.mark.usefixtures("app_ctx")
def test_session_uses_bind_key(app: Flask) -> None:
    app.config["SQLALCHEMY_BINDS"] = {"a": "sqlite://"}
    db = SQLAlchemy(app)

    class User(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)

    class Post(db.Model):
        __bind_key__ = "a"
        id = sa.Column(sa.Integer, primary_key=True)

    assert db.session.get_bind(mapper=User) is db.engine
    assert db.session.get_bind(mapper=Post) is db.engines["a"]


@pytest.mark.usefixtures("app_ctx")
def test_get_bind_inheritance(app: Flask) -> None:
    app.config["SQLALCHEMY_BINDS"] = {"a": "sqlite://"}
    db = SQLAlchemy(app)

    class User(db.Model):
        __bind_key__ = "a"
        id = sa.Column(sa.Integer, primary_key=True)
        type = sa.Column(sa.String, nullable=False)

        __mapper_args__ = {"polymorphic_on": type, "polymorphic_identity": "user"}

    class Admin(User):
        id = sa.Column(sa.ForeignKey(User.id), primary_key=True)
        org = sa.Column(sa.String, nullable=False)

        __mapper_args__ = {"polymorphic_identity": "admin"}

    db.create_all()
    db.session.add(Admin(org="pallets"))
    db.session.commit()
    admin = db.session.execute(db.select(Admin)).scalar_one()
    db.session.expire(admin)
    assert admin.org == "pallets"
