from __future__ import annotations

import typing as t
import warnings

import pytest
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
from flask import Flask
from werkzeug.exceptions import NotFound

from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.query import Query


@pytest.fixture(autouse=True)
def ignore_query_warning() -> t.Generator[None, None, None]:
    if hasattr(sa_exc, "LegacyAPIWarning"):
        with warnings.catch_warnings():
            exc = sa_exc.LegacyAPIWarning
            warnings.simplefilter("ignore", exc)
            yield
    else:
        yield


@pytest.mark.usefixtures("app_ctx")
def test_get_or_404(db: SQLAlchemy, Todo: t.Any) -> None:
    item = Todo()
    db.session.add(item)
    db.session.commit()
    assert Todo.query.get_or_404(1) is item

    with pytest.raises(NotFound):
        Todo.query.get_or_404(2)


@pytest.mark.usefixtures("app_ctx")
def test_first_or_404(db: SQLAlchemy, Todo: t.Any) -> None:
    db.session.add(Todo(title="a"))
    db.session.commit()
    assert Todo.query.filter_by(title="a").first_or_404().title == "a"

    with pytest.raises(NotFound):
        Todo.query.filter_by(title="b").first_or_404()


@pytest.mark.usefixtures("app_ctx")
def test_one_or_404(db: SQLAlchemy, Todo: t.Any) -> None:
    db.session.add(Todo(title="a"))
    db.session.add(Todo(title="b"))
    db.session.add(Todo(title="b"))
    db.session.commit()
    assert Todo.query.filter_by(title="a").one_or_404().title == "a"

    with pytest.raises(NotFound):
        # MultipleResultsFound
        Todo.query.filter_by(title="b").one_or_404()

    with pytest.raises(NotFound):
        # NoResultFound
        Todo.query.filter_by(title="c").one_or_404()


@pytest.mark.usefixtures("app_ctx")
def test_paginate(db: SQLAlchemy, Todo: t.Any) -> None:
    db.session.add_all(Todo() for _ in range(150))
    db.session.commit()
    p = Todo.query.paginate()
    assert p.total == 150
    assert len(p.items) == 20
    p2 = p.next()
    assert p2.page == 2
    assert p2.total == 150


@pytest.mark.usefixtures("app_ctx")
def test_default_query_class(db: SQLAlchemy) -> None:
    class Parent(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        children1 = db.relationship("Child", backref="parent1", lazy="dynamic")

    class Child(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        parent_id = sa.Column(sa.ForeignKey(Parent.id))  # type: ignore[var-annotated]
        parent2 = db.relationship(
            Parent,
            backref=db.backref("children2", lazy="dynamic", viewonly=True),
            viewonly=True,
        )

    p = Parent()
    assert type(Parent.query) is Query
    assert isinstance(p.children1, Query)
    assert isinstance(p.children2, Query)
    assert isinstance(db.session.query(Child), Query)


@pytest.mark.usefixtures("app_ctx")
def test_custom_query_class(app: Flask) -> None:
    class CustomQuery(Query):
        pass

    db = SQLAlchemy(app, query_class=CustomQuery)

    class Parent(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        children1 = db.relationship("Child", backref="parent1", lazy="dynamic")

    class Child(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)
        parent_id = sa.Column(sa.ForeignKey(Parent.id))  # type: ignore[var-annotated]
        parent2 = db.relationship(
            Parent,
            backref=db.backref("children2", lazy="dynamic", viewonly=True),
            viewonly=True,
        )

    p = Parent()
    assert type(Parent.query) is CustomQuery
    assert isinstance(p.children1, CustomQuery)
    assert isinstance(p.children2, CustomQuery)
    assert isinstance(db.session.query(Child), CustomQuery)
