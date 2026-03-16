from __future__ import annotations

import sqlalchemy as sa

from flask_sqlalchemy import SQLAlchemy


def test_bind_key_default(db: SQLAlchemy) -> None:
    class User(db.Model):
        id = sa.Column(sa.Integer, primary_key=True)

    assert User.metadata is db.metadata


def test_metadata_per_bind(db: SQLAlchemy) -> None:
    class User(db.Model):
        __bind_key__ = "other"
        id = sa.Column(sa.Integer, primary_key=True)

    assert User.metadata is db.metadatas["other"]


def test_multiple_binds_same_table_name(db: SQLAlchemy) -> None:
    class UserA(db.Model):
        __tablename__ = "user"
        id = sa.Column(sa.Integer, primary_key=True)

    class UserB(db.Model):
        __bind_key__ = "other"
        __tablename__ = "user"
        id = sa.Column(sa.Integer, primary_key=True)

    assert UserA.metadata is db.metadata
    assert UserB.metadata is db.metadatas["other"]
    assert UserA.__table__.metadata is not UserB.__table__.metadata


def test_inherit_parent(db: SQLAlchemy) -> None:
    class User(db.Model):
        __bind_key__ = "auth"
        id = sa.Column(sa.Integer, primary_key=True)
        type = sa.Column(sa.String)
        __mapper_args__ = {"polymorphic_on": type, "polymorphic_identity": "user"}

    class Admin(User):
        id = sa.Column(sa.Integer, sa.ForeignKey(User.id), primary_key=True)
        __mapper_args__ = {"polymorphic_identity": "admin"}

    assert "admin" in db.metadatas["auth"].tables
    # inherits metadata, doesn't set it directly
    assert "metadata" not in Admin.__dict__


def test_inherit_abstract_parent(db: SQLAlchemy) -> None:
    class AbstractUser(db.Model):
        __abstract__ = True
        __bind_key__ = "auth"

    class User(AbstractUser):
        id = sa.Column(sa.Integer, primary_key=True)

    assert "user" in db.metadatas["auth"].tables
    assert "metadata" not in User.__dict__


def test_explicit_metadata(db: SQLAlchemy) -> None:
    other_metadata = sa.MetaData()

    class User(db.Model):
        __bind_key__ = "other"
        metadata = other_metadata
        id = sa.Column(sa.Integer, primary_key=True)

    assert User.__table__.metadata is other_metadata
    assert "other" not in db.metadatas


def test_explicit_table(db: SQLAlchemy) -> None:
    user_table = db.Table(
        "user",
        sa.Column("id", sa.Integer, primary_key=True),
        bind_key="auth",
    )

    class User(db.Model):
        __bind_key__ = "other"
        __table__ = user_table

    assert User.__table__.metadata is db.metadatas["auth"]
    assert "other" not in db.metadatas
