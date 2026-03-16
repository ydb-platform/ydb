from __future__ import annotations

import sqlalchemy as sa

from flask_sqlalchemy import SQLAlchemy


def test_bind_key_default(db: SQLAlchemy) -> None:
    user_table = db.Table("user", sa.Column("id", sa.Integer, primary_key=True))
    assert user_table.metadata is db.metadata


def test_metadata_per_bind(db: SQLAlchemy) -> None:
    user_table = db.Table(
        "user", sa.Column("id", sa.Integer, primary_key=True), bind_key="other"
    )
    assert user_table.metadata is db.metadatas["other"]


def test_multiple_binds_same_table_name(db: SQLAlchemy) -> None:
    user1_table = db.Table("user", sa.Column("id", sa.Integer, primary_key=True))
    user2_table = db.Table(
        "user", sa.Column("id", sa.Integer, primary_key=True), bind_key="other"
    )

    assert user1_table.metadata is db.metadata
    assert user2_table.metadata is db.metadatas["other"]


def test_explicit_metadata(db: SQLAlchemy) -> None:
    other_metadata = sa.MetaData()
    user_table = db.Table(
        "user",
        other_metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        bind_key="other",
    )
    assert user_table.metadata is other_metadata
    assert "other" not in db.metadatas
