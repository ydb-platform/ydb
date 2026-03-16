import sqlalchemy as sa

import flask_sqlalchemy as fsa


def test_sqlalchemy_includes():
    """Various SQLAlchemy objects are exposed as attributes."""
    db = fsa.SQLAlchemy()

    assert db.Column == sa.Column

    # The Query object we expose is actually our own subclass.
    assert db.Query == fsa.BaseQuery
