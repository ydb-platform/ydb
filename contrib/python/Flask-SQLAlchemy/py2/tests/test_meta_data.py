import sqlalchemy as sa

import flask_sqlalchemy as fsa


def test_default_metadata(app):
    db = fsa.SQLAlchemy(app, metadata=None)

    class One(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        myindex = db.Column(db.Integer, index=True)

    class Two(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        one_id = db.Column(db.Integer, db.ForeignKey(One.id))
        myunique = db.Column(db.Integer, unique=True)

    assert One.metadata.__class__ is sa.MetaData
    assert Two.metadata.__class__ is sa.MetaData

    assert One.__table__.schema is None
    assert Two.__table__.schema is None


def test_custom_metadata(app):
    class CustomMetaData(sa.MetaData):
        pass

    custom_metadata = CustomMetaData(schema="test_schema")
    db = fsa.SQLAlchemy(app, metadata=custom_metadata)

    class One(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        myindex = db.Column(db.Integer, index=True)

    class Two(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        one_id = db.Column(db.Integer, db.ForeignKey(One.id))
        myunique = db.Column(db.Integer, unique=True)

    assert One.metadata is custom_metadata
    assert Two.metadata is custom_metadata

    assert One.metadata.__class__ is not sa.MetaData
    assert One.metadata.__class__ is CustomMetaData

    assert Two.metadata.__class__ is not sa.MetaData
    assert Two.metadata.__class__ is CustomMetaData

    assert One.__table__.schema == "test_schema"
    assert Two.__table__.schema == "test_schema"
