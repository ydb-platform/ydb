# coding=utf8
import pytest
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

import flask_sqlalchemy as fsa
from flask_sqlalchemy._compat import to_str
from flask_sqlalchemy.model import BindMetaMixin


def test_custom_model_class():
    class CustomModelClass(fsa.Model):
        pass

    db = fsa.SQLAlchemy(model_class=CustomModelClass)

    class SomeModel(db.Model):
        id = db.Column(db.Integer, primary_key=True)

    assert isinstance(SomeModel(), CustomModelClass)


def test_no_table_name():
    class NoNameMeta(BindMetaMixin, DeclarativeMeta):
        pass

    db = fsa.SQLAlchemy(model_class=declarative_base(
        cls=fsa.Model, metaclass=NoNameMeta, name='Model'))

    with pytest.raises(InvalidRequestError):
        class User(db.Model):
            pass


def test_repr(db):
    class User(db.Model):
        name = db.Column(db.String, primary_key=True)

    class Report(db.Model):
        id = db.Column(db.Integer, primary_key=True, autoincrement=False)
        user_name = db.Column(db.ForeignKey(User.name), primary_key=True)

    db.create_all()

    u = User(name='test')
    assert repr(u).startswith("<User (transient ")
    db.session.add(u)
    db.session.flush()
    assert repr(u) == '<User test>'
    assert repr(u) == str(u)

    u2 = User(name=u'🐍')
    db.session.add(u2)
    db.session.flush()
    assert repr(u2) == to_str(u'<User 🐍>')
    assert repr(u2) == str(u2)

    r = Report(id=2, user_name=u.name)
    db.session.add(r)
    db.session.flush()
    assert repr(r) == '<Report 2, test>'
    assert repr(u) == str(u)


def test_deprecated_meta():
    class OldMeta(fsa._BoundDeclarativeMeta):
        pass

    with pytest.warns(fsa.FSADeprecationWarning):
        declarative_base(cls=fsa.Model, metaclass=OldMeta)
