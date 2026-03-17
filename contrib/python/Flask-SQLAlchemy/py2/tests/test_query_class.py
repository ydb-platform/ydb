import flask_sqlalchemy as fsa


def test_default_query_class(db):
    class Parent(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        children = db.relationship("Child", backref="parent", lazy='dynamic')

    class Child(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        parent_id = db.Column(db.Integer, db.ForeignKey('parent.id'))

    p = Parent()
    c = Child()
    c.parent = p

    assert type(Parent.query) == fsa.BaseQuery
    assert type(Child.query) == fsa.BaseQuery
    assert isinstance(p.children, fsa.BaseQuery)
    assert isinstance(db.session.query(Parent), fsa.BaseQuery)


def test_custom_query_class(app):
    class CustomQueryClass(fsa.BaseQuery):
        pass

    db = fsa.SQLAlchemy(app, query_class=CustomQueryClass)

    class Parent(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        children = db.relationship("Child", backref="parent", lazy='dynamic')

    class Child(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        parent_id = db.Column(db.Integer, db.ForeignKey('parent.id'))

    p = Parent()
    c = Child()
    c.parent = p

    assert type(Parent.query) == CustomQueryClass
    assert type(Child.query) == CustomQueryClass
    assert isinstance(p.children, CustomQueryClass)
    assert db.Query == CustomQueryClass
    assert db.Model.query_class == CustomQueryClass
    assert isinstance(db.session.query(Parent), CustomQueryClass)


def test_dont_override_model_default(app):
    class CustomQueryClass(fsa.BaseQuery):
        pass

    db = fsa.SQLAlchemy(app, query_class=CustomQueryClass)

    class SomeModel(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        query_class = fsa.BaseQuery

    assert type(SomeModel.query) == fsa.BaseQuery
