import pytest


@pytest.fixture
def db(app, db):
    app.testing = False
    return db


def test_joined_inheritance(db):
    class Base(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        type = db.Column(db.Unicode(20))
        __mapper_args__ = {'polymorphic_on': type}

    class SubBase(Base):
        id = db.Column(db.Integer, db.ForeignKey('base.id'),
                       primary_key=True)
        __mapper_args__ = {'polymorphic_identity': 'sub'}

    assert Base.__tablename__ == 'base'
    assert SubBase.__tablename__ == 'sub_base'
    db.create_all()


def test_single_table_inheritance(db):
    class Base(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        type = db.Column(db.Unicode(20))
        __mapper_args__ = {'polymorphic_on': type}

    class SubBase(Base):
        __mapper_args__ = {'polymorphic_identity': 'sub'}

    assert Base.__tablename__ == 'base'
    assert SubBase.__tablename__ == 'base'
    db.create_all()


def test_joined_inheritance_relation(db):
    class Relation(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        base_id = db.Column(db.Integer, db.ForeignKey('base.id'))
        name = db.Column(db.Unicode(20))

        def __init__(self, name):
            self.name = name

    class Base(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        type = db.Column(db.Unicode(20))
        __mapper_args__ = {'polymorphic_on': type}

    class SubBase(Base):
        id = db.Column(db.Integer, db.ForeignKey('base.id'),
                       primary_key=True)
        __mapper_args__ = {'polymorphic_identity': u'sub'}
        relations = db.relationship(Relation)

    db.create_all()

    base = SubBase()
    base.relations = [Relation(name=u'foo')]
    db.session.add(base)
    db.session.commit()

    base = base.query.one()


def test_connection_binds(db):
    assert db.session.connection()
