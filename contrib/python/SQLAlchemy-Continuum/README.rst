SQLAlchemy-Continuum
====================

|Build Status| |Version Status| |Downloads|

Versioning and auditing extension for SQLAlchemy.


Features
--------

- Creates versions for inserts, deletes and updates
- Does not store updates which don't change anything
- Supports alembic migrations
- Can revert objects data as well as all object relations at given transaction even if the object was deleted
- Transactions can be queried afterwards using SQLAlchemy query syntax
- Query for changed records at given transaction
- Temporal relationship reflection. Version object's relationship show the parent objects relationships as they where in that point in time.
- Supports native versioning for PostgreSQL database (trigger based versioning)


QuickStart
----------

::


    pip install SQLAlchemy-Continuum



In order to make your models versioned you need two things:

1. Call make_versioned() before your models are defined.
2. Add __versioned__ to all models you wish to add versioning to


.. code-block:: python


    from sqlalchemy_continuum import make_versioned


    make_versioned(user_cls=None)


    class Article(Base):
        __versioned__ = {}
        __tablename__ = 'article'

        id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
        name = sa.Column(sa.Unicode(255))
        content = sa.Column(sa.UnicodeText)


    article = Article(name='Some article', content='Some content')
    session.add(article)
    session.commit()

    # article has now one version stored in database
    article.versions[0].name
    # 'Some article'

    article.name = 'Updated name'
    session.commit()

    article.versions[1].name
    # 'Updated name'


    # lets revert back to first version
    article.versions[0].revert()

    article.name
    # 'Some article'

For completeness, below is a working example.

.. code-block:: python

    from sqlalchemy_continuum import make_versioned
    from sqlalchemy import Column, Integer, Unicode, UnicodeText, create_engine
    from sqlalchemy.orm import create_session, configure_mappers, declarative_base

    make_versioned(user_cls=None)

    Base = declarative_base()
    class Article(Base):
        __versioned__ = {}
        __tablename__ = 'article'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(Unicode(255))
        content = Column(UnicodeText)

    configure_mappers()
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    session = create_session(bind=engine, autocommit=False)

    article = Article(name=u'Some article', content=u'Some content')
    session.add(article)
    session.commit()
    article.versions[0].name
    article.name = u'Updated name'
    session.commit()
    article.versions[1].name
    article.versions[0].revert()
    article.name

Resources
---------

- `Documentation <https://sqlalchemy-continuum.readthedocs.io/>`_
- `Issue Tracker <http://github.com/kvesteri/sqlalchemy-continuum/issues>`_
- `Code <http://github.com/kvesteri/sqlalchemy-continuum/>`_


.. image:: http://i.imgur.com/UFaRx.gif


.. |Build Status| image:: https://github.com/kvesteri/sqlalchemy-continuum/workflows/Test/badge.svg
   :target: https://github.com/kvesteri/sqlalchemy-continuum/actions?query=workflow%3ATest
.. |Version Status| image:: https://img.shields.io/pypi/v/SQLAlchemy-Continuum.png
   :target: https://pypi.python.org/pypi/SQLAlchemy-Continuum/
.. |Downloads| image:: https://img.shields.io/pypi/dm/SQLAlchemy-Continuum.png
   :target: https://pypi.python.org/pypi/SQLAlchemy-Continuum/


More information
----------------

- http://en.wikipedia.org/wiki/Slowly_changing_dimension
- http://en.wikipedia.org/wiki/Change_data_capture
- http://en.wikipedia.org/wiki/Anchor_Modeling
- http://en.wikipedia.org/wiki/Shadow_table
- https://wiki.postgresql.org/wiki/Audit_trigger
- https://wiki.postgresql.org/wiki/Audit_trigger_91plus
- http://kosalads.blogspot.fi/2014/06/implement-audit-functionality-in.html
- https://github.com/2ndQuadrant/pgaudit
