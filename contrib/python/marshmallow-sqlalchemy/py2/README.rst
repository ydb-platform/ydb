**********************
marshmallow-sqlalchemy
**********************

|pypi-package| |build-status| |docs| |marshmallow23| |black|

Homepage: https://marshmallow-sqlalchemy.readthedocs.io/

`SQLAlchemy <http://www.sqlalchemy.org/>`_ integration with the  `marshmallow <https://marshmallow.readthedocs.io/en/latest/>`_ (de)serialization library.

Declare your models
===================

.. code-block:: python

    import sqlalchemy as sa
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import scoped_session, sessionmaker, relationship, backref

    engine = sa.create_engine("sqlite:///:memory:")
    session = scoped_session(sessionmaker(bind=engine))
    Base = declarative_base()


    class Author(Base):
        __tablename__ = "authors"
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)

        def __repr__(self):
            return "<Author(name={self.name!r})>".format(self=self)


    class Book(Base):
        __tablename__ = "books"
        id = sa.Column(sa.Integer, primary_key=True)
        title = sa.Column(sa.String)
        author_id = sa.Column(sa.Integer, sa.ForeignKey("authors.id"))
        author = relationship("Author", backref=backref("books"))


    Base.metadata.create_all(engine)

Generate marshmallow schemas
============================

.. code-block:: python

    from marshmallow_sqlalchemy import ModelSchema


    class AuthorSchema(ModelSchema):
        class Meta:
            model = Author


    class BookSchema(ModelSchema):
        class Meta:
            model = Book
            # optionally attach a Session
            # to use for deserialization
            sqla_session = session


    author_schema = AuthorSchema()

(De)serialize your data
=======================

.. code-block:: python

    author = Author(name="Chuck Paluhniuk")
    author_schema = AuthorSchema()
    book = Book(title="Fight Club", author=author)
    session.add(author)
    session.add(book)
    session.commit()

    dump_data = author_schema.dump(author)
    # {'books': [123], 'id': 321, 'name': 'Chuck Paluhniuk'}

    author_schema.load(dump_data, session=session)
    # <Author(name='Chuck Paluhniuk')>

Get it now
==========
::

   pip install -U marshmallow-sqlalchemy


Documentation
=============

Documentation is available at https://marshmallow-sqlalchemy.readthedocs.io/ .

Project Links
=============

- Docs: https://marshmallow-sqlalchemy.readthedocs.io/
- Changelog: https://marshmallow-sqlalchemy.readthedocs.io/en/latest/changelog.html
- PyPI: https://pypi.python.org/pypi/marshmallow-sqlalchemy
- Issues: https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues

License
=======

MIT licensed. See the bundled `LICENSE <https://github.com/marshmallow-code/marshmallow-sqlalchemy/blob/dev/LICENSE>`_ file for more details.


.. |pypi-package| image:: https://badgen.net/pypi/v/marshmallow-sqlalchemy
    :target: https://pypi.org/project/marshmallow-sqlalchemy/
    :alt: Latest version
.. |build-status| image:: https://dev.azure.com/sloria/sloria/_apis/build/status/marshmallow-code.marshmallow-sqlalchemy?branchName=dev
    :target: https://dev.azure.com/sloria/sloria/_build/latest?definitionId=10&branchName=dev
    :alt: Build status
.. |docs| image:: https://readthedocs.org/projects/marshmallow-sqlalchemy/badge/
   :target: http://marshmallow-sqlalchemy.readthedocs.io/
   :alt: Documentation
.. |marshmallow23| image:: https://badgen.net/badge/marshmallow/2,3?list=1
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 3 compatible
.. |black| image:: https://badgen.net/badge/code%20style/black/000
    :target: https://github.com/ambv/black
    :alt: code style: black
