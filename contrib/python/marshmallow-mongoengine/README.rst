.. image:: https://travis-ci.org/MongoEngine/marshmallow-mongoengine.svg?branch=master
    :target: https://travis-ci.org/MongoEngine/marshmallow-mongoengine
    :alt: Travis-CI

.. image:: https://readthedocs.org/projects/marshmallow-mongoengine/badge/?version=latest
    :target: http://marshmallow-mongoengine.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://coveralls.io/repos/github/MongoEngine/marshmallow-mongoengine/badge.svg?branch=master
    :target: https://coveralls.io/github/MongoEngine/marshmallow-mongoengine?branch=master
    :alt: Code Coverage

marshmallow-mongoengine
=======================

`Mongoengine <http://mongoengine.org>`_ integration with the  `marshmallow
<https://marshmallow.readthedocs.org/en/latest/>`_ (de)serialization library.

See documentation at http://marshmallow-mongoengine.rtfd.org/

Declare your models
-------------------

.. code-block:: python

    import mongoengine as me

    class Author(me.Document):
        id = me.IntField(primary_key=True, default=1)
        name = me.StringField()
        books = me.ListField(me.ReferenceField('Book'))

        def __repr__(self):
            return '<Author(name={self.name!r})>'.format(self=self)


    class Book(me.Document):
        title = me.StringField()

Generate marshmallow schemas
----------------------------

.. code-block:: python

    from marshmallow_mongoengine import ModelSchema

    class AuthorSchema(ModelSchema):
        class Meta:
            model = Author

    class BookSchema(ModelSchema):
        class Meta:
            model = Book

    author_schema = AuthorSchema()

(De)serialize your data
-----------------------

.. code-block:: python

    author = Author(name='Chuck Paluhniuk').save()
    book = Book(title='Fight Club', author=author).save()

    dump_data = author_schema.dump(author).data
    # {'id': 1, 'name': 'Chuck Paluhniuk', 'books': ['5578726b7a58012298a5a7e2']}

    author_schema.load(dump_data).data
    # <Author(name='Chuck Paluhniuk')>

Get it now
----------
::

   pip install -U marshmallow-mongoengine

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/touilleMan/marshmallow-mongoengine/blob/master/LICENSE>`_ file for more details.
