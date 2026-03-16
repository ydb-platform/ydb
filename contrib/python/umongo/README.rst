======================
μMongo: sync/async ODM
======================

.. image:: https://img.shields.io/pypi/v/umongo.svg
    :target: https://pypi.python.org/pypi/umongo
    :alt: Latest version

.. image:: https://img.shields.io/pypi/pyversions/umongo.svg
    :target: https://pypi.org/project/umongo/
    :alt: Python versions

.. image:: https://img.shields.io/badge/marshmallow-3-blue.svg
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 3 only

.. image:: https://img.shields.io/pypi/l/umongo.svg
    :target: https://umongo.readthedocs.io/en/latest/license.html
    :alt: License

.. image:: https://dev.azure.com/lafrech/umongo/_apis/build/status/Scille.umongo?branchName=master
    :target: https://dev.azure.com/lafrech/umongo/_build/latest?definitionId=1&branchName=master
    :alt: Build status

.. image:: https://readthedocs.org/projects/umongo/badge/
        :target: http://umongo.readthedocs.io/
        :alt: Documentation

μMongo is a Python MongoDB ODM. It inception comes from two needs:
the lack of async ODM and the difficulty to do document (un)serialization
with existing ODMs.

From this point, μMongo made a few design choices:

- Stay close to the standards MongoDB driver to keep the same API when possible:
  use ``find({"field": "value"})`` like usual but retrieve your data nicely OO wrapped !
- Work with multiple drivers (PyMongo_, TxMongo_, motor_asyncio_ and mongomock_ for the moment)
- Tight integration with Marshmallow_ serialization library to easily
  dump and load your data with the outside world
- i18n integration to localize validation error messages
- Free software: MIT license
- Test with 90%+ coverage ;-)

.. _PyMongo: https://api.mongodb.org/python/current/
.. _TxMongo: https://txmongo.readthedocs.org/en/latest/
.. _motor_asyncio: https://motor.readthedocs.org/en/stable/
.. _mongomock: https://github.com/vmalloc/mongomock
.. _Marshmallow: http://marshmallow.readthedocs.org

µMongo requires MongoDB 4.2+ and Python 3.7+.

Quick example

.. code-block:: python

    import datetime as dt
    from pymongo import MongoClient
    from umongo import Document, fields, validate
    from umongo.frameworks import PyMongoInstance

    db = MongoClient().test
    instance = PyMongoInstance(db)

    @instance.register
    class User(Document):
        email = fields.EmailField(required=True, unique=True)
        birthday = fields.DateTimeField(validate=validate.Range(min=dt.datetime(1900, 1, 1)))
        friends = fields.ListField(fields.ReferenceField("User"))

        class Meta:
            collection_name = "user"

    # Make sure that unique indexes are created
    User.ensure_indexes()

    goku = User(email='goku@sayen.com', birthday=dt.datetime(1984, 11, 20))
    goku.commit()
    vegeta = User(email='vegeta@over9000.com', friends=[goku])
    vegeta.commit()

    vegeta.friends
    # <object umongo.data_objects.List([<object umongo.dal.pymongo.PyMongoReference(document=User, pk=ObjectId('5717568613adf27be6363f78'))>])>
    vegeta.dump()
    # {id': '570ddb311d41c89cabceeddc', 'email': 'vegeta@over9000.com', friends': ['570ddb2a1d41c89cabceeddb']}
    User.find_one({"email": 'goku@sayen.com'})
    # <object Document __main__.User({'id': ObjectId('570ddb2a1d41c89cabceeddb'), 'friends': <object umongo.data_objects.List([])>,
    #                                 'email': 'goku@sayen.com', 'birthday': datetime.datetime(1984, 11, 20, 0, 0)})>

Get it now::

    $ pip install umongo           # This installs umongo with pymongo
    $ pip install my-mongo-driver  # Other MongoDB drivers must be installed manually

Or to get it along with the MongoDB driver you're planing to use::

    $ pip install umongo[motor]
    $ pip install umongo[txmongo]
    $ pip install umongo[mongomock]
