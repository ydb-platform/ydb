"""
Helpers
=======

This module contains common helpers which make your life easier.

"""

import pymongo
from pytool.lang import UNSET

from humbledb.errors import DatabaseMismatch, NoConnection
from humbledb.mongo import Mongo


def auto_increment(database, collection, _id, field="value", increment=1):
    """
    Factory method for creating a stored default value which is
    auto-incremented.

    This uses a sidecar document to keep the increment counter sync'd
    atomically. See the MongoDB `documentation
    <http://docs.mongodb.org/manual/tutorial/create-an-auto-incrementing-field/>`_
    for more information about how this works.

    .. note::

       If a `Document` subclass is inherited and has an auto_increment helper,
       it will share the counter unless it's overriden in the inheriting
       `Document`.

    .. rubric:: Example: using auto_increment fields

    .. code-block:: python

       from humbledb.helpers import auto_increment

       class MyDoc(Document):
           config_database = 'humbledb'
           config_collection = 'examples'

           # The auto_increment helper needs arguments:
           #     - database name: Database to store the sidecar document
           #     - collection name: Collection name that stores the sidecar
           #     - Id: A unique identifier for this document and field
           auto_id = auto_increment('humbledb', 'counters', 'MyDoc_auto_id')

           # The auto_increment helper can take an increment argument
           big_auto = auto_increment('humbledb', 'counters', 'MyDoc_big_auto',
                   increment=10)

    :param database: Database name
    :param collection: Collection name
    :param _id: Unique identifier for auto increment field
    :param field: Sidecar document field name (default: ``"value"``)
    :param increment: Amount to increment counter by (default: 1)
    :type database: str
    :type collection: str
    :type _id: str
    :type field: str
    :type increment: int

    """

    def auto_incrementer():
        """
        Return an auto incremented value.

        """
        # Make sure we're executing in a Mongo connection context
        context = Mongo.context
        if not context:
            raise NoConnection(
                "A connection is required for auto_increment "
                "defaults to work correctly."
            )

        if context.database is not None:
            if context.database.name != database:
                raise DatabaseMismatch(
                    "auto_increment database %r does not match connection database %r"
                )

            # If we have a default database it should already be available
            db = context.database
        else:
            # Otherwise we need to get the correct database
            db = context.connection[database]

        # We just use this directly, instead of using a Document helper
        doc = db[collection].find_one_and_update(
            {"_id": _id},
            {"$inc": {field: increment}},
            return_document=pymongo.ReturnDocument.AFTER,
            upsert=True,
        )

        # Return the value
        if not doc:
            # TBD shakefu: Maybe a more specific error here?
            raise RuntimeError(
                "Could not get new auto_increment value for "
                "%r.%r : %r" % (database, collection, _id)
            )

        value = doc.get("value", UNSET)
        if value is UNSET:
            # TBD shakefu: Maybe a more specific error here?
            raise RuntimeError(
                "Could not get new auto_increment value for "
                "%r.%r : %r" % (database, collection, _id)
            )

        return value

    return auto_incrementer
