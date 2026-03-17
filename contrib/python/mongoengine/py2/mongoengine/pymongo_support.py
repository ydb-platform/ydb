"""
Helper functions, constants, and types to aid with PyMongo v2.7 - v3.x support.
"""
import pymongo

_PYMONGO_37 = (3, 7)

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

IS_PYMONGO_3 = PYMONGO_VERSION[0] >= 3
IS_PYMONGO_GTE_37 = PYMONGO_VERSION >= _PYMONGO_37


def count_documents(collection, filter):
    """Pymongo>3.7 deprecates count in favour of count_documents"""
    if IS_PYMONGO_GTE_37:
        return collection.count_documents(filter)
    else:
        count = collection.find(filter).count()
    return count


def list_collection_names(db, include_system_collections=False):
    """Pymongo>3.7 deprecates collection_names in favour of list_collection_names"""
    if IS_PYMONGO_GTE_37:
        collections = db.list_collection_names()
    else:
        collections = db.collection_names()

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith('system.')]

    return collections
