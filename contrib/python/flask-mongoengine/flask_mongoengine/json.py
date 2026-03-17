from bson import json_util
from flask.json import JSONEncoder
from mongoengine.base import BaseDocument
from mongoengine.queryset import QuerySet


def _make_encoder(superclass):
    class MongoEngineJSONEncoder(superclass):
        """
        A JSONEncoder which provides serialization of MongoEngine
        documents and queryset objects.
        """

        def default(self, obj):
            if isinstance(obj, BaseDocument):
                return json_util._json_convert(obj.to_mongo())
            elif isinstance(obj, QuerySet):
                return json_util._json_convert(obj.as_pymongo())
            return superclass.default(self, obj)

    return MongoEngineJSONEncoder


MongoEngineJSONEncoder = _make_encoder(JSONEncoder)


def override_json_encoder(app):
    """
    A function to dynamically create a new MongoEngineJSONEncoder class
    based upon a custom base class.
    This function allows us to combine MongoEngine serialization with
    any changes to Flask's JSONEncoder which a user may have made
    prior to calling init_app.

    NOTE: This does not cover situations where users override
    an instance's json_encoder after calling init_app.
    """
    app.json_encoder = _make_encoder(app.json_encoder)
