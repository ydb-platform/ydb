# -*- coding: utf-8 -*-
from copy import deepcopy
from proxy_storage.meta_backends.base import MetaBackendBase, MetaBackendObjectDoesNotExist


class MongoMetaBackend(MetaBackendBase):
    def __init__(self, database, collection):
        self.database = database
        self.collection = collection

    def get_collection(self):
        return getattr(self.get_database(), self.collection)

    def get_database(self):
        from pymongo.database import Database

        if isinstance(self.database, Database):
            return self.database
        else:
            return self.database()

    def _convert_obj_to_dict(self, obj):
        return obj

    def _create(self, data):
        self.get_collection().ensure_index('path', unique=True)
        object_id = self.get_collection().insert(data)
        obj = deepcopy(data)
        obj.update({
            '_id': object_id
        })
        return obj

    def _get(self, path):
        response = self.get_collection().find_one({'path': path})
        if response is None:
            raise MetaBackendObjectDoesNotExist('Could not find document in "{}"'.format(
                self.collection
            ))
        else:
            return response

    def delete(self, path):
        return self.get_collection().remove({'path': path})

    def update(self, path, update_data):
        self.get_collection().update({'path': path}, {'$set': update_data})

    def exists(self, path):
        return bool(self.get_collection().find({'path': path}).count())