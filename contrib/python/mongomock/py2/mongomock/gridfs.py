
def enable_gridfs_integration():

    """This function enables the use of mongomock Database's and Collection's inside gridfs

    Gridfs library use `isinstance` to make sure the passed elements
    are valid `pymongo.Database/Collection`. Hence we have to monkeypatch
    isinstance behaviour to also accept `mongomock.Database/Collection`.
    Note we only patch isinstance within the gridfs module, especially because
    overloading this builtins makes the code really slow.
    """

    # pylint: disable=import-outside-toplevel
    import builtins
    from importlib import import_module
    from pymongo.collection import Collection as PyMongoCollection
    from pymongo.database import Database as PyMongoDatabase
    from mongomock import Database as MongoMockDatabase, Collection as MongoMockCollection
    from mongomock.collection import Cursor as MongoMockCursor
    # pylint: enable=import-outside-toplevel

    def isinstance_patched(object, classinfo):
        if isinstance(classinfo, tuple):
            classesinfo = list(classinfo)
        else:
            classesinfo = [classinfo]
        mocked_needed = []
        for cls in classesinfo:
            if cls is PyMongoCollection:
                mocked_needed.append(MongoMockCollection)
            if cls is PyMongoDatabase:
                mocked_needed.append(MongoMockDatabase)
        return builtins.isinstance(object, tuple(classesinfo + mocked_needed))

    modules = {}
    for modname in ('gridfs', 'gridfs.grid_file', 'gridfs.errors'):
        mod = import_module(modname)
        mod.__builtins__ = mod.__builtins__.copy()
        mod.__builtins__['isinstance'] = isinstance_patched
        modules[modname] = mod

    PyMongoGridOut = modules['gridfs.grid_file'].GridOut
    PyMongoGridOutCursor = modules['gridfs.grid_file'].GridOutCursor

    # This is a copy of GridOutCursor but with a different base. Note that we
    # need both classes as one might want to access both mongomock and real
    # MongoDb.
    class MongoMockGridOutCursor(MongoMockCursor):

        def __init__(self, collection, *args, **kwargs):
            self.__root_collection = collection
            super(MongoMockGridOutCursor, self).__init__(collection.files, *args, **kwargs)

        def next(self):
            next_file = super(MongoMockGridOutCursor, self).next()
            return PyMongoGridOut(
                self.__root_collection, file_document=next_file, session=self.session)

        __next__ = next

        def add_option(self, *args, **kwargs):
            raise NotImplementedError()

        def remove_option(self, *args, **kwargs):
            raise NotImplementedError()

        def _clone_base(self, session):
            return MongoMockGridOutCursor(self.__root_collection, session=session)

    def _create_grid_out_cursor(collection, *args, **kwargs):
        if isinstance(collection, MongoMockCollection):
            return MongoMockGridOutCursor(collection, *args, **kwargs)
        return PyMongoGridOutCursor(collection, *args, **kwargs)

    modules['gridfs'].GridOutCursor = _create_grid_out_cursor
