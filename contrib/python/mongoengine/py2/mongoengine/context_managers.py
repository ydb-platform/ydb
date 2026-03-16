from contextlib import contextmanager

from pymongo.write_concern import WriteConcern
from six import iteritems

from mongoengine.common import _import_class
from mongoengine.connection import DEFAULT_CONNECTION_NAME, get_db
from mongoengine.pymongo_support import count_documents

__all__ = ('switch_db', 'switch_collection', 'no_dereference',
           'no_sub_classes', 'query_counter', 'set_write_concern')


class switch_db(object):
    """switch_db alias context manager.

    Example ::

        # Register connections
        register_connection('default', 'mongoenginetest')
        register_connection('testdb-1', 'mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_db(Group, 'testdb-1') as Group:
            Group(name='hello testdb!').save()  # Saves in testdb-1
    """

    def __init__(self, cls, db_alias):
        """Construct the switch_db context manager

        :param cls: the class to change the registered db
        :param db_alias: the name of the specific database to use
        """
        self.cls = cls
        self.collection = cls._get_collection()
        self.db_alias = db_alias
        self.ori_db_alias = cls._meta.get('db_alias', DEFAULT_CONNECTION_NAME)

    def __enter__(self):
        """Change the db_alias and clear the cached collection."""
        self.cls._meta['db_alias'] = self.db_alias
        self.cls._collection = None
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the db_alias and collection."""
        self.cls._meta['db_alias'] = self.ori_db_alias
        self.cls._collection = self.collection


class switch_collection(object):
    """switch_collection alias context manager.

    Example ::

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_collection(Group, 'group1') as Group:
            Group(name='hello testdb!').save()  # Saves in group1 collection
    """

    def __init__(self, cls, collection_name):
        """Construct the switch_collection context manager.

        :param cls: the class to change the registered db
        :param collection_name: the name of the collection to use
        """
        self.cls = cls
        self.ori_collection = cls._get_collection()
        self.ori_get_collection_name = cls._get_collection_name
        self.collection_name = collection_name

    def __enter__(self):
        """Change the _get_collection_name and clear the cached collection."""

        @classmethod
        def _get_collection_name(cls):
            return self.collection_name

        self.cls._get_collection_name = _get_collection_name
        self.cls._collection = None
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the collection."""
        self.cls._collection = self.ori_collection
        self.cls._get_collection_name = self.ori_get_collection_name


class no_dereference(object):
    """no_dereference context manager.

    Turns off all dereferencing in Documents for the duration of the context
    manager::

        with no_dereference(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls):
        """Construct the no_dereference context manager.

        :param cls: the class to turn dereferencing off on
        """
        self.cls = cls

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')
        ComplexBaseField = _import_class('ComplexBaseField')

        self.deref_fields = [k for k, v in iteritems(self.cls._fields)
                             if isinstance(v, (ReferenceField,
                                               GenericReferenceField,
                                               ComplexBaseField))]

    def __enter__(self):
        """Change the objects default and _auto_dereference values."""
        for field in self.deref_fields:
            self.cls._fields[field]._auto_dereference = False
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the default and _auto_dereference values."""
        for field in self.deref_fields:
            self.cls._fields[field]._auto_dereference = True
        return self.cls


class no_sub_classes(object):
    """no_sub_classes context manager.

    Only returns instances of this class and no sub (inherited) classes::

        with no_sub_classes(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls):
        """Construct the no_sub_classes context manager.

        :param cls: the class to turn querying sub classes on
        """
        self.cls = cls
        self.cls_initial_subclasses = None

    def __enter__(self):
        """Change the objects default and _auto_dereference values."""
        self.cls_initial_subclasses = self.cls._subclasses
        self.cls._subclasses = (self.cls._class_name,)
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the default and _auto_dereference values."""
        self.cls._subclasses = self.cls_initial_subclasses


class query_counter(object):
    """Query_counter context manager to get the number of queries.
    This works by updating the `profiling_level` of the database so that all queries get logged,
    resetting the db.system.profile collection at the beginnig of the context and counting the new entries.

    This was designed for debugging purpose. In fact it is a global counter so queries issued by other threads/processes
    can interfere with it

    Be aware that:
    - Iterating over large amount of documents (>101) makes pymongo issue `getmore` queries to fetch the next batch of
        documents (https://docs.mongodb.com/manual/tutorial/iterate-a-cursor/#cursor-batches)
    - Some queries are ignored by default by the counter (killcursors, db.system.indexes)
    """

    def __init__(self):
        """Construct the query_counter
        """
        self.db = get_db()
        self.initial_profiling_level = None
        self._ctx_query_counter = 0             # number of queries issued by the context

        self._ignored_query = {
            'ns':
                {'$ne': '%s.system.indexes' % self.db.name},
            'op':                       # MONGODB < 3.2
                {'$ne': 'killcursors'},
            'command.killCursors':      # MONGODB >= 3.2
                {'$exists': False}
        }

    def _turn_on_profiling(self):
        self.initial_profiling_level = self.db.profiling_level()
        self.db.set_profiling_level(0)
        self.db.system.profile.drop()
        self.db.set_profiling_level(2)

    def _resets_profiling(self):
        self.db.set_profiling_level(self.initial_profiling_level)

    def __enter__(self):
        self._turn_on_profiling()
        return self

    def __exit__(self, t, value, traceback):
        self._resets_profiling()

    def __eq__(self, value):
        counter = self._get_count()
        return value == counter

    def __ne__(self, value):
        return not self.__eq__(value)

    def __lt__(self, value):
        return self._get_count() < value

    def __le__(self, value):
        return self._get_count() <= value

    def __gt__(self, value):
        return self._get_count() > value

    def __ge__(self, value):
        return self._get_count() >= value

    def __int__(self):
        return self._get_count()

    def __repr__(self):
        """repr query_counter as the number of queries."""
        return u"%s" % self._get_count()

    def _get_count(self):
        """Get the number of queries by counting the current number of entries in db.system.profile
        and substracting the queries issued by this context. In fact everytime this is called, 1 query is
        issued so we need to balance that
        """
        count = count_documents(self.db.system.profile, self._ignored_query) - self._ctx_query_counter
        self._ctx_query_counter += 1    # Account for the query we just issued to gather the information
        return count


@contextmanager
def set_write_concern(collection, write_concerns):
    combined_concerns = dict(collection.write_concern.document.items())
    combined_concerns.update(write_concerns)
    yield collection.with_options(write_concern=WriteConcern(**combined_concerns))
