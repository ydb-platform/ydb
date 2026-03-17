""" """

import logging
import threading

import pyconfig
import pymongo
from pytool.lang import UNSET, classproperty

from humbledb import _version
from humbledb.errors import NestedConnection

try:
    import ssl
except ImportError:
    ssl = None

__all__ = [
    "Mongo",
]


class MongoMeta(type):
    """Metaclass to allow :class:`Mongo` to be used as a context manager
    without having to instantiate it.

    """

    _connection = None

    def __new__(mcs, name, bases, cls_dict):
        """Return the Mongo class."""
        # This ensures that a late-declared class does not inherit an existing
        # connection object.
        cls_dict["_connection"] = None

        # Choose the correct connection class
        if cls_dict.get("config_connection_cls", UNSET) is UNSET:
            # Are we using a replica?
            # XXX: Getting the connection type at class creation time rather
            # than connection instantiation time means that disabling
            # config_replica (setting to None) at runtime has no effect. I
            # doubt anyone would ever do this, but you never know.
            _replica = cls_dict.get("config_replica", UNSET)
            # Handle attribute descriptors responsibly
            if _replica and hasattr(_replica, "__get__"):
                try:
                    _replica = _replica.__get__(None, None)
                except Exception:
                    raise TypeError(
                        "'%s.config_replica' appears to be a "
                        "descriptor and its value could not be "
                        "retrieved reliably." % name
                    )
            if _version._gte("4.0"):
                # It's all the same above Pymongo 4.x
                conn = pymongo.MongoClient
            elif _replica:
                # Handle replica set connections
                if _version._lt("2.1"):
                    raise TypeError("Need pymongo.version >= 2.1 for replica sets.")
                elif _version._gte("3.0.0"):
                    conn = pymongo.MongoClient
                elif _version._gte("2.4"):
                    conn = pymongo.MongoReplicaSetClient
                else:
                    conn = pymongo.ReplicaSetConnection
            else:
                # Get the correct regular connection
                if _version._gte("2.4"):
                    conn = pymongo.MongoClient
                else:
                    conn = pymongo.Connection
            # Set our connection type
            cls_dict["config_connection_cls"] = conn

        # Specially handle base class
        if name == "Mongo" and bases == (object,):
            # Create thread local self
            cls_dict["_self"] = threading.local()
            return type.__new__(mcs, name, bases, cls_dict)

        if cls_dict.get("config_uri", UNSET) is UNSET:
            # Ensure we have minimum configuration params
            if cls_dict.get("config_host", UNSET) is UNSET:
                raise TypeError("missing required 'config_host'")

            if cls_dict.get("config_port", UNSET) is UNSET:
                raise TypeError("missing required 'config_port'")

        # Validate if pymongo version supports SSL.
        if cls_dict.get("config_ssl", False) is True and _version._lt("2.1"):
            raise TypeError("Need pymongo.version >= 2.1 to use SSL.")

        # Create new class
        cls = type.__new__(mcs, name, bases, cls_dict)

        # This reload hook uses a closure to access the class
        @pyconfig.reload_hook
        def _reload():
            """A hook for reloading the connection settings with pyconfig."""
            cls.reconnect()

        return cls

    def start(cls):
        """Public function for manually starting a session/context. Use
        carefully!
        """
        if cls in Mongo.contexts:
            raise NestedConnection(
                "Do not nest a connection within itself, it "
                "may cause undefined behavior."
            )
        if pyconfig.get("humbledb.allow_explicit_request", True) and _version._lt(
            "3.0.0"
        ):
            cls.connection.start_request()
        Mongo.contexts.append(cls)

    def end(cls):
        """Public function for manually closing a session/context. Should be
        idempotent. This must always be called after :meth:`Mongo.start`
        to ensure the socket is returned to the connection pool.
        """
        if pyconfig.get("humbledb.allow_explicit_request", True) and _version._lt(
            "3.0.0"
        ):
            cls.connection.end_request()
        try:
            Mongo.contexts.pop()
        except (IndexError, AttributeError):
            pass

    def reconnect(cls):
        """Replace the current connection with a new connection."""
        logging.getLogger(__name__).info("Reloading '{}'".format(cls.__name__))
        if cls._connection and _version._lt("3.0.0"):
            cls._connection.disconnect()
        cls._connection = cls._new_connection()

    def __enter__(cls):
        cls.start()

    def __exit__(cls, exc_type, exc_val, exc_tb):
        cls.end()


class Mongo(object, metaclass=MongoMeta):
    """
    Singleton context manager class for managing a single
    :class:`pymongo.connection.Connection` instance.  It is necessary that
    there only be one connection instance for pymongo to work efficiently with
    gevent or threading by using its built in connection pooling.

    This class also manages connection scope, so that we can prevent
    :class:`~humbledb.document.Document` instances from accessing the
    connection outside the context manager scope. This is so that we always
    ensure that :meth:`~pymongo.connection.Connection.end_request` is always
    called to release the socket back into the connection pool, and to restrict
    the scope where a socket is in use from the pool to the absolute minimum
    necessary.

    This class is made to be thread safe.

    Example subclass::

        class MyConnection(Mongo):
            config_host = 'cluster1.mongo.mydomain.com'
            config_port = 27017

    Example usage::

        with MyConnection:
            doc = MyDoc.find_one()

    """

    _self = None

    config_uri = UNSET
    """ A MongoDB URI to connect to. """

    config_host = "localhost"
    """ The host name or address to connect to. """

    config_port = 27017
    """ The port to connect to. """

    config_replica = None
    """ If you're connecting to a replica set, this holds its name. """

    config_connection_cls = UNSET
    """ This defines the connection class to use. HumbleDB will try to
    intelligently choose a class based on your replica settings and PyMongo
    version. """

    config_max_pool_size = pyconfig.setting("humbledb.connection_pool", 300)
    """ This specifies the max_pool_size of the connection. """

    config_auto_start_request = pyconfig.setting("humbledb.auto_start_request", True)
    """ This specifies the auto_start_request option to the connection. """

    config_use_greenlets = pyconfig.setting("humbledb.use_greenlets", False)
    """ This specifies the use_greenlets option to the connection. """

    config_tz_aware = pyconfig.setting("humbledb.tz_aware", True)
    """ This specifies the tz_aware option to the connection. """

    config_write_concern = pyconfig.setting("humbledb.write_concern", 1)
    """ This specifies the write concern (``w=``) for this connection. This was
        added so that Pymongo before 2.4 will by default use
        ``getLastError()``.

        .. versionadded: 4.0

    """

    config_ssl = pyconfig.setting("humbledb.ssl", False)
    """ Specifies whether or not to use SSL for a connection.
        .. versionadded: 5.5
    """

    config_mongo_client = pyconfig.setting("humbledb.mongo_client", {})
    """ Allows free-form ``pymongo.MongoClient`` constructor parameters to be
        passed to this connection to support new features.
        .. versionadded: 5.6
    """

    def __new__(cls):
        """This class cannot be instantiated."""
        return cls

    @classmethod
    def _new_connection(cls):
        """Return a new connection to this class' database."""
        kwargs = cls._connection_info()

        kwargs.update(
            {
                "max_pool_size": cls.config_max_pool_size,
                "auto_start_request": cls.config_auto_start_request,
                "use_greenlets": cls.config_use_greenlets,
                "tz_aware": cls.config_tz_aware,
                "w": cls.config_write_concern,
                "ssl": cls.config_ssl,
            }
        )

        kwargs.update(cls.config_mongo_client)

        _version._clean_connection_kwargs(kwargs)

        if cls.config_replica:
            kwargs["replicaSet"] = cls.config_replica
            logging.getLogger(__name__).info(
                "Creating new MongoDB connection to '{}:{}' replica: {}".format(
                    cls.config_host, cls.config_port, cls.config_replica
                )
            )
        else:
            logging.getLogger(__name__).info(
                "Creating new MongoDB connection to '{}:{}'".format(
                    cls.config_host, cls.config_port
                )
            )

        return cls.config_connection_cls(**kwargs)

    @classmethod
    def _connection_info(cls):
        """
        Return a dictionary containing the connection info based on
        `config_host` and `config_port` or `config_uri`. If `config_uri` is
        specified, it takes precedence.

        """
        if cls.config_uri:
            return {"host": cls.config_uri}

        return {"host": cls.config_host, "port": cls.config_port}

    @classproperty
    def connection(cls):
        """Return the current connection. If no connection exists, one is
        created.
        """
        if not cls._connection:
            cls._connection = cls._new_connection()
        return cls._connection

    @classproperty
    def contexts(cls):
        """Return the current context stack."""
        if not hasattr(Mongo._self, "contexts"):
            Mongo._self.contexts = []
        return Mongo._self.contexts

    @classproperty
    def context(cls):
        """Return the current context (a :class:`.Mongo` subclass) if it
        exists or ``None``.
        """
        try:
            return Mongo.contexts[-1]
        except IndexError:
            return None

    @classproperty
    def database(cls):
        """
        Return the default database for this connection.

        .. versionadded:: 5.3.0

        .. note:: This requires ``pymongo >= 2.6.0``.

        """
        if _version._lt("2.6.0"):
            return None
        try:
            return cls.connection.get_default_database()
        except pymongo.errors.ConfigurationError:
            return None
