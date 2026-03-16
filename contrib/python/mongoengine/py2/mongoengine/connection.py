from pymongo import MongoClient, ReadPreference, uri_parser
import six

from mongoengine.pymongo_support import IS_PYMONGO_3

__all__ = ['MongoEngineConnectionError', 'connect', 'register_connection',
           'DEFAULT_CONNECTION_NAME', 'get_db']


DEFAULT_CONNECTION_NAME = 'default'

if IS_PYMONGO_3:
    READ_PREFERENCE = ReadPreference.PRIMARY
else:
    from pymongo import MongoReplicaSetClient
    READ_PREFERENCE = False


class MongoEngineConnectionError(Exception):
    """Error raised when the database connection can't be established or
    when a connection with a requested alias can't be retrieved.
    """
    pass


_connection_settings = {}
_connections = {}
_dbs = {}


def register_connection(alias, db=None, name=None, host=None, port=None,
                        read_preference=READ_PREFERENCE,
                        username=None, password=None,
                        authentication_source=None,
                        authentication_mechanism=None,
                        **kwargs):
    """Add a connection.

    :param alias: the name that will be used to refer to this connection
        throughout MongoEngine
    :param name: the name of the specific database to use
    :param db: the name of the database to use, for compatibility with connect
    :param host: the host name of the :program:`mongod` instance to connect to
    :param port: the port that the :program:`mongod` instance is running on
    :param read_preference: The read preference for the collection
       ** Added pymongo 2.1
    :param username: username to authenticate with
    :param password: password to authenticate with
    :param authentication_source: database to authenticate against
    :param authentication_mechanism: database authentication mechanisms.
        By default, use SCRAM-SHA-1 with MongoDB 3.0 and later,
        MONGODB-CR (MongoDB Challenge Response protocol) for older servers.
    :param is_mock: explicitly use mongomock for this connection
        (can also be done by using `mongomock://` as db host prefix)
    :param kwargs: ad-hoc parameters to be passed into the pymongo driver,
        for example maxpoolsize, tz_aware, etc. See the documentation
        for pymongo's `MongoClient` for a full list.

    .. versionchanged:: 0.10.6 - added mongomock support
    """
    conn_settings = {
        'name': name or db or 'test',
        'host': host or 'localhost',
        'port': port or 27017,
        'read_preference': read_preference,
        'username': username,
        'password': password,
        'authentication_source': authentication_source,
        'authentication_mechanism': authentication_mechanism
    }

    conn_host = conn_settings['host']

    # Host can be a list or a string, so if string, force to a list.
    if isinstance(conn_host, six.string_types):
        conn_host = [conn_host]

    resolved_hosts = []
    for entity in conn_host:

        # Handle Mongomock
        if entity.startswith('mongomock://'):
            conn_settings['is_mock'] = True
            # `mongomock://` is not a valid url prefix and must be replaced by `mongodb://`
            resolved_hosts.append(entity.replace('mongomock://', 'mongodb://', 1))

        # Handle URI style connections, only updating connection params which
        # were explicitly specified in the URI.
        elif '://' in entity:
            uri_dict = uri_parser.parse_uri(entity)
            resolved_hosts.append(entity)

            if uri_dict.get('database'):
                conn_settings['name'] = uri_dict.get('database')

            for param in ('read_preference', 'username', 'password'):
                if uri_dict.get(param):
                    conn_settings[param] = uri_dict[param]

            uri_options = uri_dict['options']
            if 'replicaset' in uri_options:
                conn_settings['replicaSet'] = uri_options['replicaset']
            if 'authsource' in uri_options:
                conn_settings['authentication_source'] = uri_options['authsource']
            if 'authmechanism' in uri_options:
                conn_settings['authentication_mechanism'] = uri_options['authmechanism']
            if IS_PYMONGO_3 and 'readpreference' in uri_options:
                read_preferences = (
                    ReadPreference.NEAREST,
                    ReadPreference.PRIMARY,
                    ReadPreference.PRIMARY_PREFERRED,
                    ReadPreference.SECONDARY,
                    ReadPreference.SECONDARY_PREFERRED)
                read_pf_mode = uri_options['readpreference'].lower()
                for preference in read_preferences:
                    if preference.name.lower() == read_pf_mode:
                        conn_settings['read_preference'] = preference
                        break
        else:
            resolved_hosts.append(entity)
    conn_settings['host'] = resolved_hosts

    # Deprecated parameters that should not be passed on
    kwargs.pop('slaves', None)
    kwargs.pop('is_slave', None)

    conn_settings.update(kwargs)
    _connection_settings[alias] = conn_settings


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Close the connection with a given alias."""
    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]
    if alias in _dbs:
        del _dbs[alias]


def get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    """Return a connection with a given alias."""

    # Connect to the database if not already connected
    if reconnect:
        disconnect(alias)

    # If the requested alias already exists in the _connections list, return
    # it immediately.
    if alias in _connections:
        return _connections[alias]

    # Validate that the requested alias exists in the _connection_settings.
    # Raise MongoEngineConnectionError if it doesn't.
    if alias not in _connection_settings:
        if alias == DEFAULT_CONNECTION_NAME:
            msg = 'You have not defined a default connection'
        else:
            msg = 'Connection with alias "%s" has not been defined' % alias
        raise MongoEngineConnectionError(msg)

    def _clean_settings(settings_dict):
        # set literal more efficient than calling set function
        irrelevant_fields_set = {
            'name', 'username', 'password',
            'authentication_source', 'authentication_mechanism'
        }
        return {
            k: v for k, v in settings_dict.items()
            if k not in irrelevant_fields_set
        }

    # Retrieve a copy of the connection settings associated with the requested
    # alias and remove the database name and authentication info (we don't
    # care about them at this point).
    conn_settings = _clean_settings(_connection_settings[alias].copy())

    # Determine if we should use PyMongo's or mongomock's MongoClient.
    is_mock = conn_settings.pop('is_mock', False)
    if is_mock:
        try:
            import mongomock
        except ImportError:
            raise RuntimeError('You need mongomock installed to mock '
                               'MongoEngine.')
        connection_class = mongomock.MongoClient
    else:
        connection_class = MongoClient

        # For replica set connections with PyMongo 2.x, use
        # MongoReplicaSetClient.
        # TODO remove this once we stop supporting PyMongo 2.x.
        if 'replicaSet' in conn_settings and not IS_PYMONGO_3:
            connection_class = MongoReplicaSetClient
            conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)

            # hosts_or_uri has to be a string, so if 'host' was provided
            # as a list, join its parts and separate them by ','
            if isinstance(conn_settings['hosts_or_uri'], list):
                conn_settings['hosts_or_uri'] = ','.join(
                    conn_settings['hosts_or_uri'])

            # Discard port since it can't be used on MongoReplicaSetClient
            conn_settings.pop('port', None)

    # Iterate over all of the connection settings and if a connection with
    # the same parameters is already established, use it instead of creating
    # a new one.
    existing_connection = None
    connection_settings_iterator = (
        (db_alias, settings.copy())
        for db_alias, settings in _connection_settings.items()
    )
    for db_alias, connection_settings in connection_settings_iterator:
        connection_settings = _clean_settings(connection_settings)
        if conn_settings == connection_settings and _connections.get(db_alias):
            existing_connection = _connections[db_alias]
            break

    # If an existing connection was found, assign it to the new alias
    if existing_connection:
        _connections[alias] = existing_connection
    else:
        # Otherwise, create the new connection for this alias. Raise
        # MongoEngineConnectionError if it can't be established.
        try:
            _connections[alias] = connection_class(**conn_settings)
        except Exception as e:
            raise MongoEngineConnectionError(
                'Cannot connect to database %s :\n%s' % (alias, e))

    return _connections[alias]


def get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    if reconnect:
        disconnect(alias)

    if alias not in _dbs:
        conn = get_connection(alias)
        conn_settings = _connection_settings[alias]
        db = conn[conn_settings['name']]
        auth_kwargs = {'source': conn_settings['authentication_source']}
        if conn_settings['authentication_mechanism'] is not None:
            auth_kwargs['mechanism'] = conn_settings['authentication_mechanism']
        # Authenticate if necessary
        if conn_settings['username'] and (conn_settings['password'] or
                                          conn_settings['authentication_mechanism'] == 'MONGODB-X509'):
            db.authenticate(conn_settings['username'], conn_settings['password'], **auth_kwargs)
        _dbs[alias] = db
    return _dbs[alias]


def connect(db=None, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases. Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    See the docstring for `register_connection` for more details about all
    supported kwargs.

    .. versionchanged:: 0.6 - added multiple database support.
    """
    if alias not in _connections:
        register_connection(alias, db, **kwargs)

    return get_connection(alias)


# Support old naming convention
_get_connection = get_connection
_get_db = get_db
