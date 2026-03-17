import re
from collections import deque
from contextlib import contextmanager
from time import time
import types
from urllib.parse import urlparse

from . import errors, defines
from .block import ColumnOrientedBlock, RowOrientedBlock
from .connection import Connection
from .log import log_block
from .protocol import ServerPacketTypes
from .result import (
    IterQueryResult, ProgressQueryResult, QueryResult, QueryInfo
)
from .util.escape import escape_params
from .util.helpers import column_chunks, chunks, parse_url


class Client(object):
    """
    Client for communication with the ClickHouse server.
    Single connection is established per each connected instance of the client.

    :param settings: Dictionary of settings that passed to every query (except
                     for the client settings, see below). Defaults to ``None``
                     (no additional settings). See all available settings in
                     `ClickHouse docs
                     <https://clickhouse.com/docs/en/operations/settings/settings/>`_.
    :param \\**kwargs: All other args are passed to the
                       :py:class:`~clickhouse_driver.connection.Connection`
                       constructor.

    The following keys when passed in ``settings`` are used for configuring the
    client itself:

        * ``insert_block_size`` -- chunk size to split rows for ``INSERT``.
          Defaults to ``1048576``.
        * ``strings_as_bytes`` -- turns off string column encoding/decoding.
        * ``strings_encoding`` -- specifies string encoding. UTF-8 by default.
        * ``use_numpy`` -- Use NumPy for columns reading. New in version
                           *0.2.0*.
        * ``opentelemetry_traceparent`` -- OpenTelemetry traceparent header as
                           described by W3C Trace Context recommendation.
                           New in version *0.2.2*.
        * ``opentelemetry_tracestate`` -- OpenTelemetry tracestate header as
                           described by W3C Trace Context recommendation.
                           New in version *0.2.2*.
        * ``quota_key`` -- A string to differentiate quotas when the user have
                           keyed quotas configured on server.
                           New in version *0.2.3*.
        * ``input_format_null_as_default`` -- Initialize null fields with
                           default values if data type of this field is not
                           nullable. Does not work for NumPy. Default: False.
                           New in version *0.2.4*.
        * ``round_robin`` -- If ``alt_hosts`` are provided the query will be
                           executed on host picked with round-robin algorithm.
                           New in version *0.2.5*.
        * ``namedtuple_as_json`` -- Controls named tuple and nested types
                           deserialization. To interpret these column alongside
                           with ``allow_experimental_object_type=1`` as Python
                           tuple set ``namedtuple_as_json`` to ``False``.
                           Default: True.
                           New in version *0.2.6*.
        * ``server_side_params`` -- Species on which side query parameters
                           should be rendered into placeholders.
                           Default: False. Means that parameters are rendered
                           on driver's side.
                           New in version *0.2.7*.
    """

    available_client_settings = (
        'insert_block_size',  # TODO: rename to max_insert_block_size
        'strings_as_bytes',
        'strings_encoding',
        'use_numpy',
        'opentelemetry_traceparent',
        'opentelemetry_tracestate',
        'quota_key',
        'input_format_null_as_default',
        'namedtuple_as_json',
        'server_side_params'
    )

    def __init__(self, *args, **kwargs):
        self.settings = (kwargs.pop('settings', None) or {}).copy()

        self.client_settings = {
            'insert_block_size': int(self.settings.pop(
                'insert_block_size', defines.DEFAULT_INSERT_BLOCK_SIZE,
            )),
            'strings_as_bytes': self.settings.pop(
                'strings_as_bytes', False
            ),
            'strings_encoding': self.settings.pop(
                'strings_encoding', defines.STRINGS_ENCODING
            ),
            'use_numpy': self.settings.pop(
                'use_numpy', False
            ),
            'opentelemetry_traceparent': self.settings.pop(
                'opentelemetry_traceparent', None
            ),
            'opentelemetry_tracestate': self.settings.pop(
                'opentelemetry_tracestate', ''
            ),
            'quota_key': self.settings.pop(
                'quota_key', ''
            ),
            'input_format_null_as_default': self.settings.pop(
                'input_format_null_as_default', False
            ),
            'namedtuple_as_json': self.settings.pop(
                'namedtuple_as_json', True
            ),
            'server_side_params': self.settings.pop(
                'server_side_params', False
            )
        }

        if self.client_settings['use_numpy']:
            try:
                from .numpy.result import (
                    NumpyIterQueryResult, NumpyProgressQueryResult,
                    NumpyQueryResult
                )
                self.query_result_cls = NumpyQueryResult
                self.iter_query_result_cls = NumpyIterQueryResult
                self.progress_query_result_cls = NumpyProgressQueryResult
            except ImportError:
                raise RuntimeError('Extras for NumPy must be installed')
        else:
            self.query_result_cls = QueryResult
            self.iter_query_result_cls = IterQueryResult
            self.progress_query_result_cls = ProgressQueryResult

        round_robin = kwargs.pop('round_robin', False)
        self.connections = deque([Connection(*args, **kwargs)])

        if round_robin and 'alt_hosts' in kwargs:
            alt_hosts = kwargs.pop('alt_hosts')
            for host in alt_hosts.split(','):
                url = urlparse('clickhouse://' + host)

                connection_kwargs = kwargs.copy()
                num_args = len(args)
                if num_args >= 2:
                    # host and port as positional arguments
                    connection_args = (url.hostname, url.port) + args[2:]
                elif num_args >= 1:
                    # host as positional and port as keyword argument
                    connection_args = (url.hostname, ) + args[1:]
                    connection_kwargs['port'] = url.port
                else:
                    # host and port as keyword arguments
                    connection_args = tuple()
                    connection_kwargs['host'] = url.hostname
                    connection_kwargs['port'] = url.port

                connection = Connection(*connection_args, **connection_kwargs)
                self.connections.append(connection)

        self.connection = self.get_connection()
        self.reset_last_query()
        super(Client, self).__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def get_connection(self):
        if hasattr(self, 'connection'):
            self.connections.append(self.connection)

        connection = self.connections.popleft()

        connection.context.settings = self.settings
        connection.context.client_settings = self.client_settings
        return connection

    def disconnect(self):
        self.disconnect_connection()
        for connection in self.connections:
            connection.disconnect()

    def disconnect_connection(self):
        """
        Disconnects from the server.
        """
        self.connection.disconnect()
        self.reset_last_query()

    def reset_last_query(self):
        self.last_query = None

    def receive_result(self, with_column_types=False, progress=False,
                       columnar=False):

        gen = self.packet_generator()

        if progress:
            return self.progress_query_result_cls(
                gen, with_column_types=with_column_types, columnar=columnar
            )

        else:
            result = self.query_result_cls(
                gen, with_column_types=with_column_types, columnar=columnar
            )
            return result.get_result()

    def iter_receive_result(self, with_column_types=False):
        gen = self.packet_generator()

        result = self.iter_query_result_cls(
            gen, with_column_types=with_column_types
        )

        for rows in result:
            for row in rows:
                yield row

    def packet_generator(self):
        while True:
            try:
                packet = self.receive_packet()
                if not packet:
                    break

                if packet is True:
                    continue

                yield packet

            except (Exception, KeyboardInterrupt):
                self.disconnect()
                raise

    def receive_packet(self):
        packet = self.connection.receive_packet()

        if packet.type == ServerPacketTypes.EXCEPTION:
            raise packet.exception

        elif packet.type == ServerPacketTypes.PROGRESS:
            self.last_query.store_progress(packet.progress)
            return packet

        elif packet.type == ServerPacketTypes.END_OF_STREAM:
            return False

        elif packet.type == ServerPacketTypes.DATA:
            return packet

        elif packet.type == ServerPacketTypes.TOTALS:
            return packet

        elif packet.type == ServerPacketTypes.EXTREMES:
            return packet

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            self.last_query.store_profile(packet.profile_info)
            return True

        else:
            return True

    def make_query_settings(self, settings):
        settings = dict(settings or {})

        # Pick client-related settings.
        client_settings = self.client_settings.copy()
        for key in self.available_client_settings:
            if key in settings:
                client_settings[key] = settings.pop(key)

        self.connection.context.client_settings = client_settings

        # The rest of settings are ClickHouse-related.
        query_settings = self.settings.copy()
        query_settings.update(settings)
        self.connection.context.settings = query_settings

    def track_current_database(self, query):
        query = query.strip('; ')
        if query.lower().startswith('use '):
            self.connection.database = query[4:].strip()

    def establish_connection(self, settings):
        num_connections = len(self.connections)
        if hasattr(self, 'connection'):
            num_connections += 1

        for i in range(num_connections):
            try:
                self.connection = self.get_connection()
                self.make_query_settings(settings)
                self.connection.force_connect()
                self.last_query = QueryInfo()

            except (errors.SocketTimeoutError, errors.NetworkError):
                if i < num_connections - 1:
                    continue
                raise

            return

    @contextmanager
    def disconnect_on_error(self, query, settings):
        try:
            self.establish_connection(settings)
            self.connection.server_info.session_timezone = None

            yield

            self.track_current_database(query)

        except (Exception, KeyboardInterrupt):
            self.disconnect()
            raise

    def execute(self, query, params=None, with_column_types=False,
                external_tables=None, query_id=None, settings=None,
                types_check=False, columnar=False):
        """
        Executes query.

        Establishes new connection if it wasn't established yet.
        After query execution connection remains intact for next queries.
        If connection can't be reused it will be closed and new connection will
        be created.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result of the SELECT query will be
                         returned in column-oriented form.
                         It also allows to INSERT data in columnar form.
                         Defaults to ``False`` (row-like form).

        :return: * number of inserted rows for INSERT queries with data.
                   Returning rows count from INSERT FROM SELECT is not
                   supported.
                 * if `with_column_types=False`: `list` of `tuples` with
                   rows/columns.
                 * if `with_column_types=True`: `tuple` of 2 elements:
                    * The first element is `list` of `tuples` with
                      rows/columns.
                    * The second element information is about columns: names
                      and types.
        """

        start_time = time()

        with self.disconnect_on_error(query, settings):
            # INSERT queries can use list/tuple/generator of list/tuples/dicts.
            # For SELECT parameters can be passed in only in dict right now.
            is_insert = isinstance(params, (list, tuple, types.GeneratorType))

            if is_insert:
                rv = self.process_insert_query(
                    query, params, external_tables=external_tables,
                    query_id=query_id, types_check=types_check,
                    columnar=columnar
                )
            else:
                rv = self.process_ordinary_query(
                    query, params=params, with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id, types_check=types_check,
                    columnar=columnar
                )
            self.last_query.store_elapsed(time() - start_time)
            return rv

    def execute_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False, columnar=False):
        """
        Executes SELECT query with progress information.
        See, :ref:`execute-with-progress`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result will be returned in
                         column-oriented form.
                         Defaults to ``False`` (row-like form).
        :return: :ref:`progress-query-result` proxy.
        """

        with self.disconnect_on_error(query, settings):
            return self.process_ordinary_query_with_progress(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables, query_id=query_id,
                types_check=types_check, columnar=columnar
            )

    def execute_iter(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False, chunk_size=1):
        """
        *New in version 0.0.14.*

        Executes SELECT query with results streaming. See, :ref:`execute-iter`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param chunk_size: chunk query results.
        :return: :ref:`iter-query-result` proxy.
        """
        with self.disconnect_on_error(query, settings):
            rv = self.iter_process_ordinary_query(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id, types_check=types_check
            )
            return chunks(rv, chunk_size) if chunk_size > 1 else rv

    def query_dataframe(
            self, query, params=None, external_tables=None, query_id=None,
            settings=None, replace_nonwords=True):
        """
        *New in version 0.2.0.*

        Queries DataFrame with specified SELECT query.

        :param query: query that will be send to server.
        :param params: substitution parameters.
                       Defaults to ``None`` (no parameters  or data).
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param replace_nonwords: boolean to replace non-words in column names
                                 to underscores. Defaults to ``True``.
        :return: pandas DataFrame.
        """

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('Extras for NumPy must be installed')

        data, columns = self.execute(
            query, columnar=True, with_column_types=True, params=params,
            external_tables=external_tables, query_id=query_id,
            settings=settings
        )

        columns = [name for name, type_ in columns]
        if replace_nonwords:
            columns = [re.sub(r'\W', '_', x) for x in columns]

        return pd.DataFrame(
            {col: d for d, col in zip(data, columns)}, columns=columns
        )

    def insert_dataframe(
            self, query, dataframe, external_tables=None, query_id=None,
            settings=None):
        """
        *New in version 0.2.0.*

        Inserts pandas DataFrame with specified query.

        :param query: query that will be send to server.
        :param dataframe: pandas DataFrame.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :return: number of inserted rows.
        """

        try:
            import pandas as pd  # noqa: F401
        except ImportError:
            raise RuntimeError('Extras for NumPy must be installed')

        start_time = time()

        with self.disconnect_on_error(query, settings):
            self.connection.send_query(query, query_id=query_id)
            self.connection.send_external_tables(external_tables)

            sample_block = self.receive_sample_block()
            rv = None
            if sample_block:
                columns = [x[0] for x in sample_block.columns_with_types]
                # raise if any columns are missing from the dataframe
                diff = set(columns) - set(dataframe.columns)
                if len(diff):
                    msg = "DataFrame missing required columns: {}"
                    raise ValueError(msg.format(list(diff)))

                data = [dataframe[column].values for column in columns]
                rv = self.send_data(sample_block, data, columnar=True)
                self.receive_end_of_query()

            self.last_query.store_elapsed(time() - start_time)
            return rv

    def process_ordinary_query_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )

        self.connection.send_query(query, query_id=query_id, params=params)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   progress=True, columnar=columnar)

    def process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )
        self.connection.send_query(query, query_id=query_id, params=params)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   columnar=columnar)

    def iter_process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )

        self.connection.send_query(query, query_id=query_id, params=params)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.iter_receive_result(with_column_types=with_column_types)

    def process_insert_query(self, query_without_data, data,
                             external_tables=None, query_id=None,
                             types_check=False, columnar=False):
        self.connection.send_query(query_without_data, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        sample_block = self.receive_sample_block()

        if sample_block:
            rv = self.send_data(sample_block, data,
                                types_check=types_check, columnar=columnar)
            self.receive_end_of_insert_query()
            return rv

    def receive_sample_block(self):
        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.DATA:
                return packet.block

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
                pass

            else:
                message = self.connection.unexpected_packet_message(
                    'Data, Exception, Log or TableColumns', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def send_data(self, sample_block, data, types_check=False, columnar=False):
        inserted_rows = 0

        client_settings = self.connection.context.client_settings
        block_cls = ColumnOrientedBlock if columnar else RowOrientedBlock

        if client_settings['use_numpy']:
            try:
                from .numpy.helpers import column_chunks as numpy_column_chunks

                if columnar:
                    slicer = numpy_column_chunks
                else:
                    raise ValueError(
                        'NumPy inserts is only allowed with columnar=True'
                    )

            except ImportError:
                raise RuntimeError('Extras for NumPy must be installed')

        else:
            slicer = column_chunks if columnar else chunks

        for chunk in slicer(data, client_settings['insert_block_size']):
            block = block_cls(sample_block.columns_with_types, chunk,
                              types_check=types_check)
            self.connection.send_data(block)
            inserted_rows += block.num_rows

            # Starting from the specific revision there are profile events
            # sent by server in response to each inserted block
            self.receive_profile_events()

        # Empty block means end of data.
        self.connection.send_data(block_cls())
        # If enabled by revision profile events are also sent after empty block
        self.receive_profile_events()

        return inserted_rows

    def receive_end_of_query(self):
        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.END_OF_STREAM:
                break

            elif packet.type == ServerPacketTypes.PROGRESS:
                self.last_query.store_progress(packet.progress)

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
                pass

            elif packet.type == ServerPacketTypes.PROFILE_EVENTS:
                self.last_query.store_profile(packet.profile_info)

            else:
                message = self.connection.unexpected_packet_message(
                    'Exception, EndOfStream, Progress, TableColumns, '
                    'ProfileEvents or Log', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def receive_end_of_insert_query(self):
        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.END_OF_STREAM:
                break

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.PROGRESS:
                self.last_query.store_progress(packet.progress)

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            else:
                message = self.connection.unexpected_packet_message(
                    'EndOfStream, Log, Progress or Exception', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def receive_profile_events(self):
        revision = self.connection.server_info.used_revision
        if (
            revision <
            defines.DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT
        ):
            return None

        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.PROFILE_EVENTS:
                self.last_query.store_profile(packet.profile_info)
                break

            elif packet.type == ServerPacketTypes.PROGRESS:
                self.last_query.store_progress(packet.progress)

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.TIMEZONE_UPDATE:
                pass

            else:
                message = self.connection.unexpected_packet_message(
                    'ProfileEvents, Progress, Log, Exception or '
                    'TimezoneUpdate', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def cancel(self, with_column_types=False):
        # TODO: Add warning if already cancelled.
        self.connection.send_cancel()
        # Client must still read until END_OF_STREAM packet.
        return self.receive_result(with_column_types=with_column_types)

    def substitute_params(self, query, params, context):
        """
        Substitutes parameters into a provided query.

        For example::

            client = Client(...)

            substituted_query = client.substitute_params(
                query='SELECT 1234, %(foo)s',
                params={'foo': 'bar'},
                context=client.connection.context
            )

            # prints: SELECT 1234, 'bar'
            print(substituted_query)
        """
        # In case of server side templating we don't substitute here.
        if self.connection.context.client_settings['server_side_params']:
            return query

        if not isinstance(params, dict):
            raise ValueError('Parameters are expected in dict form')

        escaped = escape_params(params, context)
        return query % escaped

    @classmethod
    def from_url(cls, url):
        """
        Return a client configured from the given URL.

        For example::

            clickhouse://[user:password]@localhost:9000/default
            clickhouses://[user:password]@localhost:9440/default

        Three URL schemes are supported:

            * clickhouse:// creates a normal TCP socket connection
            * clickhouses:// creates a SSL wrapped TCP socket connection

        Any additional querystring arguments will be passed along to
        the Connection class's initializer.
        """
        host, kwargs = parse_url(url)

        return cls(host, **kwargs)
