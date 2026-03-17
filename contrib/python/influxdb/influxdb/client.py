# -*- coding: utf-8 -*-
"""Python client for InfluxDB."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import gzip
import itertools
import io
import json
import random
import socket
import struct
import time
from itertools import chain, islice

import msgpack
import requests
import requests.exceptions
from requests.adapters import HTTPAdapter
from six.moves.urllib.parse import urlparse

from influxdb.line_protocol import make_lines, quote_ident, quote_literal
from influxdb.resultset import ResultSet
from .exceptions import InfluxDBClientError
from .exceptions import InfluxDBServerError


class InfluxDBClient(object):
    """InfluxDBClient primary client object to connect InfluxDB.

    The :class:`~.InfluxDBClient` object holds information necessary to
    connect to InfluxDB. Requests can be made to InfluxDB directly through
    the client.

    The client supports the use as a `context manager
    <https://docs.python.org/3/reference/datamodel.html#context-managers>`_.

    :param host: hostname to connect to InfluxDB, defaults to 'localhost'
    :type host: str
    :param port: port to connect to InfluxDB, defaults to 8086
    :type port: int
    :param username: user to connect, defaults to 'root'
    :type username: str
    :param password: password of the user, defaults to 'root'
    :type password: str
    :param pool_size: urllib3 connection pool size, defaults to 10.
    :type pool_size: int
    :param database: database name to connect to, defaults to None
    :type database: str
    :param ssl: use https instead of http to connect to InfluxDB, defaults to
        False
    :type ssl: bool
    :param verify_ssl: verify SSL certificates for HTTPS requests, defaults to
        False
    :type verify_ssl: bool
    :param timeout: number of seconds Requests will wait for your client to
        establish a connection, defaults to None
    :type timeout: int
    :param retries: number of attempts your client will make before aborting,
        defaults to 3
        0 - try until success
        1 - attempt only once (without retry)
        2 - maximum two attempts (including one retry)
        3 - maximum three attempts (default option)
    :type retries: int
    :param use_udp: use UDP to connect to InfluxDB, defaults to False
    :type use_udp: bool
    :param udp_port: UDP port to connect to InfluxDB, defaults to 4444
    :type udp_port: int
    :param proxies: HTTP(S) proxy to use for Requests, defaults to {}
    :type proxies: dict
    :param path: path of InfluxDB on the server to connect, defaults to ''
    :type path: str
    :param cert: Path to client certificate information to use for mutual TLS
        authentication. You can specify a local cert to use
        as a single file containing the private key and the certificate, or as
        a tuple of both files’ paths, defaults to None
    :type cert: str
    :param gzip: use gzip content encoding to compress requests
    :type gzip: bool
    :param session: allow for the new client request to use an existing
        requests Session, defaults to None
    :type session: requests.Session
    :param headers: headers to add to Requests, will add 'Content-Type'
        and 'Accept' unless these are already present, defaults to {}
    :type headers: dict
    :param socket_options: use custom tcp socket options,
        If not specified, then defaults are loaded from
        ``HTTPConnection.default_socket_options``
    :type socket_options: list

    :raises ValueError: if cert is provided but ssl is disabled (set to False)
    """

    def __init__(self,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None,
                 ssl=False,
                 verify_ssl=False,
                 timeout=None,
                 retries=3,
                 use_udp=False,
                 udp_port=4444,
                 proxies=None,
                 pool_size=10,
                 path='',
                 cert=None,
                 gzip=False,
                 session=None,
                 headers=None,
                 socket_options=None,
                 ):
        """Construct a new InfluxDBClient object."""
        self.__host = host
        self.__port = int(port)
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout
        self._retries = retries

        self._verify_ssl = verify_ssl

        self.__use_udp = use_udp
        self.__udp_port = int(udp_port)

        if not session:
            session = requests.Session()

        self._session = session
        adapter = _SocketOptionsAdapter(
            pool_connections=int(pool_size),
            pool_maxsize=int(pool_size),
            socket_options=socket_options
        )

        if use_udp:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        if not path:
            self.__path = ''
        elif path[0] == '/':
            self.__path = path
        else:
            self.__path = '/' + path

        self._scheme = "http"

        if ssl is True:
            self._scheme = "https"

        self._session.mount(self._scheme + '://', adapter)

        if proxies is None:
            self._proxies = {}
        else:
            self._proxies = proxies

        if cert:
            if not ssl:
                raise ValueError(
                    "Client certificate provided but ssl is disabled."
                )
            else:
                self._session.cert = cert

        self.__baseurl = "{0}://{1}:{2}{3}".format(
            self._scheme,
            self._host,
            self._port,
            self._path)

        if headers is None:
            headers = {}
        headers.setdefault('Content-Type', 'application/json')
        headers.setdefault('Accept', 'application/x-msgpack')
        self._headers = headers

        self._gzip = gzip

    def __enter__(self):
        """Enter function as used by context manager."""
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """Exit function as used by context manager."""
        self.close()

    @property
    def _baseurl(self):
        return self.__baseurl

    @property
    def _host(self):
        return self.__host

    @property
    def _port(self):
        return self.__port

    @property
    def _path(self):
        return self.__path

    @property
    def _udp_port(self):
        return self.__udp_port

    @property
    def _use_udp(self):
        return self.__use_udp

    @classmethod
    def from_dsn(cls, dsn, **kwargs):
        r"""Generate an instance of InfluxDBClient from given data source name.

        Return an instance of :class:`~.InfluxDBClient` from the provided
        data source name. Supported schemes are "influxdb", "https+influxdb"
        and "udp+influxdb". Parameters for the :class:`~.InfluxDBClient`
        constructor may also be passed to this method.

        :param dsn: data source name
        :type dsn: string
        :param kwargs: additional parameters for `InfluxDBClient`
        :type kwargs: dict
        :raises ValueError: if the provided DSN has any unexpected values

        :Example:

        ::

            >> cli = InfluxDBClient.from_dsn('influxdb://username:password@\
            localhost:8086/databasename', timeout=5)
            >> type(cli)
            <class 'influxdb.client.InfluxDBClient'>
            >> cli = InfluxDBClient.from_dsn('udp+influxdb://username:pass@\
            localhost:8086/databasename', timeout=5, udp_port=159)
            >> print('{0._baseurl} - {0.use_udp} {0.udp_port}'.format(cli))
            http://localhost:8086 - True 159

        .. note:: parameters provided in `**kwargs` may override dsn parameters
        .. note:: when using "udp+influxdb" the specified port (if any) will
            be used for the TCP connection; specify the UDP port with the
            additional `udp_port` parameter (cf. examples).
        """
        init_args = _parse_dsn(dsn)
        host, port = init_args.pop('hosts')[0]
        init_args['host'] = host
        init_args['port'] = port
        init_args.update(kwargs)

        return cls(**init_args)

    def switch_database(self, database):
        """Change the client's database.

        :param database: the name of the database to switch to
        :type database: str
        """
        self._database = database

    def switch_user(self, username, password):
        """Change the client's username.

        :param username: the username to switch to
        :type username: str
        :param password: the password for the username
        :type password: str
        """
        self._username = username
        self._password = password

    def request(self, url, method='GET', params=None, data=None, stream=False,
                expected_response_code=200, headers=None):
        """Make a HTTP request to the InfluxDB API.

        :param url: the path of the HTTP request, e.g. write, query, etc.
        :type url: str
        :param method: the HTTP method for the request, defaults to GET
        :type method: str
        :param params: additional parameters for the request, defaults to None
        :type params: dict
        :param data: the data of the request, defaults to None
        :type data: str
        :param stream: True if a query uses chunked responses
        :type stream: bool
        :param expected_response_code: the expected response code of
            the request, defaults to 200
        :type expected_response_code: int
        :param headers: headers to add to the request
        :type headers: dict
        :returns: the response from the request
        :rtype: :class:`requests.Response`
        :raises InfluxDBServerError: if the response code is any server error
            code (5xx)
        :raises InfluxDBClientError: if the response code is not the
            same as `expected_response_code` and is not a server error code
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if headers is None:
            headers = self._headers

        if params is None:
            params = {}

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        if self._gzip:
            # Receive and send compressed data
            headers.update({
                'Accept-Encoding': 'gzip',
                'Content-Encoding': 'gzip',
            })
            if data is not None:
                # For Py 2.7 compatability use Gzipfile
                compressed = io.BytesIO()
                with gzip.GzipFile(
                    compresslevel=9,
                    fileobj=compressed,
                    mode='w'
                ) as f:
                    f.write(data)
                data = compressed.getvalue()

        # Try to send the request more than once by default (see #103)
        retry = True
        _try = 0
        while retry:
            try:
                if "Authorization" in headers:
                    auth = (None, None)
                else:
                    auth = (self._username, self._password)
                response = self._session.request(
                    method=method,
                    url=url,
                    auth=auth if None not in auth else None,
                    params=params,
                    data=data,
                    stream=stream,
                    headers=headers,
                    proxies=self._proxies,
                    verify=self._verify_ssl,
                    timeout=self._timeout
                )
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.HTTPError,
                    requests.exceptions.Timeout):
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
                if not retry:
                    raise
                if method == "POST":
                    time.sleep((2 ** _try) * random.random() / 100.0)

        type_header = response.headers and response.headers.get("Content-Type")
        if type_header == "application/x-msgpack" and response.content:
            response._msgpack = msgpack.unpackb(
                packed=response.content,
                ext_hook=_msgpack_parse_hook,
                raw=False)
        else:
            response._msgpack = None

        def reformat_error(response):
            if response._msgpack:
                return json.dumps(response._msgpack, separators=(',', ':'))
            else:
                return response.content

        # if there's not an error, there must have been a successful response
        if 500 <= response.status_code < 600:
            raise InfluxDBServerError(reformat_error(response))
        elif response.status_code == expected_response_code:
            return response
        else:
            err_msg = reformat_error(response)
            raise InfluxDBClientError(err_msg, response.status_code)

    def write(self, data, params=None, expected_response_code=204,
              protocol='json'):
        """Write data to InfluxDB.

        :param data: the data to be written
        :type data: (if protocol is 'json') dict
                    (if protocol is 'line') sequence of line protocol strings
                                            or single string
        :param params: additional parameters for the request, defaults to None
        :type params: dict
        :param expected_response_code: the expected response code of the write
            operation, defaults to 204
        :type expected_response_code: int
        :param protocol: protocol of input data, either 'json' or 'line'
        :type protocol: str
        :returns: True, if the write operation is successful
        :rtype: bool
        """
        headers = self._headers.copy()
        headers['Content-Type'] = 'application/octet-stream'

        if params:
            precision = params.get('precision')
        else:
            precision = None

        if protocol == 'json':
            data = make_lines(data, precision).encode('utf-8')
        elif protocol == 'line':
            if isinstance(data, str):
                data = [data]
            data = ('\n'.join(data) + '\n').encode('utf-8')

        self.request(
            url="write",
            method='POST',
            params=params,
            data=data,
            expected_response_code=expected_response_code,
            headers=headers
        )
        return True

    @staticmethod
    def _read_chunked_response(response, raise_errors=True):
        for line in response.iter_lines():
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            data = json.loads(line)
            result_set = {}
            for result in data.get('results', []):
                for _key in result:
                    if isinstance(result[_key], list):
                        result_set.setdefault(
                            _key, []).extend(result[_key])
            yield ResultSet(result_set, raise_errors=raise_errors)

    def query(self,
              query,
              params=None,
              bind_params=None,
              epoch=None,
              expected_response_code=200,
              database=None,
              raise_errors=True,
              chunked=False,
              chunk_size=0,
              method="GET"):
        """Send a query to InfluxDB.

        .. danger::
            In order to avoid injection vulnerabilities (similar to `SQL
            injection <https://www.owasp.org/index.php/SQL_Injection>`_
            vulnerabilities), do not directly include untrusted data into the
            ``query`` parameter, use ``bind_params`` instead.

        :param query: the actual query string
        :type query: str

        :param params: additional parameters for the request,
            defaults to {}
        :type params: dict

        :param bind_params: bind parameters for the query:
            any variable in the query written as ``'$var_name'`` will be
            replaced with ``bind_params['var_name']``. Only works in the
            ``WHERE`` clause and takes precedence over ``params['params']``
        :type bind_params: dict

        :param epoch: response timestamps to be in epoch format either 'h',
            'm', 's', 'ms', 'u', or 'ns',defaults to `None` which is
            RFC3339 UTC format with nanosecond precision
        :type epoch: str

        :param expected_response_code: the expected status code of response,
            defaults to 200
        :type expected_response_code: int

        :param database: database to query, defaults to None
        :type database: str

        :param raise_errors: Whether or not to raise exceptions when InfluxDB
            returns errors, defaults to True
        :type raise_errors: bool

        :param chunked: Enable to use chunked responses from InfluxDB.
            With ``chunked`` enabled, one ResultSet is returned per chunk
            containing all results within that chunk
        :type chunked: bool

        :param chunk_size: Size of each chunk to tell InfluxDB to use.
        :type chunk_size: int

        :param method: the HTTP method for the request, defaults to GET
        :type method: str

        :returns: the queried data
        :rtype: :class:`~.ResultSet`
        """
        if params is None:
            params = {}

        if bind_params is not None:
            params_dict = json.loads(params.get('params', '{}'))
            params_dict.update(bind_params)
            params['params'] = json.dumps(params_dict)

        params['q'] = query
        params['db'] = database or self._database

        if epoch is not None:
            params['epoch'] = epoch

        if chunked:
            params['chunked'] = 'true'
            if chunk_size > 0:
                params['chunk_size'] = chunk_size

        if query.lower().startswith("select ") and " into " in query.lower():
            method = "POST"

        response = self.request(
            url="query",
            method=method,
            params=params,
            data=None,
            stream=chunked,
            expected_response_code=expected_response_code
        )

        data = response._msgpack
        if not data:
            if chunked:
                return self._read_chunked_response(response)
            data = response.json()

        results = [
            ResultSet(result, raise_errors=raise_errors)
            for result
            in data.get('results', [])
        ]

        # TODO(aviau): Always return a list. (This would be a breaking change)
        if len(results) == 1:
            return results[0]

        return results

    def write_points(self,
                     points,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     tags=None,
                     batch_size=None,
                     protocol='json',
                     consistency=None
                     ):
        """Write to multiple time series names.

        :param points: the list of points to be written in the database
        :type points: list of dictionaries, each dictionary represents a point
        :type points: (if protocol is 'json') list of dicts, where each dict
                                            represents a point.
                    (if protocol is 'line') sequence of line protocol strings.
        :param time_precision: Either 's', 'm', 'ms' or 'u', defaults to None
        :type time_precision: str
        :param database: the database to write the points to. Defaults to
            the client's current database
        :type database: str
        :param tags: a set of key-value pairs associated with each point. Both
            keys and values must be strings. These are shared tags and will be
            merged with point-specific tags, defaults to None
        :type tags: dict
        :param retention_policy: the retention policy for the points. Defaults
            to None
        :type retention_policy: str
        :param batch_size: value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation,
            defaults to None
        :type batch_size: int
        :param protocol: Protocol for writing data. Either 'line' or 'json'.
        :type protocol: str
        :param consistency: Consistency for the points.
            One of {'any','one','quorum','all'}.
        :type consistency: str
        :returns: True, if the operation is successful
        :rtype: bool

        .. note:: if no retention policy is specified, the default retention
            policy for the database is used
        """
        if batch_size and batch_size > 0:
            for batch in self._batches(points, batch_size):
                self._write_points(points=batch,
                                   time_precision=time_precision,
                                   database=database,
                                   retention_policy=retention_policy,
                                   tags=tags, protocol=protocol,
                                   consistency=consistency)
            return True

        return self._write_points(points=points,
                                  time_precision=time_precision,
                                  database=database,
                                  retention_policy=retention_policy,
                                  tags=tags, protocol=protocol,
                                  consistency=consistency)

    def ping(self):
        """Check connectivity to InfluxDB.

        :returns: The version of the InfluxDB the client is connected to
        """
        response = self.request(
            url="ping",
            method='GET',
            expected_response_code=204
        )

        return response.headers['X-Influxdb-Version']

    @staticmethod
    def _batches(iterable, size):
        # Iterate over an iterable producing iterables of batches. Based on:
        # http://code.activestate.com/recipes/303279-getting-items-in-batches/
        iterator = iter(iterable)
        while True:
            try:  # Try get the first element in the iterator...
                head = (next(iterator),)
            except StopIteration:
                return  # ...so that we can stop if there isn't one
            # Otherwise, lazily slice the rest of the batch
            rest = islice(iterator, size - 1)
            yield chain(head, rest)

    def _write_points(self,
                      points,
                      time_precision,
                      database,
                      retention_policy,
                      tags,
                      protocol='json',
                      consistency=None):
        if time_precision not in ['n', 'u', 'ms', 's', 'm', 'h', None]:
            raise ValueError(
                "Invalid time precision is given. "
                "(use 'n', 'u', 'ms', 's', 'm' or 'h')")

        if consistency not in ['any', 'one', 'quorum', 'all', None]:
            raise ValueError('Invalid consistency: {}'.format(consistency))

        if protocol == 'json':
            data = {
                'points': points
            }

            if tags is not None:
                data['tags'] = tags
        else:
            data = points

        params = {
            'db': database or self._database
        }

        if consistency is not None:
            params['consistency'] = consistency

        if time_precision is not None:
            params['precision'] = time_precision

        if retention_policy is not None:
            params['rp'] = retention_policy

        if self._use_udp:
            self.send_packet(
                data, protocol=protocol, time_precision=time_precision
            )
        else:
            self.write(
                data=data,
                params=params,
                expected_response_code=204,
                protocol=protocol
            )

        return True

    def get_list_database(self):
        """Get the list of databases in InfluxDB.

        :returns: all databases in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> dbs = client.get_list_database()
            >> dbs
            [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]
        """
        return list(self.query("SHOW DATABASES").get_points())

    def get_list_series(self, database=None, measurement=None, tags=None):
        """
        Query SHOW SERIES returns the distinct series in your database.

        FROM and WHERE clauses are optional.

        :param measurement: Show all series from a measurement
        :type id: string
        :param tags: Show all series that match given tags
        :type id: dict
        :param database: the database from which the series should be
            shows, defaults to client's current database
        :type database: str
        """
        database = database or self._database
        query_str = 'SHOW SERIES'

        if measurement:
            query_str += ' FROM "{0}"'.format(measurement)

        if tags:
            query_str += ' WHERE ' + ' and '.join(["{0}='{1}'".format(k, v)
                                                   for k, v in tags.items()])

        return list(
            itertools.chain.from_iterable(
                [
                    x.values()
                    for x in (self.query(query_str, database=database)
                              .get_points())
                ]
            )
        )

    def create_database(self, dbname):
        """Create a new database in InfluxDB.

        :param dbname: the name of the database to create
        :type dbname: str
        """
        self.query("CREATE DATABASE {0}".format(quote_ident(dbname)),
                   method="POST")

    def drop_database(self, dbname):
        """Drop a database from InfluxDB.

        :param dbname: the name of the database to drop
        :type dbname: str
        """
        self.query("DROP DATABASE {0}".format(quote_ident(dbname)),
                   method="POST")

    def get_list_measurements(self):
        """Get the list of measurements in InfluxDB.

        :returns: all measurements in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> dbs = client.get_list_measurements()
            >> dbs
            [{u'name': u'measurements1'},
             {u'name': u'measurements2'},
             {u'name': u'measurements3'}]
        """
        return list(self.query("SHOW MEASUREMENTS").get_points())

    def drop_measurement(self, measurement):
        """Drop a measurement from InfluxDB.

        :param measurement: the name of the measurement to drop
        :type measurement: str
        """
        self.query("DROP MEASUREMENT {0}".format(quote_ident(measurement)),
                   method="POST")

    def create_retention_policy(self, name, duration, replication,
                                database=None,
                                default=False, shard_duration="0s"):
        """Create a retention policy for a database.

        :param name: the name of the new retention policy
        :type name: str
        :param duration: the duration of the new retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
            and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. For infinite retention - meaning the data will
            never be deleted - use 'INF' for duration.
            The minimum retention period is 1 hour.
        :type duration: str
        :param replication: the replication of the retention policy
        :type replication: str
        :param database: the database for which the retention policy is
            created. Defaults to current client's database
        :type database: str
        :param default: whether or not to set the policy as default
        :type default: bool
        :param shard_duration: the shard duration of the retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported and
            mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. Infinite retention is not supported. As a workaround,
            specify a "1000w" duration to achieve an extremely long shard group
            duration. Defaults to "0s", which is interpreted by the database
            to mean the default value given the duration.
            The minimum shard group duration is 1 hour.
        :type shard_duration: str
        """
        query_string = \
            "CREATE RETENTION POLICY {0} ON {1} " \
            "DURATION {2} REPLICATION {3} SHARD DURATION {4}".format(
                quote_ident(name), quote_ident(database or self._database),
                duration, replication, shard_duration)

        if default is True:
            query_string += " DEFAULT"

        self.query(query_string, method="POST")

    def alter_retention_policy(self, name, database=None,
                               duration=None, replication=None,
                               default=None, shard_duration=None):
        """Modify an existing retention policy for a database.

        :param name: the name of the retention policy to modify
        :type name: str
        :param database: the database for which the retention policy is
            modified. Defaults to current client's database
        :type database: str
        :param duration: the new duration of the existing retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
            and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. For infinite retention, meaning the data will
            never be deleted, use 'INF' for duration.
            The minimum retention period is 1 hour.
        :type duration: str
        :param replication: the new replication of the existing
            retention policy
        :type replication: int
        :param default: whether or not to set the modified policy as default
        :type default: bool
        :param shard_duration: the shard duration of the retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported and
            mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. Infinite retention is not supported. As a workaround,
            specify a "1000w" duration to achieve an extremely long shard group
            duration.
            The minimum shard group duration is 1 hour.
        :type shard_duration: str

        .. note:: at least one of duration, replication, or default flag
            should be set. Otherwise the operation will fail.
        """
        query_string = (
            "ALTER RETENTION POLICY {0} ON {1}"
        ).format(quote_ident(name),
                 quote_ident(database or self._database))
        if duration:
            query_string += " DURATION {0}".format(duration)
        if shard_duration:
            query_string += " SHARD DURATION {0}".format(shard_duration)
        if replication:
            query_string += " REPLICATION {0}".format(replication)
        if default is True:
            query_string += " DEFAULT"

        self.query(query_string, method="POST")

    def drop_retention_policy(self, name, database=None):
        """Drop an existing retention policy for a database.

        :param name: the name of the retention policy to drop
        :type name: str
        :param database: the database for which the retention policy is
            dropped. Defaults to current client's database
        :type database: str
        """
        query_string = (
            "DROP RETENTION POLICY {0} ON {1}"
        ).format(quote_ident(name), quote_ident(database or self._database))
        self.query(query_string, method="POST")

    def get_list_retention_policies(self, database=None):
        """Get the list of retention policies for a database.

        :param database: the name of the database, defaults to the client's
            current database
        :type database: str
        :returns: all retention policies for the database
        :rtype: list of dictionaries

        :Example:

        ::

            >> ret_policies = client.get_list_retention_policies('my_db')
            >> ret_policies
            [{u'default': True,
              u'duration': u'0',
              u'name': u'default',
              u'replicaN': 1}]
        """
        if not (database or self._database):
            raise InfluxDBClientError(
                "get_list_retention_policies() requires a database as a "
                "parameter or the client to be using a database")

        rsp = self.query(
            "SHOW RETENTION POLICIES ON {0}".format(
                quote_ident(database or self._database))
        )
        return list(rsp.get_points())

    def get_list_users(self):
        """Get the list of all users in InfluxDB.

        :returns: all users in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> users = client.get_list_users()
            >> users
            [{u'admin': True, u'user': u'user1'},
             {u'admin': False, u'user': u'user2'},
             {u'admin': False, u'user': u'user3'}]
        """
        return list(self.query("SHOW USERS").get_points())

    def create_user(self, username, password, admin=False):
        """Create a new user in InfluxDB.

        :param username: the new username to create
        :type username: str
        :param password: the password for the new user
        :type password: str
        :param admin: whether the user should have cluster administration
            privileges or not
        :type admin: boolean
        """
        text = "CREATE USER {0} WITH PASSWORD {1}".format(
            quote_ident(username), quote_literal(password))
        if admin:
            text += ' WITH ALL PRIVILEGES'
        self.query(text, method="POST")

    def drop_user(self, username):
        """Drop a user from InfluxDB.

        :param username: the username to drop
        :type username: str
        """
        text = "DROP USER {0}".format(quote_ident(username))
        self.query(text, method="POST")

    def set_user_password(self, username, password):
        """Change the password of an existing user.

        :param username: the username who's password is being changed
        :type username: str
        :param password: the new password for the user
        :type password: str
        """
        text = "SET PASSWORD FOR {0} = {1}".format(
            quote_ident(username), quote_literal(password))
        self.query(text)

    def delete_series(self, database=None, measurement=None, tags=None):
        """Delete series from a database.

        Series must be filtered by either measurement and tags.
        This method cannot be used to delete all series, use
        `drop_database` instead.

        :param database: the database from which the series should be
            deleted, defaults to client's current database
        :type database: str
        :param measurement: Delete all series from a measurement
        :type measurement: str
        :param tags: Delete all series that match given tags
        :type tags: dict
        """
        database = database or self._database
        query_str = 'DROP SERIES'
        if measurement:
            query_str += ' FROM {0}'.format(quote_ident(measurement))

        if tags:
            tag_eq_list = ["{0}={1}".format(quote_ident(k), quote_literal(v))
                           for k, v in tags.items()]
            query_str += ' WHERE ' + ' AND '.join(tag_eq_list)
        self.query(query_str, database=database, method="POST")

    def grant_admin_privileges(self, username):
        """Grant cluster administration privileges to a user.

        :param username: the username to grant privileges to
        :type username: str

        .. note:: Only a cluster administrator can create/drop databases
            and manage users.
        """
        text = "GRANT ALL PRIVILEGES TO {0}".format(quote_ident(username))
        self.query(text, method="POST")

    def revoke_admin_privileges(self, username):
        """Revoke cluster administration privileges from a user.

        :param username: the username to revoke privileges from
        :type username: str

        .. note:: Only a cluster administrator can create/ drop databases
            and manage users.
        """
        text = "REVOKE ALL PRIVILEGES FROM {0}".format(quote_ident(username))
        self.query(text, method="POST")

    def grant_privilege(self, privilege, database, username):
        """Grant a privilege on a database to a user.

        :param privilege: the privilege to grant, one of 'read', 'write'
            or 'all'. The string is case-insensitive
        :type privilege: str
        :param database: the database to grant the privilege on
        :type database: str
        :param username: the username to grant the privilege to
        :type username: str
        """
        text = "GRANT {0} ON {1} TO {2}".format(privilege,
                                                quote_ident(database),
                                                quote_ident(username))
        self.query(text, method="POST")

    def revoke_privilege(self, privilege, database, username):
        """Revoke a privilege on a database from a user.

        :param privilege: the privilege to revoke, one of 'read', 'write'
            or 'all'. The string is case-insensitive
        :type privilege: str
        :param database: the database to revoke the privilege on
        :type database: str
        :param username: the username to revoke the privilege from
        :type username: str
        """
        text = "REVOKE {0} ON {1} FROM {2}".format(privilege,
                                                   quote_ident(database),
                                                   quote_ident(username))
        self.query(text, method="POST")

    def get_list_privileges(self, username):
        """Get the list of all privileges granted to given user.

        :param username: the username to get privileges of
        :type username: str

        :returns: all privileges granted to given user
        :rtype: list of dictionaries

        :Example:

        ::

            >> privileges = client.get_list_privileges('user1')
            >> privileges
            [{u'privilege': u'WRITE', u'database': u'db1'},
             {u'privilege': u'ALL PRIVILEGES', u'database': u'db2'},
             {u'privilege': u'NO PRIVILEGES', u'database': u'db3'}]
        """
        text = "SHOW GRANTS FOR {0}".format(quote_ident(username))
        return list(self.query(text).get_points())

    def get_list_continuous_queries(self):
        """Get the list of continuous queries in InfluxDB.

        :return: all CQs in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> cqs = client.get_list_cqs()
            >> cqs
            [
                {
                    u'db1': []
                },
                {
                    u'db2': [
                        {
                            u'name': u'vampire',
                            u'query': u'CREATE CONTINUOUS QUERY vampire ON '
                                       'mydb BEGIN SELECT count(dracula) INTO '
                                       'mydb.autogen.all_of_them FROM '
                                       'mydb.autogen.one GROUP BY time(5m) END'
                        }
                    ]
                }
            ]
        """
        query_string = "SHOW CONTINUOUS QUERIES"
        return [{sk[0]: list(p)} for sk, p in self.query(query_string).items()]

    def create_continuous_query(self, name, select, database=None,
                                resample_opts=None):
        r"""Create a continuous query for a database.

        :param name: the name of continuous query to create
        :type name: str
        :param select: select statement for the continuous query
        :type select: str
        :param database: the database for which the continuous query is
            created. Defaults to current client's database
        :type database: str
        :param resample_opts: resample options
        :type resample_opts: str

        :Example:

        ::

            >> select_clause = 'SELECT mean("value") INTO "cpu_mean" ' \
            ...                 'FROM "cpu" GROUP BY time(1m)'
            >> client.create_continuous_query(
            ...     'cpu_mean', select_clause, 'db_name', 'EVERY 10s FOR 2m'
            ... )
            >> client.get_list_continuous_queries()
            [
                {
                    'db_name': [
                        {
                            'name': 'cpu_mean',
                            'query': 'CREATE CONTINUOUS QUERY "cpu_mean" '
                                    'ON "db_name" '
                                    'RESAMPLE EVERY 10s FOR 2m '
                                    'BEGIN SELECT mean("value") '
                                    'INTO "cpu_mean" FROM "cpu" '
                                    'GROUP BY time(1m) END'
                        }
                    ]
                }
            ]
        """
        query_string = (
            "CREATE CONTINUOUS QUERY {0} ON {1}{2} BEGIN {3} END"
        ).format(quote_ident(name), quote_ident(database or self._database),
                 ' RESAMPLE ' + resample_opts if resample_opts else '', select)
        self.query(query_string)

    def drop_continuous_query(self, name, database=None):
        """Drop an existing continuous query for a database.

        :param name: the name of continuous query to drop
        :type name: str
        :param database: the database for which the continuous query is
            dropped. Defaults to current client's database
        :type database: str
        """
        query_string = (
            "DROP CONTINUOUS QUERY {0} ON {1}"
        ).format(quote_ident(name), quote_ident(database or self._database))
        self.query(query_string)

    def send_packet(self, packet, protocol='json', time_precision=None):
        """Send an UDP packet.

        :param packet: the packet to be sent
        :type packet: (if protocol is 'json') dict
                      (if protocol is 'line') list of line protocol strings
        :param protocol: protocol of input data, either 'json' or 'line'
        :type protocol: str
        :param time_precision: Either 's', 'm', 'ms' or 'u', defaults to None
        :type time_precision: str
        """
        if protocol == 'json':
            data = make_lines(packet, time_precision).encode('utf-8')
        elif protocol == 'line':
            data = ('\n'.join(packet) + '\n').encode('utf-8')
        self.udp_socket.sendto(data, (self._host, self._udp_port))

    def close(self):
        """Close http session."""
        if isinstance(self._session, requests.Session):
            self._session.close()


def _parse_dsn(dsn):
    """Parse data source name.

    This is a helper function to split the data source name provided in
    the from_dsn classmethod
    """
    conn_params = urlparse(dsn)
    init_args = {}
    scheme_info = conn_params.scheme.split('+')
    if len(scheme_info) == 1:
        scheme = scheme_info[0]
        modifier = None
    else:
        modifier, scheme = scheme_info

    if scheme != 'influxdb':
        raise ValueError('Unknown scheme "{0}".'.format(scheme))

    if modifier:
        if modifier == 'udp':
            init_args['use_udp'] = True
        elif modifier == 'https':
            init_args['ssl'] = True
        else:
            raise ValueError('Unknown modifier "{0}".'.format(modifier))

    netlocs = conn_params.netloc.split(',')

    init_args['hosts'] = []
    for netloc in netlocs:
        parsed = _parse_netloc(netloc)
        init_args['hosts'].append((parsed['host'], int(parsed['port'])))
        init_args['username'] = parsed['username']
        init_args['password'] = parsed['password']

    if conn_params.path and len(conn_params.path) > 1:
        init_args['database'] = conn_params.path[1:]

    return init_args


def _parse_netloc(netloc):
    info = urlparse("http://{0}".format(netloc))
    return {'username': info.username or None,
            'password': info.password or None,
            'host': info.hostname or 'localhost',
            'port': info.port or 8086}


def _msgpack_parse_hook(code, data):
    if code == 5:
        (epoch_s, epoch_ns) = struct.unpack(">QI", data)
        timestamp = datetime.datetime.utcfromtimestamp(epoch_s)
        timestamp += datetime.timedelta(microseconds=(epoch_ns / 1000))
        return timestamp.isoformat() + 'Z'
    return msgpack.ExtType(code, data)


class _SocketOptionsAdapter(HTTPAdapter):
    """_SocketOptionsAdapter injects socket_options into HTTP Adapter."""

    def __init__(self, *args, **kwargs):
        self.socket_options = kwargs.pop("socket_options", None)
        super(_SocketOptionsAdapter, self).__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if self.socket_options is not None:
            kwargs["socket_options"] = self.socket_options
        super(_SocketOptionsAdapter, self).init_poolmanager(*args, **kwargs)
