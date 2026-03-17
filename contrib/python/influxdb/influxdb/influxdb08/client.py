# -*- coding: utf-8 -*-
"""Python client for InfluxDB v0.8."""

import warnings

import json
import socket
import requests
import requests.exceptions
from six.moves import xrange
from six.moves.urllib.parse import urlparse

from influxdb import chunked_json

session = requests.Session()


class InfluxDBClientError(Exception):
    """Raised when an error occurs in the request."""

    def __init__(self, content, code=-1):
        """Initialize an InfluxDBClientError handler."""
        super(InfluxDBClientError, self).__init__(
            "{0}: {1}".format(code, content))
        self.content = content
        self.code = code


class InfluxDBClient(object):
    """Define the standard InfluxDBClient for influxdb v0.8.

    The ``InfluxDBClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.

    :param host: hostname to connect to InfluxDB, defaults to 'localhost'
    :type host: string
    :param port: port to connect to InfluxDB, defaults to 'localhost'
    :type port: int
    :param username: user to connect, defaults to 'root'
    :type username: string
    :param password: password of the user, defaults to 'root'
    :type password: string
    :param database: database name to connect to, defaults is None
    :type database: string
    :param ssl: use https instead of http to connect to InfluxDB, defaults is
        False
    :type ssl: boolean
    :param verify_ssl: verify SSL certificates for HTTPS requests, defaults is
        False
    :type verify_ssl: boolean
    :param retries: number of retries your client will try before aborting,
        defaults to 3. 0 indicates try until success
    :type retries: int
    :param timeout: number of seconds Requests will wait for your client to
        establish a connection, defaults to None
    :type timeout: int
    :param use_udp: use UDP to connect to InfluxDB, defaults is False
    :type use_udp: int
    :param udp_port: UDP port to connect to InfluxDB, defaults is 4444
    :type udp_port: int
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
                 udp_port=4444):
        """Construct a new InfluxDBClient object."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout
        self._retries = retries

        self._verify_ssl = verify_ssl

        self._use_udp = use_udp
        self._udp_port = udp_port
        if use_udp:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._scheme = "http"

        if ssl is True:
            self._scheme = "https"

        self._baseurl = "{0}://{1}:{2}".format(
            self._scheme,
            self._host,
            self._port)

        self._headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'}

    @staticmethod
    def from_dsn(dsn, **kwargs):
        r"""Return an instaance of InfluxDBClient from given data source name.

        Returns an instance of InfluxDBClient from the provided data source
        name. Supported schemes are "influxdb", "https+influxdb",
        "udp+influxdb". Parameters for the InfluxDBClient constructor may be
        also be passed to this function.

        Examples:
            >> cli = InfluxDBClient.from_dsn('influxdb://username:password@\
            ... localhost:8086/databasename', timeout=5)
            >> type(cli)
            <class 'influxdb.client.InfluxDBClient'>
            >> cli = InfluxDBClient.from_dsn('udp+influxdb://username:pass@\
            ... localhost:8086/databasename', timeout=5, udp_port=159)
            >> print('{0._baseurl} - {0.use_udp} {0.udp_port}'.format(cli))
            http://localhost:8086 - True 159

        :param dsn: data source name
        :type dsn: string
        :param **kwargs: additional parameters for InfluxDBClient.
        :type **kwargs: dict
        :note: parameters provided in **kwargs may override dsn parameters.
        :note: when using "udp+influxdb" the specified port (if any) will be
        used for the TCP connection; specify the udp port with the additional
        udp_port parameter (cf. examples).
        :raise ValueError: if the provided DSN has any unexpected value.

        """
        init_args = {}
        conn_params = urlparse(dsn)
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

        if conn_params.hostname:
            init_args['host'] = conn_params.hostname
        if conn_params.port:
            init_args['port'] = conn_params.port
        if conn_params.username:
            init_args['username'] = conn_params.username
        if conn_params.password:
            init_args['password'] = conn_params.password
        if conn_params.path and len(conn_params.path) > 1:
            init_args['database'] = conn_params.path[1:]

        init_args.update(kwargs)

        return InfluxDBClient(**init_args)

    # Change member variables

    def switch_database(self, database):
        """Change client database.

        :param database: the new database name to switch to
        :type database: string
        """
        self._database = database

    def switch_db(self, database):
        """Change client database.

        DEPRECATED.
        """
        warnings.warn(
            "switch_db is deprecated, and will be removed "
            "in future versions. Please use "
            "``InfluxDBClient.switch_database(database)`` instead.",
            FutureWarning)
        return self.switch_database(database)

    def switch_user(self, username, password):
        """Change client username.

        :param username: the new username to switch to
        :type username: string
        :param password: the new password to switch to
        :type password: string
        """
        self._username = username
        self._password = password

    def request(self, url, method='GET', params=None, data=None,
                expected_response_code=200):
        """Make a http request to API."""
        url = "{0}/{1}".format(self._baseurl, url)

        if params is None:
            params = {}

        auth = {
            'u': self._username,
            'p': self._password
        }

        params.update(auth)

        if data is not None and not isinstance(data, str):
            data = json.dumps(data)

        retry = True
        _try = 0
        # Try to send the request more than once by default (see #103)
        while retry:
            try:
                response = session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=self._headers,
                    verify=self._verify_ssl,
                    timeout=self._timeout
                )
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
        else:
            raise requests.exceptions.ConnectionError

        if response.status_code == expected_response_code:
            return response
        else:
            raise InfluxDBClientError(response.content, response.status_code)

    def write(self, data):
        """Provide as convenience for influxdb v0.9.0, this may change."""
        self.request(
            url="write",
            method='POST',
            params=None,
            data=data,
            expected_response_code=200
        )
        return True

    # Writing Data
    #
    # Assuming you have a database named foo_production you can write data
    # by doing a POST to /db/foo_production/series?u=some_user&p=some_password
    # with a JSON body of points.

    def write_points(self, data, time_precision='s', *args, **kwargs):
        """Write to multiple time series names.

        An example data blob is:

        data = [
            {
                "points": [
                    [
                        12
                    ]
                ],
                "name": "cpu_load_short",
                "columns": [
                    "value"
                ]
            }
        ]

        :param data: A list of dicts in InfluxDB 0.8.x data format.
        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int

        """
        def list_chunks(data_list, n):
            """Yield successive n-sized chunks from l."""
            for i in xrange(0, len(data_list), n):
                yield data_list[i:i + n]

        batch_size = kwargs.get('batch_size')
        if batch_size and batch_size > 0:
            for item in data:
                name = item.get('name')
                columns = item.get('columns')
                point_list = item.get('points', [])

                for batch in list_chunks(point_list, batch_size):
                    item = [{
                        "points": batch,
                        "name": name,
                        "columns": columns
                    }]
                    self._write_points(
                        data=item,
                        time_precision=time_precision)
            return True

        return self._write_points(data=data,
                                  time_precision=time_precision)

    def write_points_with_precision(self, data, time_precision='s'):
        """Write to multiple time series names.

        DEPRECATED.
        """
        warnings.warn(
            "write_points_with_precision is deprecated, and will be removed "
            "in future versions. Please use "
            "``InfluxDBClient.write_points(time_precision='..')`` instead.",
            FutureWarning)
        return self._write_points(data=data, time_precision=time_precision)

    def _write_points(self, data, time_precision):
        if time_precision not in ['s', 'm', 'ms', 'u']:
            raise Exception(
                "Invalid time precision is given. (use 's', 'm', 'ms' or 'u')")

        if self._use_udp and time_precision != 's':
            raise Exception(
                "InfluxDB only supports seconds precision for udp writes"
            )

        url = "db/{0}/series".format(self._database)

        params = {
            'time_precision': time_precision
        }

        if self._use_udp:
            self.send_packet(data)
        else:
            self.request(
                url=url,
                method='POST',
                params=params,
                data=data,
                expected_response_code=200
            )

        return True

    # One Time Deletes

    def delete_points(self, name):
        """Delete an entire series."""
        url = "db/{0}/series/{1}".format(self._database, name)

        self.request(
            url=url,
            method='DELETE',
            expected_response_code=204
        )

        return True

    # Regularly Scheduled Deletes

    def create_scheduled_delete(self, json_body):
        """Create schedule delete from database.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    # get list of deletes
    # curl http://localhost:8086/db/site_dev/scheduled_deletes
    #
    # remove a regularly scheduled delete
    # curl -X DELETE http://localhost:8086/db/site_dev/scheduled_deletes/:id

    def get_list_scheduled_delete(self):
        """Get list of scheduled deletes.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def remove_scheduled_delete(self, delete_id):
        """Remove scheduled delete.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def query(self, query, time_precision='s', chunked=False):
        """Query data from the influxdb v0.8 database.

        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param chunked: [Optional, default=False] True if the data shall be
            retrieved in chunks, False otherwise.
        """
        return self._query(query, time_precision=time_precision,
                           chunked=chunked)

    # Querying Data
    #
    # GET db/:name/series. It takes five parameters
    def _query(self, query, time_precision='s', chunked=False):
        if time_precision not in ['s', 'm', 'ms', 'u']:
            raise Exception(
                "Invalid time precision is given. (use 's', 'm', 'ms' or 'u')")

        if chunked is True:
            chunked_param = 'true'
        else:
            chunked_param = 'false'

        # Build the URL of the series to query
        url = "db/{0}/series".format(self._database)

        params = {
            'q': query,
            'time_precision': time_precision,
            'chunked': chunked_param
        }

        response = self.request(
            url=url,
            method='GET',
            params=params,
            expected_response_code=200
        )

        if chunked:
            try:
                decoded = chunked_json.loads(response.content.decode())
            except UnicodeDecodeError:
                decoded = chunked_json.loads(response.content.decode('utf-8'))

            return list(decoded)

        return response.json()

    # Creating and Dropping Databases
    #
    # ### create a database
    # curl -X POST http://localhost:8086/db -d '{"name": "site_development"}'
    #
    # ### drop a database
    # curl -X DELETE http://localhost:8086/db/site_development

    def create_database(self, database):
        """Create a database on the InfluxDB server.

        :param database: the name of the database to create
        :type database: string
        :rtype: boolean
        """
        url = "db"

        data = {'name': database}

        self.request(
            url=url,
            method='POST',
            data=data,
            expected_response_code=201
        )

        return True

    def delete_database(self, database):
        """Drop a database on the InfluxDB server.

        :param database: the name of the database to delete
        :type database: string
        :rtype: boolean
        """
        url = "db/{0}".format(database)

        self.request(
            url=url,
            method='DELETE',
            expected_response_code=204
        )

        return True

    # ### get list of databases
    # curl -X GET http://localhost:8086/db

    def get_list_database(self):
        """Get the list of databases."""
        url = "db"

        response = self.request(
            url=url,
            method='GET',
            expected_response_code=200
        )

        return response.json()

    def get_database_list(self):
        """Get the list of databases.

        DEPRECATED.
        """
        warnings.warn(
            "get_database_list is deprecated, and will be removed "
            "in future versions. Please use "
            "``InfluxDBClient.get_list_database`` instead.",
            FutureWarning)
        return self.get_list_database()

    def delete_series(self, series):
        """Drop a series on the InfluxDB server.

        :param series: the name of the series to delete
        :type series: string
        :rtype: boolean
        """
        url = "db/{0}/series/{1}".format(
            self._database,
            series
        )

        self.request(
            url=url,
            method='DELETE',
            expected_response_code=204
        )

        return True

    def get_list_series(self):
        """Get a list of all time series in a database."""
        response = self._query('list series')
        return [series[1] for series in response[0]['points']]

    def get_list_continuous_queries(self):
        """Get a list of continuous queries."""
        response = self._query('list continuous queries')
        return [query[2] for query in response[0]['points']]

    # Security
    # get list of cluster admins
    # curl http://localhost:8086/cluster_admins?u=root&p=root

    # add cluster admin
    # curl -X POST http://localhost:8086/cluster_admins?u=root&p=root \
    #      -d '{"name": "paul", "password": "i write teh docz"}'

    # update cluster admin password
    # curl -X POST http://localhost:8086/cluster_admins/paul?u=root&p=root \
    #      -d '{"password": "new pass"}'

    # delete cluster admin
    # curl -X DELETE http://localhost:8086/cluster_admins/paul?u=root&p=root

    # Database admins, with a database name of site_dev
    # get list of database admins
    # curl http://localhost:8086/db/site_dev/admins?u=root&p=root

    # add database admin
    # curl -X POST http://localhost:8086/db/site_dev/admins?u=root&p=root \
    #      -d '{"name": "paul", "password": "i write teh docz"}'

    # update database admin password
    # curl -X POST http://localhost:8086/db/site_dev/admins/paul?u=root&p=root\
    #      -d '{"password": "new pass"}'

    # delete database admin
    # curl -X DELETE \
    #        http://localhost:8086/db/site_dev/admins/paul?u=root&p=root

    def get_list_cluster_admins(self):
        """Get list of cluster admins."""
        response = self.request(
            url="cluster_admins",
            method='GET',
            expected_response_code=200
        )

        return response.json()

    def add_cluster_admin(self, new_username, new_password):
        """Add cluster admin."""
        data = {
            'name': new_username,
            'password': new_password
        }

        self.request(
            url="cluster_admins",
            method='POST',
            data=data,
            expected_response_code=200
        )

        return True

    def update_cluster_admin_password(self, username, new_password):
        """Update cluster admin password."""
        url = "cluster_admins/{0}".format(username)

        data = {
            'password': new_password
        }

        self.request(
            url=url,
            method='POST',
            data=data,
            expected_response_code=200
        )

        return True

    def delete_cluster_admin(self, username):
        """Delete cluster admin."""
        url = "cluster_admins/{0}".format(username)

        self.request(
            url=url,
            method='DELETE',
            expected_response_code=200
        )

        return True

    def set_database_admin(self, username):
        """Set user as database admin."""
        return self.alter_database_admin(username, True)

    def unset_database_admin(self, username):
        """Unset user as database admin."""
        return self.alter_database_admin(username, False)

    def alter_database_admin(self, username, is_admin):
        """Alter the database admin."""
        url = "db/{0}/users/{1}".format(self._database, username)

        data = {'admin': is_admin}

        self.request(
            url=url,
            method='POST',
            data=data,
            expected_response_code=200
        )

        return True

    def get_list_database_admins(self):
        """Get list of database admins.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def add_database_admin(self, new_username, new_password):
        """Add cluster admin.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def update_database_admin_password(self, username, new_password):
        """Update database admin password.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def delete_database_admin(self, username):
        """Delete database admin.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    ###
    # Limiting User Access

    # Database users
    # get list of database users
    # curl http://localhost:8086/db/site_dev/users?u=root&p=root

    # add database user
    # curl -X POST http://localhost:8086/db/site_dev/users?u=root&p=root \
    #       -d '{"name": "paul", "password": "i write teh docz"}'

    # update database user password
    # curl -X POST http://localhost:8086/db/site_dev/users/paul?u=root&p=root \
    #       -d '{"password": "new pass"}'

    # delete database user
    # curl -X DELETE http://localhost:8086/db/site_dev/users/paul?u=root&p=root

    def get_database_users(self):
        """Get list of database users."""
        url = "db/{0}/users".format(self._database)

        response = self.request(
            url=url,
            method='GET',
            expected_response_code=200
        )

        return response.json()

    def add_database_user(self, new_username, new_password, permissions=None):
        """Add database user.

        :param permissions: A ``(readFrom, writeTo)`` tuple
        """
        url = "db/{0}/users".format(self._database)

        data = {
            'name': new_username,
            'password': new_password
        }

        if permissions:
            try:
                data['readFrom'], data['writeTo'] = permissions
            except (ValueError, TypeError):
                raise TypeError(
                    "'permissions' must be (readFrom, writeTo) tuple"
                )

        self.request(
            url=url,
            method='POST',
            data=data,
            expected_response_code=200
        )

        return True

    def update_database_user_password(self, username, new_password):
        """Update password."""
        return self.alter_database_user(username, new_password)

    def alter_database_user(self, username, password=None, permissions=None):
        """Alter a database user and/or their permissions.

        :param permissions: A ``(readFrom, writeTo)`` tuple
        :raise TypeError: if permissions cannot be read.
        :raise ValueError: if neither password nor permissions provided.
        """
        url = "db/{0}/users/{1}".format(self._database, username)

        if not password and not permissions:
            raise ValueError("Nothing to alter for user {0}.".format(username))

        data = {}

        if password:
            data['password'] = password

        if permissions:
            try:
                data['readFrom'], data['writeTo'] = permissions
            except (ValueError, TypeError):
                raise TypeError(
                    "'permissions' must be (readFrom, writeTo) tuple"
                )

        self.request(
            url=url,
            method='POST',
            data=data,
            expected_response_code=200
        )

        if username == self._username:
            self._password = password

        return True

    def delete_database_user(self, username):
        """Delete database user."""
        url = "db/{0}/users/{1}".format(self._database, username)

        self.request(
            url=url,
            method='DELETE',
            expected_response_code=200
        )

        return True

    # update the user by POSTing to db/site_dev/users/paul

    def update_permission(self, username, json_body):
        """Update read/write permission.

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57

        """
        raise NotImplementedError()

    def send_packet(self, packet):
        """Send a UDP packet along the wire."""
        data = json.dumps(packet)
        byte = data.encode('utf-8')
        self.udp_socket.sendto(byte, (self._host, self._udp_port))
