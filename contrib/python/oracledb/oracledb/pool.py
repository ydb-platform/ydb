# -----------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# pool.py
#
# Contains the ConnectionPool class and the factory method create_pool() used
# for creating connection pools.
#
# *** NOTICE *** This file is generated from a template and should not be
# modified directly. See build_from_template.py in the utils subdirectory for
# more information.
# -----------------------------------------------------------------------------

import functools
import ssl
from typing import Callable, Type, Union, Any

import oracledb

from . import base_impl, thick_impl, thin_impl
from . import connection as connection_module
from . import driver_mode
from . import errors
from .pool_params import PoolParams


class BaseConnectionPool:
    __module__ = oracledb.__name__
    _impl = None

    def __init__(
        self, dsn: str = None, *, params: PoolParams = None, **kwargs
    ) -> None:
        """
        Constructor for creating a connection pool. Connection pooling creates
        a pool of available connections to the database, allowing applications
        to acquire a connection very quickly. It is of primary use in a server
        where connections are requested in rapid succession and used for a
        short period of time, for example in a web server.

        The dsn parameter (data source name) can be a string in the format
        user/password@connect_string or can simply be the connect string (in
        which case authentication credentials such as the username and password
        need to be specified separately). See the documentation on connection
        strings for more information.

        The params parameter is expected to be of type PoolParams and contains
        parameters that are used to create the pool. See the documentation on
        PoolParams for more information. If this parameter is not specified,
        the additional keyword parameters will be used to create an instance of
        PoolParams. If both the params parameter and additional keyword
        parameters are specified, the values in the keyword parameters have
        precedence. Note that if a dsn is also supplied, then in the
        python-oracledb Thin mode, the values of the parameters specified
        (if any) within the dsn will override the values passed as additional
        keyword parameters, which themselves override the values set in the
        params parameter object.
        """
        if params is None:
            params_impl = base_impl.PoolParamsImpl()
        elif not isinstance(params, PoolParams):
            errors._raise_err(errors.ERR_INVALID_POOL_PARAMS)
        else:
            params_impl = params._impl.copy()
        with driver_mode.get_manager() as mode_mgr:
            thin = mode_mgr.thin
            dsn = params_impl.process_args(dsn, kwargs, thin)
            self._set_connection_type(params_impl.connectiontype)
            if issubclass(
                self._connection_type, connection_module.AsyncConnection
            ):
                impl = thin_impl.AsyncThinPoolImpl(dsn, params_impl)
            elif thin:
                impl = thin_impl.ThinPoolImpl(dsn, params_impl)
            else:
                impl = thick_impl.ThickPoolImpl(dsn, params_impl)
            self._impl = impl
            self.session_callback = params_impl.session_callback

    def _verify_open(self) -> None:
        """
        Verifies that the pool is open and able to perform its work.
        """
        if self._impl is None:
            errors._raise_err(errors.ERR_POOL_NOT_OPEN)

    @property
    def busy(self) -> int:
        """
        Returns the number of connections that have been acquired from the pool
        and have not yet been returned to the pool.
        """
        self._verify_open()
        return self._impl.get_busy_count()

    @property
    def dsn(self) -> str:
        """
        Returns the connection string (TNS entry) of the database to which
        connections in the pool have been established.
        """
        self._verify_open()
        return self._impl.dsn

    @property
    def getmode(self) -> oracledb.PoolGetMode:
        self._verify_open()
        return oracledb.PoolGetMode(self._impl.get_getmode())

    @getmode.setter
    def getmode(self, value: oracledb.PoolGetMode) -> None:
        self._verify_open()
        self._impl.set_getmode(value)

    @property
    def homogeneous(self) -> bool:
        """
        Returns a boolean indicating if the pool is homogeneous or not. If the
        pool is not homogeneous, different authentication can be used for each
        connection acquired from the pool.
        """
        self._verify_open()
        return self._impl.homogeneous

    @property
    def increment(self) -> int:
        """
        Returns the number of connections that will be created when additional
        connections need to be created to satisfy requests.
        """
        self._verify_open()
        return self._impl.increment

    @property
    def max(self) -> int:
        """
        Returns the maximum number of connections that the pool can control.
        """
        self._verify_open()
        return self._impl.max

    @property
    def max_lifetime_session(self) -> int:
        """
        Returns the maximum length of time (in seconds) that a pooled
        connection may exist. Connections that are in use will not be closed.
        They become candidates for termination only when they are released back
        to the pool and have existed for longer than max_lifetime_session
        seconds. Note that termination only occurs when the pool is accessed. A
        value of 0 means that there is no maximum length of time that a pooled
        connection may exist. This attribute is only available in Oracle
        Database 12.1.
        """
        self._verify_open()
        return self._impl.get_max_lifetime_session()

    @max_lifetime_session.setter
    def max_lifetime_session(self, value: int) -> None:
        self._verify_open()
        self._impl.set_max_lifetime_session(value)

    @property
    def max_sessions_per_shard(self) -> int:
        """
        Returns the number of sessions that can be created per shard in the
        pool.  Setting this attribute greater than zero specifies the maximum
        number of sessions in the pool that can be used for any given shard in
        a sharded database. This lets connections in the pool be balanced
        across the shards.  A value of zero will not set any maximum number of
        sessions for each shard.  This attribute is only available in Oracle
        Client 18.3 and higher.
        """
        self._verify_open()
        return self._impl.get_max_sessions_per_shard()

    @max_sessions_per_shard.setter
    def max_sessions_per_shard(self, value: int) -> None:
        self._verify_open()
        self._impl.set_max_sessions_per_shard(value)

    @property
    def min(self) -> int:
        """
        Returns the minimum number of connections that the pool will control.
        These are created when the pool is first created.
        """
        self._verify_open()
        return self._impl.min

    @property
    def name(self) -> str:
        """
        Returns the name assigned to the pool by Oracle. This attribute is only
        relevant in python-oracledb thick mode.
        """
        self._verify_open()
        return self._impl.name

    @property
    def opened(self) -> int:
        """
        Returns the number of connections currently opened by the pool.
        """
        self._verify_open()
        return self._impl.get_open_count()

    @property
    def ping_interval(self) -> int:
        """
        Returns the pool ping interval in seconds. When a connection is
        acquired from the pool, a check is first made to see how long it
        has been since the connection was put into the pool. If
        this idle time exceeds ping_interval, then a round-trip ping to the
        database is performed. If the connection is unusable, it is discarded
        and a different connection is selected to be returned by
        SessionPool.acquire(). Setting ping_interval to a negative value
        disables pinging. Setting it to 0 forces a ping for every aquire()
        and is not recommended.
        """
        self._verify_open()
        return self._impl.get_ping_interval()

    @ping_interval.setter
    def ping_interval(self, value: int) -> None:
        self._impl.set_ping_interval(value)

    @property
    def soda_metadata_cache(self) -> bool:
        """
        Specifies whether the SODA metadata cache is enabled or not. Enabling
        the cache significantly improves the performance of methods
        SodaDatabase.createCollection() (when not specifying a value for the
        metadata parameter) and SodaDatabase.openCollection(). Note that the
        cache can become out of date if changes to the metadata of cached
        collections are made externally.
        """
        self._verify_open()
        return self._impl.get_soda_metadata_cache()

    @soda_metadata_cache.setter
    def soda_metadata_cache(self, value: bool) -> None:
        if not isinstance(value, bool):
            message = "soda_metadata_cache must be a boolean value."
            raise TypeError(message)
        self._verify_open()
        self._impl.set_soda_metadata_cache(value)

    @property
    def stmtcachesize(self) -> int:
        """
        Specifies the size of the statement cache that will be used as the
        starting point for any connections that are created by the pool. Once a
        connection is created, that connection’s statement cache size can only
        be changed by setting the stmtcachesize attribute on the connection
        itself.
        """
        self._verify_open()
        return self._impl.get_stmt_cache_size()

    @stmtcachesize.setter
    def stmtcachesize(self, value: int) -> None:
        self._verify_open()
        self._impl.set_stmt_cache_size(value)

    @property
    def thin(self) -> bool:
        """
        Returns a boolean indicating if the pool was created in
        python-oracledb's thin mode (True) or thick mode (False).
        """
        self._verify_open()
        return not isinstance(self._impl, thick_impl.ThickPoolImpl)

    @property
    def timeout(self) -> int:
        """
        Specifies the time (in seconds) after which idle connections will be
        terminated in order to maintain an optimum number of open connections.
        A value of 0 means that no idle connections are terminated. Note that
        in thick mode with older Oracle Client libraries termination only
        occurs when the pool is accessed.
        """
        self._verify_open()
        return self._impl.get_timeout()

    @timeout.setter
    def timeout(self, value: int) -> None:
        self._verify_open()
        self._impl.set_timeout(value)

    @property
    def tnsentry(self) -> str:
        """
        Deprecated. Use dsn instead.
        """
        return self.dsn

    @property
    def username(self) -> str:
        """
        Returns the name of the user which was used to create the pool.
        """
        self._verify_open()
        return self._impl.username

    @property
    def wait_timeout(self) -> int:
        """
        Specifies the time (in milliseconds) that the caller should wait for a
        connection to become available in the pool before returning with an
        error.  This value is only used if the getmode parameter used to create
        the pool was POOL_GETMODE_TIMEDWAIT.
        """
        self._verify_open()
        return self._impl.get_wait_timeout()

    @wait_timeout.setter
    def wait_timeout(self, value: int) -> None:
        self._verify_open()
        self._impl.set_wait_timeout(value)


class ConnectionPool(BaseConnectionPool):
    __module__ = oracledb.__name__

    def __del__(self):
        if self._impl is not None:
            self._impl.close(True)
            self._impl = None

    def _set_connection_type(self, conn_class):
        """
        Called internally when the pool is created to ensure that the correct
        connection class is used for all connections created by the pool.
        """
        if conn_class is None:
            conn_class = connection_module.Connection
        elif not issubclass(
            conn_class, connection_module.Connection
        ) or issubclass(conn_class, connection_module.AsyncConnection):
            errors._raise_err(errors.ERR_INVALID_CONN_CLASS)
        self._connection_type = conn_class

    def acquire(
        self,
        user: str = None,
        password: str = None,
        cclass: str = None,
        purity: int = oracledb.PURITY_DEFAULT,
        tag: str = None,
        matchanytag: bool = False,
        shardingkey: list = None,
        supershardingkey: list = None,
    ) -> "connection_module.Connection":
        """
        Acquire a connection from the pool and return it.

        If the pool is homogeneous, the user and password parameters cannot be
        specified. If they are, an exception will be raised.

        The cclass parameter, if specified, should be a string corresponding to
        the connection class for database resident connection pooling (DRCP).

        The purity parameter is expected to be one of PURITY_DEFAULT,
        PURITY_NEW, or PURITY_SELF.

        The tag parameter, if specified, is expected to be a string with
        name=value pairs like “k1=v1;k2=v2” and will limit the connections that
        can be returned from a pool unless the matchanytag parameter is
        set to True. In that case connections with the specified tag will be
        preferred over others, but if no such connections are available a
        connection with a different tag may be returned instead. In any case,
        untagged connections will always be returned if no connections with the
        specified tag are available. Connections are tagged when they are
        released back to the pool.

        The shardingkey and supershardingkey parameters, if specified, are
        expected to be a sequence of values which will be used to identify the
        database shard to connect to. The key values can be strings, numbers,
        bytes or dates.
        """
        self._verify_open()

        return oracledb.connect(
            conn_class=self._connection_type,
            user=user,
            password=password,
            cclass=cclass,
            purity=purity,
            tag=tag,
            matchanytag=matchanytag,
            shardingkey=shardingkey,
            supershardingkey=supershardingkey,
            pool=self,
        )

    def close(self, force: bool = False) -> None:
        """
        Close the pool now, rather than when the last reference to it is
        released, which makes it unusable for further work.

        If any connections have been acquired and not released back to the
        pool, this method will fail unless the force parameter is set to True.
        """
        self._verify_open()
        self._impl.close(force)
        self._impl = None

    def drop(self, connection: "connection_module.Connection") -> None:
        """
        Drop the connection from the pool, which is useful if the connection is
        no longer usable (such as when the database session is killed).
        """
        self._verify_open()
        if not isinstance(connection, connection_module.Connection):
            message = "connection must be an instance of oracledb.Connection"
            raise TypeError(message)
        connection._verify_connected()
        self._impl.drop(connection._impl)
        connection._impl = None

    def release(
        self, connection: "connection_module.Connection", tag: str = None
    ) -> None:
        """
        Release the connection back to the pool now, rather than whenever
        __del__ is called. The connection will be unusable from this point
        forward; an Error exception will be raised if any operation is
        attempted with the connection. Any cursors or LOBs created by the
        connection will also be marked unusable and an Error exception will be
        raised if any operation is attempted with them.

        Internally, references to the connection are held by cursor objects,
        LOB objects, etc. Once all of these references are released, the
        connection itself will be released back to the pool automatically.
        Either control references to these related objects carefully or
        explicitly release connections back to the pool in order to ensure
        sufficient resources are available.

        If the tag is not None, it is expected to be a string with name=value
        pairs like “k1=v1;k2=v2” and will override the value in the property
        Connection.tag. If either Connection.tag or the tag parameter are not
        None, the connection will be retagged when it is released back to the
        pool.
        """
        self._verify_open()
        if not isinstance(connection, connection_module.Connection):
            message = "connection must be an instance of oracledb.Connection"
            raise TypeError(message)
        if tag is not None:
            connection.tag = tag
        connection.close()

    def reconfigure(
        self,
        min: int = None,
        max: int = None,
        increment: int = None,
        getmode: int = None,
        timeout: int = None,
        wait_timeout: int = None,
        max_lifetime_session: int = None,
        max_sessions_per_shard: int = None,
        soda_metadata_cache: bool = None,
        stmtcachesize: int = None,
        ping_interval: int = None,
    ) -> None:
        """
        Reconfigures various parameters of a connection pool. The pool size
        can be altered with reconfigure() by passing values for min, max
        or increment. The getmode, timeout, wait_timeout,
        max_lifetime_session, max_sessions_per_shard, soda_metadata_cache,
        stmtcachesize and ping_interval can be set directly or by using
        reconfigure(). All parameters are optional. Unspecified parameters
        will leave those pool attributes unchanged. The parameters are
        processed in two stages. After any size change has been processed,
        reconfiguration on the other parameters is done sequentially. If
        an error such as an invalid value occurs when changing one attribute,
        then an exception will be generated but any already changed
        attributes will retain their new values.

        During reconfiguration of a pool's size, the behavior of acquire()
        depends on the getmode in effect when acquire() is called:

        * With mode POOL_GETMODE_FORCEGET, an acquire() call will wait until
          the pool has been reconfigured.

        * With mode POOL_GETMODE__TIMEDWAIT, an acquire() call will try to
          acquire a connection in the time specified by pool.wait_timeout and
          return an error if the time taken exceeds that value.

        * With mode POOL_GETMODE_WAIT, an acquire() call will wait until after
          the pool has been reconfigured and a connection is available.

        * With mode POOL_GETMODE_NOWAIT, if the number of busy connections is
          less than the pool size, acquire() will return a new connection
          after pool reconfiguration is complete.

        Closing connections with pool.release() or connection.close() will
        wait until any pool size reconfiguration is complete.

        Closing the connection pool with pool.close() will wait until
        reconfiguration is complete.
        """

        if min is None:
            min = self.min
        if max is None:
            max = self.max
        if increment is None:
            increment = self.increment
        if self.min != min or self.max != max or self.increment != increment:
            self._impl.reconfigure(min, max, increment)
        if getmode is not None:
            self.getmode = getmode
        if timeout is not None:
            self.timeout = timeout
        if wait_timeout is not None:
            self.wait_timeout = wait_timeout
        if max_lifetime_session is not None:
            self.max_lifetime_session = max_lifetime_session
        if max_sessions_per_shard is not None:
            self.max_sessions_per_shard = max_sessions_per_shard
        if soda_metadata_cache is not None:
            self.soda_metadata_cache = soda_metadata_cache
        if stmtcachesize is not None:
            self.stmtcachesize = stmtcachesize
        if ping_interval is not None:
            self.ping_interval = ping_interval


def _pool_factory(f):
    """
    Decorator which checks the validity of the supplied keyword parameters by
    calling the original function (which does nothing), then creates and
    returns an instance of the requested ConnectionPool class. The base
    ConnectionPool class constructor does not check the validity of the
    supplied keyword parameters.
    """

    @functools.wraps(f)
    def create_pool(
        dsn: str = None,
        *,
        pool_class: Type[ConnectionPool] = ConnectionPool,
        params: PoolParams = None,
        **kwargs,
    ) -> ConnectionPool:
        f(dsn=dsn, pool_class=pool_class, params=params, **kwargs)
        if not issubclass(pool_class, ConnectionPool):
            errors._raise_err(errors.ERR_INVALID_POOL_CLASS)
        return pool_class(dsn, params=params, **kwargs)

    return create_pool


@_pool_factory
def create_pool(
    dsn: str = None,
    *,
    pool_class: Type[ConnectionPool] = ConnectionPool,
    params: PoolParams = None,
    min: int = 1,
    max: int = 2,
    increment: int = 1,
    connectiontype: Type["oracledb.Connection"] = None,
    getmode: oracledb.PoolGetMode = oracledb.POOL_GETMODE_WAIT,
    homogeneous: bool = True,
    timeout: int = 0,
    wait_timeout: int = 0,
    max_lifetime_session: int = 0,
    session_callback: Callable = None,
    max_sessions_per_shard: int = 0,
    soda_metadata_cache: bool = False,
    ping_interval: int = 60,
    ping_timeout: int = 5000,
    user: str = None,
    proxy_user: str = None,
    password: str = None,
    newpassword: str = None,
    wallet_password: str = None,
    access_token: Union[str, tuple, Callable] = None,
    host: str = None,
    port: int = 1521,
    protocol: str = "tcp",
    https_proxy: str = None,
    https_proxy_port: int = 0,
    service_name: str = None,
    sid: str = None,
    server_type: str = None,
    cclass: str = None,
    purity: oracledb.Purity = oracledb.PURITY_DEFAULT,
    expire_time: int = 0,
    retry_count: int = 0,
    retry_delay: int = 1,
    tcp_connect_timeout: float = 20.0,
    ssl_server_dn_match: bool = True,
    ssl_server_cert_dn: str = None,
    wallet_location: str = None,
    events: bool = False,
    externalauth: bool = False,
    mode: oracledb.AuthMode = oracledb.AUTH_MODE_DEFAULT,
    disable_oob: bool = False,
    stmtcachesize: int = oracledb.defaults.stmtcachesize,
    edition: str = None,
    tag: str = None,
    matchanytag: bool = False,
    config_dir: str = oracledb.defaults.config_dir,
    appcontext: list = None,
    shardingkey: list = None,
    supershardingkey: list = None,
    debug_jdwp: str = None,
    connection_id_prefix: str = None,
    ssl_context: Any = None,
    sdu: int = 8192,
    pool_boundary: str = None,
    use_tcp_fast_open: bool = False,
    ssl_version: ssl.TLSVersion = None,
    program: str = oracledb.defaults.program,
    machine: str = oracledb.defaults.machine,
    terminal: str = oracledb.defaults.terminal,
    osuser: str = oracledb.defaults.osuser,
    driver_name: str = oracledb.defaults.driver_name,
    handle: int = 0,
) -> ConnectionPool:
    """
    Creates a connection pool with the supplied parameters and returns it.

    The dsn parameter (data source name) can be a string in the format
    user/password@connect_string or can simply be the connect string (in
    which case authentication credentials such as the username and password
    need to be specified separately). See the documentation on connection
    strings for more information.

    The pool_class parameter is expected to be ConnectionPool or a subclass of
    ConnectionPool.

    The params parameter is expected to be of type PoolParams and contains
    parameters that are used to create the pool. See the documentation on
    PoolParams for more information. If this parameter is not specified, the
    additional keyword parameters will be used to create an instance of
    PoolParams. If both the params parameter and additional keyword parameters
    are specified, the values in the keyword parameters have precedence.
    Note that if a dsn is also supplied, then in the python-oracledb Thin mode,
    the values of the parameters specified (if any) within the dsn will
    override the values passed as additional keyword parameters, which
    themselves override the values set in the params parameter object.

    The following parameters are all optional. A brief description of each
    parameter follows:

    - min: the minimum number of connections the pool should contain (default:
      1)

    - max: the maximum number of connections the pool should contain (default:
      2)

    - increment: the number of connections that should be added to the pool
      whenever a new connection needs to be created (default: 1)

    - connectiontype: the class of the connection that should be returned
      during calls to pool.acquire(). It must be oracledb.Connection or a
      subclass of oracledb.Connection (default: None)

    - getmode: how pool.acquire() will behave. One of the constants
      oracledb.POOL_GETMODE_WAIT, oracledb.POOL_GETMODE_NOWAIT,
      oracledb.POOL_GETMODE_FORCEGET, or oracledb.POOL_GETMODE_TIMEDWAIT
      (default: oracledb.POOL_GETMODE_WAIT)

    - homogeneous: a boolean indicating whether the connections are homogeneous
      (same user) or heterogeneous (multiple users) (default: True)

    - timeout: length of time (in seconds) that a connection may remain idle in
      the pool before it is terminated. If it is 0 then connections are never
      terminated (default: 0)

    - wait_timeout: length of time (in milliseconds) that a caller should wait
      when acquiring a connection from the pool with getmode set to
      oracledb.POOL_GETMODE_TIMEDWAIT (default: 0)

    - max_lifetime_session: length of time (in seconds) that connections can
      remain in the pool. If it is 0 then connections may remain in the pool
      indefinitely (default: 0)

    - session_callback: a callable that is invoked when a connection is
      returned from the pool for the first time, or when the connection tag
      differs from the one requested (default: None)

    - max_sessions_per_shard: the maximum number of connections that may be
      associated with a particular shard (default: 0)

    - soda_metadata_cache: boolean indicating whether or not the SODA metadata
      cache should be enabled (default: False)

    - ping_interval: length of time (in seconds) after which an unused
      connection in the pool will be a candidate for pinging when
      pool.acquire() is called. If the ping to the database indicates the
      connection is not alive a replacement connection will be returned by
      pool.acquire(). If ping_interval is a negative value the ping
      functionality will be disabled (default: 60)

    - ping_timeout: maximum length of time (in milliseconds) to wait for a
      connection in the pool to respond to an internal ping to the database
      before being discarded and replaced during a call to acquire() (default:
      5000)

    - user: the name of the user to connect to (default: None)

    - proxy_user: the name of the proxy user to connect to. If this value is
      not specified, it will be parsed out of user if user is in the form
      "user[proxy_user]" (default: None)

    - password: the password for the user (default: None)

    - newpassword: the new password for the user. The new password will take
      effect immediately upon a successful connection to the database (default:
      None)

    - wallet_password: the password to use to decrypt the wallet, if it is
      encrypted. This value is only used in thin mode (default: None)

    - access_token: expected to be a string or a 2-tuple or a callable. If it
      is a string, it specifies an Azure AD OAuth2 token used for Open
      Authorization (OAuth 2.0) token based authentication. If it is a 2-tuple,
      it specifies the token and private key strings used for Oracle Cloud
      Infrastructure (OCI) Identity and Access Management (IAM) token based
      authentication. If it is a callable, it returns either a string or a
      2-tuple used for OAuth 2.0 or OCI IAM token based authentication and is
      useful when the pool needs to expand and create new connections but the
      current authentication token has expired (default: None)

    - host: the name or IP address of the machine hosting the database or the
      database listener (default: None)

    - port: the port number on which the database listener is listening
      (default: 1521)

    - protocol: one of the strings "tcp" or "tcps" indicating whether to use
      unencrypted network traffic or encrypted network traffic (TLS) (default:
      "tcp")

    - https_proxy: the name or IP address of a proxy host to use for tunneling
      secure connections (default: None)

    - https_proxy_port: the port on which to communicate with the proxy host
      (default: 0)

    - service_name: the service name of the database (default: None)

    - sid: the system identifier (SID) of the database. Note using a
      service_name instead is recommended (default: None)

    - server_type: the type of server connection that should be established. If
      specified, it should be one of "dedicated", "shared" or "pooled"
      (default: None)

    - cclass: connection class to use for Database Resident Connection Pooling
      (DRCP) (default: None)

    - purity: purity to use for Database Resident Connection Pooling (DRCP)
      (default: oracledb.PURITY_DEFAULT)

    - expire_time: an integer indicating the number of minutes between the
      sending of keepalive probes. If this parameter is set to a value greater
      than zero it enables keepalive (default: 0)

    - retry_count: the number of times that a connection attempt should be
      retried before the attempt is terminated (default: 0)

    - retry_delay: the number of seconds to wait before making a new connection
      attempt (default: 1)

    - tcp_connect_timeout: a float indicating the maximum number of seconds to
      wait for establishing a connection to the database host (default: 20.0)

    - ssl_server_dn_match: boolean indicating whether the server certificate
      distinguished name (DN) should be matched in addition to the regular
      certificate verification that is performed. Note that if the
      ssl_server_cert_dn parameter is not privided, host name matching is
      performed instead (default: True)

    - ssl_server_cert_dn: the distinguished name (DN) which should be matched
      with the server. This value is ignored if the ssl_server_dn_match
      parameter is not set to the value True. If specified this value is used
      for any verfication. Otherwise the hostname will be used. (default: None)

    - wallet_location: the directory where the wallet can be found. In thin
      mode this must be the directory containing the PEM-encoded wallet file
      ewallet.pem. In thick mode this must be the directory containing the file
      cwallet.sso (default: None)

    - events: boolean specifying whether events mode should be enabled. This
      value is only used in thick mode and is needed for continuous query
      notification and high availability event notifications (default: False)

    - externalauth: a boolean indicating whether to use external authentication
      (default: False)

    - mode: authorization mode to use. For example oracledb.AUTH_MODE_SYSDBA
      (default: oracledb.AUTH_MODE_DEFAULT)

    - disable_oob: boolean indicating whether out-of-band breaks should be
      disabled. This value is only used in thin mode. It has no effect on
      Windows which does not support this functionality (default: False)

    - stmtcachesize: identifies the initial size of the statement cache
      (default: oracledb.defaults.stmtcachesize)

    - edition: edition to use for the connection. This parameter cannot be used
      simultaneously with the cclass parameter (default: None)

    - tag: identifies the type of connection that should be returned from a
      pool. This value is only used in thick mode (default: None)

    - matchanytag: boolean specifying whether any tag can be used when
      acquiring a connection from the pool. This value is only used in thick
      mode. (default: False)

    - config_dir: directory in which the optional tnsnames.ora configuration
      file is located. This value is only used in thin mode. For thick mode use
      the config_dir parameter of init_oracle_client() (default:
      oracledb.defaults.config_dir)

    - appcontext: application context used by the connection. It should be a
      list of 3-tuples (namespace, name, value) and each entry in the tuple
      should be a string. This value is only used in thick mode (default: None)

    - shardingkey: a list of strings, numbers, bytes or dates that identify the
      database shard to connect to. This value is only used in thick mode
      (default: None)

    - supershardingkey: a list of strings, numbers, bytes or dates that
      identify the database shard to connect to. This value is only used in
      thick mode (default: None)

    - debug_jdwp: a string with the format "host=<host>;port=<port>" that
      specifies the host and port of the PL/SQL debugger. This value is only
      used in thin mode. For thick mode set the ORA_DEBUG_JDWP environment
      variable (default: None)

    - connection_id_prefix: an application specific prefix that is added to the
      connection identifier used for tracing (default: None)

    - ssl_context: an SSLContext object used for connecting to the database
      using TLS.  This SSL context will be modified to include the private key
      or any certificates found in a separately supplied wallet. This parameter
      should only be specified if the default SSLContext object cannot be used
      (default: None)

    - sdu: the requested size of the Session Data Unit (SDU), in bytes. The
      value tunes internal buffers used for communication to the database.
      Bigger values can increase throughput for large queries or bulk data
      loads, but at the cost of higher memory use. The SDU size that will
      actually be used is negotiated down to the lower of this value and the
      database network SDU configuration value (default: 8192)

    - pool_boundary: one of the values "statement" or "transaction" indicating
      when pooled DRCP connections can be returned to the pool. This requires
      the use of DRCP with Oracle Database 23.4 or higher (default: None)

    - use_tcp_fast_open: boolean indicating whether to use TCP fast open. This
      is an Oracle Autonomous Database Serverless (ADB-S) specific property for
      clients connecting from within OCI Cloud network. Please refer to the
      ADB-S documentation for more information (default: False)

    - ssl_version: one of the values ssl.TLSVersion.TLSv1_2 or
      ssl.TLSVersion.TLSv1_3 indicating which TLS version to use (default:
      None)

    - program: the name of the executable program or application connected to
      the Oracle Database (default: oracledb.defaults.program)

    - machine: the machine name of the client connecting to the Oracle Database
      (default: oracledb.defaults.machine)

    - terminal: the terminal identifier from which the connection originates
      (default: oracledb.defaults.terminal)

    - osuser: the operating system user that initiates the database connection
      (default: oracledb.defaults.osuser)

    - driver_name: the driver name used by the client to connect to the Oracle
      Database (default: oracledb.defaults.driver_name)

    - handle: an integer representing a pointer to a valid service context
      handle. This value is only used in thick mode. It should be used with
      extreme caution (default: 0)
    """
    pass


class AsyncConnectionPool(BaseConnectionPool):
    __module__ = oracledb.__name__

    def _set_connection_type(self, conn_class):
        """
        Called internally when the pool is created to ensure that the correct
        connection class is used for all connections created by the pool.
        """
        if conn_class is None:
            conn_class = connection_module.AsyncConnection
        elif not issubclass(conn_class, connection_module.AsyncConnection):
            errors._raise_err(errors.ERR_INVALID_CONN_CLASS)
        self._connection_type = conn_class

    def acquire(
        self,
        user: str = None,
        password: str = None,
        cclass: str = None,
        purity: int = oracledb.PURITY_DEFAULT,
        tag: str = None,
        matchanytag: bool = False,
        shardingkey: list = None,
        supershardingkey: list = None,
    ) -> "connection_module.AsyncConnection":
        """
        Acquire a connection from the pool and return it.

        If the pool is homogeneous, the user and password parameters cannot be
        specified. If they are, an exception will be raised.

        The cclass parameter, if specified, should be a string corresponding to
        the connection class for database resident connection pooling (DRCP).

        The purity parameter is expected to be one of PURITY_DEFAULT,
        PURITY_NEW, or PURITY_SELF.

        The tag parameter, if specified, is expected to be a string with
        name=value pairs like “k1=v1;k2=v2” and will limit the connections that
        can be returned from a pool unless the matchanytag parameter is
        set to True. In that case connections with the specified tag will be
        preferred over others, but if no such connections are available a
        connection with a different tag may be returned instead. In any case,
        untagged connections will always be returned if no connections with the
        specified tag are available. Connections are tagged when they are
        released back to the pool.

        The shardingkey and supershardingkey parameters, if specified, are
        expected to be a sequence of values which will be used to identify the
        database shard to connect to. The key values can be strings, numbers,
        bytes or dates.
        """
        self._verify_open()

        return oracledb.connect_async(
            conn_class=self._connection_type,
            user=user,
            password=password,
            cclass=cclass,
            purity=purity,
            tag=tag,
            matchanytag=matchanytag,
            shardingkey=shardingkey,
            supershardingkey=supershardingkey,
            pool=self,
        )

    async def close(self, force: bool = False) -> None:
        """
        Close the pool now, rather than when the last reference to it is
        released, which makes it unusable for further work.

        If any connections have been acquired and not released back to the
        pool, this method will fail unless the force parameter is set to True.
        """
        self._verify_open()
        await self._impl.close(force)
        self._impl = None

    async def drop(self, connection: "connection_module.Connection") -> None:
        """
        Drop the connection from the pool, which is useful if the connection is
        no longer usable (such as when the database session is killed).
        """
        self._verify_open()
        if not isinstance(connection, connection_module.AsyncConnection):
            message = (
                "connection must be an instance of oracledb.AsyncConnection"
            )
            raise TypeError(message)
        connection._verify_connected()
        await self._impl.drop(connection._impl)
        connection._impl = None

    async def release(
        self, connection: "connection_module.AsyncConnection", tag: str = None
    ) -> None:
        """
        Release the connection back to the pool now, rather than whenever
        __del__ is called. The connection will be unusable from this point
        forward; an Error exception will be raised if any operation is
        attempted with the connection. Any cursors or LOBs created by the
        connection will also be marked unusable and an Error exception will be
        raised if any operation is attempted with them.

        Internally, references to the connection are held by cursor objects,
        LOB objects, etc. Once all of these references are released, the
        connection itself will be released back to the pool automatically.
        Either control references to these related objects carefully or
        explicitly release connections back to the pool in order to ensure
        sufficient resources are available.

        If the tag is not None, it is expected to be a string with name=value
        pairs like “k1=v1;k2=v2” and will override the value in the property
        Connection.tag. If either Connection.tag or the tag parameter are not
        None, the connection will be retagged when it is released back to the
        pool.
        """
        self._verify_open()
        if not isinstance(connection, connection_module.AsyncConnection):
            message = (
                "connection must be an instance of oracledb.AsyncConnection"
            )
            raise TypeError(message)
        if tag is not None:
            connection.tag = tag
        await connection.close()


def _async_pool_factory(f):
    """
    Decorator which checks the validity of the supplied keyword parameters by
    calling the original function (which does nothing), then creates and
    returns an instance of the requested ConnectionPool class. The base
    ConnectionPool class constructor does not check the validity of the
    supplied keyword parameters.
    """

    @functools.wraps(f)
    def create_pool_async(
        dsn: str = None,
        *,
        pool_class: Type[ConnectionPool] = AsyncConnectionPool,
        params: PoolParams = None,
        **kwargs,
    ) -> AsyncConnectionPool:
        f(dsn=dsn, pool_class=pool_class, params=params, **kwargs)
        if not issubclass(pool_class, AsyncConnectionPool):
            errors._raise_err(errors.ERR_INVALID_POOL_CLASS)
        return pool_class(dsn, params=params, **kwargs)

    return create_pool_async


@_async_pool_factory
def create_pool_async(
    dsn: str = None,
    *,
    pool_class: Type[ConnectionPool] = AsyncConnectionPool,
    params: PoolParams = None,
    min: int = 1,
    max: int = 2,
    increment: int = 1,
    connectiontype: Type["oracledb.AsyncConnection"] = None,
    getmode: oracledb.PoolGetMode = oracledb.POOL_GETMODE_WAIT,
    homogeneous: bool = True,
    timeout: int = 0,
    wait_timeout: int = 0,
    max_lifetime_session: int = 0,
    session_callback: Callable = None,
    max_sessions_per_shard: int = 0,
    soda_metadata_cache: bool = False,
    ping_interval: int = 60,
    ping_timeout: int = 5000,
    user: str = None,
    proxy_user: str = None,
    password: str = None,
    newpassword: str = None,
    wallet_password: str = None,
    access_token: Union[str, tuple, Callable] = None,
    host: str = None,
    port: int = 1521,
    protocol: str = "tcp",
    https_proxy: str = None,
    https_proxy_port: int = 0,
    service_name: str = None,
    sid: str = None,
    server_type: str = None,
    cclass: str = None,
    purity: oracledb.Purity = oracledb.PURITY_DEFAULT,
    expire_time: int = 0,
    retry_count: int = 0,
    retry_delay: int = 1,
    tcp_connect_timeout: float = 20.0,
    ssl_server_dn_match: bool = True,
    ssl_server_cert_dn: str = None,
    wallet_location: str = None,
    events: bool = False,
    externalauth: bool = False,
    mode: oracledb.AuthMode = oracledb.AUTH_MODE_DEFAULT,
    disable_oob: bool = False,
    stmtcachesize: int = oracledb.defaults.stmtcachesize,
    edition: str = None,
    tag: str = None,
    matchanytag: bool = False,
    config_dir: str = oracledb.defaults.config_dir,
    appcontext: list = None,
    shardingkey: list = None,
    supershardingkey: list = None,
    debug_jdwp: str = None,
    connection_id_prefix: str = None,
    ssl_context: Any = None,
    sdu: int = 8192,
    pool_boundary: str = None,
    use_tcp_fast_open: bool = False,
    ssl_version: ssl.TLSVersion = None,
    program: str = oracledb.defaults.program,
    machine: str = oracledb.defaults.machine,
    terminal: str = oracledb.defaults.terminal,
    osuser: str = oracledb.defaults.osuser,
    driver_name: str = oracledb.defaults.driver_name,
    handle: int = 0,
) -> AsyncConnectionPool:
    """
    Creates a connection pool with the supplied parameters and returns it.

    The dsn parameter (data source name) can be a string in the format
    user/password@connect_string or can simply be the connect string (in
    which case authentication credentials such as the username and password
    need to be specified separately). See the documentation on connection
    strings for more information.

    The pool_class parameter is expected to be AsyncConnectionPool or a
    subclass of AsyncConnectionPool.

    The params parameter is expected to be of type PoolParams and contains
    parameters that are used to create the pool. See the documentation on
    PoolParams for more information. If this parameter is not specified, the
    additional keyword parameters will be used to create an instance of
    PoolParams. If both the params parameter and additional keyword parameters
    are specified, the values in the keyword parameters have precedence.
    Note that if a dsn is also supplied, then in the python-oracledb Thin mode,
    the values of the parameters specified (if any) within the dsn will
    override the values passed as additional keyword parameters, which
    themselves override the values set in the params parameter object.

    The following parameters are all optional. A brief description of each
    parameter follows:

    - min: the minimum number of connections the pool should contain (default:
      1)

    - max: the maximum number of connections the pool should contain (default:
      2)

    - increment: the number of connections that should be added to the pool
      whenever a new connection needs to be created (default: 1)

    - connectiontype: the class of the connection that should be returned
      during calls to pool.acquire(). It must be oracledb.AsyncConnection or a
      subclass of oracledb.AsyncConnection (default: None)

    - getmode: how pool.acquire() will behave. One of the constants
      oracledb.POOL_GETMODE_WAIT, oracledb.POOL_GETMODE_NOWAIT,
      oracledb.POOL_GETMODE_FORCEGET, or oracledb.POOL_GETMODE_TIMEDWAIT
      (default: oracledb.POOL_GETMODE_WAIT)

    - homogeneous: a boolean indicating whether the connections are homogeneous
      (same user) or heterogeneous (multiple users) (default: True)

    - timeout: length of time (in seconds) that a connection may remain idle in
      the pool before it is terminated. If it is 0 then connections are never
      terminated (default: 0)

    - wait_timeout: length of time (in milliseconds) that a caller should wait
      when acquiring a connection from the pool with getmode set to
      oracledb.POOL_GETMODE_TIMEDWAIT (default: 0)

    - max_lifetime_session: length of time (in seconds) that connections can
      remain in the pool. If it is 0 then connections may remain in the pool
      indefinitely (default: 0)

    - session_callback: a callable that is invoked when a connection is
      returned from the pool for the first time, or when the connection tag
      differs from the one requested (default: None)

    - max_sessions_per_shard: the maximum number of connections that may be
      associated with a particular shard (default: 0)

    - soda_metadata_cache: boolean indicating whether or not the SODA metadata
      cache should be enabled (default: False)

    - ping_interval: length of time (in seconds) after which an unused
      connection in the pool will be a candidate for pinging when
      pool.acquire() is called. If the ping to the database indicates the
      connection is not alive a replacement connection will be returned by
      pool.acquire(). If ping_interval is a negative value the ping
      functionality will be disabled (default: 60)

    - ping_timeout: maximum length of time (in milliseconds) to wait for a
      connection in the pool to respond to an internal ping to the database
      before being discarded and replaced during a call to acquire() (default:
      5000)

    - user: the name of the user to connect to (default: None)

    - proxy_user: the name of the proxy user to connect to. If this value is
      not specified, it will be parsed out of user if user is in the form
      "user[proxy_user]" (default: None)

    - password: the password for the user (default: None)

    - newpassword: the new password for the user. The new password will take
      effect immediately upon a successful connection to the database (default:
      None)

    - wallet_password: the password to use to decrypt the wallet, if it is
      encrypted. This value is only used in thin mode (default: None)

    - access_token: expected to be a string or a 2-tuple or a callable. If it
      is a string, it specifies an Azure AD OAuth2 token used for Open
      Authorization (OAuth 2.0) token based authentication. If it is a 2-tuple,
      it specifies the token and private key strings used for Oracle Cloud
      Infrastructure (OCI) Identity and Access Management (IAM) token based
      authentication. If it is a callable, it returns either a string or a
      2-tuple used for OAuth 2.0 or OCI IAM token based authentication and is
      useful when the pool needs to expand and create new connections but the
      current authentication token has expired (default: None)

    - host: the name or IP address of the machine hosting the database or the
      database listener (default: None)

    - port: the port number on which the database listener is listening
      (default: 1521)

    - protocol: one of the strings "tcp" or "tcps" indicating whether to use
      unencrypted network traffic or encrypted network traffic (TLS) (default:
      "tcp")

    - https_proxy: the name or IP address of a proxy host to use for tunneling
      secure connections (default: None)

    - https_proxy_port: the port on which to communicate with the proxy host
      (default: 0)

    - service_name: the service name of the database (default: None)

    - sid: the system identifier (SID) of the database. Note using a
      service_name instead is recommended (default: None)

    - server_type: the type of server connection that should be established. If
      specified, it should be one of "dedicated", "shared" or "pooled"
      (default: None)

    - cclass: connection class to use for Database Resident Connection Pooling
      (DRCP) (default: None)

    - purity: purity to use for Database Resident Connection Pooling (DRCP)
      (default: oracledb.PURITY_DEFAULT)

    - expire_time: an integer indicating the number of minutes between the
      sending of keepalive probes. If this parameter is set to a value greater
      than zero it enables keepalive (default: 0)

    - retry_count: the number of times that a connection attempt should be
      retried before the attempt is terminated (default: 0)

    - retry_delay: the number of seconds to wait before making a new connection
      attempt (default: 1)

    - tcp_connect_timeout: a float indicating the maximum number of seconds to
      wait for establishing a connection to the database host (default: 20.0)

    - ssl_server_dn_match: boolean indicating whether the server certificate
      distinguished name (DN) should be matched in addition to the regular
      certificate verification that is performed. Note that if the
      ssl_server_cert_dn parameter is not privided, host name matching is
      performed instead (default: True)

    - ssl_server_cert_dn: the distinguished name (DN) which should be matched
      with the server. This value is ignored if the ssl_server_dn_match
      parameter is not set to the value True. If specified this value is used
      for any verfication. Otherwise the hostname will be used. (default: None)

    - wallet_location: the directory where the wallet can be found. In thin
      mode this must be the directory containing the PEM-encoded wallet file
      ewallet.pem. In thick mode this must be the directory containing the file
      cwallet.sso (default: None)

    - events: boolean specifying whether events mode should be enabled. This
      value is only used in thick mode and is needed for continuous query
      notification and high availability event notifications (default: False)

    - externalauth: a boolean indicating whether to use external authentication
      (default: False)

    - mode: authorization mode to use. For example oracledb.AUTH_MODE_SYSDBA
      (default: oracledb.AUTH_MODE_DEFAULT)

    - disable_oob: boolean indicating whether out-of-band breaks should be
      disabled. This value is only used in thin mode. It has no effect on
      Windows which does not support this functionality (default: False)

    - stmtcachesize: identifies the initial size of the statement cache
      (default: oracledb.defaults.stmtcachesize)

    - edition: edition to use for the connection. This parameter cannot be used
      simultaneously with the cclass parameter (default: None)

    - tag: identifies the type of connection that should be returned from a
      pool. This value is only used in thick mode (default: None)

    - matchanytag: boolean specifying whether any tag can be used when
      acquiring a connection from the pool. This value is only used in thick
      mode. (default: False)

    - config_dir: directory in which the optional tnsnames.ora configuration
      file is located. This value is only used in thin mode. For thick mode use
      the config_dir parameter of init_oracle_client() (default:
      oracledb.defaults.config_dir)

    - appcontext: application context used by the connection. It should be a
      list of 3-tuples (namespace, name, value) and each entry in the tuple
      should be a string. This value is only used in thick mode (default: None)

    - shardingkey: a list of strings, numbers, bytes or dates that identify the
      database shard to connect to. This value is only used in thick mode
      (default: None)

    - supershardingkey: a list of strings, numbers, bytes or dates that
      identify the database shard to connect to. This value is only used in
      thick mode (default: None)

    - debug_jdwp: a string with the format "host=<host>;port=<port>" that
      specifies the host and port of the PL/SQL debugger. This value is only
      used in thin mode. For thick mode set the ORA_DEBUG_JDWP environment
      variable (default: None)

    - connection_id_prefix: an application specific prefix that is added to the
      connection identifier used for tracing (default: None)

    - ssl_context: an SSLContext object used for connecting to the database
      using TLS.  This SSL context will be modified to include the private key
      or any certificates found in a separately supplied wallet. This parameter
      should only be specified if the default SSLContext object cannot be used
      (default: None)

    - sdu: the requested size of the Session Data Unit (SDU), in bytes. The
      value tunes internal buffers used for communication to the database.
      Bigger values can increase throughput for large queries or bulk data
      loads, but at the cost of higher memory use. The SDU size that will
      actually be used is negotiated down to the lower of this value and the
      database network SDU configuration value (default: 8192)

    - pool_boundary: one of the values "statement" or "transaction" indicating
      when pooled DRCP connections can be returned to the pool. This requires
      the use of DRCP with Oracle Database 23.4 or higher (default: None)

    - use_tcp_fast_open: boolean indicating whether to use TCP fast open. This
      is an Oracle Autonomous Database Serverless (ADB-S) specific property for
      clients connecting from within OCI Cloud network. Please refer to the
      ADB-S documentation for more information (default: False)

    - ssl_version: one of the values ssl.TLSVersion.TLSv1_2 or
      ssl.TLSVersion.TLSv1_3 indicating which TLS version to use (default:
      None)

    - program: the name of the executable program or application connected to
      the Oracle Database (default: oracledb.defaults.program)

    - machine: the machine name of the client connecting to the Oracle Database
      (default: oracledb.defaults.machine)

    - terminal: the terminal identifier from which the connection originates
      (default: oracledb.defaults.terminal)

    - osuser: the operating system user that initiates the database connection
      (default: oracledb.defaults.osuser)

    - driver_name: the driver name used by the client to connect to the Oracle
      Database (default: oracledb.defaults.driver_name)

    - handle: an integer representing a pointer to a valid service context
      handle. This value is only used in thick mode. It should be used with
      extreme caution (default: 0)
    """
    pass
