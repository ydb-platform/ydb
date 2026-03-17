# -----------------------------------------------------------------------------
# Copyright (c) 2022, 2024, Oracle and/or its affiliates.
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
# pool_params.py
#
# Contains the PoolParams class used for managing the parameters required to
# create a connection pool.
#
# *** NOTICE *** This file is generated from a template and should not be
# modified directly. See build_from_template.py in the utils subdirectory for
# more information.
# -----------------------------------------------------------------------------

import ssl
from typing import Callable, Type, Union, Any

import oracledb

from . import base_impl, utils
from .connect_params import ConnectParams


class PoolParams(ConnectParams):
    """
    Contains all parameters used for creating a connection pool.
    """

    __module__ = oracledb.__name__
    __slots__ = ["_impl"]
    _impl_class = base_impl.PoolParamsImpl

    @utils.params_initer
    def __init__(
        self,
        *,
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
    ):
        """
        All parameters are optional. A brief description of each parameter
        follows:

        - min: the minimum number of connections the pool should contain
          (default: 1)

        - max: the maximum number of connections the pool should contain
          (default: 2)

        - increment: the number of connections that should be added to the pool
          whenever a new connection needs to be created (default: 1)

        - connectiontype: the class of the connection that should be returned
          during calls to pool.acquire(). It must be oracledb.Connection or a
          subclass of oracledb.Connection (default: None)

        - getmode: how pool.acquire() will behave. One of the constants
          oracledb.POOL_GETMODE_WAIT, oracledb.POOL_GETMODE_NOWAIT,
          oracledb.POOL_GETMODE_FORCEGET, or oracledb.POOL_GETMODE_TIMEDWAIT
          (default: oracledb.POOL_GETMODE_WAIT)

        - homogeneous: a boolean indicating whether the connections are
          homogeneous (same user) or heterogeneous (multiple users) (default:
          True)

        - timeout: length of time (in seconds) that a connection may remain
          idle in the pool before it is terminated. If it is 0 then connections
          are never terminated (default: 0)

        - wait_timeout: length of time (in milliseconds) that a caller should
          wait when acquiring a connection from the pool with getmode set to
          oracledb.POOL_GETMODE_TIMEDWAIT (default: 0)

        - max_lifetime_session: length of time (in seconds) that connections
          can remain in the pool. If it is 0 then connections may remain in the
          pool indefinitely (default: 0)

        - session_callback: a callable that is invoked when a connection is
          returned from the pool for the first time, or when the connection tag
          differs from the one requested (default: None)

        - max_sessions_per_shard: the maximum number of connections that may be
          associated with a particular shard (default: 0)

        - soda_metadata_cache: boolean indicating whether or not the SODA
          metadata cache should be enabled (default: False)

        - ping_interval: length of time (in seconds) after which an unused
          connection in the pool will be a candidate for pinging when
          pool.acquire() is called. If the ping to the database indicates the
          connection is not alive a replacement connection will be returned by
          pool.acquire(). If ping_interval is a negative value the ping
          functionality will be disabled (default: 60)

        - ping_timeout: maximum length of time (in milliseconds) to wait for a
          connection in the pool to respond to an internal ping to the database
          before being discarded and replaced during a call to acquire()
          (default: 5000)

        - user: the name of the user to connect to (default: None)

        - proxy_user: the name of the proxy user to connect to. If this value
          is not specified, it will be parsed out of user if user is in the
          form "user[proxy_user]" (default: None)

        - password: the password for the user (default: None)

        - newpassword: the new password for the user. The new password will
          take effect immediately upon a successful connection to the database
          (default: None)

        - wallet_password: the password to use to decrypt the wallet, if it is
          encrypted. This value is only used in thin mode (default: None)

        - access_token: expected to be a string or a 2-tuple or a callable. If
          it is a string, it specifies an Azure AD OAuth2 token used for Open
          Authorization (OAuth 2.0) token based authentication. If it is a
          2-tuple, it specifies the token and private key strings used for
          Oracle Cloud Infrastructure (OCI) Identity and Access Management
          (IAM) token based authentication. If it is a callable, it returns
          either a string or a 2-tuple used for OAuth 2.0 or OCI IAM token
          based authentication and is useful when the pool needs to expand and
          create new connections but the current authentication token has
          expired (default: None)

        - host: the name or IP address of the machine hosting the database or
          the database listener (default: None)

        - port: the port number on which the database listener is listening
          (default: 1521)

        - protocol: one of the strings "tcp" or "tcps" indicating whether to
          use unencrypted network traffic or encrypted network traffic (TLS)
          (default: "tcp")

        - https_proxy: the name or IP address of a proxy host to use for
          tunneling secure connections (default: None)

        - https_proxy_port: the port on which to communicate with the proxy
          host (default: 0)

        - service_name: the service name of the database (default: None)

        - sid: the system identifier (SID) of the database. Note using a
          service_name instead is recommended (default: None)

        - server_type: the type of server connection that should be
          established. If specified, it should be one of "dedicated", "shared"
          or "pooled" (default: None)

        - cclass: connection class to use for Database Resident Connection
          Pooling (DRCP) (default: None)

        - purity: purity to use for Database Resident Connection Pooling (DRCP)
          (default: oracledb.PURITY_DEFAULT)

        - expire_time: an integer indicating the number of minutes between the
          sending of keepalive probes. If this parameter is set to a value
          greater than zero it enables keepalive (default: 0)

        - retry_count: the number of times that a connection attempt should be
          retried before the attempt is terminated (default: 0)

        - retry_delay: the number of seconds to wait before making a new
          connection attempt (default: 1)

        - tcp_connect_timeout: a float indicating the maximum number of seconds
          to wait for establishing a connection to the database host (default:
          20.0)

        - ssl_server_dn_match: boolean indicating whether the server
          certificate distinguished name (DN) should be matched in addition to
          the regular certificate verification that is performed. Note that if
          the ssl_server_cert_dn parameter is not privided, host name matching
          is performed instead (default: True)

        - ssl_server_cert_dn: the distinguished name (DN) which should be
          matched with the server. This value is ignored if the
          ssl_server_dn_match parameter is not set to the value True. If
          specified this value is used for any verfication. Otherwise the
          hostname will be used. (default: None)

        - wallet_location: the directory where the wallet can be found. In thin
          mode this must be the directory containing the PEM-encoded wallet
          file ewallet.pem. In thick mode this must be the directory containing
          the file cwallet.sso (default: None)

        - events: boolean specifying whether events mode should be enabled.
          This value is only used in thick mode and is needed for continuous
          query notification and high availability event notifications
          (default: False)

        - externalauth: a boolean indicating whether to use external
          authentication (default: False)

        - mode: authorization mode to use. For example
          oracledb.AUTH_MODE_SYSDBA (default: oracledb.AUTH_MODE_DEFAULT)

        - disable_oob: boolean indicating whether out-of-band breaks should be
          disabled. This value is only used in thin mode. It has no effect on
          Windows which does not support this functionality (default: False)

        - stmtcachesize: identifies the initial size of the statement cache
          (default: oracledb.defaults.stmtcachesize)

        - edition: edition to use for the connection. This parameter cannot be
          used simultaneously with the cclass parameter (default: None)

        - tag: identifies the type of connection that should be returned from a
          pool. This value is only used in thick mode (default: None)

        - matchanytag: boolean specifying whether any tag can be used when
          acquiring a connection from the pool. This value is only used in
          thick mode. (default: False)

        - config_dir: directory in which the optional tnsnames.ora
          configuration file is located. This value is only used in thin mode.
          For thick mode use the config_dir parameter of init_oracle_client()
          (default: oracledb.defaults.config_dir)

        - appcontext: application context used by the connection. It should be
          a list of 3-tuples (namespace, name, value) and each entry in the
          tuple should be a string. This value is only used in thick mode
          (default: None)

        - shardingkey: a list of strings, numbers, bytes or dates that identify
          the database shard to connect to. This value is only used in thick
          mode (default: None)

        - supershardingkey: a list of strings, numbers, bytes or dates that
          identify the database shard to connect to. This value is only used in
          thick mode (default: None)

        - debug_jdwp: a string with the format "host=<host>;port=<port>" that
          specifies the host and port of the PL/SQL debugger. This value is
          only used in thin mode. For thick mode set the ORA_DEBUG_JDWP
          environment variable (default: None)

        - connection_id_prefix: an application specific prefix that is added to
          the connection identifier used for tracing (default: None)

        - ssl_context: an SSLContext object used for connecting to the database
          using TLS.  This SSL context will be modified to include the private
          key or any certificates found in a separately supplied wallet. This
          parameter should only be specified if the default SSLContext object
          cannot be used (default: None)

        - sdu: the requested size of the Session Data Unit (SDU), in bytes. The
          value tunes internal buffers used for communication to the database.
          Bigger values can increase throughput for large queries or bulk data
          loads, but at the cost of higher memory use. The SDU size that will
          actually be used is negotiated down to the lower of this value and
          the database network SDU configuration value (default: 8192)

        - pool_boundary: one of the values "statement" or "transaction"
          indicating when pooled DRCP connections can be returned to the pool.
          This requires the use of DRCP with Oracle Database 23.4 or higher
          (default: None)

        - use_tcp_fast_open: boolean indicating whether to use TCP fast open.
          This is an Oracle Autonomous Database Serverless (ADB-S) specific
          property for clients connecting from within OCI Cloud network. Please
          refer to the ADB-S documentation for more information (default:
          False)

        - ssl_version: one of the values ssl.TLSVersion.TLSv1_2 or
          ssl.TLSVersion.TLSv1_3 indicating which TLS version to use (default:
          None)

        - program: the name of the executable program or application connected
          to the Oracle Database (default: oracledb.defaults.program)

        - machine: the machine name of the client connecting to the Oracle
          Database (default: oracledb.defaults.machine)

        - terminal: the terminal identifier from which the connection
          originates (default: oracledb.defaults.terminal)

        - osuser: the operating system user that initiates the database
          connection (default: oracledb.defaults.osuser)

        - driver_name: the driver name used by the client to connect to the
          Oracle Database (default: oracledb.defaults.driver_name)

        - handle: an integer representing a pointer to a valid service context
          handle. This value is only used in thick mode. It should be used with
          extreme caution (default: 0)
        """
        pass

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + "("
            + f"min={self.min!r}, "
            + f"max={self.max!r}, "
            + f"increment={self.increment!r}, "
            + f"connectiontype={self.connectiontype!r}, "
            + f"getmode={self.getmode!r}, "
            + f"homogeneous={self.homogeneous!r}, "
            + f"timeout={self.timeout!r}, "
            + f"wait_timeout={self.wait_timeout!r}, "
            + f"max_lifetime_session={self.max_lifetime_session!r}, "
            + f"session_callback={self.session_callback!r}, "
            + f"max_sessions_per_shard={self.max_sessions_per_shard!r}, "
            + f"soda_metadata_cache={self.soda_metadata_cache!r}, "
            + f"ping_interval={self.ping_interval!r}, "
            + f"ping_timeout={self.ping_timeout!r}, "
            + f"user={self.user!r}, "
            + f"proxy_user={self.proxy_user!r}, "
            + f"host={self.host!r}, "
            + f"port={self.port!r}, "
            + f"protocol={self.protocol!r}, "
            + f"https_proxy={self.https_proxy!r}, "
            + f"https_proxy_port={self.https_proxy_port!r}, "
            + f"service_name={self.service_name!r}, "
            + f"sid={self.sid!r}, "
            + f"server_type={self.server_type!r}, "
            + f"cclass={self.cclass!r}, "
            + f"purity={self.purity!r}, "
            + f"expire_time={self.expire_time!r}, "
            + f"retry_count={self.retry_count!r}, "
            + f"retry_delay={self.retry_delay!r}, "
            + f"tcp_connect_timeout={self.tcp_connect_timeout!r}, "
            + f"ssl_server_dn_match={self.ssl_server_dn_match!r}, "
            + f"ssl_server_cert_dn={self.ssl_server_cert_dn!r}, "
            + f"wallet_location={self.wallet_location!r}, "
            + f"events={self.events!r}, "
            + f"externalauth={self.externalauth!r}, "
            + f"mode={self.mode!r}, "
            + f"disable_oob={self.disable_oob!r}, "
            + f"stmtcachesize={self.stmtcachesize!r}, "
            + f"edition={self.edition!r}, "
            + f"tag={self.tag!r}, "
            + f"matchanytag={self.matchanytag!r}, "
            + f"config_dir={self.config_dir!r}, "
            + f"appcontext={self.appcontext!r}, "
            + f"shardingkey={self.shardingkey!r}, "
            + f"supershardingkey={self.supershardingkey!r}, "
            + f"debug_jdwp={self.debug_jdwp!r}, "
            + f"connection_id_prefix={self.connection_id_prefix!r}, "
            + f"ssl_context={self.ssl_context!r}, "
            + f"sdu={self.sdu!r}, "
            + f"pool_boundary={self.pool_boundary!r}, "
            + f"use_tcp_fast_open={self.use_tcp_fast_open!r}, "
            + f"ssl_version={self.ssl_version!r}, "
            + f"program={self.program!r}, "
            + f"machine={self.machine!r}, "
            + f"terminal={self.terminal!r}, "
            + f"osuser={self.osuser!r}, "
            + f"driver_name={self.driver_name!r}"
            + ")"
        )

    @property
    def connectiontype(self) -> Type["oracledb.Connection"]:
        """
        The class of the connection that should be returned during calls to
        pool.acquire(). It must be oracledb.Connection or a subclass of
        oracledb.Connection.
        """
        return self._impl.connectiontype

    @property
    def getmode(self) -> oracledb.PoolGetMode:
        """
        How pool.acquire() will behave. One of the constants
        oracledb.POOL_GETMODE_WAIT, oracledb.POOL_GETMODE_NOWAIT,
        oracledb.POOL_GETMODE_FORCEGET, or oracledb.POOL_GETMODE_TIMEDWAIT.
        """
        return oracledb.PoolGetMode(self._impl.getmode)

    @property
    def homogeneous(self) -> bool:
        """
        A boolean indicating whether the connections are homogeneous (same
        user) or heterogeneous (multiple users).
        """
        return self._impl.homogeneous

    @property
    def increment(self) -> int:
        """
        The number of connections that should be added to the pool whenever a
        new connection needs to be created.
        """
        return self._impl.increment

    @property
    def max(self) -> int:
        """
        The maximum number of connections the pool should contain.
        """
        return self._impl.max

    @property
    def max_lifetime_session(self) -> int:
        """
        Length of time (in seconds) that connections can remain in the pool. If
        it is 0 then connections may remain in the pool indefinitely.
        """
        return self._impl.max_lifetime_session

    @property
    def max_sessions_per_shard(self) -> int:
        """
        The maximum number of connections that may be associated with a
        particular shard.
        """
        return self._impl.max_sessions_per_shard

    @property
    def min(self) -> int:
        """
        The minimum number of connections the pool should contain.
        """
        return self._impl.min

    @property
    def ping_interval(self) -> int:
        """
        Length of time (in seconds) after which an unused connection in the
        pool will be a candidate for pinging when pool.acquire() is called. If
        the ping to the database indicates the connection is not alive a
        replacement connection will be returned by pool.acquire(). If
        ping_interval is a negative value the ping functionality will be
        disabled.
        """
        return self._impl.ping_interval

    @property
    def ping_timeout(self) -> int:
        """
        Maximum length of time (in milliseconds) to wait for a connection in
        the pool to respond to an internal ping to the database before being
        discarded and replaced during a call to acquire().
        """
        return self._impl.ping_timeout

    @property
    def session_callback(self) -> Callable:
        """
        A callable that is invoked when a connection is returned from the pool
        for the first time, or when the connection tag differs from the one
        requested.
        """
        return self._impl.session_callback

    @property
    def soda_metadata_cache(self) -> bool:
        """
        Boolean indicating whether or not the SODA metadata cache should be
        enabled.
        """
        return self._impl.soda_metadata_cache

    @property
    def timeout(self) -> int:
        """
        Length of time (in seconds) that a connection may remain idle in the
        pool before it is terminated. If it is 0 then connections are never
        terminated.
        """
        return self._impl.timeout

    @property
    def wait_timeout(self) -> int:
        """
        Length of time (in milliseconds) that a caller should wait when
        acquiring a connection from the pool with getmode set to
        oracledb.POOL_GETMODE_TIMEDWAIT.
        """
        return self._impl.wait_timeout

    def copy(self) -> "PoolParams":
        """
        Creates a copy of the parameters and returns it.
        """
        params = PoolParams.__new__(PoolParams)
        params._impl = self._impl.copy()
        return params

    @utils.params_setter
    def set(
        self,
        *,
        min: int = None,
        max: int = None,
        increment: int = None,
        connectiontype: Type["oracledb.Connection"] = None,
        getmode: oracledb.PoolGetMode = None,
        homogeneous: bool = None,
        timeout: int = None,
        wait_timeout: int = None,
        max_lifetime_session: int = None,
        session_callback: Callable = None,
        max_sessions_per_shard: int = None,
        soda_metadata_cache: bool = None,
        ping_interval: int = None,
        ping_timeout: int = None,
        user: str = None,
        proxy_user: str = None,
        password: str = None,
        newpassword: str = None,
        wallet_password: str = None,
        access_token: Union[str, tuple, Callable] = None,
        host: str = None,
        port: int = None,
        protocol: str = None,
        https_proxy: str = None,
        https_proxy_port: int = None,
        service_name: str = None,
        sid: str = None,
        server_type: str = None,
        cclass: str = None,
        purity: oracledb.Purity = None,
        expire_time: int = None,
        retry_count: int = None,
        retry_delay: int = None,
        tcp_connect_timeout: float = None,
        ssl_server_dn_match: bool = None,
        ssl_server_cert_dn: str = None,
        wallet_location: str = None,
        events: bool = None,
        externalauth: bool = None,
        mode: oracledb.AuthMode = None,
        disable_oob: bool = None,
        stmtcachesize: int = None,
        edition: str = None,
        tag: str = None,
        matchanytag: bool = None,
        config_dir: str = None,
        appcontext: list = None,
        shardingkey: list = None,
        supershardingkey: list = None,
        debug_jdwp: str = None,
        connection_id_prefix: str = None,
        ssl_context: Any = None,
        sdu: int = None,
        pool_boundary: str = None,
        use_tcp_fast_open: bool = None,
        ssl_version: ssl.TLSVersion = None,
        program: str = None,
        machine: str = None,
        terminal: str = None,
        osuser: str = None,
        driver_name: str = None,
        handle: int = None,
    ):
        """
        All parameters are optional. A brief description of each parameter
        follows:

        - min: the minimum number of connections the pool should contain

        - max: the maximum number of connections the pool should contain

        - increment: the number of connections that should be added to the pool
          whenever a new connection needs to be created

        - connectiontype: the class of the connection that should be returned
          during calls to pool.acquire(). It must be oracledb.Connection or a
          subclass of oracledb.Connection

        - getmode: how pool.acquire() will behave. One of the constants
          oracledb.POOL_GETMODE_WAIT, oracledb.POOL_GETMODE_NOWAIT,
          oracledb.POOL_GETMODE_FORCEGET, or oracledb.POOL_GETMODE_TIMEDWAIT

        - homogeneous: a boolean indicating whether the connections are
          homogeneous (same user) or heterogeneous (multiple users)

        - timeout: length of time (in seconds) that a connection may remain
          idle in the pool before it is terminated. If it is 0 then connections
          are never terminated

        - wait_timeout: length of time (in milliseconds) that a caller should
          wait when acquiring a connection from the pool with getmode set to
          oracledb.POOL_GETMODE_TIMEDWAIT

        - max_lifetime_session: length of time (in seconds) that connections
          can remain in the pool. If it is 0 then connections may remain in the
          pool indefinitely

        - session_callback: a callable that is invoked when a connection is
          returned from the pool for the first time, or when the connection tag
          differs from the one requested

        - max_sessions_per_shard: the maximum number of connections that may be
          associated with a particular shard

        - soda_metadata_cache: boolean indicating whether or not the SODA
          metadata cache should be enabled

        - ping_interval: length of time (in seconds) after which an unused
          connection in the pool will be a candidate for pinging when
          pool.acquire() is called. If the ping to the database indicates the
          connection is not alive a replacement connection will be returned by
          pool.acquire(). If ping_interval is a negative value the ping
          functionality will be disabled

        - ping_timeout: maximum length of time (in milliseconds) to wait for a
          connection in the pool to respond to an internal ping to the database
          before being discarded and replaced during a call to acquire()

        - user: the name of the user to connect to

        - proxy_user: the name of the proxy user to connect to. If this value
          is not specified, it will be parsed out of user if user is in the
          form "user[proxy_user]"

        - password: the password for the user

        - newpassword: the new password for the user. The new password will
          take effect immediately upon a successful connection to the database

        - wallet_password: the password to use to decrypt the wallet, if it is
          encrypted. This value is only used in thin mode

        - access_token: expected to be a string or a 2-tuple or a callable. If
          it is a string, it specifies an Azure AD OAuth2 token used for Open
          Authorization (OAuth 2.0) token based authentication. If it is a
          2-tuple, it specifies the token and private key strings used for
          Oracle Cloud Infrastructure (OCI) Identity and Access Management
          (IAM) token based authentication. If it is a callable, it returns
          either a string or a 2-tuple used for OAuth 2.0 or OCI IAM token
          based authentication and is useful when the pool needs to expand and
          create new connections but the current authentication token has
          expired

        - host: the name or IP address of the machine hosting the database or
          the database listener

        - port: the port number on which the database listener is listening

        - protocol: one of the strings "tcp" or "tcps" indicating whether to
          use unencrypted network traffic or encrypted network traffic (TLS)

        - https_proxy: the name or IP address of a proxy host to use for
          tunneling secure connections

        - https_proxy_port: the port on which to communicate with the proxy
          host

        - service_name: the service name of the database

        - sid: the system identifier (SID) of the database. Note using a
          service_name instead is recommended

        - server_type: the type of server connection that should be
          established. If specified, it should be one of "dedicated", "shared"
          or "pooled"

        - cclass: connection class to use for Database Resident Connection
          Pooling (DRCP)

        - purity: purity to use for Database Resident Connection Pooling (DRCP)

        - expire_time: an integer indicating the number of minutes between the
          sending of keepalive probes. If this parameter is set to a value
          greater than zero it enables keepalive

        - retry_count: the number of times that a connection attempt should be
          retried before the attempt is terminated

        - retry_delay: the number of seconds to wait before making a new
          connection attempt

        - tcp_connect_timeout: a float indicating the maximum number of seconds
          to wait for establishing a connection to the database host

        - ssl_server_dn_match: boolean indicating whether the server
          certificate distinguished name (DN) should be matched in addition to
          the regular certificate verification that is performed. Note that if
          the ssl_server_cert_dn parameter is not privided, host name matching
          is performed instead

        - ssl_server_cert_dn: the distinguished name (DN) which should be
          matched with the server. This value is ignored if the
          ssl_server_dn_match parameter is not set to the value True. If
          specified this value is used for any verfication. Otherwise the
          hostname will be used.

        - wallet_location: the directory where the wallet can be found. In thin
          mode this must be the directory containing the PEM-encoded wallet
          file ewallet.pem. In thick mode this must be the directory containing
          the file cwallet.sso

        - events: boolean specifying whether events mode should be enabled.
          This value is only used in thick mode and is needed for continuous
          query notification and high availability event notifications

        - externalauth: a boolean indicating whether to use external
          authentication

        - mode: authorization mode to use. For example
          oracledb.AUTH_MODE_SYSDBA

        - disable_oob: boolean indicating whether out-of-band breaks should be
          disabled. This value is only used in thin mode. It has no effect on
          Windows which does not support this functionality

        - stmtcachesize: identifies the initial size of the statement cache

        - edition: edition to use for the connection. This parameter cannot be
          used simultaneously with the cclass parameter

        - tag: identifies the type of connection that should be returned from a
          pool. This value is only used in thick mode

        - matchanytag: boolean specifying whether any tag can be used when
          acquiring a connection from the pool. This value is only used in
          thick mode.

        - config_dir: directory in which the optional tnsnames.ora
          configuration file is located. This value is only used in thin mode.
          For thick mode use the config_dir parameter of init_oracle_client()

        - appcontext: application context used by the connection. It should be
          a list of 3-tuples (namespace, name, value) and each entry in the
          tuple should be a string. This value is only used in thick mode

        - shardingkey: a list of strings, numbers, bytes or dates that identify
          the database shard to connect to. This value is only used in thick
          mode

        - supershardingkey: a list of strings, numbers, bytes or dates that
          identify the database shard to connect to. This value is only used in
          thick mode

        - debug_jdwp: a string with the format "host=<host>;port=<port>" that
          specifies the host and port of the PL/SQL debugger. This value is
          only used in thin mode. For thick mode set the ORA_DEBUG_JDWP
          environment variable

        - connection_id_prefix: an application specific prefix that is added to
          the connection identifier used for tracing

        - ssl_context: an SSLContext object used for connecting to the database
          using TLS.  This SSL context will be modified to include the private
          key or any certificates found in a separately supplied wallet. This
          parameter should only be specified if the default SSLContext object
          cannot be used

        - sdu: the requested size of the Session Data Unit (SDU), in bytes. The
          value tunes internal buffers used for communication to the database.
          Bigger values can increase throughput for large queries or bulk data
          loads, but at the cost of higher memory use. The SDU size that will
          actually be used is negotiated down to the lower of this value and
          the database network SDU configuration value

        - pool_boundary: one of the values "statement" or "transaction"
          indicating when pooled DRCP connections can be returned to the pool.
          This requires the use of DRCP with Oracle Database 23.4 or higher

        - use_tcp_fast_open: boolean indicating whether to use TCP fast open.
          This is an Oracle Autonomous Database Serverless (ADB-S) specific
          property for clients connecting from within OCI Cloud network. Please
          refer to the ADB-S documentation for more information

        - ssl_version: one of the values ssl.TLSVersion.TLSv1_2 or
          ssl.TLSVersion.TLSv1_3 indicating which TLS version to use

        - program: the name of the executable program or application connected
          to the Oracle Database

        - machine: the machine name of the client connecting to the Oracle
          Database

        - terminal: the terminal identifier from which the connection
          originates

        - osuser: the operating system user that initiates the database
          connection

        - driver_name: the driver name used by the client to connect to the
          Oracle Database

        - handle: an integer representing a pointer to a valid service context
          handle. This value is only used in thick mode. It should be used with
          extreme caution
        """
        pass
