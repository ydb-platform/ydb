# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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
# connect_params.py
#
# Contains the ConnectParams class used for managing the parameters required to
# establish a connection to the database.
#
# *** NOTICE *** This file is generated from a template and should not be
# modified directly. See build_from_template.py in the utils subdirectory for
# more information.
# -----------------------------------------------------------------------------

import functools
import ssl
from typing import Union, Callable, Any

import oracledb

from . import base_impl, utils


class ConnectParams:
    """
    Contains all parameters used for establishing a connection to the
    database.
    """

    __module__ = oracledb.__name__
    __slots__ = ["_impl"]
    _impl_class = base_impl.ConnectParamsImpl

    @utils.params_initer
    def __init__(
        self,
        *,
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

    def _flatten_value(f):
        """
        Helper function used to flatten arrays of values if they only contain a
        single item.
        """

        @functools.wraps(f)
        def wrapped(self):
            values = f(self)
            return values if len(values) > 1 else values[0]

        return wrapped

    @property
    def appcontext(self) -> list:
        """
        Application context used by the connection. It should be a list of
        3-tuples (namespace, name, value) and each entry in the tuple should be
        a string. This value is only used in thick mode.
        """
        return self._impl.appcontext

    @property
    @_flatten_value
    def cclass(self) -> Union[list, str]:
        """
        Connection class to use for Database Resident Connection Pooling
        (DRCP).
        """
        return [d.cclass for d in self._impl.description_list.children]

    @property
    def config_dir(self) -> str:
        """
        Directory in which the optional tnsnames.ora configuration file is
        located. This value is only used in thin mode. For thick mode use the
        config_dir parameter of init_oracle_client().
        """
        return self._impl.config_dir

    @property
    @_flatten_value
    def connection_id_prefix(self) -> Union[list, str]:
        """
        An application specific prefix that is added to the connection
        identifier used for tracing.
        """
        return [
            d.connection_id_prefix
            for d in self._impl.description_list.children
        ]

    @property
    def debug_jdwp(self) -> str:
        """
        A string with the format "host=<host>;port=<port>" that specifies the
        host and port of the PL/SQL debugger. This value is only used in thin
        mode. For thick mode set the ORA_DEBUG_JDWP environment variable.
        """
        return self._impl.debug_jdwp

    @property
    def disable_oob(self) -> bool:
        """
        Boolean indicating whether out-of-band breaks should be disabled. This
        value is only used in thin mode. It has no effect on Windows which does
        not support this functionality.
        """
        return self._impl.disable_oob

    @property
    def driver_name(self) -> str:
        """
        The driver name used by the client to connect to the Oracle Database.
        """
        return self._impl.driver_name

    @property
    def edition(self) -> str:
        """
        Edition to use for the connection. This parameter cannot be used
        simultaneously with the cclass parameter.
        """
        return self._impl.edition

    @property
    def events(self) -> bool:
        """
        Boolean specifying whether events mode should be enabled. This value is
        only used in thick mode and is needed for continuous query notification
        and high availability event notifications.
        """
        return self._impl.events

    @property
    @_flatten_value
    def expire_time(self) -> Union[list, int]:
        """
        An integer indicating the number of minutes between the sending of
        keepalive probes. If this parameter is set to a value greater than zero
        it enables keepalive.
        """
        return [d.expire_time for d in self._impl.description_list.children]

    @property
    def externalauth(self) -> bool:
        """
        A boolean indicating whether to use external authentication.
        """
        return self._impl.externalauth

    @property
    @_flatten_value
    def host(self) -> Union[list, str]:
        """
        The name or IP address of the machine hosting the database or the
        database listener.
        """
        return [a.host for a in self._impl._get_addresses()]

    @property
    @_flatten_value
    def https_proxy(self) -> Union[list, str]:
        """
        The name or IP address of a proxy host to use for tunneling secure
        connections.
        """
        return [a.https_proxy for a in self._impl._get_addresses()]

    @property
    @_flatten_value
    def https_proxy_port(self) -> Union[list, int]:
        """
        The port on which to communicate with the proxy host.
        """
        return [a.https_proxy_port for a in self._impl._get_addresses()]

    @property
    def machine(self) -> str:
        """
        The machine name of the client connecting to the Oracle Database.
        """
        return self._impl.machine

    @property
    def matchanytag(self) -> bool:
        """
        Boolean specifying whether any tag can be used when acquiring a
        connection from the pool. This value is only used in thick mode..
        """
        return self._impl.matchanytag

    @property
    def mode(self) -> oracledb.AuthMode:
        """
        Authorization mode to use. For example oracledb.AUTH_MODE_SYSDBA.
        """
        return oracledb.AuthMode(self._impl.mode)

    @property
    def osuser(self) -> str:
        """
        The operating system user that initiates the database connection.
        """
        return self._impl.osuser

    @property
    @_flatten_value
    def pool_boundary(self) -> Union[list, str]:
        """
        One of the values "statement" or "transaction" indicating when pooled
        DRCP connections can be returned to the pool. This requires the use of
        DRCP with Oracle Database 23.4 or higher.
        """
        return [d.pool_boundary for d in self._impl.description_list.children]

    @property
    @_flatten_value
    def port(self) -> Union[list, int]:
        """
        The port number on which the database listener is listening.
        """
        return [a.port for a in self._impl._get_addresses()]

    @property
    def program(self) -> str:
        """
        The name of the executable program or application connected to the
        Oracle Database.
        """
        return self._impl.program

    @property
    @_flatten_value
    def protocol(self) -> Union[list, str]:
        """
        One of the strings "tcp" or "tcps" indicating whether to use
        unencrypted network traffic or encrypted network traffic (TLS).
        """
        return [a.protocol for a in self._impl._get_addresses()]

    @property
    def proxy_user(self) -> str:
        """
        The name of the proxy user to connect to. If this value is not
        specified, it will be parsed out of user if user is in the form
        "user[proxy_user]".
        """
        return self._impl.proxy_user

    @property
    @_flatten_value
    def purity(self) -> Union[list, oracledb.Purity]:
        """
        Purity to use for Database Resident Connection Pooling (DRCP).
        """
        return [
            oracledb.Purity(d.purity)
            for d in self._impl.description_list.children
        ]

    @property
    @_flatten_value
    def retry_count(self) -> Union[list, int]:
        """
        The number of times that a connection attempt should be retried before
        the attempt is terminated.
        """
        return [d.retry_count for d in self._impl.description_list.children]

    @property
    @_flatten_value
    def retry_delay(self) -> Union[list, int]:
        """
        The number of seconds to wait before making a new connection attempt.
        """
        return [d.retry_delay for d in self._impl.description_list.children]

    @property
    @_flatten_value
    def sdu(self) -> Union[list, int]:
        """
        The requested size of the Session Data Unit (SDU), in bytes. The value
        tunes internal buffers used for communication to the database. Bigger
        values can increase throughput for large queries or bulk data loads,
        but at the cost of higher memory use. The SDU size that will actually
        be used is negotiated down to the lower of this value and the database
        network SDU configuration value.
        """
        return [d.sdu for d in self._impl.description_list.children]

    @property
    @_flatten_value
    def server_type(self) -> Union[list, str]:
        """
        The type of server connection that should be established. If specified,
        it should be one of "dedicated", "shared" or "pooled".
        """
        return [d.server_type for d in self._impl.description_list.children]

    @property
    @_flatten_value
    def service_name(self) -> Union[list, str]:
        """
        The service name of the database.
        """
        return [d.service_name for d in self._impl.description_list.children]

    @property
    def shardingkey(self) -> list:
        """
        A list of strings, numbers, bytes or dates that identify the database
        shard to connect to. This value is only used in thick mode.
        """
        return self._impl.shardingkey

    @property
    @_flatten_value
    def sid(self) -> Union[list, str]:
        """
        The system identifier (SID) of the database. Note using a service_name
        instead is recommended.
        """
        return [d.sid for d in self._impl.description_list.children]

    @property
    def ssl_context(self) -> Any:
        """
        An SSLContext object used for connecting to the database using TLS.
        This SSL context will be modified to include the private key or any
        certificates found in a separately supplied wallet. This parameter
        should only be specified if the default SSLContext object cannot be
        used.
        """
        return self._impl.ssl_context

    @property
    @_flatten_value
    def ssl_server_cert_dn(self) -> Union[list, str]:
        """
        The distinguished name (DN) which should be matched with the server.
        This value is ignored if the ssl_server_dn_match parameter is not set
        to the value True. If specified this value is used for any verfication.
        Otherwise the hostname will be used..
        """
        return [
            d.ssl_server_cert_dn for d in self._impl.description_list.children
        ]

    @property
    @_flatten_value
    def ssl_server_dn_match(self) -> Union[list, bool]:
        """
        Boolean indicating whether the server certificate distinguished name
        (DN) should be matched in addition to the regular certificate
        verification that is performed. Note that if the ssl_server_cert_dn
        parameter is not privided, host name matching is performed instead.
        """
        return [
            d.ssl_server_dn_match for d in self._impl.description_list.children
        ]

    @property
    @_flatten_value
    def ssl_version(self) -> Union[list, ssl.TLSVersion]:
        """
        One of the values ssl.TLSVersion.TLSv1_2 or ssl.TLSVersion.TLSv1_3
        indicating which TLS version to use.
        """
        return [d.ssl_version for d in self._impl.description_list.children]

    @property
    def stmtcachesize(self) -> int:
        """
        Identifies the initial size of the statement cache.
        """
        return self._impl.stmtcachesize

    @property
    def supershardingkey(self) -> list:
        """
        A list of strings, numbers, bytes or dates that identify the database
        shard to connect to. This value is only used in thick mode.
        """
        return self._impl.supershardingkey

    @property
    def tag(self) -> str:
        """
        Identifies the type of connection that should be returned from a pool.
        This value is only used in thick mode.
        """
        return self._impl.tag

    @property
    @_flatten_value
    def tcp_connect_timeout(self) -> Union[list, float]:
        """
        A float indicating the maximum number of seconds to wait for
        establishing a connection to the database host.
        """
        return [
            d.tcp_connect_timeout for d in self._impl.description_list.children
        ]

    @property
    def terminal(self) -> str:
        """
        The terminal identifier from which the connection originates.
        """
        return self._impl.terminal

    @property
    def user(self) -> str:
        """
        The name of the user to connect to.
        """
        return self._impl.user

    @property
    @_flatten_value
    def use_tcp_fast_open(self) -> Union[list, bool]:
        """
        Boolean indicating whether to use TCP fast open. This is an Oracle
        Autonomous Database Serverless (ADB-S) specific property for clients
        connecting from within OCI Cloud network. Please refer to the ADB-S
        documentation for more information.
        """
        return [
            d.use_tcp_fast_open for d in self._impl.description_list.children
        ]

    @property
    @_flatten_value
    def wallet_location(self) -> Union[list, str]:
        """
        The directory where the wallet can be found. In thin mode this must be
        the directory containing the PEM-encoded wallet file ewallet.pem. In
        thick mode this must be the directory containing the file cwallet.sso.
        """
        return [
            d.wallet_location for d in self._impl.description_list.children
        ]

    def copy(self) -> "ConnectParams":
        """
        Creates a copy of the parameters and returns it.
        """
        params = ConnectParams.__new__(ConnectParams)
        params._impl = self._impl.copy()
        return params

    def get_connect_string(self) -> str:
        """
        Returns a connect string generated from the parameters.
        """
        return self._impl.get_connect_string()

    def get_network_service_names(self) -> list:
        """
        Returns a list of the network service names found in the tnsnames.ora
        file found in the configuration directory associated with the
        parameters. If no such file exists, an error is raised.
        """
        return self._impl.get_network_service_names()

    def parse_connect_string(self, connect_string: str) -> None:
        """
        Parses the connect string into its components and stores the
        parameters.  The connect string could be an Easy Connect string,
        name-value pairs or a simple alias which is looked up in tnsnames.ora.
        Any parameters found in the connect string override any currently
        stored values.
        """
        self._impl.parse_connect_string(connect_string)

    def parse_dsn_with_credentials(self, dsn: str) -> tuple:
        """
        Parses a dsn in the form <user>/<password>@<connect_string> or in the
        form <user>/<password> and returns a 3-tuple containing the parsed
        user, password and connect string. Empty strings are returned as the
        value None. This is done automatically when a value is passed to
        the dsn parameter but no value is passed to the user password when
        creating a standalone connection or connection pool.
        """
        return self._impl.parse_dsn_with_credentials(dsn)

    @utils.params_setter
    def set(
        self,
        *,
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
