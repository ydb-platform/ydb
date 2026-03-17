#------------------------------------------------------------------------------
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# connect_params.pyx
#
# Cython file defining the base ConnectParams implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

# dictionary of tnsnames.ora files, indexed by the directory in which the file
# is found; the results are cached in order to avoid parsing a file multiple
# times; the modification time of the file is checked each time, though, to
# ensure that no changes were made since the last time that the file was read
# and parsed.
_tnsnames_files = {}

# internal default values
cdef str DEFAULT_PROTOCOL = "tcp"
cdef uint32_t DEFAULT_PORT = 1521
cdef double DEFAULT_TCP_CONNECT_TIMEOUT = 20
cdef uint32_t DEFAULT_RETRY_DELAY = 1
cdef uint32_t DEFAULT_SDU = 8192


cdef class ConnectParamsImpl:

    def __init__(self):
        cdef AddressList address_list
        self.stmtcachesize = C_DEFAULTS.stmtcachesize
        self.config_dir = C_DEFAULTS.config_dir
        self._default_description = Description()
        self._default_address = Address()
        self.description_list = DescriptionList()
        self.description_list.children.append(self._default_description)
        self.debug_jdwp = os.getenv("ORA_DEBUG_JDWP")
        self.edition = os.getenv("ORA_EDITION")
        address_list = AddressList()
        address_list.children.append(self._default_address)
        self._default_description.children.append(address_list)
        self.program = C_DEFAULTS.program
        self.terminal = C_DEFAULTS.terminal
        self.machine = C_DEFAULTS.machine
        self.osuser = C_DEFAULTS.osuser
        self.driver_name = C_DEFAULTS.driver_name

    def set(self, dict args):
        """
        Sets the property values based on the supplied arguments. All values
        not supplied will be left unchanged.
        """
        cdef:
            Description description
            Address address

        # set parameters found directly on the ConnectParamsImpl object
        self._external_handle = args.get("handle", self._external_handle)
        _set_str_param(args, "user", self)
        _set_str_param(args, "proxy_user", self)
        if self.proxy_user is None and self.user is not None:
            self.parse_user(self.user)
        self._set_password(args.get("password"))
        self._set_new_password(args.get("newpassword"))
        self._set_wallet_password(args.get("wallet_password"))
        _set_bool_param(args, "events", &self.events)
        _set_uint_param(args, "mode", &self.mode)
        _set_str_param(args, "edition", self)
        _set_str_param(args, "tag", self)
        _set_bool_param(args, "matchanytag", &self.matchanytag)
        _set_uint_param(args, "stmtcachesize", &self.stmtcachesize)
        _set_bool_param(args, "disable_oob", &self.disable_oob)
        _set_obj_param(args, "ssl_context", self)
        _set_str_param(args, "debug_jdwp", self)
        _set_str_param(args, "config_dir", self)
        _set_app_context_param(args, "appcontext", self)
        _set_obj_param(args, "shardingkey", self)
        _set_obj_param(args, "supershardingkey", self)
        _set_bool_param(args, "externalauth", &self.externalauth)
        _set_str_param(args, "program", self, check_network_character_set=True)
        _set_str_param(args, "terminal", self)
        _set_str_param(args, "machine", self, check_network_character_set=True)
        _set_str_param(args, "osuser", self, check_network_character_set=True)
        _set_str_param(args, "driver_name", self)
        self._set_access_token_param(args.get("access_token"))

        # set parameters found on Description instances
        self._default_description.set_from_args(args)
        for description in self.description_list.children:
            if description is not self._default_description:
                description.set_from_args(args)

        # set parameters found on Address instances
        self._default_address.set_from_args(args)
        for address in self.description_list.get_addresses():
            if address is not self._default_address:
                address.set_from_args(args)

    cdef int _check_credentials(self) except -1:
        """
        Check to see that credentials have been supplied: either a password or
        an access token.
        """
        if self._password is None and self._token is None \
                and self.access_token_callback is None:
            errors._raise_err(errors.ERR_NO_CREDENTIALS)

    cdef int _copy(self, ConnectParamsImpl other_params) except -1:
        """
        Internal method for copying attributes from another set of parameters.
        """
        self.config_dir = other_params.config_dir
        self.user = other_params.user
        self.proxy_user = other_params.proxy_user
        self.events = other_params.events
        self.externalauth = other_params.externalauth
        self.mode = other_params.mode
        self.edition = other_params.edition
        self.appcontext = other_params.appcontext
        self.tag = other_params.tag
        self.matchanytag = other_params.matchanytag
        self.shardingkey = other_params.shardingkey
        self.supershardingkey = other_params.supershardingkey
        self.stmtcachesize = other_params.stmtcachesize
        self.disable_oob = other_params.disable_oob
        self.debug_jdwp = other_params.debug_jdwp
        self.ssl_context = other_params.ssl_context
        self.description_list = other_params.description_list
        self.access_token_callback = other_params.access_token_callback
        self.access_token_expires = other_params.access_token_expires
        self._external_handle = other_params._external_handle
        self._default_description = other_params._default_description
        self._default_address = other_params._default_address
        self._password = other_params._password
        self._password_obfuscator = other_params._password_obfuscator
        self._new_password = other_params._new_password
        self._new_password_obfuscator = other_params._new_password_obfuscator
        self._wallet_password = other_params._wallet_password
        self._wallet_password_obfuscator = \
                other_params._wallet_password_obfuscator
        self._token = other_params._token
        self._token_obfuscator = other_params._token_obfuscator
        self._private_key = other_params._private_key
        self._private_key_obfuscator = other_params._private_key_obfuscator
        self.program = other_params.program
        self.terminal = other_params.terminal
        self.machine = other_params.machine
        self.osuser = other_params.osuser
        self.driver_name = other_params.driver_name

    cdef bytes _get_new_password(self):
        """
        Returns the new password, after removing the obfuscation.
        """
        if self._new_password is not None:
            return bytes(self._xor_bytes(self._new_password,
                                         self._new_password_obfuscator))

    cdef bytearray _get_obfuscator(self, str secret_value):
        """
        Return a byte array suitable for obfuscating the specified secret
        value.
        """
        return bytearray(secrets.token_bytes(len(secret_value.encode())))

    cdef bytes _get_password(self):
        """
        Returns the password, after removing the obfuscation.
        """
        if self._password is not None:
            return bytes(self._xor_bytes(self._password,
                                         self._password_obfuscator))

    cdef str _get_private_key(self):
        """
        Returns the private key, after removing the obfuscation.
        """
        if self._private_key is not None:
            return self._xor_bytes(self._private_key,
                                   self._private_key_obfuscator).decode()

    cdef str _get_token(self):
        """
        Returns the token, after removing the obfuscation.

        If a callable has been registered and there is no token stored yet, the
        callable will be invoked with the refresh parameter set to False. If
        the token returned by the callable is expired, the callable will be
        invoked a second time with the refresh parameter set to True. If this
        token is also expired, an exception will be raised.

        If the stored token has expired and no callable has been registered, an
        exception will be raised; otherwise, the callable will be invoked with
        the refresh parameter set to True.
        """
        cdef:
            object returned_val, current_date = datetime.datetime.utcnow()
            bint expired
        if self._token is None and self.access_token_callback is not None:
            returned_val = self.access_token_callback(False)
            self._set_access_token(returned_val,
                                   errors.ERR_INVALID_ACCESS_TOKEN_RETURNED)
        expired = self.access_token_expires < current_date
        if expired and self.access_token_callback is not None:
            returned_val = self.access_token_callback(True)
            self._set_access_token(returned_val,
                                   errors.ERR_INVALID_ACCESS_TOKEN_RETURNED)
            expired = self.access_token_expires < current_date
        if expired:
            errors._raise_err(errors.ERR_EXPIRED_ACCESS_TOKEN)
        return self._xor_bytes(self._token, self._token_obfuscator).decode()

    cdef object _get_token_expires(self, str token):
        """
        Gets the expiry date from the token.
        """
        cdef:
            str header_seg
            dict header
            int num_pad
        header_seg = token.split(".")[1]
        num_pad = len(header_seg) % 4
        if num_pad != 0:
            header_seg += '=' * num_pad
        header = json.loads(base64.b64decode(header_seg))
        return datetime.datetime.utcfromtimestamp(header["exp"])

    cdef str _get_wallet_password(self):
        """
        Returns the wallet password, after removing the obfuscation.
        """
        if self._wallet_password is not None:
            return self._xor_bytes(self._wallet_password,
                                   self._wallet_password_obfuscator).decode()

    cdef int _parse_connect_string(self, str connect_string) except -1:
        """
        Internal method for parsing a connect string.
        """
        cdef:
            TnsnamesFile tnsnames_file
            ConnectStringParser parser
            TnsnamesFileReader reader

        # attempt to parse the connect string directly
        parser = ConnectStringParser.__new__(ConnectStringParser)
        parser.template_description = self._default_description
        parser.template_address = self._default_address
        parser.params_impl = self
        parser.parse(connect_string)

        # otherwise, see if the name is a connect alias in a tnsnames.ora
        # configuration file
        if parser.description_list is None:
            reader = TnsnamesFileReader()
            tnsnames_file = reader.read_tnsnames(self.config_dir)
            name = connect_string
            connect_string = tnsnames_file.entries.get(name.upper())
            if connect_string is None:
                errors._raise_err(errors.ERR_TNS_ENTRY_NOT_FOUND, name=name,
                                  file_name=tnsnames_file.file_name)
            parser.parse(connect_string)
            if parser.description_list is None:
                errors._raise_err(errors.ERR_CANNOT_PARSE_CONNECT_STRING,
                                  data=connect_string)
        self.description_list = parser.description_list

    cdef int _set_access_token(self, object val, int error_num) except -1:
        """
        Sets the access token either supplied directly by the user or
        indirectly via a callback.
        """
        cdef:
            str token, private_key = None
            object token_expires
        if isinstance(val, tuple) and len(val) == 2:
            token, private_key = val
            if token is None or private_key is None:
                errors._raise_err(error_num)
        elif isinstance(val, str):
            token = val
        else:
            errors._raise_err(error_num)
        try:
            token_expires = self._get_token_expires(token)
        except Exception as e:
            errors._raise_err(error_num, cause=e)
        self._token_obfuscator = self._get_obfuscator(token)
        self._token = self._xor_bytes(bytearray(token.encode()),
                                                self._token_obfuscator)
        if private_key is not None:
            self._private_key_obfuscator = self._get_obfuscator(private_key)
            self._private_key = self._xor_bytes(bytearray(private_key.encode()),
                                                self._private_key_obfuscator)
        self.access_token_expires = token_expires

    cdef int _set_access_token_param(self, object val) except -1:
        """
        Sets the access token parameter.
        """
        if val is not None:
            if callable(val):
                self.access_token_callback = val
            else:
                self._set_access_token(val,
                                       errors.ERR_INVALID_ACCESS_TOKEN_PARAM)

    cdef int _set_new_password(self, str password) except -1:
        """
        Sets the new password on the instance after first obfuscating it.
        """
        if password is not None:
            self._new_password_obfuscator = self._get_obfuscator(password)
            self._new_password = self._xor_bytes(bytearray(password.encode()),
                                                 self._new_password_obfuscator)

    cdef int _set_password(self, str password) except -1:
        """
        Sets the password on the instance after first obfuscating it.
        """
        if password is not None:
            self._password_obfuscator = self._get_obfuscator(password)
            self._password = self._xor_bytes(bytearray(password.encode()),
                                             self._password_obfuscator)

    cdef int _set_wallet_password(self, str password) except -1:
        """
        Sets the wallet password on the instance after first obfuscating it.
        """
        if password is not None:
            self._wallet_password_obfuscator = self._get_obfuscator(password)
            self._wallet_password = \
                    self._xor_bytes(bytearray(password.encode()),
                                    self._wallet_password_obfuscator)

    cdef bytearray _xor_bytes(self, bytearray a, bytearray b):
        """
        Perform an XOR of two byte arrays as a means of obfuscating a password
        that is stored on the class. It is assumed that the byte arrays are of
        the same length.
        """
        cdef:
            ssize_t length, i
            bytearray result
        length = len(a)
        result = bytearray(length)
        for i in range(length):
            result[i] = a[i] ^ b[i]
        return result

    def copy(self):
        """
        Creates a copy of the connection parameters and returns it.
        """
        cdef ConnectParamsImpl new_params
        new_params = ConnectParamsImpl.__new__(ConnectParamsImpl)
        new_params._copy(self)
        return new_params

    def _get_addresses(self):
        """
        Return a list of the stored addresses.
        """
        return self.description_list.get_addresses()

    def get_connect_string(self):
        """
        Returns a connect string generated from the parameters.
        """
        return self.description_list.build_connect_string()

    def get_full_user(self):
        """
        Internal method used for getting the full user (including any proxy
        user) which is used in thick mode exlusively and which is used in
        the repr() methods for Connection and ConnectionPool.
        """
        if self.proxy_user is not None:
            return f"{self.user}[{self.proxy_user}]"
        return self.user

    def get_network_service_names(self):
        """
        Returns a list of the network service names found in the tnsnames.ora
        file found in the configuration directory associated with the
        parameters. If no such file exists, an error is raised.
        """
        cdef:
            TnsnamesFileReader reader = TnsnamesFileReader()
            TnsnamesFile tnsnames_file
        tnsnames_file = reader.read_tnsnames(self.config_dir)
        return list(tnsnames_file.entries.keys())

    def parse_connect_string(self, str connect_string):
        """
        Internal method for parsing the connect string.
        """
        connect_string = connect_string.strip()
        try:
            self._parse_connect_string(connect_string)
        except exceptions.Error:
            raise
        except Exception as e:
            errors._raise_err(errors.ERR_CANNOT_PARSE_CONNECT_STRING, cause=e,
                              data=connect_string)

    def parse_dsn_with_credentials(self, str dsn):
        """
        Parse a dsn (data source name) string supplied by the user. This can be
        in the form user/password@connect_string or it can be in the form
        user/password. The user, password and connect string are returned in a
        3-tuple.
        """
        pos = dsn.rfind("@")
        if pos >= 0:
            credentials = dsn[:pos]
            connect_string = dsn[pos + 1:] or None
        else:
            credentials = dsn
            connect_string = None
        pos = credentials.find("/")
        if pos > 0 and credentials[pos - 1] != ':' \
                or pos == 0 and len(credentials) == 1:
            user = credentials[:pos] or None
            password = credentials[pos + 1:] or None
        elif connect_string is None:
            connect_string = dsn or None
            user = password = None
        else:
            user = credentials or None
            password = None
        return (user, password, connect_string)

    def parse_user(self, str user):
        """
        Parses a user string into its component parts, if applicable. The user
        string may be in the form user[proxy_user] or it may simply be a simple
        user string.
        """
        start_pos = user.find("[")
        if start_pos > 0 and user.endswith("]"):
            self.proxy_user = user[start_pos + 1:-1]
            self.user = user[:start_pos]
        else:
            self.user = user

    def process_args(self, str dsn, dict kwargs, bint thin):
        """
        Processes the arguments to connect() and create_pool().

            - the keyword arguments are set
            - if no user was specified in the keyword arguments and a dsn is
              specified, it is parsed to determine the user, password and
              connect string and the user and password are stored
            - in thin mode, the connect string is then parsed into its
              components and stored
            - if no dsn was specified, one is built from the components
            - the connect string is returned
        """
        if kwargs:
            self.set(kwargs)
        if self.user is None and not self.externalauth and dsn is not None:
            user, password, dsn = self.parse_dsn_with_credentials(dsn)
            self.set(dict(user=user, password=password))
        if dsn is not None and thin:
            self.parse_connect_string(dsn)
        if dsn is None:
            dsn = self.get_connect_string()
        return dsn


cdef class ConnectParamsNode:

    def __init__(self, bint must_have_children):
        self.must_have_children = must_have_children
        self.failover = True
        if must_have_children:
            self.children = []

    cdef int _copy(self, ConnectParamsNode source) except -1:
        """
        Copies data from the source to this node.
        """
        self.must_have_children = source.must_have_children
        if self.must_have_children:
            self.children = []
            self.failover = source.failover
            self.load_balance = source.load_balance
            self.source_route = source.source_route

    cdef int _set_active_children(self) except -1:
        """
        Set the active children to process when connecting to the database.
        This call is recursive and will set the active children of each of its
        children.
        """
        cdef ConnectParamsNode child

        # if only one child is present, that child is considered active
        if len(self.children) == 1:
            self.active_children = self.children

        # for source route, only the first child is considered active
        elif self.source_route:
            self.active_children = self.children[:1]

        # for failover with load balance, all of the children are active but
        # are processed in a random order
        elif self.failover and self.load_balance:
            self.active_children = random.sample(self.children,
                                                 k=len(self.children))

        # for failover without load balance, all of the children are active and
        # are processed in the same order
        elif self.failover:
            self.active_children = self.children

        # without failover, load balance indicates that only one of the
        # children is considered active and which one is selected randomly
        elif self.load_balance:
            self.active_children = random.sample(self.children, k=1)

        # without failover or load balance, just the first child is navigated
        else:
            self.active_children = self.children[:1]

        for child in self.children:
            if child.must_have_children:
                child._set_active_children()


cdef class Address(ConnectParamsNode):
    """
    Internal class used to hold parameters for an address used to create a
    connection to the database.
    """

    def __init__(self):
        ConnectParamsNode.__init__(self, False)
        self.protocol = DEFAULT_PROTOCOL
        self.port = DEFAULT_PORT

    cdef str build_connect_string(self):
        """
        Build a connect string from the components. If no host is specified,
        None is returned (used for bequeath connections).
        """
        if self.host is not None:
            parts = [f"(PROTOCOL={self.protocol})",
                     f"(HOST={self.host})",
                     f"(PORT={self.port})"]
            if self.https_proxy is not None:
                parts.append(f"(HTTPS_PROXY={self.https_proxy})")
            if self.https_proxy_port != 0:
                parts.append(f"(HTTPS_PROXY_PORT={self.https_proxy_port})")
            return f'(ADDRESS={"".join(parts)})'

    def copy(self):
        """
        Creates a copy of the address and returns it.
        """
        cdef Address address = Address.__new__(Address)
        address._copy(self)
        address.host = self.host
        address.port = self.port
        address.protocol = self.protocol
        address.https_proxy = self.https_proxy
        address.https_proxy_port = self.https_proxy_port
        return address

    @classmethod
    def from_args(cls, dict args):
        """
        Creates an address and sets the arguments before returning it. This is
        used within connect descriptors containing address lists.
        """
        address = cls()
        address.set_from_args(args)
        return address

    def set_from_args(self, dict args):
        """
        Sets parameter values from an argument dictionary or an (ADDRESS) node
        in a connect descriptor.
        """
        _set_str_param(args, "host", self)
        _set_uint_param(args, "port", &self.port)
        protocol = args.get("protocol")
        if protocol is not None:
            self.set_protocol(protocol)
        _set_str_param(args, "https_proxy", self)
        _set_uint_param(args, "https_proxy_port", &self.https_proxy_port)

    cdef int set_protocol(self, str value) except -1:
        """
        Sets the protocol in the address to the specified value.
        """
        value = value.lower()
        if value not in ("tcp", "tcps"):
            errors._raise_err(errors.ERR_INVALID_PROTOCOL, protocol=value)
        self.protocol = value


cdef class AddressList(ConnectParamsNode):
    """
    Internal class used to hold address list parameters and a list of addresses
    used to create connections to the database.
    """

    def __init__(self):
        ConnectParamsNode.__init__(self, True)

    cdef bint _uses_tcps(self):
        """
        Returns a boolean indicating if any of the addresses in the address
        list use the protocol TCPS.
        """
        cdef Address address
        for address in self.children:
            if address.protocol == "tcps":
                return True
        return False

    cdef str build_connect_string(self):
        """
        Build a connect string from the components.
        """
        cdef Address a
        parts = [a.build_connect_string() for a in self.children]
        if len(parts) == 1:
            return parts[0]
        return f'(ADDRESS_LIST={"".join(parts)})'

    def set_from_args(self, dict args):
        """
        Set paramter values from an argument dictionary or an (ADDRESS_LIST)
        node in a connect descriptor.
        """
        _set_bool_param(args, "failover", &self.failover)
        _set_bool_param(args, "load_balance", &self.load_balance)
        _set_bool_param(args, "source_route", &self.source_route)


cdef class Description(ConnectParamsNode):
    """
    Internal class used to hold description parameters.
    """

    def __init__(self):
        ConnectParamsNode.__init__(self, True)
        self.tcp_connect_timeout = DEFAULT_TCP_CONNECT_TIMEOUT
        self.retry_delay = DEFAULT_RETRY_DELAY
        self.ssl_server_dn_match = True
        self.sdu = DEFAULT_SDU

    cdef str _build_duration_str(self, double value):
        """
        Build up the value to display for a duration in the connect string.
        This must be an integer with the units following it.
        """
        cdef int value_int, value_minutes
        value_int = <int> value
        if value != value_int:
            return f"{int(value * 1000)}ms"
        value_minutes = (value_int // 60)
        if value_minutes * 60 == value_int:
            return f"{value_minutes}min"
        return f"{value_int}"

    cdef str build_connect_string(self, str cid=None):
        """
        Build a connect string from the components.
        """
        cdef:
            AddressList address_list
            list parts, temp_parts
            bint uses_tcps = False
            str temp

        # build top-level description parts
        parts = []
        if self.load_balance:
            parts.append("(LOAD_BALANCE=ON)")
        if self.source_route:
            parts.append("(SOURCE_ROUTE=ON)")
        if self.retry_count != 0:
            parts.append(f"(RETRY_COUNT={self.retry_count})")
        if self.retry_delay != 0:
            parts.append(f"(RETRY_DELAY={self.retry_delay})")
        if self.expire_time != 0:
            parts.append(f"(EXPIRE_TIME={self.expire_time})")
        if self.tcp_connect_timeout != DEFAULT_TCP_CONNECT_TIMEOUT:
            temp = self._build_duration_str(self.tcp_connect_timeout)
            parts.append(f"(TRANSPORT_CONNECT_TIMEOUT={temp})")
        if self.sdu != DEFAULT_SDU:
            parts.append(f"(SDU={self.sdu})")

        # add address lists, but if the address list contains only a single
        # entry and that entry does not have a host, the other parts aren't
        # relevant anyway!
        for address_list in self.children:
            temp = address_list.build_connect_string()
            if temp is None:
                return None
            parts.append(temp)
            if not uses_tcps:
                uses_tcps = address_list._uses_tcps()

        # build connect data segment
        temp_parts = []
        if self.service_name is not None:
            temp_parts.append(f"(SERVICE_NAME={self.service_name})")
        elif self.sid is not None:
            temp_parts.append(f"(SID={self.sid})")
        if self.server_type is not None:
            temp_parts.append(f"(SERVER={self.server_type})")
        if self.use_tcp_fast_open:
            temp_parts.append("(USE_TCP_FAST_OPEN=ON)")
        if self.pool_boundary is not None:
            temp_parts.append(f"(POOL_BOUNDARY={self.pool_boundary})")
        if cid is not None:
            temp_parts.append(f"(CID={cid})")
        else:
            if self.cclass is not None:
                temp_parts.append(f"(POOL_CONNECTION_CLASS={self.cclass})")
            if self.purity == PURITY_SELF:
                temp_parts.append(f"(POOL_PURITY=SELF)")
            elif self.purity == PURITY_NEW:
                temp_parts.append(f"(POOL_PURITY=NEW)")
        if self.connection_id is not None:
            temp_parts.append(f"(CONNECTION_ID={self.connection_id})")
        if temp_parts:
            parts.append(f'(CONNECT_DATA={"".join(temp_parts)})')

        # build security segment, if applicable
        if uses_tcps:
            temp_parts = []
            if self.ssl_server_dn_match:
                temp_parts.append("(SSL_SERVER_DN_MATCH=ON)")
            if self.ssl_server_cert_dn is not None:
                temp = f"(SSL_SERVER_CERT_DN={self.ssl_server_cert_dn})"
                temp_parts.append(temp)
            if self.ssl_version is not None:
                if self.ssl_version is ssl.TLSVersion.TLSv1_2:
                    temp_parts.append(f"(SSL_VERSION=TLSv1.2)")
                elif self.ssl_version is ssl.TLSVersion.TLSv1_3:
                    temp_parts.append(f"(SSL_VERSION=TLSv1.3)")
            if self.wallet_location is not None:
                temp = f"(MY_WALLET_DIRECTORY={self.wallet_location})"
                temp_parts.append(temp)
            parts.append(f'(SECURITY={"".join(temp_parts)})')

        return f'(DESCRIPTION={"".join(parts)})'

    def copy(self):
        """
        Creates a copy of the description (except for the address lists) and
        returns it.
        """
        cdef Description description = Description.__new__(Description)
        description._copy(self)
        description.expire_time = self.expire_time
        description.retry_count = self.retry_count
        description.retry_delay = self.retry_delay
        description.sdu = self.sdu
        description.tcp_connect_timeout = self.tcp_connect_timeout
        description.service_name = self.service_name
        description.server_type = self.server_type
        description.sid = self.sid
        description.cclass = self.cclass
        description.connection_id_prefix = self.connection_id_prefix
        description.pool_boundary = self.pool_boundary
        description.purity = self.purity
        description.ssl_server_dn_match = self.ssl_server_dn_match
        description.use_tcp_fast_open = self.use_tcp_fast_open
        description.ssl_server_cert_dn = self.ssl_server_cert_dn
        description.ssl_version = self.ssl_version
        description.wallet_location = self.wallet_location
        return description

    def set_from_args(self, dict args):
        """
        Set parameter values from an argument dictionary.
        """
        self.set_from_connect_data_args(args)
        self.set_from_description_args(args)
        self.set_from_security_args(args)

    def set_from_connect_data_args(self, dict args):
        """
        Set parameter values from an argument dictionary or a (CONNECT_DATA)
        node in a connect descriptor.
        """
        _set_str_param(args, "service_name", self)
        _set_str_param(args, "sid", self)
        server_type = args.get("server_type")
        if server_type is not None:
            self.set_server_type(server_type)
        _set_str_param(args, "cclass", self)
        _set_purity_param(args, "purity", &self.purity)
        _set_str_param(args, "pool_boundary", self)
        _set_str_param(args, "connection_id_prefix", self)
        _set_bool_param(args, "use_tcp_fast_open", &self.use_tcp_fast_open)

    def set_from_description_args(self, dict args):
        """
        Set parameter values from an argument dictionary or a (DESCRIPTION)
        node in a connect descriptor.
        """
        cdef Address address
        _set_uint_param(args, "expire_time", &self.expire_time)
        _set_bool_param(args, "failover", &self.failover)
        _set_bool_param(args, "load_balance", &self.load_balance)
        _set_bool_param(args, "source_route", &self.source_route)
        _set_uint_param(args, "retry_count", &self.retry_count)
        _set_uint_param(args, "retry_delay", &self.retry_delay)
        _set_uint_param(args, "sdu", &self.sdu)
        self.sdu = min(max(self.sdu, 512), 2097152)         # sanitize SDU
        _set_duration_param(args, "tcp_connect_timeout",
                            &self.tcp_connect_timeout)

    def set_from_security_args(self, dict args):
        """
        Set parameter values from an argument dictionary or a (SECURITY)
        node in a connect descriptor.
        """
        _set_bool_param(args, "ssl_server_dn_match", &self.ssl_server_dn_match)
        _set_str_param(args, "ssl_server_cert_dn", self)
        _set_ssl_version_param(args, "ssl_version", self)
        _set_str_param(args, "wallet_location", self)

    cdef int set_server_type(self, str value) except -1:
        """
        Sets the server type in the description to the specified value.
        """
        value = value.lower()
        if value not in ("dedicated", "pooled", "shared"):
            errors._raise_err(errors.ERR_INVALID_SERVER_TYPE,
                              server_type=value)
        self.server_type = value


cdef class DescriptionList(ConnectParamsNode):
    """
    Internal class used to hold description list parameters and a list of
    descriptions.
    """

    def __init__(self):
        ConnectParamsNode.__init__(self, True)

    cdef str build_connect_string(self):
        """
        Build a connect string from the components.
        """
        cdef:
            Description d
            list parts
        parts = [d.build_connect_string() for d in self.children]
        if len(parts) == 1:
            return parts[0]
        return f'(DESCIPTION_LIST={"".join(parts)})'

    cdef list get_addresses(self):
        """
        Return a list of the stored addresses.
        """
        cdef:
            AddressList addr_list
            Description desc
            Address addr
        return [addr for desc in self.children \
                for addr_list in desc.children \
                for addr in addr_list.children]

    def set_from_args(self, dict args):
        """
        Set paramter values from an argument dictionary or a (DESCRIPTION_LIST)
        node in a connect descriptor.
        """
        _set_bool_param(args, "failover", &self.failover)
        _set_bool_param(args, "load_balance", &self.load_balance)
        _set_bool_param(args, "source_route", &self.source_route)


cdef class TnsnamesFile:
    """
    Internal class used to parse and retain connect descriptor entries found in
    a tnsnames.ora file or any included file.
    """
    cdef:
        str file_name
        int mtime
        dict entries
        set included_files

    def __init__(self, str file_name):
        self.file_name = file_name
        self.clear()
        self._get_mtime(&self.mtime)

    cdef int _get_mtime(self, int* mtime) except -1:
        """
        Returns the modification time of the file or throws an exception if the
        file cannot be found.
        """
        try:
            mtime[0] = os.stat(self.file_name).st_mtime
        except Exception as e:
            errors._raise_err(errors.ERR_MISSING_FILE, str(e),
                              file_name=self.file_name)

    cdef int clear(self) except -1:
        """
        Clear all entries in the file.
        """
        self.entries = {}
        self.included_files = set()

    def is_current(self):
        """
        Returns a boolean indicating if the contents are current or not.
        """
        cdef:
            TnsnamesFile included_file
            int mtime
        self._get_mtime(&mtime)
        if mtime != self.mtime:
            return False
        for included_file in self.included_files:
            if not included_file.is_current():
                return False
        return True



cdef class TnsnamesFileReader:
    """
    Internal class used to read a tnsnames.ora file and all of its included
    files.
    """
    cdef:
        TnsnamesFile primary_file
        list files_in_progress
        dict entries

    cdef TnsnamesFile _get_file(self, file_name):
        """
        Get the file from the cache or read it from the file system.
        """
        cdef TnsnamesFile tnsnames_file
        if file_name in self.files_in_progress:
            errors._raise_err(errors.ERR_IFILE_CYCLE_DETECTED,
                              including_file_name=self.files_in_progress[-1],
                              included_file_name=file_name)
        tnsnames_file = _tnsnames_files.get(file_name)
        if tnsnames_file is None:
            tnsnames_file = TnsnamesFile(file_name)
        else:
            if tnsnames_file.is_current():
                return tnsnames_file
            del _tnsnames_files[file_name]
        if self.primary_file is None:
            self.primary_file = tnsnames_file
        self.files_in_progress.append(file_name)
        self._read_file(tnsnames_file)
        _tnsnames_files[file_name] = tnsnames_file
        self.files_in_progress.pop()
        return tnsnames_file

    cdef int _read_file(self, TnsnamesFile tnsnames_file) except -1:
        """
        Reads the file and parses the contents.
        """
        cdef:
            TnsnamesFile included_file
            TnsnamesFileParser parser
            int line_no = 0
        def add_entry(key, value):
            if key == "IFILE":
                if not os.path.isabs(value):
                    dir_name = os.path.dirname(tnsnames_file.file_name)
                    value = os.path.join(dir_name, value)
                included_file = self._get_file(value)
                tnsnames_file.included_files.add(included_file)
            else:
                entry_names = [
                    s.strip().splitlines()[-1] for s in key.split(",")
                ]
                for name in entry_names:
                    self.primary_file.entries[name] = value
                    if tnsnames_file is not self.primary_file:
                        tnsnames_file.entries[name] = value
        tnsnames_file.clear()
        parser = TnsnamesFileParser.__new__(TnsnamesFileParser)
        with open(tnsnames_file.file_name) as f:
            parser.parse(f.read(), add_entry)

    cdef TnsnamesFile read_tnsnames(self, str dir_name):
        """
        Read the tnsnames.ora file found in the given directory or raise an
        exception if no such file can be found.
        """
        self.primary_file = None
        self.files_in_progress = []
        if dir_name is None:
            errors._raise_err(errors.ERR_NO_CONFIG_DIR)
        file_name = os.path.join(dir_name, "tnsnames.ora")
        return self._get_file(file_name)
