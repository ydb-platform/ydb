#------------------------------------------------------------------------------
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# pool.pyx
#
# Cython file defining the thick Pool implementation class (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------

cdef int _token_callback_handler(void *context,
                                 dpiAccessToken *refresh_token) with gil:
    cdef:
        ThickPoolImpl pool_impl = <object> context
    pool_impl._token_handler(refresh_token, pool_impl.connect_params)


cdef class ThickPoolImpl(BasePoolImpl):
    cdef:
        dpiPool *_handle
        object warning

    def __init__(self, str dsn, PoolParamsImpl params):
        cdef:
            bytes session_callback_bytes, name_bytes, driver_name_bytes
            bytes edition_bytes, user_bytes, password_bytes, dsn_bytes
            uint32_t password_len = 0, user_len = 0, dsn_len = 0
            dpiCommonCreateParams common_params
            dpiPoolCreateParams create_params
            const char *password_ptr = NULL
            const char *user_ptr = NULL
            const char *dsn_ptr = NULL
            bytes token_bytes, private_key_bytes
            uint32_t token_len = 0, private_key_len = 0
            const char *token_ptr = NULL
            const char *private_key_ptr = NULL
            dpiAccessToken access_token
            dpiErrorInfo error_info
            str token, private_key
            int status

        # save parameters
        self.connect_params = params
        self.username = params.user
        self.dsn = dsn
        self.min = params.min
        self.max = params.max
        self.increment = params.increment
        self.homogeneous = params.homogeneous

        # set up token parameters if provided
        if params._token is not None \
                or params.access_token_callback is not None:
            token = params._get_token()
            token_bytes = token.encode()
            token_ptr = token_bytes
            token_len = <uint32_t> len(token_bytes)
            private_key = params._get_private_key()
            if private_key is not None:
                private_key_bytes = private_key.encode()
                private_key_ptr = private_key_bytes
                private_key_len = <uint32_t> len(private_key_bytes)

        # set up common creation parameters
        if dpiContext_initCommonCreateParams(driver_info.context,
                                             &common_params) < 0:
            _raise_from_odpi()
        common_params.createMode |= DPI_MODE_CREATE_THREADED
        if params.events:
            common_params.createMode |= DPI_MODE_CREATE_EVENTS
        if params.edition is not None:
            edition_bytes = params.edition.encode()
            common_params.edition = edition_bytes
            common_params.editionLength = <uint32_t> len(edition_bytes)
        if params._token is not None:
            access_token.token = token_ptr
            access_token.tokenLength = token_len
            access_token.privateKey = private_key_ptr
            access_token.privateKeyLength = private_key_len
            common_params.accessToken = &access_token
        if params.driver_name is not None:
            driver_name_bytes = params.driver_name.encode()[:30]
            common_params.driverName = driver_name_bytes
            common_params.driverNameLength = <uint32_t> len(driver_name_bytes)

        # set up pool creation parameters
        if dpiContext_initPoolCreateParams(driver_info.context,
                                           &create_params) < 0:
            _raise_from_odpi()
        create_params.minSessions = self.min
        create_params.maxSessions = self.max
        create_params.sessionIncrement = self.increment
        create_params.homogeneous = self.homogeneous
        create_params.getMode = params.getmode
        if params.session_callback is not None \
                and not callable(params.session_callback):
            session_callback_bytes = params.session_callback.encode()
            create_params.plsqlFixupCallback = session_callback_bytes
            create_params.plsqlFixupCallbackLength = \
                    <uint32_t> len(session_callback_bytes)
        if params.access_token_callback is not None:
            create_params.accessTokenCallback = _token_callback_handler
            create_params.accessTokenCallbackContext = <void*> self
        create_params.timeout = params.timeout
        create_params.waitTimeout = params.wait_timeout
        create_params.maxSessionsPerShard = params.max_sessions_per_shard
        create_params.maxLifetimeSession = params.max_lifetime_session
        create_params.pingInterval = params.ping_interval
        create_params.pingTimeout = params.ping_timeout
        common_params.stmtCacheSize = params.stmtcachesize
        common_params.sodaMetadataCache = params.soda_metadata_cache
        create_params.externalAuth = params.externalauth

        # prepare user, password and DSN for use
        if self.username is not None:
            user_bytes = params.get_full_user().encode()
            user_ptr = user_bytes
            user_len = <uint32_t> len(user_bytes)
        password_bytes = params._get_password()
        if password_bytes is not None:
            password_ptr = password_bytes
            password_len = <uint32_t> len(password_bytes)
        if self.dsn is not None:
            dsn_bytes = self.dsn.encode()
            dsn_ptr = dsn_bytes
            dsn_len = <uint32_t> len(dsn_bytes)

        # create pool
        with nogil:
            status = dpiPool_create(driver_info.context, user_ptr, user_len,
                                    password_ptr, password_len, dsn_ptr,
                                    dsn_len, &common_params, &create_params,
                                    &self._handle)
            dpiContext_getError(driver_info.context, &error_info)
        if status < 0:
            _raise_from_info(&error_info)
        elif error_info.isWarning:
            self.warning = _create_new_from_info(&error_info)

        name_bytes = create_params.outPoolName[:create_params.outPoolNameLength]
        self.name = name_bytes.decode()

    def __dealloc__(self):
        if self._handle != NULL:
            dpiPool_release(self._handle)

    cdef object _token_handler(self, dpiAccessToken *access_token,
                               ConnectParamsImpl params):
        cdef:
            str token, private_key
            bytes token_bytes, private_key_bytes
            uint32_t token_len = 0, private_key_len = 0
            const char *token_ptr = NULL
            const char *private_key_ptr = NULL
        token = params._get_token()
        token_bytes = token.encode()
        token_ptr = token_bytes
        token_len = <uint32_t> len(token_bytes)
        private_key = params._get_private_key()
        if private_key is not None:
            private_key_bytes = private_key.encode()
            private_key_ptr = private_key_bytes
            private_key_len = <uint32_t> len(private_key_bytes)
        access_token.token = token_ptr
        access_token.tokenLength = token_len
        access_token.privateKey = private_key_ptr
        access_token.privateKeyLength = private_key_len

    def close(self, bint force):
        """
        Internal method for closing the pool.
        """
        cdef:
            uint32_t close_mode
            int status
        close_mode = DPI_MODE_POOL_CLOSE_FORCE if force \
                     else DPI_MODE_POOL_CLOSE_DEFAULT
        with nogil:
            status = dpiPool_close(self._handle, close_mode);
        if status < 0:
            _raise_from_odpi()

    def drop(self, ThickConnImpl conn_impl):
        """
        Internal method for dropping a connection from the pool.
        """
        cdef int status
        with nogil:
            status = dpiConn_close(conn_impl._handle, DPI_MODE_CONN_CLOSE_DROP,
                                   NULL, 0)
        if status < 0:
            _raise_from_odpi()
        dpiConn_release(conn_impl._handle)
        conn_impl._handle = NULL

    def get_busy_count(self):
        """
        Internal method for getting the number of busy connections in the pool.
        """
        cdef uint32_t value
        if dpiPool_getBusyCount(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_getmode(self):
        """
        Internal method for getting the method by which connections are
        acquired from the pool.
        """
        cdef uint8_t value
        if dpiPool_getGetMode(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_max_lifetime_session(self):
        """
        Internal method for getting the maximum lifetime of each session.
        """
        cdef uint32_t value
        if dpiPool_getMaxLifetimeSession(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_max_sessions_per_shard(self):
        """
        Internal method for getting the maximum sessions per shard in the pool.
        """
        cdef uint32_t value
        if dpiPool_getMaxSessionsPerShard(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_open_count(self):
        """
        Internal method for getting the number of connections in the pool.
        """
        cdef uint32_t value
        if dpiPool_getOpenCount(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_ping_interval(self):
        """
        Internal method for getting the value of the pool-ping-interval.
        """
        cdef int value
        if dpiPool_getPingInterval(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_soda_metadata_cache(self):
        """
        Internal method for getting the value of soda metadata cache.
        """
        cdef bint value
        if dpiPool_getSodaMetadataCache(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_stmt_cache_size(self):
        """
        Internal method for getting the size of the statement cache.
        """
        cdef uint32_t value
        if dpiPool_getStmtCacheSize(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_timeout(self):
        """
        Internal method for getting the timeout for idle sessions.
        """
        cdef uint32_t value
        if dpiPool_getTimeout(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_wait_timeout(self):
        """
        Internal method for getting the wait timeout for acquiring sessions.
        """
        cdef uint32_t value
        if dpiPool_getWaitTimeout(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def reconfigure(self, uint32_t min, uint32_t max, uint32_t increment):
        """
        Internal method for reconfiguring the size of the pool.
        """
        if dpiPool_reconfigure(self._handle, min, max, increment) < 0:
            _raise_from_odpi()
        self.min = min
        self.max = max
        self.increment = increment

    def set_getmode(self, uint8_t value):
        """
        Internal method for setting the method by which connections are
        acquired from the pool.
        """
        if dpiPool_setGetMode(self._handle, value) < 0:
            _raise_from_odpi()

    def set_max_lifetime_session(self, uint32_t value):
        """
        Internal method for setting the maximum lifetime of each session.
        """
        if dpiPool_setMaxLifetimeSession(self._handle, value) < 0:
            _raise_from_odpi()

    def set_max_sessions_per_shard(self, uint32_t value):
        """
        Internal method for setting the maximum sessions per shard in the pool.
        """
        if dpiPool_setMaxSessionsPerShard(self._handle, value) < 0:
            _raise_from_odpi()

    def set_ping_interval(self, int value):
        """
        Internal method for setting the value of the pool-ping-interval.
        """
        if dpiPool_setPingInterval(self._handle, value) < 0:
            _raise_from_odpi()

    def set_soda_metadata_cache(self, bint value):
        """
        Internal method for enabling or disabling the soda metadata cache.
        """
        if dpiPool_setSodaMetadataCache(self._handle, value) < 0:
            _raise_from_odpi()

    def set_stmt_cache_size(self, uint32_t value):
        """
        Internal method for setting the size of the statement cache.
        """
        if dpiPool_setStmtCacheSize(self._handle, value) < 0:
            _raise_from_odpi()

    def set_timeout(self, uint32_t value):
        """
        Internal method for setting the timeout for idle sessions.
        """
        if dpiPool_setTimeout(self._handle, value) < 0:
            _raise_from_odpi()

    def set_wait_timeout(self, uint32_t value):
        """
        Internal method for setting the wait timeout for acquiring sessions.
        """
        if dpiPool_setWaitTimeout(self._handle, value) < 0:
            _raise_from_odpi()
