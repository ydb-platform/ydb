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
# Cython file defining the pool implementation class (embedded in
# thin_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseThinPoolImpl(BasePoolImpl):

    cdef:
        list _free_new_conn_impls
        list _free_used_conn_impls
        list _busy_conn_impls
        list _conn_impls_to_drop
        list _requests
        uint32_t _getmode
        uint32_t _stmt_cache_size
        uint32_t _timeout
        uint32_t _max_lifetime_session
        uint32_t _auth_mode
        uint32_t _open_count
        uint32_t _num_to_create
        int _ping_interval
        uint32_t _ping_timeout
        object _wait_timeout
        object _bg_task
        object _bg_task_condition
        object _condition
        object _timeout_task
        object _ssl_session
        bint _force_get
        bint _open

    def __init__(self, str dsn, PoolParamsImpl params):
        if not HAS_CRYPTOGRAPHY:
            errors._raise_err(errors.ERR_NO_CRYPTOGRAPHY_PACKAGE)
        params._check_credentials()
        self.connect_params = params
        self.username = params.user
        self.dsn = dsn
        self.min = params.min
        self.max = params.max
        self.increment = params.increment
        self.homogeneous = params.homogeneous
        self.set_getmode(params.getmode)
        self.set_wait_timeout(params.wait_timeout)
        self.set_timeout(params.timeout)
        self._stmt_cache_size = params.stmtcachesize
        self._ping_interval = params.ping_interval
        self._ping_timeout = params.ping_timeout
        self._free_new_conn_impls = []
        self._free_used_conn_impls = []
        self._busy_conn_impls = []
        self._conn_impls_to_drop = []
        self._requests = []
        self._num_to_create = self.min
        self._auth_mode = AUTH_MODE_DEFAULT
        self._open = True

    cdef int _add_request(self, PooledConnRequest request) except -1:
        """
        Adds a request for the background task to process.
        """
        request.bg_processing = True
        request.completed = False
        self._requests.append(request)
        self._notify_bg_task()

    cdef int _check_timeout(self) except -1:
        """
        Checks whether a timeout is in effect and that the number of
        connections exceeds the minimum and if so, starts a timer to check
        if these connections have expired. Only one task is ever started.
        The timeout value is increased by a second to allow for small time
        discrepancies.
        """
        if self._open and self._timeout_task is None and self._timeout > 0 \
                and self._open_count > self.min:
            self._start_timeout_task()

    cdef int _close_helper(self, bint force) except -1:
        """
        Helper function that closes all of the connections in the pool.
        """
        cdef BaseThinConnImpl conn_impl

        # if force parameter is not True and busy connections exist in the
        # pool or there are outstanding requests, raise an exception
        if not force and (self.get_busy_count() > 0 or self._requests):
            errors._raise_err(errors.ERR_POOL_HAS_BUSY_CONNECTIONS)

        # close all connections in the pool; this is done by simply adding
        # to the list of connections that require closing and then notifying
        # the background task to perform the work
        self._open = False
        for lst in (self._free_used_conn_impls,
                    self._free_new_conn_impls,
                    self._busy_conn_impls):
            self._conn_impls_to_drop.extend(lst)
            for conn_impl in lst:
                conn_impl._pool = None
            lst.clear()
        self._notify_bg_task()

    cdef PooledConnRequest _create_request(self, ConnectParamsImpl params):
        """
        Returns a poooled connection request suitable for establishing a
        connection to the pool with the given parameters.
        """
        cdef:
            ConnectParamsImpl creation_params = self.connect_params
            str pool_cclass = creation_params._default_description.cclass
            PooledConnRequest request
        request = PooledConnRequest.__new__(PooledConnRequest)
        request.pool_impl = self
        request.params = params
        request.cclass = params._default_description.cclass
        request.wants_new = (params._default_description.purity == PURITY_NEW)
        request.cclass_matches = \
                (request.cclass is None or request.cclass == pool_cclass)
        request.waiting = True
        return request

    cdef int _drop_conn_impl(self, BaseThinConnImpl conn_impl) except -1:
        """
        Helper method which adds a connection to the list of connections to be
        closed and notifies the background task.
        """
        conn_impl._pool = None
        if conn_impl._protocol._transport is not None:
            self._conn_impls_to_drop.append(conn_impl)
            self._notify_bg_task()

    cdef int _drop_conn_impls_helper(self, list conn_impls_to_drop) except -1:
        """
        Helper method which drops the requested list of connections. Exceptions
        that take place while attempting to close the connection are ignored.
        """
        cdef BaseThinConnImpl conn_impl
        for conn_impl in conn_impls_to_drop:
            try:
                conn_impl._force_close()
            except:
                pass

    cdef PooledConnRequest _get_next_request(self):
        """
        Get the next request to process.
        """
        cdef PooledConnRequest request
        for request in self._requests:
            if not request.waiting \
                    or request.requires_ping \
                    or request.is_replacing \
                    or request.is_extra \
                    or self._open_count < self.max:
                request.in_progress = request.waiting
                return request
            break

    cdef int _post_create_conn_impl(self,
                                    BaseThinConnImpl conn_impl) except -1:
        """
        Called after a connection has been created without an associated
        request.
        """
        cdef PooledConnRequest request
        if conn_impl is None:
            self._num_to_create = 0
        elif not self._open:
            conn_impl._force_close()
        else:
            self._open_count += 1
            if self._num_to_create > 0:
                self._num_to_create -= 1
            for request in self._requests:
                if request.in_progress or request.conn_impl is not None \
                        or not request.waiting:
                    continue
                if request.cclass is None \
                        or request.cclass == conn_impl._cclass:
                    request.conn_impl = conn_impl
                    request.completed = True
                    self._requests.remove(request)
                    self._condition.notify_all()
                    break
                elif not request.cclass_matches \
                        and self._open_count >= self.max:
                    request.conn_impl = conn_impl
                    request.is_replacing = True
                    break
            else:
                self._free_new_conn_impls.append(conn_impl)
            self._check_timeout()

    cdef int _post_process_request(self, PooledConnRequest request) except -1:
        """
        Called after the request has been processed. This removes the request
        from the list of requests and adds the connection to the appropriate
        list depending on whether the waiter is still waiting for the request
        to be satisfied!
        """
        request.in_progress = False
        request.bg_processing = False
        if request.conn_impl is not None:
            request.completed = True
            if not request.is_replacing and not request.requires_ping:
                self._open_count += 1
                if self._num_to_create > 0:
                    self._num_to_create -= 1
            if not request.waiting:
                request.reject()
        elif request.requires_ping:
            self._open_count -= 1
            if self._num_to_create == 0 and self._open_count < self.min:
                self._num_to_create = self.min - self._open_count
        self._requests.remove(request)
        self._condition.notify_all()

    cdef int _pre_connect(self, BaseThinConnImpl conn_impl,
                          ConnectParamsImpl params) except -1:
        """
        Called before the connection is connected. The connection class and
        pool attributes are updated and the TLS session is stored on the
        transport for reuse.
        """
        if params is not None:
            conn_impl._cclass = params._default_description.cclass
        else:
            conn_impl._cclass = self.connect_params._default_description.cclass
        conn_impl._pool = self

    def _process_timeout(self):
        """
        Processes the timeout after the timer task completes. Drops any free
        connections that have expired (while maintaining the minimum number of
        connections in the pool).
        """
        self._timeout_task = None
        self._timeout_helper(self._free_new_conn_impls)
        self._timeout_helper(self._free_used_conn_impls)
        self._check_timeout()

    cdef int _return_connection_helper(self,
                                       BaseThinConnImpl conn_impl) except -1:
        """
        Returns the connection to the pool. If the connection was closed for
        some reason it will be dropped; otherwise, it will be returned to the
        list of connections available for further use. If an "extra" connection
        was created (because the pool has a mode of "force" get or because a
        different connection class than that used by the pool was requested)
        then it will be added to the pool or will replace an unused new
        connection or will be discarded depending on the current pool size.
        """
        cdef:
            bint is_open = conn_impl._protocol._transport is not None
            BaseThinDbObjectTypeCache type_cache
            PooledConnRequest request
            int cache_num
        self._busy_conn_impls.remove(conn_impl)
        if conn_impl._dbobject_type_cache_num > 0:
            cache_num = conn_impl._dbobject_type_cache_num
            type_cache = get_dbobject_type_cache(cache_num)
            type_cache._clear_cursors()
        if conn_impl._is_pool_extra:
            conn_impl._is_pool_extra = False
            if is_open and self._open_count >= self.max:
                if self._free_new_conn_impls and self._open_count == self.max:
                    self._drop_conn_impl(self._free_new_conn_impls.pop(0))
                else:
                    self._drop_conn_impl(conn_impl)
                    is_open = False
        if not is_open:
            self._open_count -= 1
        else:
            conn_impl.warning = None
            conn_impl._time_in_pool = time.monotonic()
            for request in self._requests:
                if request.in_progress or request.wants_new \
                        or request.conn_impl is not None \
                        or not request.waiting:
                    continue
                if request.cclass is None \
                        or request.conn_impl._cclass == self.cclass:
                    request.conn_impl = conn_impl
                    request.completed = True
                    self._requests.remove(request)
                    self._condition.notify_all()
                    return 0
            self._free_used_conn_impls.append(conn_impl)
        self._check_timeout()

    cdef int _shutdown(self) except -1:
        """
        Called when the main interpreter has completed and only shutdown code
        is being executed. All connections in the pool are marked as non-pooled
        and the pool itself terminated.
        """
        cdef BaseThinConnImpl conn_impl
        with self._condition:
            self._open = False
            for lst in (self._free_used_conn_impls,
                        self._free_new_conn_impls,
                        self._busy_conn_impls):
                for conn_impl in lst:
                    conn_impl._pool = None
                lst.clear()
            self._requests.clear()
            self._notify_bg_task()
        self._bg_task.join()

    cdef int _start_timeout_task(self) except -1:
        """
        Starts the task for checking timeouts (differs for sync and async).
        """
        pass

    cdef int _timeout_helper(self, list conn_impls_to_check) except -1:
        """
        Helper method which checks the list of connections to see if any
        connections have expired (while maintaining the minimum number of
        connections in the pool).
        """
        cdef BaseThinConnImpl conn_impl
        current_time = time.monotonic()
        while conn_impls_to_check and self._open_count > self.min:
            conn_impl = conn_impls_to_check[0]
            if current_time - conn_impl._time_in_pool < self._timeout:
                break
            conn_impls_to_check.pop(0)
            self._drop_conn_impl(conn_impl)
            self._open_count -= 1

    def get_busy_count(self):
        """
        Internal method for getting the number of busy connections in the pool.
        """
        return len(self._busy_conn_impls)

    def get_getmode(self):
        """
        Internal method for getting the method by which connections are
        acquired from the pool.
        """
        return self._getmode

    def get_max_lifetime_session(self):
        """
        Internal method for getting the maximum lifetime of each session.
        """
        return self._max_lifetime_session

    def get_open_count(self):
        """
        Internal method for getting the number of connections in the pool.
        """
        return self._open_count

    def get_ping_interval(self):
        """
        Internal method for getting the value of the pool-ping-interval.
        """
        return self._ping_interval

    def get_stmt_cache_size(self):
        """
        Internal method for getting the size of the statement cache.
        """
        return self._stmt_cache_size

    def get_timeout(self):
        """
        Internal method for getting the timeout for idle sessions.
        """
        return self._timeout

    def get_wait_timeout(self):
        """
        Internal method for getting the wait timeout for acquiring sessions.
        """
        if self._getmode == POOL_GETMODE_TIMEDWAIT:
            return self._wait_timeout
        return 0

    def set_getmode(self, uint32_t value):
        """
        Internal method for setting the method by which connections are
        acquired from the pool.
        """
        if self._getmode != value:
            self._getmode = value
            self._force_get = (self._getmode == POOL_GETMODE_FORCEGET)
            if self._getmode == POOL_GETMODE_TIMEDWAIT:
                self._wait_timeout = 0
            else:
                self._wait_timeout = None

    def set_max_lifetime_session(self, uint32_t value):
        """
        Internal method for setting the maximum lifetime of each session.
        """
        self._max_lifetime_session = value

    def set_ping_interval(self, int value):
        """
        Internal method for setting the value of the pool-ping-interval.
        """
        self._ping_interval = value

    def set_stmt_cache_size(self, uint32_t value):
        """
        Internal method for setting the size of the statement cache.
        """
        self._stmt_cache_size = value

    def set_timeout(self, uint32_t value):
        """
        Internal method for setting the timeout for idle sessions.
        """
        self._timeout = value

    def set_wait_timeout(self, uint32_t value):
        """
        Internal method for setting the wait timeout for acquiring sessions.
        """
        if self._getmode == POOL_GETMODE_TIMEDWAIT:
            self._wait_timeout = value / 1000
        else:
            self._wait_timeout = None


cdef class ThinPoolImpl(BaseThinPoolImpl):

    def __init__(self, str dsn, PoolParamsImpl params):
        super().__init__(dsn, params)
        self._condition = threading.Condition()
        self._bg_task_condition = threading.Condition()
        self._bg_task = threading.Thread(target=self._bg_task_func)
        self._bg_task.start()

    def _bg_task_func(self):
        """
        Method which runs in a dedicated thread and is used to create
        connections and close them when needed. When first started, it creates
        pool.min connections. After that, it creates pool.increment connections
        up to the value of pool.max when needed and destroys connections when
        needed. It also pings connections when requested to do so. The thread
        terminates automatically when the pool is closed.
        """
        cdef:
            PooledConnRequest request = None
            BaseThinConnImpl conn_impl
            list conn_impls_to_drop
            uint32_t num_to_create

        # add to the list of pools that require closing
        pools_to_close.add(self)

        # perform task until pool is closed
        while self._open or self._conn_impls_to_drop:

            # check to see if there a request to process
            if request is None and self._open:
                with self._condition:
                    request = self._get_next_request()
            if request is not None and self._open:
                self._process_request(request)
                with self._condition:
                    self._post_process_request(request)
                    request = self._get_next_request()
                    continue

            # check to see if there is a connection that needs to be built
            with self._condition:
                num_to_create = self._num_to_create
            if num_to_create > 0 and self._open:
                try:
                    conn_impl = self._create_conn_impl()
                except:
                    conn_impl = None
                with self._condition:
                    self._post_create_conn_impl(conn_impl)
                    continue

            # check to see if there are any connections to drop
            with self._condition:
                conn_impls_to_drop = self._conn_impls_to_drop
                self._conn_impls_to_drop = []
            if conn_impls_to_drop:
                self._drop_conn_impls_helper(conn_impls_to_drop)
                continue

            # otherwise, nothing to do yet, wait for notifications!
            with self._bg_task_condition:
                self._bg_task_condition.wait()

        # stop the timeout task, if one is active
        if self._timeout_task is not None:
            self._timeout_task.cancel()

        # remove from the list of pools that require closing
        pools_to_close.remove(self)

    cdef ThinConnImpl _create_conn_impl(self, ConnectParamsImpl params=None):
        """
        Create a single connection using the pool's information. This
        connection may be placed in the pool or may be returned directly (such
        as when the pool is full and POOL_GETMODE_FORCEGET is being used).
        """
        cdef ThinConnImpl conn_impl
        conn_impl = ThinConnImpl(self.dsn, self.connect_params)
        self._pre_connect(conn_impl, params)
        conn_impl.connect(self.connect_params)
        conn_impl._time_in_pool = time.monotonic()
        return conn_impl

    def _notify_bg_task(self):
        """
        Notify the background task that work needs to be done.
        """
        with self._bg_task_condition:
            self._bg_task_condition.notify()

    cdef int _process_request(self, PooledConnRequest request) except -1:
        """
        Processes a request.
        """
        cdef BaseThinConnImpl conn_impl
        try:
            if request.requires_ping:
                try:
                    request.conn_impl.set_call_timeout(self._ping_timeout)
                    request.conn_impl.ping()
                    request.conn_impl.set_call_timeout(0)
                except exceptions.Error:
                    request.conn_impl._force_close()
                    request.conn_impl = None
            else:
                conn_impl = self._create_conn_impl(request.params)
                if request.conn_impl is not None:
                    request.conn_impl._force_close()
                request.conn_impl = conn_impl
                request.conn_impl._is_pool_extra = request.is_extra
        except Exception as e:
            request.exception = e

    cdef int _return_connection(self, BaseThinConnImpl conn_impl) except -1:
        """
        Returns the connection to the pool.
        """
        with self._condition:
            self._return_connection_helper(conn_impl)

    cdef int _start_timeout_task(self) except -1:
        """
        Starts the task for checking timeouts. The timeout value is increased
        by a second to allow for small time discrepancies.
        """
        def handler():
            with self._condition:
                self._process_timeout()
        self._timeout_task = threading.Timer(self._timeout + 1, handler)
        self._timeout_task.start()

    def acquire(self, ConnectParamsImpl params):
        """
        Internal method for acquiring a connection from the pool.
        """
        cdef PooledConnRequest request

        # if pool is closed, raise an exception
        if not self._open:
            errors._raise_err(errors.ERR_POOL_NOT_OPEN)

        # session tagging has not been implemented yet
        if params.tag is not None:
            raise NotImplementedError("Tagging has not been implemented yet")

        # wait until an acceptable connection is found
        request = self._create_request(params)
        with self._condition:
            try:
                self._condition.wait_for(request.fulfill, self._wait_timeout)
            except:
                if not request.bg_processing:
                    request.reject()
                raise
            finally:
                request.waiting = False
            if not request.completed:
                errors._raise_err(errors.ERR_POOL_NO_CONNECTION_AVAILABLE)
            self._busy_conn_impls.append(request.conn_impl)
            return request.conn_impl

    def close(self, bint force):
        """
        Internal method for closing the pool. Note that the thread to destroy
        pools gracefully may have already run, so if the close has already
        happened, nothing more needs to be done!
        """
        if self in pools_to_close:
            with self._condition:
                self._close_helper(force)
            self._bg_task.join()

    def drop(self, ThinConnImpl conn_impl):
        """
        Internal method for dropping a connection from the pool.
        """
        with self._condition:
            self._open_count -= 1
            self._busy_conn_impls.remove(conn_impl)
            self._drop_conn_impl(conn_impl)
            self._condition.notify()


cdef class AsyncThinPoolImpl(BaseThinPoolImpl):

    cdef:
        object _bg_notify_task

    def __init__(self, str dsn, PoolParamsImpl params):
        super().__init__(dsn, params)
        self._condition = asyncio.Condition()
        self._bg_task_condition = asyncio.Condition()
        self._bg_task = asyncio.create_task(self._bg_task_func())

    async def _acquire_helper(self, PooledConnRequest request):
        """
        Helper function for acquiring a connection from the pool.
        """
        async with self._condition:
            try:
                await self._condition.wait_for(request.fulfill)
            except:
                if not request.bg_processing:
                    request.reject()
                raise
            finally:
                request.waiting = False

    async def _bg_task_func(self):
        """
        Method which runs in a dedicated task and is used to create connections
        and close them when needed. When first started, it creates pool.min
        connections. After that, it creates pool.increment connections up to
        the value of pool.max when needed and destroys connections when needed.
        The task terminates automatically when the pool is closed.
        """
        cdef:
            PooledConnRequest request = None
            BaseThinConnImpl conn_impl
            list conn_impls_to_drop
            uint32_t num_to_create

        # perform task until pool is closed
        while self._open or self._conn_impls_to_drop:

            # check to see if there a request to process
            if request is None and self._open:
                async with self._condition:
                    request = self._get_next_request()
            if request is not None and self._open:
                await self._process_request(request)
                async with self._condition:
                    self._post_process_request(request)
                    request = self._get_next_request()
                    continue

            # check to see if there is a connection that needs to be built
            async with self._condition:
                num_to_create = self._num_to_create
            if num_to_create > 0 and self._open:
                try:
                    conn_impl = await self._create_conn_impl()
                except:
                    conn_impl = None
                async with self._condition:
                    self._post_create_conn_impl(conn_impl)
                    continue

            # check to see if there are any connections to drop
            async with self._condition:
                conn_impls_to_drop = self._conn_impls_to_drop
                self._conn_impls_to_drop = []
            if conn_impls_to_drop:
                self._drop_conn_impls_helper(conn_impls_to_drop)
                continue

            # otherwise, nothing to do yet, wait for notifications!
            async with self._bg_task_condition:
                await self._bg_task_condition.wait()

        # stop the timeout task, if one is active
        if self._timeout_task is not None:
            self._timeout_task.cancel()

    async def _create_conn_impl(self, ConnectParamsImpl params=None):
        """
        Create a single connection using the pool's information. This
        connection may be placed in the pool or may be returned directly (such
        as when the pool is full and POOL_GETMODE_FORCEGET is being used).
        """
        cdef AsyncThinConnImpl conn_impl
        conn_impl = AsyncThinConnImpl(self.dsn, self.connect_params)
        self._pre_connect(conn_impl, params)
        await conn_impl.connect(self.connect_params)
        conn_impl._time_in_pool = time.monotonic()
        return conn_impl

    def _notify_bg_task(self):
        """
        Notify the background task that work needs to be done.
        """
        if self._bg_notify_task is None or self._bg_notify_task.done():
            async def helper():
                async with self._bg_task_condition:
                    self._bg_task_condition.notify()
            self._bg_notify_task = asyncio.create_task(helper())

    async def _process_request(self, PooledConnRequest request):
        """
        Processes a request.
        """
        cdef BaseThinConnImpl conn_impl
        try:
            if request.requires_ping:
                try:
                    request.conn_impl.set_call_timeout(self._ping_timeout)
                    await request.conn_impl.ping()
                    request.conn_impl.set_call_timeout(0)
                except exceptions.Error:
                    request.conn_impl._force_close()
                    request.conn_impl = None
            else:
                conn_impl = await self._create_conn_impl(request.params)
                if request.conn_impl is not None:
                    request.conn_impl._force_close()
                request.conn_impl = conn_impl
                request.conn_impl._is_pool_extra = request.is_extra
        except Exception as e:
            request.exception = e

    async def _return_connection(self, BaseThinConnImpl conn_impl):
        """
        Returns the connection to the pool.
        """
        async with self._condition:
            self._return_connection_helper(conn_impl)

    cdef int _start_timeout_task(self) except -1:
        """
        Starts the task for checking timeouts. The timeout value is increased
        by a second to allow for small time discrepancies.
        """
        async def process_timeout():
            await asyncio.sleep(self._timeout + 1)
            async with self._condition:
                self._process_timeout()
        self._timeout_task = asyncio.create_task(process_timeout())

    async def acquire(self, ConnectParamsImpl params):
        """
        Internal method for acquiring a connection from the pool.
        """
        cdef PooledConnRequest request

        # if pool is closed, raise an exception
        if not self._open:
            errors._raise_err(errors.ERR_POOL_NOT_OPEN)

        # session tagging has not been implemented yet
        if params.tag is not None:
            raise NotImplementedError("Tagging has not been implemented yet")

        # use the helper function to allow for a timeout since asyncio
        # condition variables do not have that capability directly
        request = self._create_request(params)
        try:
            await asyncio.wait_for(
                self._acquire_helper(request), self._wait_timeout
            )
        except asyncio.TimeoutError:
            errors._raise_err(errors.ERR_POOL_NO_CONNECTION_AVAILABLE)
        self._busy_conn_impls.append(request.conn_impl)
        return request.conn_impl

    async def close(self, bint force):
        """
        Internal method for closing the pool.
        """
        async with self._condition:
            self._close_helper(force)
        await self._bg_task

    async def drop(self, AsyncThinConnImpl conn_impl):
        """
        Internal method for dropping a connection from the pool.
        """
        async with self._condition:
            self._open_count -= 1
            self._busy_conn_impls.remove(conn_impl)
            self._drop_conn_impl(conn_impl)
            self._condition.notify()


@cython.freelist(20)
cdef class PooledConnRequest:
    cdef:
        BaseThinPoolImpl pool_impl
        BaseThinConnImpl conn_impl
        ConnectParamsImpl params
        str cclass
        object exception
        bint cclass_matches
        bint requires_ping
        bint wants_new
        bint bg_processing
        bint is_extra
        bint is_replacing
        bint in_progress
        bint completed
        bint waiting

    cdef int _check_connection(self, BaseThinConnImpl conn_impl) except -1:
        """
        Checks the connection to see if it can be used. First, if any control
        packets are sent that indicate that the connection should be closed,
        the connection is indeed closed. After that, a flag is updated to the
        caller indicating that a ping is required according to the pool
        configuration.
        """
        cdef:
            ReadBuffer buf = conn_impl._protocol._read_buf
            double elapsed_time
            bint has_data_ready
        if not buf._transport._is_async:
            while buf._pending_error_num == 0:
                buf._transport.has_data_ready(&has_data_ready)
                if not has_data_ready:
                    break
                buf.check_control_packet()
        if buf._pending_error_num != 0:
            self.pool_impl._drop_conn_impl(conn_impl)
            self.pool_impl._open_count -= 1
        else:
            self.conn_impl = conn_impl
            if self.pool_impl._ping_interval == 0:
                self.requires_ping = True
            elif self.pool_impl._ping_interval > 0:
                elapsed_time = time.monotonic() - conn_impl._time_in_pool
                if elapsed_time > self.pool_impl._ping_interval:
                    self.requires_ping = True
            if self.requires_ping:
                self.pool_impl._add_request(self)
            else:
                self.completed = True

    def fulfill(self):
        """
        Fulfills the connection request. If a connection is available and does
        not require a ping to validate it, it is returned immediately. If a
        connection needs to be created or requires a ping to validate it, the
        background function is notified.
        """
        cdef:
            BaseThinPoolImpl pool = self.pool_impl
            BaseThinConnImpl conn_impl
            object exc
            ssize_t i

        # if an exception was raised in the background thread, raise it now
        if self.exception is not None:
            raise self.exception

        # if the request is completed, waiting can end
        elif self.completed:
            return True

        # if the background task is still working on this request, go back to
        # waiting
        elif self.bg_processing:
            return False

        # check for an available used connection (only permitted if a new
        # connection is not required); in addition, ensure that the connection
        # class matches
        if not self.wants_new and pool._free_used_conn_impls:
            for i, conn_impl in enumerate(reversed(pool._free_used_conn_impls)):
                if self.cclass is None or conn_impl._cclass == self.cclass:
                    i = len(pool._free_used_conn_impls) - i - 1
                    pool._free_used_conn_impls.pop(i)
                    self._check_connection(conn_impl)
                    if self.completed or self.requires_ping:
                        return self.completed

        # check for an available new connection (only permitted if the
        # connection class matches)
        if self.cclass_matches:
            while pool._free_new_conn_impls:
                conn_impl = pool._free_new_conn_impls.pop()
                self._check_connection(conn_impl)
                if self.completed or self.requires_ping:
                    return self.completed

        # no matching connections are available; if the pool is full, see if
        # any connections are available and if so, ask the background task to
        # create a new one to replace the non-matching one
        self.requires_ping = False
        if pool._open_count + pool._num_to_create >= pool.max:
            if pool._free_new_conn_impls:
                self.is_replacing = True
                conn_impl = pool._free_new_conn_impls.pop()
                pool._conn_impls_to_drop.append(conn_impl)
                pool._add_request(self)
                return False
            elif pool._free_used_conn_impls:
                self.is_replacing = True
                conn_impl = pool._free_used_conn_impls.pop()
                pool._conn_impls_to_drop.append(conn_impl)
                pool._add_request(self)
                return False
            elif pool._force_get:
                self.is_extra = True
                pool._add_request(self)
                return False
            elif pool._getmode == POOL_GETMODE_NOWAIT:
                errors._raise_err(errors.ERR_POOL_NO_CONNECTION_AVAILABLE)

        # the pool has room to grow; ask the background task to create a
        # specific connection if the connection class does not match;
        # otherwise, ask the pool to grow (if it is not already growing)
        elif self.cclass_matches:
            if pool._num_to_create == 0:
                pool._num_to_create = min(pool.increment,
                                          pool.max - pool._open_count)

        # wait for the pool to grow or a connection to be returned to the pool
        pool._add_request(self)
        return False

    cdef int reject(self) except -1:
        """
        Called when a request has been rejected for any reason (such as when a
        wait timeout has been exceeded). Any connection that is associated with
        the request is returned to the pool or destroyed, depending on the
        request.
        """
        cdef:
            BaseThinPoolImpl pool_impl = self.pool_impl
            BaseThinConnImpl conn_impl = self.conn_impl
        if conn_impl is not None:
            self.conn_impl = None
            if conn_impl._is_pool_extra:
                conn_impl._is_pool_extra = False
                pool_impl._conn_impls_to_drop.append(conn_impl)
            elif conn_impl.invoke_session_callback:
                pool_impl._free_new_conn_impls.append(conn_impl)
            else:
                pool_impl._free_used_conn_impls.append(conn_impl)


# keep track of which pools need to be closed and ensure that they are closed
# gracefully when the main thread finishes its work
pools_to_close = set()
def close_pools_gracefully():
    cdef ThinPoolImpl pool_impl
    threading.main_thread().join()          # wait for main thread to finish
    for pool_impl in list(pools_to_close):
        pool_impl._shutdown()
threading.Thread(target=close_pools_gracefully).start()
