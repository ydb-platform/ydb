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
# connection.pyx
#
# Cython file defining the base Connection implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseConnImpl:

    def __init__(self, str dsn, ConnectParamsImpl params):
        self.dsn = dsn
        self.username = params.user
        self.proxy_user = params.proxy_user
        self._oson_max_fname_size = 255

    cdef object _check_value(self, DbType dbtype, BaseDbObjectTypeImpl objtype,
                             object value, bint* is_ok):
        """
        Checks that the specified Python value is acceptable for the given
        database type. If the "is_ok" parameter is passed as NULL, an exception
        is raised.  The value to use is returned (possibly modified from the
        value passed in).
        """
        cdef:
            uint32_t db_type_num
            BaseLobImpl lob_impl

        # null values are always accepted
        if value is None:
            return value

        # check to see if the Python value is accepted and perform any
        # necessary adjustments
        db_type_num = dbtype.num
        if db_type_num in (DB_TYPE_NUM_NUMBER,
                           DB_TYPE_NUM_BINARY_INTEGER,
                           DB_TYPE_NUM_BINARY_DOUBLE,
                           DB_TYPE_NUM_BINARY_FLOAT):
            if isinstance(value, (PY_TYPE_BOOL, int, float, PY_TYPE_DECIMAL)):
                if db_type_num in (DB_TYPE_NUM_BINARY_FLOAT,
                                   DB_TYPE_NUM_BINARY_DOUBLE):
                    return float(value)
                elif db_type_num == DB_TYPE_NUM_BINARY_INTEGER \
                        or cpython.PyBool_Check(value):
                    return int(value)
                return value
        elif db_type_num in (DB_TYPE_NUM_CHAR,
                             DB_TYPE_NUM_VARCHAR,
                             DB_TYPE_NUM_NCHAR,
                             DB_TYPE_NUM_NVARCHAR,
                             DB_TYPE_NUM_LONG_VARCHAR,
                             DB_TYPE_NUM_LONG_NVARCHAR):
            if isinstance(value, bytes):
                return (<bytes> value).decode()
            elif isinstance(value, str):
                return value
        elif db_type_num in (DB_TYPE_NUM_RAW, DB_TYPE_NUM_LONG_RAW):
            if isinstance(value, str):
                return (<str> value).encode()
            elif isinstance(value, bytes):
                return value
        elif db_type_num in (DB_TYPE_NUM_DATE,
                             DB_TYPE_NUM_TIMESTAMP,
                             DB_TYPE_NUM_TIMESTAMP_LTZ,
                             DB_TYPE_NUM_TIMESTAMP_TZ):
            if cydatetime.PyDateTime_Check(value) \
                    or cydatetime.PyDate_Check(value):
                return value
        elif db_type_num == DB_TYPE_NUM_INTERVAL_DS:
            if isinstance(value, PY_TYPE_TIMEDELTA):
                return value
        elif db_type_num in (DB_TYPE_NUM_CLOB,
                             DB_TYPE_NUM_NCLOB,
                             DB_TYPE_NUM_BLOB,
                             DB_TYPE_NUM_BFILE):
            if isinstance(value, (PY_TYPE_LOB, PY_TYPE_ASYNC_LOB)):
                lob_impl = value._impl
                if lob_impl.dbtype is not dbtype:
                    if is_ok != NULL:
                        is_ok[0] = False
                        return value
                    errors._raise_err(errors.ERR_LOB_OF_WRONG_TYPE,
                                      actual_type_name=lob_impl.dbtype.name,
                                      expected_type_name=dbtype.name)
                return value
            elif self._allow_bind_str_to_lob \
                    and db_type_num != DB_TYPE_NUM_BFILE \
                    and isinstance(value, (bytes, str)):
                if db_type_num == DB_TYPE_NUM_BLOB:
                    if isinstance(value, str):
                        value = value.encode()
                elif isinstance(value, bytes):
                    value = value.decode()
                lob_impl = self.create_temp_lob_impl(dbtype)
                if value:
                    lob_impl.write(value, 1)
                return PY_TYPE_LOB._from_impl(lob_impl)
        elif db_type_num == DB_TYPE_NUM_OBJECT:
            if isinstance(value, PY_TYPE_DB_OBJECT):
                if value._impl.type != objtype:
                    if is_ok != NULL:
                        is_ok[0] = False
                        return value
                    errors._raise_err(errors.ERR_WRONG_OBJECT_TYPE,
                                      actual_schema=value.type.schema,
                                      actual_name=value.type.name,
                                      expected_schema=objtype.schema,
                                      expected_name=objtype.name)
                return value
        elif db_type_num == DB_TYPE_NUM_CURSOR:
            if isinstance(value, (PY_TYPE_CURSOR, PY_TYPE_ASYNC_CURSOR)):
                value._verify_open()
                if value.connection._impl is not self:
                    errors._raise_err(errors.ERR_CURSOR_DIFF_CONNECTION)
                return value
        elif db_type_num == DB_TYPE_NUM_BOOLEAN:
            return bool(value)
        elif db_type_num == DB_TYPE_NUM_JSON:
            return value
        elif db_type_num == DB_TYPE_NUM_VECTOR:
            if isinstance(value, list):
                if len(value) == 0:
                    errors._raise_err(errors.ERR_INVALID_VECTOR)
                return array.array('d', value)
            elif isinstance(value, array.array) \
                    and value.typecode in ('f', 'd', 'b', 'B'):
                if len(value) == 0:
                    errors._raise_err(errors.ERR_INVALID_VECTOR)
                return value
        elif db_type_num == DB_TYPE_NUM_INTERVAL_YM:
            if isinstance(value, PY_TYPE_INTERVAL_YM):
                return value
        else:
            if is_ok != NULL:
                is_ok[0] = False
                return value
            errors._raise_err(errors.ERR_UNSUPPORTED_TYPE_SET,
                              db_type_name=dbtype.name)

        # the Python value was not considered acceptable
        if is_ok != NULL:
            is_ok[0] = False
            return value
        errors._raise_err(errors.ERR_UNSUPPORTED_PYTHON_TYPE_FOR_DB_TYPE,
                          py_type_name=type(value).__name__,
                          db_type_name=dbtype.name)

    cdef BaseCursorImpl _create_cursor_impl(self):
        """
        Internal method for creating an empty cursor implementation object.
        """
        raise NotImplementedError()

    def _get_oci_attr(self, uint32_t handle_type, uint32_t attr_num,
                      uint32_t attr_type):
        errors._raise_not_supported("getting a connection OCI attribute")

    def _set_oci_attr(self, uint32_t handle_type, uint32_t attr_num,
                      uint32_t attr_type, object value):
        errors._raise_not_supported("setting a connection OCI attribute")

    def cancel(self):
        errors._raise_not_supported("aborting a currently executing statement")

    def change_password(self, old_password, new_password):
        errors._raise_not_supported("changing a password")

    def decode_oson(self, bytes data):
        """
        Decode OSON encoded bytes and return the object encoded in them.
        """
        cdef OsonDecoder decoder = OsonDecoder.__new__(OsonDecoder)
        return decoder.decode(data)

    def encode_oson(self, object value):
        """
        Return OSON encoded bytes encoded from the supplied object.
        """
        cdef OsonEncoder encoder = OsonEncoder.__new__(OsonEncoder)
        encoder.encode(value, self._oson_max_fname_size)
        return encoder._data[:encoder._pos]

    def get_is_healthy(self):
        errors._raise_not_supported("checking if the connection is healthy")

    def close(self, in_del=False):
        errors._raise_not_supported("closing a connection")

    def commit(self):
        errors._raise_not_supported("committing a transaction")

    def create_cursor_impl(self, bint scrollable):
        """
        Create the cursor implementation object.
        """
        cdef BaseCursorImpl cursor_impl = self._create_cursor_impl()
        cursor_impl.scrollable = scrollable
        cursor_impl.arraysize = C_DEFAULTS.arraysize
        cursor_impl.prefetchrows = C_DEFAULTS.prefetchrows
        return cursor_impl

    def create_queue_impl(self):
        errors._raise_not_supported("creating a queue")

    def create_soda_database_impl(self, conn):
        errors._raise_not_supported("creating a SODA database object")

    def create_subscr_impl(self, object conn, object callback,
                           uint32_t namespace, str name, uint32_t protocol,
                           str ip_address, uint32_t port, uint32_t timeout,
                           uint32_t operations, uint32_t qos,
                           uint8_t grouping_class, uint32_t grouping_value,
                           uint8_t grouping_type, bint client_initiated):
        errors._raise_not_supported("creating a subscription")

    def create_temp_lob_impl(self, DbType dbtype):
        errors._raise_not_supported("creating a temporary LOB")

    def get_call_timeout(self):
        errors._raise_not_supported("getting the call timeout")

    def get_current_schema(self):
        errors._raise_not_supported("getting the current schema")

    def get_db_domain(self):
        errors._raise_not_supported("getting the database domain name")

    def get_db_name(self):
        errors._raise_not_supported("getting the database name")

    def get_edition(self):
        errors._raise_not_supported("getting the edition")

    def get_external_name(self):
        errors._raise_not_supported("getting the external name")

    def get_handle(self):
        errors._raise_not_supported("getting the OCI service context handle")

    def get_instance_name(self):
        errors._raise_not_supported("getting the instance name")

    def get_internal_name(self):
        errors._raise_not_supported("getting the internal name")

    def get_ltxid(self):
        errors._raise_not_supported("getting the logical transaction id")

    def get_max_identifier_length(self):
        errors._raise_not_supported("getting the maximum identifier length")

    def get_max_open_cursors(self):
        errors._raise_not_supported(
            "getting the maximum number of open cursors"
        )

    def get_sdu(self):
        errors._raise_not_supported("getting the session data unit (SDU)")

    def get_serial_num(self):
        errors._raise_not_supported("getting the session serial number")

    def get_service_name(self):
        errors._raise_not_supported("getting the service name")

    def get_session_id(self):
        errors._raise_not_supported("getting the session id")

    def get_stmt_cache_size(self):
        errors._raise_not_supported("getting the statement cache size")

    def get_transaction_in_progress(self):
        errors._raise_not_supported("getting if a transaction is in progress")

    def get_type(self, object conn, str name):
        errors._raise_not_supported("getting an object type")

    def ping(self):
        errors._raise_not_supported("pinging the database")

    def rollback(self):
        errors._raise_not_supported("rolling back a transaction")

    def set_action(self, value):
        errors._raise_not_supported("setting the action")

    def set_call_timeout(self, value):
        errors._raise_not_supported("setting the call timeout")

    def set_client_identifier(self, value):
        errors._raise_not_supported("setting the client identifier")

    def set_client_info(self, value):
        errors._raise_not_supported("setting the client info")

    def set_current_schema(self, value):
        errors._raise_not_supported("setting the current schema")

    def set_dbop(self, value):
        errors._raise_not_supported("setting the database operation")

    def set_econtext_id(self, value):
        errors._raise_not_supported("setting the execution context id")

    def set_external_name(self, value):
        errors._raise_not_supported("setting the external name")

    def set_internal_name(self, value):
        errors._raise_not_supported("setting the internal name")

    def set_module(self, value):
        errors._raise_not_supported("setting the module")

    def set_stmt_cache_size(self, value):
        errors._raise_not_supported("setting the statement cache size")

    def shutdown(self, uint32_t mode):
        errors._raise_not_supported("shutting down the database")

    def startup(self, bint force, bint restrict, str pfile):
        errors._raise_not_supported("starting up the database")

    def supports_pipelining(self):
        return False

    def tpc_begin(self, xid, uint32_t flags, uint32_t timeout):
        errors._raise_not_supported(
            "starting a TPC (two-phase commit) transaction"
        )

    def tpc_commit(self, xid, bint one_phase):
        errors._raise_not_supported(
            "committing a TPC (two-phase commit) transaction"
        )

    def tpc_end(self, xid, uint32_t flags):
        errors._raise_not_supported(
            "ending a TPC (two-phase commit) transaction"
        )

    def tpc_forget(self, xid):
        errors._raise_not_supported(
            "forgetting a TPC (two-phase commit) transaction"
        )

    def tpc_prepare(self, xid):
        errors._raise_not_supported(
            "preparing a TPC (two-phase commit) transaction"
        )

    def tpc_rollback(self, xid):
        errors._raise_not_supported(
            "rolling back a TPC (two-phase commit) transaction"
        )
