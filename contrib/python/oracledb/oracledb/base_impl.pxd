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
# base_impl.pxd
#
# Cython definition file defining the base classes from which the thick and
# thin implementations derive their classes.
#------------------------------------------------------------------------------

# cython: language_level=3

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from cpython cimport array

ctypedef unsigned char char_type

cdef enum:
    NUM_TYPE_FLOAT = 0
    NUM_TYPE_INT = 1
    NUM_TYPE_DECIMAL = 2
    NUM_TYPE_STR = 3

cdef enum:
    DB_TYPE_NUM_MIN = 2000
    DB_TYPE_NUM_MAX = 2034

    DB_TYPE_NUM_BFILE = 2020
    DB_TYPE_NUM_BINARY_DOUBLE = 2008
    DB_TYPE_NUM_BINARY_FLOAT = 2007
    DB_TYPE_NUM_BINARY_INTEGER = 2009
    DB_TYPE_NUM_BLOB = 2019
    DB_TYPE_NUM_BOOLEAN = 2022
    DB_TYPE_NUM_CHAR = 2003
    DB_TYPE_NUM_CLOB = 2017
    DB_TYPE_NUM_CURSOR = 2021
    DB_TYPE_NUM_DATE = 2011
    DB_TYPE_NUM_INTERVAL_DS = 2015
    DB_TYPE_NUM_INTERVAL_YM = 2016
    DB_TYPE_NUM_JSON = 2027
    DB_TYPE_NUM_LONG_NVARCHAR = 2031
    DB_TYPE_NUM_LONG_RAW = 2025
    DB_TYPE_NUM_LONG_VARCHAR = 2024
    DB_TYPE_NUM_NCHAR = 2004
    DB_TYPE_NUM_NCLOB = 2018
    DB_TYPE_NUM_NUMBER = 2010
    DB_TYPE_NUM_NVARCHAR = 2002
    DB_TYPE_NUM_OBJECT = 2023
    DB_TYPE_NUM_RAW = 2006
    DB_TYPE_NUM_ROWID = 2005
    DB_TYPE_NUM_TIMESTAMP = 2012
    DB_TYPE_NUM_TIMESTAMP_LTZ = 2014
    DB_TYPE_NUM_TIMESTAMP_TZ = 2013
    DB_TYPE_NUM_UNKNOWN = 0
    DB_TYPE_NUM_UROWID = 2030
    DB_TYPE_NUM_VARCHAR = 2001
    DB_TYPE_NUM_VECTOR = 2033
    DB_TYPE_NUM_XMLTYPE = 2032

cdef enum:
    NATIVE_TYPE_NUM_BOOLEAN = 3011
    NATIVE_TYPE_NUM_BYTES = 3004
    NATIVE_TYPE_NUM_DOUBLE = 3003
    NATIVE_TYPE_NUM_FLOAT = 3002
    NATIVE_TYPE_NUM_INTERVAL_DS = 3006
    NATIVE_TYPE_NUM_INTERVAL_YM = 3007
    NATIVE_TYPE_NUM_INT64 = 3000
    NATIVE_TYPE_NUM_JSON = 3013
    NATIVE_TYPE_NUM_LOB = 3008
    NATIVE_TYPE_NUM_OBJECT = 3009
    NATIVE_TYPE_NUM_ROWID = 3012
    NATIVE_TYPE_NUM_STMT = 3010
    NATIVE_TYPE_NUM_TIMESTAMP = 3005
    NATIVE_TYPE_NUM_VECTOR = 3017

cdef enum:
    CS_FORM_IMPLICIT = 1
    CS_FORM_NCHAR = 2

cdef enum:
    BYTE_ORDER_LSB = 1
    BYTE_ORDER_MSB = 2

cdef enum:
    TNS_LONG_LENGTH_INDICATOR = 254
    TNS_NULL_LENGTH_INDICATOR = 255

cpdef enum:
    AUTH_MODE_DEFAULT = 0
    AUTH_MODE_PRELIM = 0x00000008
    AUTH_MODE_SYSASM = 0x00008000
    AUTH_MODE_SYSBKP = 0x00020000
    AUTH_MODE_SYSDBA = 0x00000002
    AUTH_MODE_SYSDGD = 0x00040000
    AUTH_MODE_SYSKMT = 0x00080000
    AUTH_MODE_SYSOPER = 0x00000004
    AUTH_MODE_SYSRAC = 0x00100000

cpdef enum:
    PIPELINE_OP_TYPE_CALL_FUNC = 1
    PIPELINE_OP_TYPE_CALL_PROC = 2
    PIPELINE_OP_TYPE_COMMIT = 3
    PIPELINE_OP_TYPE_EXECUTE = 4
    PIPELINE_OP_TYPE_EXECUTE_MANY = 5
    PIPELINE_OP_TYPE_FETCH_ALL = 6
    PIPELINE_OP_TYPE_FETCH_MANY = 7
    PIPELINE_OP_TYPE_FETCH_ONE = 8

cpdef enum:
    POOL_GETMODE_WAIT = 0
    POOL_GETMODE_NOWAIT = 1
    POOL_GETMODE_FORCEGET = 2
    POOL_GETMODE_TIMEDWAIT = 3

cpdef enum:
    PURITY_DEFAULT = 0
    PURITY_NEW = 1
    PURITY_SELF = 2

cpdef enum:
    VECTOR_FORMAT_BINARY = 5
    VECTOR_FORMAT_FLOAT32 = 2
    VECTOR_FORMAT_FLOAT64 = 3
    VECTOR_FORMAT_INT8 = 4

cdef type PY_TYPE_ASYNC_CURSOR
cdef type PY_TYPE_ASYNC_LOB
cdef type PY_TYPE_BOOL
cdef type PY_TYPE_CURSOR
cdef type PY_TYPE_DATE
cdef type PY_TYPE_DATETIME
cdef type PY_TYPE_DECIMAL
cdef type PY_TYPE_DB_OBJECT
cdef type PY_TYPE_DB_OBJECT_TYPE
cdef type PY_TYPE_FETCHINFO
cdef type PY_TYPE_JSON_ID
cdef type PY_TYPE_INTERVAL_YM
cdef type PY_TYPE_LOB
cdef type PY_TYPE_MESSAGE
cdef type PY_TYPE_MESSAGE_QUERY
cdef type PY_TYPE_MESSAGE_ROW
cdef type PY_TYPE_MESSAGE_TABLE
cdef type PY_TYPE_TIMEDELTA
cdef type PY_TYPE_VAR

cdef str DRIVER_NAME
cdef str DRIVER_VERSION
cdef str DRIVER_INSTALLATION_URL

cdef const char* ENCODING_UTF8
cdef const char* ENCODING_UTF16

cdef class ApiType:
    cdef:
        readonly str name
        tuple dbtypes


cdef class DbType:
    cdef:
        readonly uint32_t num
        readonly str name
        readonly uint32_t default_size
        uint32_t _native_num
        uint32_t _buffer_size_factor
        str _ora_name
        uint8_t _ora_type_num
        uint8_t _csfrm

    @staticmethod
    cdef DbType _from_num(uint32_t num)

    @staticmethod
    cdef DbType _from_ora_name(str name)

    @staticmethod
    cdef DbType _from_ora_type_and_csfrm(uint8_t ora_type_num, uint8_t csfrm)


cdef class DefaultsImpl:
    cdef:
        public uint32_t arraysize
        public str config_dir
        public bint fetch_lobs
        public bint fetch_decimals
        public uint32_t prefetchrows
        public uint32_t stmtcachesize
        public str program
        public str machine
        public str terminal
        public str osuser
        public str driver_name

cdef DefaultsImpl C_DEFAULTS


cdef class Buffer:
    cdef:
        ssize_t _max_size, _size, _pos
        char_type[:] _data_view
        char_type *_data
        bytearray _data_obj

    cdef int _get_int_length_and_sign(self, uint8_t *length,
                                      bint *is_negative,
                                      uint8_t max_length) except -1
    cdef const char_type* _get_raw(self, ssize_t num_bytes) except NULL
    cdef int _initialize(self, ssize_t max_size=*) except -1
    cdef int _populate_from_bytes(self, bytes data) except -1
    cdef int _read_raw_bytes_and_length(self, const char_type **ptr,
                                        ssize_t *num_bytes) except -1
    cdef int _resize(self, ssize_t new_max_size) except -1
    cdef int _skip_int(self, uint8_t max_length, bint *is_negative) except -1
    cdef uint64_t _unpack_int(self, const char_type *ptr, uint8_t length)
    cdef int _write_more_data(self, ssize_t num_bytes_available,
                              ssize_t num_bytes_wanted) except -1
    cdef int _write_raw_bytes_and_length(self, const char_type *ptr,
                                         ssize_t num_bytes) except -1
    cdef inline ssize_t bytes_left(self)
    cdef int parse_binary_double(self, const uint8_t* ptr,
                                 double *double_ptr) except -1
    cdef int parse_binary_float(self, const uint8_t* ptr,
                                float *float_ptr) except -1
    cdef object parse_date(self, const uint8_t* ptr, ssize_t num_bytes)
    cdef object parse_interval_ds(self, const uint8_t* ptr)
    cdef object parse_interval_ym(self, const uint8_t* ptr)
    cdef object parse_oracle_number(self, const uint8_t* ptr,
                                    ssize_t num_bytes, int preferred_num_type)
    cdef object read_binary_double(self)
    cdef object read_binary_float(self)
    cdef object read_binary_integer(self)
    cdef object read_bool(self)
    cdef object read_bytes(self)
    cdef object read_date(self)
    cdef object read_interval_ds(self)
    cdef object read_interval_ym(self)
    cdef int read_int32(self, int32_t *value, int byte_order=*) except -1
    cdef object read_oracle_number(self, int preferred_num_type)
    cdef const char_type* read_raw_bytes(self, ssize_t num_bytes) except NULL
    cdef int read_raw_bytes_and_length(self, const char_type **ptr,
                                       ssize_t *num_bytes) except -1
    cdef int read_sb1(self, int8_t *value) except -1
    cdef int read_sb2(self, int16_t *value) except -1
    cdef int read_sb4(self, int32_t *value) except -1
    cdef int read_sb8(self, int64_t *value) except -1
    cdef bytes read_null_terminated_bytes(self)
    cdef object read_str(self, int csfrm, const char* encoding_errors=*)
    cdef int read_ub1(self, uint8_t *value) except -1
    cdef int read_ub2(self, uint16_t *value) except -1
    cdef int read_ub4(self, uint32_t *value) except -1
    cdef int read_ub8(self, uint64_t *value) except -1
    cdef int read_uint16(self, uint16_t *value, int byte_order=*) except -1
    cdef int read_uint32(self, uint32_t *value, int byte_order=*) except -1
    cdef int skip_raw_bytes(self, ssize_t num_bytes) except -1
    cdef inline int skip_sb4(self) except -1
    cdef inline void skip_to(self, ssize_t pos)
    cdef inline int skip_ub1(self) except -1
    cdef inline int skip_ub2(self) except -1
    cdef inline int skip_ub4(self) except -1
    cdef inline int skip_ub8(self) except -1
    cdef int write_binary_double(self, double value,
                                 bint write_length=*) except -1
    cdef int write_binary_float(self, float value,
                                bint write_length=*) except -1
    cdef int write_bool(self, bint value) except -1
    cdef int write_bytes(self, bytes value) except -1
    cdef int write_bytes_with_length(self, bytes value) except -1
    cdef int write_interval_ds(self, object value,
                               bint write_length=*) except -1
    cdef int write_interval_ym(self, object value,
                               bint write_length=*) except -1
    cdef int write_oracle_date(self, object value, uint8_t length,
                               bint write_length=*) except -1
    cdef int write_oracle_number(self, bytes num_bytes) except -1
    cdef int write_raw(self, const char_type *data, ssize_t length) except -1
    cdef int write_str(self, str value) except -1
    cdef int write_uint8(self, uint8_t value) except -1
    cdef int write_uint16(self, uint16_t value, int byte_order=*) except -1
    cdef int write_uint32(self, uint32_t value, int byte_order=*) except -1
    cdef int write_uint64(self, uint64_t value, byte_order=*) except -1
    cdef int write_ub2(self, uint16_t value) except -1
    cdef int write_ub4(self, uint32_t value) except -1
    cdef int write_ub8(self, uint64_t value) except -1


cdef class GrowableBuffer(Buffer):

    cdef int _reserve_space(self, ssize_t num_bytes) except -1
    cdef int _write_more_data(self, ssize_t num_bytes_available,
                              ssize_t num_bytes_wanted) except -1


cdef class OsonDecoder(Buffer):

    cdef:
        uint16_t primary_flags, secondary_flags
        ssize_t field_id_length
        ssize_t tree_seg_pos
        list field_names
        uint8_t version
        bint relative_offsets

    cdef object _decode_container_node(self, uint8_t node_type)
    cdef object _decode_node(self)
    cdef list _get_long_field_names(self, uint32_t num_fields,
                                    ssize_t offsets_size,
                                    uint32_t field_names_seg_size)
    cdef int _get_num_children(self, uint8_t node_type, uint32_t* num_children,
                               bint* is_shared) except -1
    cdef int _get_offset(self, uint8_t node_type, uint32_t* offset) except -1
    cdef list _get_short_field_names(self, uint32_t num_fields,
                                     ssize_t offsets_size,
                                     uint32_t field_names_seg_size)
    cdef object decode(self, bytes data)


cdef class OsonFieldName:

    cdef:
        str name
        bytes name_bytes
        ssize_t name_bytes_len
        uint32_t hash_id
        uint32_t offset
        uint32_t field_id

    cdef int _calc_hash_id(self) except -1
    @staticmethod
    cdef OsonFieldName create(str name, ssize_t max_fname_size)


cdef class OsonFieldNamesSegment(GrowableBuffer):

    cdef:
        uint32_t num_field_names
        list field_names

    cdef int add_name(self, OsonFieldName field_name) except -1
    @staticmethod
    cdef OsonFieldNamesSegment create()
    cdef int process_field_names(self, ssize_t field_id_offset) except -1


cdef class OsonEncoder(GrowableBuffer):

    cdef:
        OsonFieldNamesSegment short_fnames_seg
        OsonFieldNamesSegment long_fnames_seg
        uint32_t num_field_names
        ssize_t max_fname_size
        dict field_names_dict
        uint8_t field_id_size

    cdef int _add_field_name(self, str name) except -1
    cdef int _determine_flags(self, object value, uint16_t *flags) except -1
    cdef int _examine_node(self, object value) except -1
    cdef int _write_extended_header(self) except -1
    cdef int _write_fnames_seg(self, OsonFieldNamesSegment seg) except -1
    cdef int encode(self, object value, ssize_t max_fname_size) except -1


cdef class VectorDecoder(Buffer):

    cdef object decode(self, bytes data)


cdef class VectorEncoder(GrowableBuffer):

    cdef int encode(self, array.array value) except -1


cdef class ConnectParamsNode:
    cdef:
        public bint source_route
        public bint load_balance
        public bint failover
        public bint must_have_children
        public list children
        public list active_children

    cdef int _copy(self, ConnectParamsNode source) except -1
    cdef int _set_active_children(self) except -1


cdef class Address(ConnectParamsNode):
    cdef:
        public str host
        public uint32_t port
        public str protocol
        public str https_proxy
        public uint32_t https_proxy_port

    cdef str build_connect_string(self)
    cdef int set_protocol(self, str value) except -1


cdef class AddressList(ConnectParamsNode):

    cdef bint _uses_tcps(self)
    cdef str build_connect_string(self)


cdef class Description(ConnectParamsNode):
    cdef:
        public uint32_t expire_time
        public uint32_t retry_count
        public uint32_t retry_delay
        public uint32_t sdu
        public double tcp_connect_timeout
        public str service_name
        public str server_type
        public str sid
        public str cclass
        public str connection_id_prefix
        public str pool_boundary
        public uint32_t purity
        public bint ssl_server_dn_match
        public bint use_tcp_fast_open
        public str ssl_server_cert_dn
        public object ssl_version
        public str wallet_location
        str connection_id

    cdef str _build_duration_str(self, double value)
    cdef str build_connect_string(self, str cid=*)
    cdef int set_server_type(self, str value) except -1


cdef class DescriptionList(ConnectParamsNode):

    cdef str build_connect_string(self)
    cdef list get_addresses(self)


cdef class ConnectParamsImpl:
    cdef:
        public str config_dir
        public str user
        public str proxy_user
        public bint events
        public bint externalauth
        public uint32_t mode
        public str edition
        public list appcontext
        public str tag
        public bint matchanytag
        public list shardingkey
        public list supershardingkey
        public uint32_t stmtcachesize
        public bint disable_oob
        public object ssl_context
        public DescriptionList description_list
        uint64_t _external_handle
        public str debug_jdwp
        object access_token_callback
        object access_token_expires
        Description _default_description
        Address _default_address
        bytearray _password
        bytearray _password_obfuscator
        bytearray _new_password
        bytearray _new_password_obfuscator
        bytearray _wallet_password
        bytearray _wallet_password_obfuscator
        bytearray _token
        bytearray _token_obfuscator
        bytearray _private_key
        bytearray _private_key_obfuscator
        public str program
        public str machine
        public str terminal
        public str osuser
        public str driver_name

    cdef int _check_credentials(self) except -1
    cdef int _copy(self, ConnectParamsImpl other_params) except -1
    cdef bytes _get_new_password(self)
    cdef bytearray _get_obfuscator(self, str secret_value)
    cdef bytes _get_password(self)
    cdef str _get_private_key(self)
    cdef str _get_token(self)
    cdef object _get_token_expires(self, str token)
    cdef str _get_wallet_password(self)
    cdef int _parse_connect_string(self, str connect_string) except -1
    cdef int _set_access_token(self, object val, int error_num) except -1
    cdef int _set_access_token_param(self, object val) except -1
    cdef int _set_new_password(self, str password) except -1
    cdef int _set_password(self, str password) except -1
    cdef int _set_wallet_password(self, str password) except -1
    cdef bytearray _xor_bytes(self, bytearray a, bytearray b)


cdef class PoolParamsImpl(ConnectParamsImpl):
    cdef:
        public uint32_t min
        public uint32_t max
        public uint32_t increment
        public type connectiontype
        public uint32_t getmode
        public bint homogeneous
        public uint32_t timeout
        public uint32_t wait_timeout
        public uint32_t max_lifetime_session
        public object session_callback
        public uint32_t max_sessions_per_shard
        public bint soda_metadata_cache
        public int ping_interval
        public uint32_t ping_timeout


cdef class BaseConnImpl:
    cdef:
        readonly bint thin
        readonly str username
        readonly str dsn
        readonly str proxy_user
        public object inputtypehandler
        public object outputtypehandler
        public object warning
        public bint autocommit
        public bint invoke_session_callback
        readonly tuple server_version
        readonly bint supports_bool
        ssize_t _oson_max_fname_size
        bint _allow_bind_str_to_lob

    cdef object _check_value(self, DbType dbtype, BaseDbObjectTypeImpl objtype,
                             object value, bint* is_ok)
    cdef BaseCursorImpl _create_cursor_impl(self)


cdef class BasePoolImpl:
    cdef:
        readonly str dsn
        readonly bint homogeneous
        readonly uint32_t increment
        readonly uint32_t min
        readonly uint32_t max
        readonly str username
        readonly str name
        ConnectParamsImpl connect_params


cdef class BaseCursorImpl:
    cdef:
        readonly str statement
        readonly uint64_t rowcount
        public uint32_t arraysize
        public uint32_t prefetchrows
        public object inputtypehandler
        public object outputtypehandler
        public object rowfactory
        public bint scrollable
        public bint set_input_sizes
        public list fetch_info_impls
        public list fetch_vars
        public list fetch_var_impls
        public list bind_vars
        public type bind_style
        public dict bind_vars_by_name
        public object warning
        uint32_t _buffer_rowcount
        uint32_t _buffer_index
        uint32_t _fetch_array_size
        bint _more_rows_to_fetch

    cdef int _bind_values(self, object cursor, object type_handler,
                          object params, uint32_t num_rows, uint32_t row_num,
                          bint defer_type_assignment) except -1
    cdef int _bind_values_by_name(self, object cursor, object type_handler,
                                  dict params, uint32_t num_rows,
                                  uint32_t row_num,
                                  bint defer_type_assignment) except -1
    cdef int _bind_values_by_position(self, object cursor, object type_handler,
                                      object params, uint32_t num_rows,
                                      uint32_t row_num,
                                      bint defer_type_assignment) except -1
    cdef int _check_binds(self, uint32_t num_execs) except -1
    cdef int _close(self, bint in_del) except -1
    cdef BaseVarImpl _create_fetch_var(self, object conn, object cursor,
                                       object type_handler, bint
                                       uses_fetch_info, ssize_t pos,
                                       FetchInfoImpl fetch_info)
    cdef object _create_row(self)
    cdef BaseVarImpl _create_var_impl(self, object conn)
    cdef int _fetch_rows(self, object cursor) except -1
    cdef BaseConnImpl _get_conn_impl(self)
    cdef object _get_input_type_handler(self)
    cdef object _get_output_type_handler(self, bint* uses_fetch_info)
    cdef int _init_fetch_vars(self, uint32_t num_columns) except -1
    cdef bint _is_plsql(self)
    cdef int _perform_binds(self, object conn, uint32_t num_execs) except -1
    cdef int _prepare(self, str statement, str tag,
                      bint cache_statement) except -1
    cdef int _reset_bind_vars(self, uint32_t num_rows) except -1
    cdef int _verify_var(self, object var) except -1
    cdef int bind_many(self, object cursor, list parameters) except -1
    cdef int bind_one(self, object cursor, object parameters) except -1


cdef class FetchInfoImpl:
    cdef:
        readonly int16_t precision
        readonly int16_t scale
        readonly uint32_t buffer_size
        readonly uint32_t size
        readonly bint nulls_allowed
        readonly str name
        readonly DbType dbtype
        readonly BaseDbObjectTypeImpl objtype
        readonly bint is_json
        readonly bint is_oson
        readonly str domain_schema
        readonly str domain_name
        readonly dict annotations
        readonly uint32_t vector_dimensions
        readonly uint8_t vector_format
        readonly uint8_t vector_flags


cdef class BaseVarImpl:
    cdef:
        readonly str name
        readonly int16_t precision
        readonly int16_t scale
        readonly uint32_t num_elements
        readonly object inconverter
        readonly object outconverter
        readonly uint32_t size
        readonly uint32_t buffer_size
        readonly bint bypass_decode
        readonly str encoding_errors
        readonly bint is_array
        readonly bint nulls_allowed
        readonly bint convert_nulls
        public uint32_t num_elements_in_array
        readonly DbType dbtype
        readonly BaseDbObjectTypeImpl objtype
        BaseConnImpl _conn_impl
        int _preferred_num_type
        FetchInfoImpl _fetch_info
        list _values
        bint _is_value_set

    cdef int _bind(self, object conn, BaseCursorImpl cursor,
                   uint32_t num_execs, object name, uint32_t pos) except -1
    cdef int _check_and_set_scalar_value(self, uint32_t pos, object value,
                                         bint* was_set) except -1
    cdef int _check_and_set_value(self, uint32_t pos, object value,
                                  bint* was_set) except -1
    cdef int _finalize_init(self) except -1
    cdef list _get_array_value(self)
    cdef object _get_scalar_value(self, uint32_t pos)
    cdef int _on_reset_bind(self, uint32_t num_rows) except -1
    cdef int _resize(self, uint32_t new_size) except -1
    cdef int _set_scalar_value(self, uint32_t pos, object value) except -1
    cdef int _set_num_elements_in_array(self, uint32_t num_elements) except -1
    cdef int _set_type_info_from_type(self, object typ) except -1
    cdef int _set_type_info_from_value(self, object value,
                                       bint is_plsql) except -1


cdef class BaseLobImpl:
    cdef:
        readonly DbType dbtype


cdef class BaseDbObjectTypeImpl:
    cdef:
        readonly str schema
        readonly str name
        readonly str package_name
        readonly list attrs
        readonly bint is_collection
        readonly dict attrs_by_name
        readonly DbType element_dbtype
        readonly BaseDbObjectTypeImpl element_objtype
        readonly int8_t element_precision
        readonly int8_t element_scale
        readonly uint32_t element_max_size
        readonly BaseConnImpl _conn_impl
        int _element_preferred_num_type

    cpdef str _get_fqn(self)


cdef class BaseDbObjectAttrImpl:
    cdef:
        readonly str name
        readonly DbType dbtype
        readonly BaseDbObjectTypeImpl objtype
        readonly int8_t precision
        readonly int8_t scale
        readonly uint32_t max_size
        int _preferred_num_type


cdef class BaseDbObjectImpl:
    cdef:
        readonly BaseDbObjectTypeImpl type

    cdef int _check_max_size(self, object value, uint32_t max_size,
                             ssize_t* actual_size, bint* violated) except -1


cdef class BaseSodaDbImpl:
    cdef:
        public bint supports_json
        object _conn


cdef class BaseSodaCollImpl:
    cdef:
        readonly str name


cdef class BaseSodaDocImpl:
    pass


cdef class BaseSodaDocCursorImpl:
    pass


cdef class BaseQueueImpl:
    cdef:
        readonly str name
        readonly BaseDbObjectTypeImpl payload_type
        readonly BaseDeqOptionsImpl deq_options_impl
        readonly BaseEnqOptionsImpl enq_options_impl
        readonly bint is_json


cdef class BaseDeqOptionsImpl:
    pass


cdef class BaseEnqOptionsImpl:
    pass


cdef class BaseMsgPropsImpl:
    cdef:
        public object payload


cdef class BaseSubscrImpl:
    cdef:
        readonly object callback
        readonly object connection
        readonly uint32_t namespace
        readonly str name
        readonly uint32_t protocol
        readonly str ip_address
        readonly uint32_t port
        readonly uint32_t timeout
        readonly uint32_t operations
        readonly uint32_t qos
        readonly uint64_t id
        readonly uint8_t grouping_class
        readonly uint32_t grouping_value
        readonly uint8_t grouping_type
        readonly bint client_initiated


cdef class BindVar:
    cdef:
        object var
        BaseVarImpl var_impl
        object name
        ssize_t pos
        bint has_value

    cdef int _create_var_from_type(self, object conn,
                                   BaseCursorImpl cursor_impl,
                                   object value) except -1
    cdef int _create_var_from_value(self, object conn,
                                    BaseCursorImpl cursor_impl, object value,
                                    uint32_t num_elements) except -1
    cdef int _set_by_type(self, object conn, BaseCursorImpl cursor_impl,
                          object typ) except -1
    cdef int _set_by_value(self, object conn, BaseCursorImpl cursor_impl,
                           object cursor, object value, object type_handler,
                           uint32_t row_num, uint32_t num_elements,
                           bint defer_type_assignment) except -1


cdef class BaseParser:

    cdef:
        ssize_t pos, temp_pos, num_chars
        str data_as_str
        int data_kind
        void *data

    cdef Py_UCS4 get_current_char(self)
    cdef int initialize(self, str data_to_parse) except -1
    cdef int parse_keyword(self) except -1
    cdef int parse_quoted_string(self, Py_UCS4 quote_type) except -1
    cdef int skip_spaces(self) except -1


cdef class PipelineImpl:
    cdef:
        readonly list operations


cdef class PipelineOpImpl:
    cdef:
        readonly str statement
        readonly str name
        readonly object parameters
        readonly object keyword_parameters
        readonly object return_type
        readonly object rowfactory
        readonly uint32_t arraysize
        readonly uint32_t num_rows
        readonly uint8_t op_type
        uint32_t num_execs


cdef class PipelineOpResultImpl:
    cdef:
        readonly PipelineOpImpl operation
        readonly object return_value
        readonly list rows
        readonly object error
        readonly object warning
        readonly list fetch_info_impls

    cdef int _capture_err(self, Exception exc) except -1


cdef int get_preferred_num_type(int16_t precision, int8_t scale)
cdef void pack_uint16(char_type *buf, uint16_t x, int order)
cdef void pack_uint32(char_type *buf, uint32_t x, int order)
cdef void pack_uint64(char_type *buf, uint64_t x, int order)
cdef uint16_t unpack_uint16(const char_type *buf, int order)
cdef uint32_t unpack_uint32(const char_type *buf, int order)
