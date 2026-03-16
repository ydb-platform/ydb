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
# errors.py
#
# Contains the _Error class and all of the errors that are raised explicitly by
# the package. Oracle Database errors and ODPI-C errors (when using thick mode)
# are only referenced here if they are transformed into package specific
# errors.
# -----------------------------------------------------------------------------

import re

from .driver_mode import is_thin_mode
from . import exceptions


class _Error:
    """
    Error class which is used for all errors that are raised by the driver.
    """

    def __init__(
        self,
        message: str = None,
        context: str = None,
        isrecoverable: bool = False,
        iswarning: bool = False,
        code: int = 0,
        offset: int = 0,
    ) -> None:
        self.message = message
        self.context = context
        self.isrecoverable = isrecoverable
        self.iswarning = iswarning
        self.code = code
        self.offset = offset
        self.is_session_dead = False
        self.full_code = ""
        self.exc_type = exceptions.DatabaseError
        self._make_adjustments()

    def _make_adjustments(self):
        """
        Make adjustments to the error, if needed, and calculate the full_code
        attribute.
        """
        if self.message is not None:
            pos = self.message.find(":")
            if pos > 0:
                self.full_code = self.message[:pos]

                # add Oracle Database Error Help Portal URL for database error
                # messages, but only in thin mode since this is done
                # automatically in thick mode with Oracle Client 23ai and
                # higher
                if (
                    self.code != 0
                    and (self.code < 20000 or self.code >= 21000)
                    and is_thin_mode()
                ):
                    self.message = (
                        self.message
                        + "\n"
                        + "Help: https://docs.oracle.com/error-help/db/ora-"
                        + f"{self.code:05}/"
                    )
                elif self.full_code in ERR_TROUBLESHOOTING_AVAILABLE:
                    self.message = (
                        self.message
                        + "\n"
                        + "Help: https://python-oracledb.readthedocs.io/en/"
                        + "latest/user_guide/troubleshooting.html#"
                        + self.full_code.lower()
                    )

        # transform Oracle and ODPI-C specific error codes to driver errors,
        # if applicable
        if self.code != 0 or self.full_code.startswith("DPI-"):
            args = {}
            if self.code != 0:
                driver_error_info = ERR_ORACLE_ERROR_XREF.get(self.code)
            else:
                error_num = int(self.full_code[4:])
                driver_error_info = ERR_DPI_ERROR_XREF.get(error_num)
            if driver_error_info is not None:
                if isinstance(driver_error_info, tuple):
                    driver_error_num, pattern = driver_error_info
                    match = re.search(pattern, self.message)
                    args = {} if match is None else match.groupdict()
                else:
                    driver_error_num = driver_error_info
                if driver_error_num == ERR_CONNECTION_CLOSED:
                    self.is_session_dead = True
                driver_error = _get_error_text(driver_error_num, **args)
                self.message = f"{driver_error}\n{self.message}"
                self.full_code = f"{ERR_PREFIX}-{driver_error_num:04}"

        # determine exception class to use when raising this error
        if self.full_code.startswith("DPY-"):
            driver_error_num = int(self.full_code[4:])
            self.exc_type = ERR_EXCEPTION_TYPES[driver_error_num // 1000]
        elif self.code != 0:
            if self.code in ERR_INTEGRITY_ERROR_CODES:
                self.exc_type = exceptions.IntegrityError
            elif self.code in ERR_INTERFACE_ERROR_CODES:
                self.exc_type = exceptions.InterfaceError
            elif self.code in ERR_OPERATIONAL_ERROR_CODES:
                self.exc_type = exceptions.OperationalError

    def __str__(self):
        return self.message


def _get_error_text(error_num: int, **args) -> str:
    """
    Return the error text for the driver specific error number.
    """
    message_format = ERR_MESSAGE_FORMATS.get(error_num)
    if message_format is None:
        message_format = "missing error {error_num}"
        args = dict(error_num=error_num)
        error_num = ERR_MISSING_ERROR
    try:
        message = message_format.format(**args)
    except KeyError:
        message = (
            message_format
            + "\nWrong arguments to message format:\n"
            + repr(args)
        )
    return f"{ERR_PREFIX}-{error_num:04}: {message}"


def _create_err(
    error_num: int,
    context_error_message: str = None,
    cause: Exception = None,
    **args,
) -> _Error:
    """
    Returns a driver specific error object for the specified error number and
    supplied arguments.
    """
    message = _get_error_text(error_num, **args)
    if context_error_message is None and cause is not None:
        context_error_message = str(cause)
    if context_error_message is not None:
        message = f"{message}\n{context_error_message}"
    return _Error(message)


def _create_warning(error_num: int, **args) -> _Error:
    """
    Returns a warning error object for the specified error number and supplied
    arguments.
    """
    message = _get_error_text(error_num, **args)
    return _Error(message, iswarning=True)


def _raise_err(
    error_num: int,
    context_error_message: str = None,
    cause: Exception = None,
    **args,
) -> None:
    """
    Raises a driver specific exception from the specified error number and
    supplied arguments.
    """
    error = _create_err(error_num, context_error_message, cause, **args)
    raise error.exc_type(error) from cause


def _raise_not_supported(feature: str) -> None:
    """
    Raises an exception that the specified feature is not supported. This is
    used as the default implementation of all functions for the implementation
    objects.
    """
    driver_type = "thick" if is_thin_mode() else "thin"
    _raise_err(
        ERR_FEATURE_NOT_SUPPORTED, feature=feature, driver_type=driver_type
    )


# prefix used for all error messages
ERR_PREFIX = "DPY"

# error numbers that result in InterfaceError
ERR_MISSING_ERROR = 1000
ERR_NOT_CONNECTED = 1001
ERR_POOL_NOT_OPEN = 1002
ERR_NOT_A_QUERY = 1003
ERR_NO_STATEMENT_EXECUTED = 1004
ERR_POOL_HAS_BUSY_CONNECTIONS = 1005
ERR_CURSOR_NOT_OPEN = 1006

# error numbers that result in ProgrammingError
ERR_MESSAGE_HAS_NO_PAYLOAD = 2000
ERR_NO_STATEMENT = 2001
ERR_NO_STATEMENT_PREPARED = 2002
ERR_WRONG_EXECUTE_PARAMETERS_TYPE = 2003
ERR_WRONG_EXECUTEMANY_PARAMETERS_TYPE = 2004
ERR_ARGS_AND_KEYWORD_ARGS = 2005
ERR_MIXED_POSITIONAL_AND_NAMED_BINDS = 2006
ERR_EXPECTING_TYPE = 2007
ERR_WRONG_OBJECT_TYPE = 2008
ERR_WRONG_SCROLL_MODE = 2009
ERR_MIXED_ELEMENT_TYPES = 2010
ERR_WRONG_ARRAY_DEFINITION = 2011
ERR_ARGS_MUST_BE_LIST_OR_TUPLE = 2012
ERR_KEYWORD_ARGS_MUST_BE_DICT = 2013
ERR_DUPLICATED_PARAMETER = 2014
ERR_EXPECTING_VAR = 2015
ERR_INCORRECT_VAR_ARRAYSIZE = 2016
ERR_LIBRARY_ALREADY_INITIALIZED = 2017
ERR_WALLET_FILE_MISSING = 2018
ERR_THIN_CONNECTION_ALREADY_CREATED = 2019
ERR_INVALID_MAKEDSN_ARG = 2020
ERR_INIT_ORACLE_CLIENT_NOT_CALLED = 2021
ERR_INVALID_OCI_ATTR_TYPE = 2022
ERR_INVALID_CONN_CLASS = 2023
ERR_INVALID_CONNECT_PARAMS = 2025
ERR_INVALID_POOL_CLASS = 2026
ERR_INVALID_POOL_PARAMS = 2027
ERR_EXPECTING_LIST_FOR_ARRAY_VAR = 2028
ERR_HTTPS_PROXY_REQUIRES_TCPS = 2029
ERR_INVALID_LOB_OFFSET = 2030
ERR_INVALID_ACCESS_TOKEN_PARAM = 2031
ERR_INVALID_ACCESS_TOKEN_RETURNED = 2032
ERR_EXPIRED_ACCESS_TOKEN = 2033
ERR_ACCESS_TOKEN_REQUIRES_TCPS = 2034
ERR_INVALID_OBJECT_TYPE_NAME = 2035
ERR_OBJECT_IS_NOT_A_COLLECTION = 2036
ERR_MISSING_TYPE_NAME_FOR_OBJECT_VAR = 2037
ERR_INVALID_COLL_INDEX_GET = 2038
ERR_INVALID_COLL_INDEX_SET = 2039
ERR_EXECUTE_MODE_ONLY_FOR_DML = 2040
ERR_MISSING_ENDING_SINGLE_QUOTE = 2041
ERR_MISSING_ENDING_DOUBLE_QUOTE = 2042
ERR_DBOBJECT_ATTR_MAX_SIZE_VIOLATED = 2043
ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED = 2044
ERR_INVALID_ARRAYSIZE = 2045
ERR_CURSOR_HAS_BEEN_CLOSED = 2046
ERR_INVALID_LOB_AMOUNT = 2047
ERR_DML_RETURNING_DUP_BINDS = 2048
ERR_MISSING_ADDRESS = 2049
ERR_INVALID_TPC_BEGIN_FLAGS = 2050
ERR_INVALID_TPC_END_FLAGS = 2051
ERR_MISMATCHED_TOKEN = 2052
ERR_THICK_MODE_ENABLED = 2053

# error numbers that result in NotSupportedError
ERR_TIME_NOT_SUPPORTED = 3000
ERR_FEATURE_NOT_SUPPORTED = 3001
ERR_PYTHON_VALUE_NOT_SUPPORTED = 3002
ERR_PYTHON_TYPE_NOT_SUPPORTED = 3003
ERR_UNSUPPORTED_TYPE_SET = 3004
ERR_ARRAYS_OF_ARRAYS = 3005
ERR_ORACLE_TYPE_NOT_SUPPORTED = 3006
ERR_DB_TYPE_NOT_SUPPORTED = 3007
ERR_UNSUPPORTED_INBAND_NOTIFICATION = 3008
ERR_SELF_BIND_NOT_SUPPORTED = 3009
ERR_SERVER_VERSION_NOT_SUPPORTED = 3010
ERR_NCHAR_CS_NOT_SUPPORTED = 3012
ERR_UNSUPPORTED_PYTHON_TYPE_FOR_DB_TYPE = 3013
ERR_LOB_OF_WRONG_TYPE = 3014
ERR_UNSUPPORTED_VERIFIER_TYPE = 3015
ERR_NO_CRYPTOGRAPHY_PACKAGE = 3016
ERR_ORACLE_TYPE_NAME_NOT_SUPPORTED = 3017
ERR_TDS_TYPE_NOT_SUPPORTED = 3018
ERR_OSON_NODE_TYPE_NOT_SUPPORTED = 3019
ERR_OSON_FIELD_NAME_LIMITATION = 3020
ERR_OSON_VERSION_NOT_SUPPORTED = 3021
ERR_NAMED_TIMEZONE_NOT_SUPPORTED = 3022
ERR_VECTOR_VERSION_NOT_SUPPORTED = 3023
ERR_VECTOR_FORMAT_NOT_SUPPORTED = 3024
ERR_OPERATION_NOT_SUPPORTED_ON_BFILE = 3025
ERR_OPERATION_ONLY_SUPPORTED_ON_BFILE = 3026
ERR_CURSOR_DIFF_CONNECTION = 3027
ERR_UNSUPPORTED_PIPELINE_OPERATION = 3028
ERR_INVALID_NETWORK_NAME = 3029

# error numbers that result in DatabaseError
ERR_TNS_ENTRY_NOT_FOUND = 4000
ERR_NO_CREDENTIALS = 4001
ERR_COLUMN_TRUNCATED = 4002
ERR_ORACLE_NUMBER_NO_REPR = 4003
ERR_INVALID_NUMBER = 4004
ERR_POOL_NO_CONNECTION_AVAILABLE = 4005
ERR_ARRAY_DML_ROW_COUNTS_NOT_ENABLED = 4006
ERR_INCONSISTENT_DATATYPES = 4007
ERR_INVALID_BIND_NAME = 4008
ERR_WRONG_NUMBER_OF_POSITIONAL_BINDS = 4009
ERR_MISSING_BIND_VALUE = 4010
ERR_CONNECTION_CLOSED = 4011
ERR_NUMBER_WITH_INVALID_EXPONENT = 4012
ERR_NUMBER_STRING_OF_ZERO_LENGTH = 4013
ERR_NUMBER_STRING_TOO_LONG = 4014
ERR_NUMBER_WITH_EMPTY_EXPONENT = 4015
ERR_CONTENT_INVALID_AFTER_NUMBER = 4016
ERR_INVALID_CONNECT_DESCRIPTOR = 4017
ERR_CANNOT_PARSE_CONNECT_STRING = 4018
ERR_INVALID_REDIRECT_DATA = 4019
ERR_INVALID_PROTOCOL = 4021
ERR_INVALID_POOL_PURITY = 4022
ERR_CALL_TIMEOUT_EXCEEDED = 4024
ERR_INVALID_REF_CURSOR = 4025
ERR_MISSING_FILE = 4026
ERR_NO_CONFIG_DIR = 4027
ERR_INVALID_SERVER_TYPE = 4028
ERR_TOO_MANY_BATCH_ERRORS = 4029
ERR_IFILE_CYCLE_DETECTED = 4030
ERR_INVALID_VECTOR = 4031
ERR_INVALID_SSL_VERSION = 4032
ERR_EXCEEDED_IDLE_TIME = 4033

# error numbers that result in InternalError
ERR_MESSAGE_TYPE_UNKNOWN = 5000
ERR_BUFFER_LENGTH_INSUFFICIENT = 5001
ERR_INTEGER_TOO_LARGE = 5002
ERR_UNEXPECTED_NEGATIVE_INTEGER = 5003
ERR_UNEXPECTED_DATA = 5004
ERR_UNEXPECTED_REFUSE = 5005
ERR_UNEXPECTED_END_OF_DATA = 5006
ERR_UNEXPECTED_XML_TYPE = 5007
ERR_UNKNOWN_SERVER_PIGGYBACK = 5009
ERR_UNKNOWN_TRANSACTION_STATE = 5010
ERR_UNEXPECTED_PIPELINE_FAILURE = 5011

# error numbers that result in OperationalError
ERR_LISTENER_REFUSED_CONNECTION = 6000
ERR_INVALID_SERVICE_NAME = 6001
ERR_INVALID_SERVER_CERT_DN = 6002
ERR_INVALID_SID = 6003
ERR_PROXY_FAILURE = 6004
ERR_CONNECTION_FAILED = 6005

# error numbers that result in Warning
WRN_COMPILATION_ERROR = 7000

# Oracle error number cross reference
ERR_ORACLE_ERROR_XREF = {
    28: ERR_CONNECTION_CLOSED,
    600: ERR_CONNECTION_CLOSED,
    1005: ERR_NO_CREDENTIALS,
    1740: ERR_MISSING_ENDING_DOUBLE_QUOTE,
    1756: ERR_MISSING_ENDING_SINGLE_QUOTE,
    22165: (
        ERR_INVALID_COLL_INDEX_SET,
        r"index \[(?P<index>\d+)\] must be in the range of "
        r"\[(?P<min_index>\d+)\] to \[(?P<max_index>\d+)\]",
    ),
    22303: (ERR_INVALID_OBJECT_TYPE_NAME, r'type "(?P<name>[^"]*"."[^"]*)"'),
    24422: ERR_POOL_HAS_BUSY_CONNECTIONS,
    24349: ERR_ARRAY_DML_ROW_COUNTS_NOT_ENABLED,
    24457: ERR_POOL_NO_CONNECTION_AVAILABLE,
    24459: ERR_POOL_NO_CONNECTION_AVAILABLE,
    24496: ERR_POOL_NO_CONNECTION_AVAILABLE,
    24338: ERR_INVALID_REF_CURSOR,
    24344: WRN_COMPILATION_ERROR,
    38902: ERR_TOO_MANY_BATCH_ERRORS,
}

# ODPI-C error number cross reference
ERR_DPI_ERROR_XREF = {
    1010: ERR_NOT_CONNECTED,
    1024: (ERR_INVALID_COLL_INDEX_GET, r"at index (?P<index>\d+) does"),
    1043: ERR_INVALID_NUMBER,
    1044: ERR_ORACLE_NUMBER_NO_REPR,
    1063: ERR_EXECUTE_MODE_ONLY_FOR_DML,
    1067: (ERR_CALL_TIMEOUT_EXCEEDED, r"call timeout of (?P<timeout>\d+) ms"),
    1080: ERR_CONNECTION_CLOSED,
}

# Oracle error codes that result in IntegrityError exceptions
ERR_INTEGRITY_ERROR_CODES = [
    1,  # unique constraint violated
    1400,  # cannot insert NULL
    1438,  # value larger than specified precision
    2290,  # check constraint violated
    2291,  # integrity constraint violated - parent key not found
    2292,  # integrity constraint violated - child record found
    21525,  # attribute or collection element violated its constraints
    40479,  # internal JSON serializer error
]

# Oracle error codes that result in InterfaceError exceptions
ERR_INTERFACE_ERROR_CODES = [
    24422,  # error occurred while trying to destroy the Session Pool
]

# Oracle error codes that result in OperationalError exceptions
ERR_OPERATIONAL_ERROR_CODES = [
    22,  # invalid session ID; access denied
    378,  # buffer pools cannot be created as specified
    600,  # internal error code
    602,  # internal programming exception
    603,  # ORACLE server session terminated by fatal error
    604,  # error occurred at recursive SQL level
    609,  # could not attach to incoming connection
    1012,  # not logged on
    1013,  # user requested cancel of current operation
    1033,  # ORACLE initialization or shutdown in progress
    1034,  # ORACLE not available
    1041,  # internal error. hostdef extension doesn't exist
    1043,  # user side memory corruption
    1089,  # immediate shutdown or close in progress
    1090,  # shutdown in progress - connection is not permitted
    1092,  # ORACLE instance terminated. Disconnection forced
    3111,  # break received on communication channel
    3113,  # end-of-file on communication channel
    3114,  # not connected to ORACLE
    3122,  # attempt to close ORACLE-side window on user side
    3135,  # connection lost contact
    12153,  # TNS:not connected
    12203,  # TNS:unable to connect to destination
    12500,  # TNS:listener failed to start a dedicated server process
    12571,  # TNS:packet writer failure
    27146,  # post/wait initialization failed
    28511,  # lost RPC connection to heterogeneous remote agent
]

# driver error message exception types (multiples of 1000)
ERR_EXCEPTION_TYPES = {
    1: exceptions.InterfaceError,
    2: exceptions.ProgrammingError,
    3: exceptions.NotSupportedError,
    4: exceptions.DatabaseError,
    5: exceptions.InternalError,
    6: exceptions.OperationalError,
    7: exceptions.Warning,
}

# error messages that have a troubleshooting section available
ERR_TROUBLESHOOTING_AVAILABLE = set(
    [
        "DPI-1047",  # Oracle Client library cannot be loaded
        "DPI-1072",  # Oracle Client library version is unsupported
        "DPY-3010",  # connections to Oracle Database version not supported
        "DPY-3015",  # password verifier type is not supported
        "DPY-4011",  # the database or network closed the connection
    ]
)

# error message formats
ERR_MESSAGE_FORMATS = {
    ERR_ACCESS_TOKEN_REQUIRES_TCPS: (
        "access_token requires use of the tcps protocol"
    ),
    ERR_ARGS_MUST_BE_LIST_OR_TUPLE: "arguments must be a list or tuple",
    ERR_ARGS_AND_KEYWORD_ARGS: (
        "expecting positional arguments or keyword arguments, not both"
    ),
    ERR_ARRAY_DML_ROW_COUNTS_NOT_ENABLED: (
        "array DML row counts mode is not enabled"
    ),
    ERR_ARRAYS_OF_ARRAYS: "arrays of arrays are not supported",
    ERR_BUFFER_LENGTH_INSUFFICIENT: (
        "internal error: buffer of length {actual_buffer_len} "
        "insufficient to hold {required_buffer_len} bytes"
    ),
    ERR_CALL_TIMEOUT_EXCEEDED: "call timeout of {timeout} ms exceeded",
    ERR_CANNOT_PARSE_CONNECT_STRING: 'cannot parse connect string "{data}"',
    ERR_COLUMN_TRUNCATED: (
        "column truncated to {col_value_len} {unit}. "
        "Untruncated was {actual_len}"
    ),
    ERR_CONNECTION_FAILED: (
        "cannot connect to database (CONNECTION_ID={connection_id})."
    ),
    ERR_CONTENT_INVALID_AFTER_NUMBER: "invalid number (content after number)",
    ERR_CURSOR_DIFF_CONNECTION: (
        "binding a cursor from a different connection is not supported"
    ),
    ERR_CURSOR_NOT_OPEN: "cursor is not open",
    ERR_CURSOR_HAS_BEEN_CLOSED: "cursor has been closed by the database",
    ERR_DBOBJECT_ATTR_MAX_SIZE_VIOLATED: (
        "attribute {attr_name} of type {type_name} exceeds its maximum size "
        "(actual: {actual_size}, maximum: {max_size})"
    ),
    ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED: (
        "element {index} of type {type_name} exceeds its maximum size "
        "(actual: {actual_size}, maximum: {max_size})"
    ),
    ERR_DB_TYPE_NOT_SUPPORTED: 'database type "{name}" is not supported',
    ERR_DML_RETURNING_DUP_BINDS: (
        'the bind variable placeholder ":{name}" cannot be used both before '
        "and after the RETURNING clause in a DML RETURNING statement"
    ),
    ERR_DUPLICATED_PARAMETER: (
        '"{deprecated_name}" and "{new_name}" cannot be specified together'
    ),
    ERR_EXCEEDED_IDLE_TIME: (
        "the database closed the connection because the connection's idle "
        "time has been exceeded"
    ),
    ERR_EXECUTE_MODE_ONLY_FOR_DML: (
        'parameters "batcherrors" and "arraydmlrowcounts" may only be '
        "true when used with insert, update, delete and merge statements"
    ),
    ERR_EXPECTING_LIST_FOR_ARRAY_VAR: (
        "expecting list when setting array variables"
    ),
    ERR_EXPECTING_TYPE: "expected a type",
    ERR_EXPECTING_VAR: (
        "type handler should return None or the value returned by a call "
        "to cursor.var()"
    ),
    ERR_EXPIRED_ACCESS_TOKEN: "access token has expired",
    ERR_FEATURE_NOT_SUPPORTED: (
        "{feature} is only supported in python-oracledb {driver_type} mode"
    ),
    ERR_HTTPS_PROXY_REQUIRES_TCPS: (
        "https_proxy requires use of the tcps protocol"
    ),
    ERR_IFILE_CYCLE_DETECTED: (
        "file '{including_file_name}' includes file '{included_file_name}', "
        "which forms a cycle"
    ),
    ERR_INCONSISTENT_DATATYPES: (
        "cannot convert from data type {input_type} to {output_type}"
    ),
    ERR_INCORRECT_VAR_ARRAYSIZE: (
        "variable array size of {var_arraysize} is "
        "too small (should be at least {required_arraysize})"
    ),
    ERR_INIT_ORACLE_CLIENT_NOT_CALLED: (
        "init_oracle_client() must be called first"
    ),
    ERR_INTEGER_TOO_LARGE: (
        "internal error: read integer of length {length} when expecting "
        "integer of no more than length {max_length}"
    ),
    ERR_INVALID_ACCESS_TOKEN_PARAM: (
        "invalid access token: value must be a string (for OAuth), a "
        "2-tuple containing the token and private key strings (for IAM), "
        "or a callable that returns a string or 2-tuple"
    ),
    ERR_INVALID_ACCESS_TOKEN_RETURNED: (
        "invalid access token returned from callable: value must be a "
        "string (for OAuth) or a 2-tuple containing the token and private "
        "key strings (for IAM)"
    ),
    ERR_INVALID_ARRAYSIZE: "arraysize must be an integer greater than zero",
    ERR_INVALID_BIND_NAME: (
        'no bind placeholder named ":{name}" was found in the SQL text'
    ),
    ERR_INVALID_CONN_CLASS: "invalid connection class",
    ERR_INVALID_CONNECT_DESCRIPTOR: 'invalid connect descriptor "{data}"',
    ERR_INVALID_CONNECT_PARAMS: "invalid connection params",
    ERR_INVALID_COLL_INDEX_GET: "element at index {index} does not exist",
    ERR_INVALID_COLL_INDEX_SET: (
        "given index {index} must be in the range of {min_index} to "
        "{max_index}"
    ),
    ERR_INVALID_LOB_AMOUNT: "LOB amount must be greater than zero",
    ERR_INVALID_LOB_OFFSET: "LOB offset must be greater than zero",
    ERR_INVALID_MAKEDSN_ARG: '"{name}" argument contains invalid values',
    ERR_INVALID_NUMBER: "invalid number",
    ERR_INVALID_OBJECT_TYPE_NAME: 'invalid object type name: "{name}"',
    ERR_INVALID_OCI_ATTR_TYPE: "invalid OCI attribute type {attr_type}",
    ERR_INVALID_POOL_CLASS: "invalid connection pool class",
    ERR_INVALID_POOL_PARAMS: "invalid pool params",
    ERR_INVALID_POOL_PURITY: "invalid DRCP purity {purity}",
    ERR_INVALID_PROTOCOL: 'invalid protocol "{protocol}"',
    ERR_INVALID_REDIRECT_DATA: "invalid redirect data {data}",
    ERR_INVALID_REF_CURSOR: "invalid REF CURSOR: never opened in PL/SQL",
    ERR_INVALID_SERVER_CERT_DN: (
        "The distinguished name (DN) on the server certificate does not "
        "match the expected value"
    ),
    ERR_INVALID_SERVER_TYPE: "invalid server_type: {server_type}",
    ERR_INVALID_SERVICE_NAME: (
        'Service "{service_name}" is not registered with the listener at '
        'host "{host}" port {port}. (Similar to ORA-12514)'
    ),
    ERR_INVALID_SID: (
        'SID "{sid}" is not registered with the listener at host "{host}" '
        "port {port}. (Similar to ORA-12505)"
    ),
    ERR_INVALID_SSL_VERSION: 'invalid value for ssl_version: "{ssl_version}"',
    ERR_INVALID_TPC_BEGIN_FLAGS: "invalid flags for tpc_begin()",
    ERR_INVALID_TPC_END_FLAGS: "invalid flags for tpc_end()",
    ERR_INVALID_VECTOR: "vector cannot contain zero dimensions",
    ERR_KEYWORD_ARGS_MUST_BE_DICT: (
        '"keyword_parameters" argument must be a dict'
    ),
    ERR_LIBRARY_ALREADY_INITIALIZED: (
        "init_oracle_client() was already called with different arguments"
    ),
    ERR_LISTENER_REFUSED_CONNECTION: (
        "Listener refused connection. (Similar to ORA-{error_code})"
    ),
    ERR_LOB_OF_WRONG_TYPE: (
        "LOB is of type {actual_type_name} but must be of type "
        "{expected_type_name}"
    ),
    ERR_MESSAGE_HAS_NO_PAYLOAD: "message has no payload",
    ERR_MESSAGE_TYPE_UNKNOWN: (
        "internal error: unknown protocol message type {message_type} "
        "at position {position}"
    ),
    ERR_MISMATCHED_TOKEN: (
        "internal error: pipeline token number {token_num} does not match "
        "expected token number {expected_token_num}"
    ),
    ERR_MISSING_ADDRESS: (
        "no addresses are defined in connect descriptor: {connect_string}"
    ),
    ERR_MISSING_BIND_VALUE: (
        'a bind variable replacement value for placeholder ":{name}" was '
        "not provided"
    ),
    ERR_MISSING_FILE: "file '{file_name}' is missing or unreadable",
    ERR_MISSING_ENDING_DOUBLE_QUOTE: 'missing ending quote (")',
    ERR_MISSING_ENDING_SINGLE_QUOTE: "missing ending quote (')",
    ERR_MISSING_TYPE_NAME_FOR_OBJECT_VAR: (
        "no object type specified for object variable"
    ),
    ERR_MIXED_ELEMENT_TYPES: (
        "element {element} is not the same data type as previous elements"
    ),
    ERR_MIXED_POSITIONAL_AND_NAMED_BINDS: (
        "positional and named binds cannot be intermixed"
    ),
    ERR_NAMED_TIMEZONE_NOT_SUPPORTED: (
        "named time zones are not supported in thin mode"
    ),
    ERR_NCHAR_CS_NOT_SUPPORTED: (
        "national character set id {charset_id} is not supported by "
        "python-oracledb in thin mode"
    ),
    ERR_NO_CONFIG_DIR: "no configuration directory to search for tnsnames.ora",
    ERR_NO_CREDENTIALS: "no credentials specified",
    ERR_NO_CRYPTOGRAPHY_PACKAGE: (
        "python-oracledb thin mode cannot be used because the "
        "cryptography package is not installed"
    ),
    ERR_NO_STATEMENT: "no statement specified and no prior statement prepared",
    ERR_NO_STATEMENT_EXECUTED: "no statement executed",
    ERR_NO_STATEMENT_PREPARED: "statement must be prepared first",
    ERR_NOT_A_QUERY: "the executed statement does not return rows",
    ERR_NOT_CONNECTED: "not connected to database",
    ERR_NUMBER_STRING_OF_ZERO_LENGTH: "invalid number: zero length string",
    ERR_NUMBER_STRING_TOO_LONG: "invalid number: string too long",
    ERR_NUMBER_WITH_EMPTY_EXPONENT: "invalid number: empty exponent",
    ERR_NUMBER_WITH_INVALID_EXPONENT: "invalid number: invalid exponent",
    ERR_OBJECT_IS_NOT_A_COLLECTION: "object {name} is not a collection",
    ERR_OPERATION_NOT_SUPPORTED_ON_BFILE: (
        "operation is not supported on BFILE LOBs"
    ),
    ERR_OPERATION_ONLY_SUPPORTED_ON_BFILE: (
        "operation is only supported on BFILE LOBs"
    ),
    ERR_ORACLE_NUMBER_NO_REPR: (
        "value cannot be represented as an Oracle number"
    ),
    ERR_ORACLE_TYPE_NAME_NOT_SUPPORTED: (
        'Oracle data type name "{name}" is not supported'
    ),
    ERR_ORACLE_TYPE_NOT_SUPPORTED: "Oracle data type {num} is not supported",
    ERR_OSON_FIELD_NAME_LIMITATION: (
        "OSON field names may not exceed {max_fname_size} UTF-8 encoded bytes"
    ),
    ERR_OSON_NODE_TYPE_NOT_SUPPORTED: (
        "OSON node type 0x{node_type:x} is not supported"
    ),
    ERR_OSON_VERSION_NOT_SUPPORTED: "OSON version {version} is not supported",
    ERR_POOL_HAS_BUSY_CONNECTIONS: (
        "connection pool cannot be closed because connections are busy"
    ),
    ERR_POOL_NO_CONNECTION_AVAILABLE: (
        "timed out waiting for the connection pool to return a connection"
    ),
    ERR_POOL_NOT_OPEN: "connection pool is not open",
    ERR_PROXY_FAILURE: "network proxy failed: response was {response}",
    ERR_PYTHON_TYPE_NOT_SUPPORTED: "Python type {typ} is not supported",
    ERR_PYTHON_VALUE_NOT_SUPPORTED: (
        'Python value of type "{type_name}" is not supported'
    ),
    ERR_SELF_BIND_NOT_SUPPORTED: "binding to self is not supported",
    ERR_CONNECTION_CLOSED: "the database or network closed the connection",
    ERR_SERVER_VERSION_NOT_SUPPORTED: (
        "connections to this database server version are not supported "
        "by python-oracledb in thin mode"
    ),
    ERR_TDS_TYPE_NOT_SUPPORTED: "Oracle TDS data type {num} is not supported",
    ERR_THICK_MODE_ENABLED: (
        "python-oracledb thin mode cannot be used because thick mode has "
        "already been enabled"
    ),
    ERR_THIN_CONNECTION_ALREADY_CREATED: (
        "python-oracledb thick mode cannot be used because thin mode has "
        "already been enabled or a thin mode connection has already been "
        "created"
    ),
    ERR_TIME_NOT_SUPPORTED: (
        "Oracle Database does not support time only variables"
    ),
    ERR_TNS_ENTRY_NOT_FOUND: 'unable to find "{name}" in {file_name}',
    ERR_TOO_MANY_BATCH_ERRORS: (
        "the number of batch errors from executemany() exceeds 65535"
    ),
    ERR_UNEXPECTED_PIPELINE_FAILURE: "unexpected pipeline failure",
    ERR_UNEXPECTED_DATA: "unexpected data received: {data}",
    ERR_UNEXPECTED_END_OF_DATA: (
        "unexpected end of data: want {num_bytes_wanted} bytes but "
        "only {num_bytes_available} bytes are available"
    ),
    ERR_UNEXPECTED_NEGATIVE_INTEGER: (
        "internal error: read a negative integer when expecting a "
        "positive integer"
    ),
    ERR_UNEXPECTED_REFUSE: (
        "the listener refused the connection but an unexpected error "
        "format was returned"
    ),
    ERR_UNEXPECTED_XML_TYPE: "unexpected XMLType with flag {flag}",
    ERR_UNKNOWN_SERVER_PIGGYBACK: (
        "internal error: unknown server side piggyback opcode {opcode}"
    ),
    ERR_UNKNOWN_TRANSACTION_STATE: (
        "internal error: unknown transaction state {state}"
    ),
    ERR_UNSUPPORTED_PIPELINE_OPERATION: (
        "unsupported pipeline operation type: {op_type}"
    ),
    ERR_UNSUPPORTED_INBAND_NOTIFICATION: (
        "unsupported in-band notification with error number {err_num}"
    ),
    ERR_UNSUPPORTED_PYTHON_TYPE_FOR_DB_TYPE: (
        "unsupported Python type {py_type_name} for database type "
        "{db_type_name}"
    ),
    ERR_UNSUPPORTED_TYPE_SET: "type {db_type_name} does not support being set",
    ERR_UNSUPPORTED_VERIFIER_TYPE: (
        "password verifier type 0x{verifier_type:x} is not supported by "
        "python-oracledb in thin mode"
    ),
    ERR_VECTOR_FORMAT_NOT_SUPPORTED: (
        "VECTOR type {vector_format} is not supported"
    ),
    ERR_VECTOR_VERSION_NOT_SUPPORTED: (
        "VECTOR version {version} is not supported"
    ),
    ERR_WALLET_FILE_MISSING: "wallet file {name} was not found",
    ERR_WRONG_ARRAY_DEFINITION: (
        "expecting a list of two elements [type, numelems]"
    ),
    ERR_WRONG_EXECUTE_PARAMETERS_TYPE: (
        "expecting a dictionary, list or tuple, or keyword args"
    ),
    ERR_WRONG_EXECUTEMANY_PARAMETERS_TYPE: (
        '"parameters" argument should be a list of sequences or '
        "dictionaries, or an integer specifying the number of "
        "times to execute the statement"
    ),
    ERR_WRONG_NUMBER_OF_POSITIONAL_BINDS: (
        "{expected_num} positional bind values are required but "
        "{actual_num} were provided"
    ),
    ERR_WRONG_OBJECT_TYPE: (
        'found object of type "{actual_schema}.{actual_name}" when '
        'expecting object of type "{expected_schema}.{expected_name}"'
    ),
    ERR_WRONG_SCROLL_MODE: (
        "scroll mode must be relative, absolute, first or last"
    ),
    WRN_COMPILATION_ERROR: "creation succeeded with compilation errors",
    ERR_INVALID_NETWORK_NAME: (
        '"{name}" includes characters that are not allowed'
    ),
}
