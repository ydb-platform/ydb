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
# __init__.py
#
# Package initialization module.
# -----------------------------------------------------------------------------

import collections
import sys
import warnings

if sys.version_info[:2] < (3, 8):
    message = (
        f"Python {sys.version_info[0]}.{sys.version_info[1]} is no longer "
        "supported by the Python core team. Therefore, support for it is "
        "deprecated in python-oracledb and will be removed in a future release"
    )
    warnings.warn(message)

from . import base_impl, thick_impl, thin_impl

from .base_impl import (
    # database types
    DB_TYPE_BFILE as DB_TYPE_BFILE,
    DB_TYPE_BINARY_DOUBLE as DB_TYPE_BINARY_DOUBLE,
    DB_TYPE_BINARY_FLOAT as DB_TYPE_BINARY_FLOAT,
    DB_TYPE_BINARY_INTEGER as DB_TYPE_BINARY_INTEGER,
    DB_TYPE_BLOB as DB_TYPE_BLOB,
    DB_TYPE_BOOLEAN as DB_TYPE_BOOLEAN,
    DB_TYPE_CHAR as DB_TYPE_CHAR,
    DB_TYPE_CLOB as DB_TYPE_CLOB,
    DB_TYPE_CURSOR as DB_TYPE_CURSOR,
    DB_TYPE_DATE as DB_TYPE_DATE,
    DB_TYPE_INTERVAL_DS as DB_TYPE_INTERVAL_DS,
    DB_TYPE_INTERVAL_YM as DB_TYPE_INTERVAL_YM,
    DB_TYPE_JSON as DB_TYPE_JSON,
    DB_TYPE_LONG as DB_TYPE_LONG,
    DB_TYPE_LONG_NVARCHAR as DB_TYPE_LONG_NVARCHAR,
    DB_TYPE_LONG_RAW as DB_TYPE_LONG_RAW,
    DB_TYPE_NCHAR as DB_TYPE_NCHAR,
    DB_TYPE_NCLOB as DB_TYPE_NCLOB,
    DB_TYPE_NUMBER as DB_TYPE_NUMBER,
    DB_TYPE_NVARCHAR as DB_TYPE_NVARCHAR,
    DB_TYPE_OBJECT as DB_TYPE_OBJECT,
    DB_TYPE_RAW as DB_TYPE_RAW,
    DB_TYPE_ROWID as DB_TYPE_ROWID,
    DB_TYPE_TIMESTAMP as DB_TYPE_TIMESTAMP,
    DB_TYPE_TIMESTAMP_LTZ as DB_TYPE_TIMESTAMP_LTZ,
    DB_TYPE_TIMESTAMP_TZ as DB_TYPE_TIMESTAMP_TZ,
    DB_TYPE_UNKNOWN as DB_TYPE_UNKNOWN,
    DB_TYPE_UROWID as DB_TYPE_UROWID,
    DB_TYPE_VARCHAR as DB_TYPE_VARCHAR,
    DB_TYPE_VECTOR as DB_TYPE_VECTOR,
    DB_TYPE_XMLTYPE as DB_TYPE_XMLTYPE,
    # API types
    BINARY as BINARY,
    DATETIME as DATETIME,
    NUMBER as NUMBER,
    ROWID as ROWID,
    STRING as STRING,
)

from .enums import (
    # authentication modes
    AuthMode as AuthMode,
    AUTH_MODE_DEFAULT as AUTH_MODE_DEFAULT,
    AUTH_MODE_PRELIM as AUTH_MODE_PRELIM,
    AUTH_MODE_SYSASM as AUTH_MODE_SYSASM,
    AUTH_MODE_SYSBKP as AUTH_MODE_SYSBKP,
    AUTH_MODE_SYSDBA as AUTH_MODE_SYSDBA,
    AUTH_MODE_SYSDGD as AUTH_MODE_SYSDGD,
    AUTH_MODE_SYSKMT as AUTH_MODE_SYSKMT,
    AUTH_MODE_SYSOPER as AUTH_MODE_SYSOPER,
    AUTH_MODE_SYSRAC as AUTH_MODE_SYSRAC,
    # pipeline operation types
    PipelineOpType as PipelineOpType,
    PIPELINE_OP_TYPE_CALL_FUNC as PIPELINE_OP_TYPE_CALL_FUNC,
    PIPELINE_OP_TYPE_CALL_PROC as PIPELINE_OP_TYPE_CALL_PROC,
    PIPELINE_OP_TYPE_COMMIT as PIPELINE_OP_TYPE_COMMIT,
    PIPELINE_OP_TYPE_EXECUTE as PIPELINE_OP_TYPE_EXECUTE,
    PIPELINE_OP_TYPE_EXECUTE_MANY as PIPELINE_OP_TYPE_EXECUTE_MANY,
    PIPELINE_OP_TYPE_FETCH_ALL as PIPELINE_OP_TYPE_FETCH_ALL,
    PIPELINE_OP_TYPE_FETCH_MANY as PIPELINE_OP_TYPE_FETCH_MANY,
    PIPELINE_OP_TYPE_FETCH_ONE as PIPELINE_OP_TYPE_FETCH_ONE,
    # pool "get" modes
    PoolGetMode as PoolGetMode,
    POOL_GETMODE_WAIT as POOL_GETMODE_WAIT,
    POOL_GETMODE_NOWAIT as POOL_GETMODE_NOWAIT,
    POOL_GETMODE_FORCEGET as POOL_GETMODE_FORCEGET,
    POOL_GETMODE_TIMEDWAIT as POOL_GETMODE_TIMEDWAIT,
    # purity values
    Purity as Purity,
    PURITY_DEFAULT as PURITY_DEFAULT,
    PURITY_NEW as PURITY_NEW,
    PURITY_SELF as PURITY_SELF,
    # vector formats
    VectorFormat as VectorFormat,
    VECTOR_FORMAT_BINARY as VECTOR_FORMAT_BINARY,
    VECTOR_FORMAT_FLOAT32 as VECTOR_FORMAT_FLOAT32,
    VECTOR_FORMAT_FLOAT64 as VECTOR_FORMAT_FLOAT64,
    VECTOR_FORMAT_INT8 as VECTOR_FORMAT_INT8,
)

from .version import __version__ as __version__

from .constants import (
    # mandated DB API constants
    apilevel as apilevel,
    threadsafety as threadsafety,
    paramstyle as paramstyle,
    # AQ delivery modes
    MSG_BUFFERED as MSG_BUFFERED,
    MSG_PERSISTENT as MSG_PERSISTENT,
    MSG_PERSISTENT_OR_BUFFERED as MSG_PERSISTENT_OR_BUFFERED,
    # AQ dequeue modes
    DEQ_BROWSE as DEQ_BROWSE,
    DEQ_LOCKED as DEQ_LOCKED,
    DEQ_REMOVE as DEQ_REMOVE,
    DEQ_REMOVE_NODATA as DEQ_REMOVE_NODATA,
    # AQ dequeue navigation modes
    DEQ_FIRST_MSG as DEQ_FIRST_MSG,
    DEQ_NEXT_MSG as DEQ_NEXT_MSG,
    DEQ_NEXT_TRANSACTION as DEQ_NEXT_TRANSACTION,
    # AQ dequeue visibility modes
    DEQ_IMMEDIATE as DEQ_IMMEDIATE,
    DEQ_ON_COMMIT as DEQ_ON_COMMIT,
    # AQ dequeue wait modes
    DEQ_NO_WAIT as DEQ_NO_WAIT,
    DEQ_WAIT_FOREVER as DEQ_WAIT_FOREVER,
    # AQ enqueue visibility modes
    ENQ_IMMEDIATE as ENQ_IMMEDIATE,
    ENQ_ON_COMMIT as ENQ_ON_COMMIT,
    # AQ message states
    MSG_EXPIRED as MSG_EXPIRED,
    MSG_PROCESSED as MSG_PROCESSED,
    MSG_READY as MSG_READY,
    MSG_WAITING as MSG_WAITING,
    # AQ other constants
    MSG_NO_DELAY as MSG_NO_DELAY,
    MSG_NO_EXPIRATION as MSG_NO_EXPIRATION,
    # shutdown modes
    DBSHUTDOWN_ABORT as DBSHUTDOWN_ABORT,
    DBSHUTDOWN_FINAL as DBSHUTDOWN_FINAL,
    DBSHUTDOWN_IMMEDIATE as DBSHUTDOWN_IMMEDIATE,
    DBSHUTDOWN_TRANSACTIONAL as DBSHUTDOWN_TRANSACTIONAL,
    DBSHUTDOWN_TRANSACTIONAL_LOCAL as DBSHUTDOWN_TRANSACTIONAL_LOCAL,
    # subscription grouping classes
    SUBSCR_GROUPING_CLASS_NONE as SUBSCR_GROUPING_CLASS_NONE,
    SUBSCR_GROUPING_CLASS_TIME as SUBSCR_GROUPING_CLASS_TIME,
    # subscription grouping types
    SUBSCR_GROUPING_TYPE_SUMMARY as SUBSCR_GROUPING_TYPE_SUMMARY,
    SUBSCR_GROUPING_TYPE_LAST as SUBSCR_GROUPING_TYPE_LAST,
    # subscription namespaces
    SUBSCR_NAMESPACE_AQ as SUBSCR_NAMESPACE_AQ,
    SUBSCR_NAMESPACE_DBCHANGE as SUBSCR_NAMESPACE_DBCHANGE,
    # subscription protocols
    SUBSCR_PROTO_HTTP as SUBSCR_PROTO_HTTP,
    SUBSCR_PROTO_MAIL as SUBSCR_PROTO_MAIL,
    SUBSCR_PROTO_CALLBACK as SUBSCR_PROTO_CALLBACK,
    SUBSCR_PROTO_SERVER as SUBSCR_PROTO_SERVER,
    # subscription quality of service
    SUBSCR_QOS_BEST_EFFORT as SUBSCR_QOS_BEST_EFFORT,
    SUBSCR_QOS_DEFAULT as SUBSCR_QOS_DEFAULT,
    SUBSCR_QOS_DEREG_NFY as SUBSCR_QOS_DEREG_NFY,
    SUBSCR_QOS_QUERY as SUBSCR_QOS_QUERY,
    SUBSCR_QOS_RELIABLE as SUBSCR_QOS_RELIABLE,
    SUBSCR_QOS_ROWIDS as SUBSCR_QOS_ROWIDS,
    # event types
    EVENT_AQ as EVENT_AQ,
    EVENT_DEREG as EVENT_DEREG,
    EVENT_NONE as EVENT_NONE,
    EVENT_OBJCHANGE as EVENT_OBJCHANGE,
    EVENT_QUERYCHANGE as EVENT_QUERYCHANGE,
    EVENT_SHUTDOWN as EVENT_SHUTDOWN,
    EVENT_SHUTDOWN_ANY as EVENT_SHUTDOWN_ANY,
    EVENT_STARTUP as EVENT_STARTUP,
    # operation codes
    OPCODE_ALLOPS as OPCODE_ALLOPS,
    OPCODE_ALLROWS as OPCODE_ALLROWS,
    OPCODE_ALTER as OPCODE_ALTER,
    OPCODE_DELETE as OPCODE_DELETE,
    OPCODE_DROP as OPCODE_DROP,
    OPCODE_INSERT as OPCODE_INSERT,
    OPCODE_UPDATE as OPCODE_UPDATE,
    # flags for tpc_begin()
    TPC_BEGIN_JOIN as TPC_BEGIN_JOIN,
    TPC_BEGIN_NEW as TPC_BEGIN_NEW,
    TPC_BEGIN_PROMOTE as TPC_BEGIN_PROMOTE,
    TPC_BEGIN_RESUME as TPC_BEGIN_RESUME,
    # flags for tpc_end()
    TPC_END_NORMAL as TPC_END_NORMAL,
    TPC_END_SUSPEND as TPC_END_SUSPEND,
)

from .exceptions import (
    Warning as Warning,
    Error as Error,
    DatabaseError as DatabaseError,
    DataError as DataError,
    IntegrityError as IntegrityError,
    InterfaceError as InterfaceError,
    InternalError as InternalError,
    NotSupportedError as NotSupportedError,
    OperationalError as OperationalError,
    ProgrammingError as ProgrammingError,
)

from .errors import _Error as _Error

from .defaults import defaults as defaults

from .pipeline import (
    Pipeline as Pipeline,
    PipelineOp as PipelineOp,
    PipelineOpResult as PipelineOpResult,
    create_pipeline as create_pipeline,
)

from .connection import (
    AsyncConnection as AsyncConnection,
    connect as connect,
    connect_async as connect_async,
    Connection as Connection,
)

from .cursor import (
    AsyncCursor as AsyncCursor,
    Cursor as Cursor,
)

from .pool import (
    AsyncConnectionPool as AsyncConnectionPool,
    ConnectionPool as ConnectionPool,
    create_pool as create_pool,
    create_pool_async as create_pool_async,
)

from .subscr import (
    Message as Message,
    MessageQuery as MessageQuery,
    MessageRow as MessageRow,
    MessageTable as MessageTable,
)

from .connect_params import ConnectParams as ConnectParams

from .pool_params import PoolParams as PoolParams

from .lob import (
    LOB as LOB,
    AsyncLOB as AsyncLOB,
)

from .dbobject import DbObject as DbObject, DbObjectType as DbObjectType

from .fetch_info import FetchInfo as FetchInfo

from .var import Var as Var

from .dsn import makedsn as makedsn

from .driver_mode import is_thin_mode as is_thin_mode

from .utils import (
    enable_thin_mode as enable_thin_mode,
    register_protocol as register_protocol,
)

from .thick_impl import (
    clientversion as clientversion,
    init_oracle_client as init_oracle_client,
)

from .constructors import (
    Binary as Binary,
    Date as Date,
    DateFromTicks as DateFromTicks,
    Time as Time,
    TimeFromTicks as TimeFromTicks,
    Timestamp as Timestamp,
    TimestampFromTicks as TimestampFromTicks,
)

from .future import (
    future as __future__,  # noqa: F401
)


IntervalYM = collections.namedtuple("IntervalYM", ["years", "months"])


class JsonId(bytes):
    pass


package = sys.modules[__name__]
base_impl.init_base_impl(package)
thick_impl.init_thick_impl(package)
thin_impl.init_thin_impl(package)
del package

# remove unnecessary symbols
del sys, warnings
del aq, base_impl, connect_params, connection, constants, constructors  # noqa
del cursor, dbobject, driver_mode, dsn, errors, exceptions, fetch_info  # noqa
del future, lob, pool, pool_params, soda, subscr, thick_impl, thin_impl  # noqa
del utils, var  # noqa

# general aliases (for backwards compatibility)
ObjectType = DbObjectType
Object = DbObject
SessionPool = ConnectionPool
version = __version__

# aliases for database types (for backwards compatibility)
BFILE = DB_TYPE_BFILE
BLOB = DB_TYPE_BLOB
BOOLEAN = DB_TYPE_BOOLEAN
CLOB = DB_TYPE_CLOB
CURSOR = DB_TYPE_CURSOR
FIXED_CHAR = DB_TYPE_CHAR
FIXED_NCHAR = DB_TYPE_NCHAR
INTERVAL = DB_TYPE_INTERVAL_DS
LONG_BINARY = DB_TYPE_LONG_RAW
LONG_STRING = DB_TYPE_LONG
NATIVE_INT = DB_TYPE_BINARY_INTEGER
NATIVE_FLOAT = DB_TYPE_BINARY_DOUBLE
NCHAR = DB_TYPE_NVARCHAR
OBJECT = DB_TYPE_OBJECT
NCLOB = DB_TYPE_NCLOB
TIMESTAMP = DB_TYPE_TIMESTAMP

# aliases for authhentication modes (for backwards compatibility)
DEFAULT_AUTH = AUTH_MODE_DEFAULT
SYSASM = AUTH_MODE_SYSASM
SYSBKP = AUTH_MODE_SYSBKP
SYSDBA = AUTH_MODE_SYSDBA
SYSDGD = AUTH_MODE_SYSDGD
SYSKMT = AUTH_MODE_SYSKMT
SYSOPER = AUTH_MODE_SYSOPER
SYSRAC = AUTH_MODE_SYSRAC
PRELIM_AUTH = AUTH_MODE_PRELIM

# aliases for pool "get" modes (for backwards compatibility)
SPOOL_ATTRVAL_WAIT = POOL_GETMODE_WAIT
SPOOL_ATTRVAL_NOWAIT = POOL_GETMODE_NOWAIT
SPOOL_ATTRVAL_FORCEGET = POOL_GETMODE_FORCEGET
SPOOL_ATTRVAL_TIMEDWAIT = POOL_GETMODE_TIMEDWAIT

# aliases for purity (for backwards compatibility)
ATTR_PURITY_DEFAULT = PURITY_DEFAULT
ATTR_PURITY_NEW = PURITY_NEW
ATTR_PURITY_SELF = PURITY_SELF

# aliases for subscription protocols (for backwards compatibility)
SUBSCR_PROTO_OCI = SUBSCR_PROTO_CALLBACK
