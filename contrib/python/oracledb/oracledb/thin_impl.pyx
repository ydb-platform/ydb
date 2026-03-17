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
# thin_impl.pyx
#
# Cython file for communicating with the server directly without the use of
# any Oracle Client library.
#------------------------------------------------------------------------------

# cython: language_level=3

cimport cython
cimport cpython
cimport cpython.datetime as cydatetime
cimport cpython.ref

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libc.string cimport memcpy, memset
from cpython cimport array

import array
import asyncio
import base64
import collections
import datetime
import decimal
import getpass
import hashlib
import inspect
import json
import os
import socket
import re
import secrets
import select
import ssl
import subprocess
import sys
import threading
import time
import uuid

try:
    import certifi
except ImportError:
    certifi = None
macos_certs = None

cydatetime.import_datetime()

from .base_impl cimport (
    Address,
    AddressList,
    AUTH_MODE_DEFAULT,
    AUTH_MODE_PRELIM,
    AUTH_MODE_SYSASM,
    AUTH_MODE_SYSBKP,
    AUTH_MODE_SYSDBA,
    AUTH_MODE_SYSDGD,
    AUTH_MODE_SYSKMT,
    AUTH_MODE_SYSOPER,
    AUTH_MODE_SYSRAC,
    BaseConnImpl,
    BaseCursorImpl,
    BaseDbObjectAttrImpl,
    BaseDbObjectImpl,
    BaseDbObjectTypeImpl,
    BaseLobImpl,
    BaseParser,
    BasePoolImpl,
    BaseVarImpl,
    PipelineOpImpl,
    PipelineOpResultImpl,
    PIPELINE_OP_TYPE_CALL_FUNC,
    PIPELINE_OP_TYPE_CALL_PROC,
    PIPELINE_OP_TYPE_COMMIT,
    PIPELINE_OP_TYPE_EXECUTE,
    PIPELINE_OP_TYPE_EXECUTE_MANY,
    PIPELINE_OP_TYPE_FETCH_ALL,
    PIPELINE_OP_TYPE_FETCH_MANY,
    PIPELINE_OP_TYPE_FETCH_ONE,
    BindVar,
    Buffer,
    BYTE_ORDER_LSB,
    BYTE_ORDER_MSB,
    ConnectParamsImpl,
    CS_FORM_IMPLICIT,
    CS_FORM_NCHAR,
    DbType,
    Description,
    DescriptionList,
    DRIVER_NAME,
    DRIVER_VERSION,
    ENCODING_UTF8,
    ENCODING_UTF16,
    FetchInfoImpl,
    get_preferred_num_type,
    GrowableBuffer,
    NUM_TYPE_FLOAT,
    NUM_TYPE_INT,
    NUM_TYPE_DECIMAL,
    NUM_TYPE_STR,
    OsonDecoder,
    OsonEncoder,
    POOL_GETMODE_FORCEGET,
    POOL_GETMODE_NOWAIT,
    POOL_GETMODE_TIMEDWAIT,
    POOL_GETMODE_WAIT,
    PoolParamsImpl,
    PURITY_DEFAULT,
    PURITY_NEW,
    PURITY_SELF,
    PY_TYPE_ASYNC_LOB,
    PY_TYPE_DATE,
    PY_TYPE_DATETIME,
    PY_TYPE_DB_OBJECT,
    PY_TYPE_DECIMAL,
    PY_TYPE_INTERVAL_YM,
    PY_TYPE_LOB,
    PY_TYPE_TIMEDELTA,
    TNS_LONG_LENGTH_INDICATOR,
    TNS_NULL_LENGTH_INDICATOR,
    unpack_uint16,
    unpack_uint32,
    VectorDecoder,
    VectorEncoder,
    pack_uint16,
)

from .base_impl import (
    DB_TYPE_BLOB,
    DB_TYPE_CLOB,
    DB_TYPE_NCLOB,
    DB_TYPE_BINARY_INTEGER,
    DB_TYPE_CURSOR,
    DB_TYPE_OBJECT,
    DB_TYPE_XMLTYPE,
)

ctypedef unsigned char char_type

# flag whether the cryptography package exists
cdef bint HAS_CRYPTOGRAPHY = True

include "impl/thin/constants.pxi"
include "impl/thin/utils.pyx"
include "impl/thin/crypto.pyx"
include "impl/thin/capabilities.pyx"
include "impl/thin/transport.pyx"
include "impl/thin/packet.pyx"
include "impl/thin/data_types.pyx"
include "impl/thin/messages.pyx"
include "impl/thin/protocol.pyx"
include "impl/thin/connection.pyx"
include "impl/thin/statement.pyx"
include "impl/thin/statement_cache.pyx"
include "impl/thin/cursor.pyx"
include "impl/thin/var.pyx"
include "impl/thin/dbobject.pyx"
include "impl/thin/dbobject_cache.pyx"
include "impl/thin/lob.pyx"
include "impl/thin/pool.pyx"
include "impl/thin/conversions.pyx"
