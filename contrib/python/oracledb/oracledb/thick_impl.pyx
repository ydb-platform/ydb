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
# thick_impl.pyx
#
# Cython file for interfacing with ODPI-C.
#------------------------------------------------------------------------------

# cython: language_level=3

cimport cython
cimport cpython
cimport cpython.datetime as cydatetime
from cpython cimport array

import array
import datetime
import decimal
import locale
import sys

cydatetime.import_datetime()

from .base_impl cimport (
    BaseConnImpl,
    BaseCursorImpl,
    BaseDbObjectImpl,
    BaseDbObjectAttrImpl,
    BaseDbObjectTypeImpl,
    BaseDeqOptionsImpl,
    BaseEnqOptionsImpl,
    BaseLobImpl,
    BaseMsgPropsImpl,
    BasePoolImpl,
    BaseQueueImpl,
    BaseSodaCollImpl,
    BaseSodaDbImpl,
    BaseSodaDocImpl,
    BaseSodaDocCursorImpl,
    BaseSubscrImpl,
    BaseVarImpl,
    BindVar,
    C_DEFAULTS,
    ConnectParamsImpl,
    DbType,
    DB_TYPE_NUM_CURSOR,
    DRIVER_NAME,
    DRIVER_VERSION,
    DRIVER_INSTALLATION_URL,
    ENCODING_UTF8,
    PURITY_DEFAULT,
    PY_TYPE_DATE,
    PY_TYPE_DATETIME,
    PY_TYPE_DB_OBJECT,
    PY_TYPE_DECIMAL,
    PY_TYPE_JSON_ID,
    PY_TYPE_INTERVAL_YM,
    PY_TYPE_LOB,
    PY_TYPE_MESSAGE,
    PY_TYPE_MESSAGE_QUERY,
    PY_TYPE_MESSAGE_ROW,
    PY_TYPE_MESSAGE_TABLE,
    PY_TYPE_TIMEDELTA,
    FetchInfoImpl,
    PoolParamsImpl,
    get_preferred_num_type,
    NUM_TYPE_FLOAT,
    NUM_TYPE_INT,
    NUM_TYPE_DECIMAL,
    VectorDecoder,
    VectorEncoder,
)
from libc.string cimport memchr, memcpy, memset

include "impl/thick/odpi.pxd"

cdef struct DriverInfo:
    dpiContext *context
    dpiVersionInfo client_version_info
    bint soda_use_json_desc

cdef DriverInfo driver_info = \
        DriverInfo(NULL, dpiVersionInfo(0, 0, 0, 0, 0, 0), True)

driver_context_params = None

include "impl/thick/buffer.pyx"
include "impl/thick/connection.pyx"
include "impl/thick/pool.pyx"
include "impl/thick/cursor.pyx"
include "impl/thick/lob.pyx"
include "impl/thick/json.pyx"
include "impl/thick/var.pyx"
include "impl/thick/dbobject.pyx"
include "impl/thick/soda.pyx"
include "impl/thick/queue.pyx"
include "impl/thick/subscr.pyx"
include "impl/thick/utils.pyx"
