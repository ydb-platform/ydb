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
# json.pyx
#
# Cython file defining methods and classes used for processing JSON data
# (embedded in thick_impl.pyx).
#------------------------------------------------------------------------------

cdef class JsonBuffer:
    cdef:
        dpiJsonNode _top_node
        dpiDataBuffer _top_node_buf
        list _buffers

    def __dealloc__(self):
        _free_node(&self._top_node)

    cdef int _add_buf(self, object value, char **ptr,
                      uint32_t *length) except -1:
        cdef StringBuffer buf = StringBuffer()
        buf.set_value(value)
        if self._buffers is None:
            self._buffers = []
        self._buffers.append(buf)
        ptr[0] = buf.ptr
        length[0] = buf.length

    cdef int _populate_array_node(self, dpiJsonNode *node,
                                  list value) except -1:
        cdef:
            dpiJsonArray *array
            object child_value
            uint32_t i
        node.oracleTypeNum = DPI_ORACLE_TYPE_JSON_ARRAY
        node.nativeTypeNum = DPI_NATIVE_TYPE_JSON_ARRAY
        array = &node.value.asJsonArray
        array.numElements = <uint32_t> len(value)
        array.elements = <dpiJsonNode*> _calloc(array.numElements,
                                                sizeof(dpiJsonNode))
        array.elementValues = <dpiDataBuffer*> _calloc(array.numElements,
                                                       sizeof(dpiDataBuffer))
        for i, child_value in enumerate(value):
            array.elements[i].value = &array.elementValues[i]
            self._populate_node(&array.elements[i], child_value)

    cdef int _populate_obj_node(self, dpiJsonNode *node, dict value) except -1:
        cdef:
            object child_key, child_value
            dpiJsonObject *obj
            uint32_t i
        node.oracleTypeNum = DPI_ORACLE_TYPE_JSON_OBJECT
        node.nativeTypeNum = DPI_NATIVE_TYPE_JSON_OBJECT
        obj = &node.value.asJsonObject
        obj.numFields = <uint32_t> len(value)
        obj.fieldNames = <char**> _calloc(obj.numFields, sizeof(char*))
        obj.fieldNameLengths = <uint32_t*> _calloc(obj.numFields,
                                                   sizeof(uint32_t))
        obj.fields = <dpiJsonNode*> _calloc(obj.numFields, sizeof(dpiJsonNode))
        obj.fieldValues = <dpiDataBuffer*> _calloc(obj.numFields,
                                                   sizeof(dpiDataBuffer))
        i = 0
        for child_key, child_value in value.items():
            obj.fields[i].value = &obj.fieldValues[i]
            self._add_buf(child_key, &obj.fieldNames[i],
                          &obj.fieldNameLengths[i])
            self._populate_node(&obj.fields[i], child_value)
            i += 1

    cdef int _populate_node(self, dpiJsonNode *node, object value) except -1:
        cdef:
            VectorEncoder vector_encoder
            dpiTimestamp *timestamp
            dpiIntervalDS *interval
            int seconds
        if value is None:
            node.oracleTypeNum = DPI_ORACLE_TYPE_NONE
            node.nativeTypeNum = DPI_NATIVE_TYPE_NULL
        elif isinstance(value, list):
            self._populate_array_node(node, value)
        elif isinstance(value, dict):
            self._populate_obj_node(node, value)
        elif isinstance(value, str):
            node.oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR
            node.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            self._add_buf(value, &node.value.asBytes.ptr,
                          &node.value.asBytes.length)
        elif isinstance(value, bytes):
            node.oracleTypeNum = DPI_ORACLE_TYPE_JSON_ID \
                    if isinstance(value, PY_TYPE_JSON_ID) \
                    else DPI_ORACLE_TYPE_RAW
            node.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            self._add_buf(value, &node.value.asBytes.ptr,
                          &node.value.asBytes.length)
        elif isinstance(value, bool):
            node.oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN
            node.nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN
            node.value.asBoolean = <bint> value
        elif isinstance(value, (int, PY_TYPE_DECIMAL)):
            node.oracleTypeNum = DPI_ORACLE_TYPE_NUMBER
            node.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            self._add_buf(str(value), &node.value.asBytes.ptr,
                          &node.value.asBytes.length)
        elif isinstance(value, float):
            node.oracleTypeNum = DPI_ORACLE_TYPE_NUMBER
            node.nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE
            node.value.asDouble = <double> value
        elif isinstance(value, PY_TYPE_DATETIME):
            node.oracleTypeNum = DPI_ORACLE_TYPE_TIMESTAMP
            node.nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP
            memset(&node.value.asTimestamp, 0, sizeof(node.value.asTimestamp))
            timestamp = &node.value.asTimestamp
            timestamp.year = cydatetime.datetime_year(value)
            timestamp.month = cydatetime.datetime_month(value)
            timestamp.day = cydatetime.datetime_day(value)
            timestamp.hour = cydatetime.datetime_hour(value)
            timestamp.minute = cydatetime.datetime_minute(value)
            timestamp.second = cydatetime.datetime_second(value)
            timestamp.fsecond = cydatetime.datetime_microsecond(value) * 1000
        elif isinstance(value, PY_TYPE_DATE):
            node.oracleTypeNum = DPI_ORACLE_TYPE_DATE
            node.nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP
            memset(&node.value.asTimestamp, 0, sizeof(node.value.asTimestamp))
            timestamp = &node.value.asTimestamp
            timestamp = &node.value.asTimestamp
            timestamp.year = cydatetime.date_year(value)
            timestamp.month = cydatetime.date_month(value)
            timestamp.day = cydatetime.date_day(value)
        elif isinstance(value, datetime.timedelta):
            node.oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_DS
            node.nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_DS
            interval = &node.value.asIntervalDS
            seconds = cydatetime.timedelta_seconds(value)
            interval.days = cydatetime.timedelta_days(value)
            interval.hours = seconds // 3600
            seconds = seconds % 3600
            interval.minutes = seconds // 60
            interval.seconds = seconds % 60
            interval.fseconds = cydatetime.timedelta_microseconds(value) * 1000
        elif isinstance(value, array.array):
            node.oracleTypeNum = DPI_ORACLE_TYPE_VECTOR
            node.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            vector_encoder = VectorEncoder.__new__(VectorEncoder)
            vector_encoder.encode(value)
            self._add_buf(vector_encoder._data[:vector_encoder._pos],
                          &node.value.asBytes.ptr, &node.value.asBytes.length)
        else:
            errors._raise_err(errors.ERR_PYTHON_TYPE_NOT_SUPPORTED,
                              typ=type(value))

    cdef int from_object(self, object value) except -1:
        self._top_node.value = &self._top_node_buf
        self._populate_node(&self._top_node, value)


cdef void* _calloc(size_t num_elements, size_t size) except NULL:
    cdef:
        size_t num_bytes = num_elements * size
        void *ptr
    ptr = cpython.PyMem_Malloc(num_bytes)
    memset(ptr, 0, num_bytes)
    return ptr

cdef void _free_node(dpiJsonNode *node):
    cdef:
        dpiJsonArray *array
        dpiJsonObject *obj
        uint32_t i
    if node.nativeTypeNum == DPI_NATIVE_TYPE_JSON_ARRAY:
        array = &node.value.asJsonArray
        if array.elements != NULL:
            for i in range(array.numElements):
                if array.elements[i].value != NULL:
                    _free_node(&array.elements[i])
            cpython.PyMem_Free(array.elements)
            array.elements = NULL
        if array.elementValues != NULL:
            cpython.PyMem_Free(array.elementValues)
            array.elementValues = NULL
    elif node.nativeTypeNum == DPI_NATIVE_TYPE_JSON_OBJECT:
        obj = &node.value.asJsonObject
        if obj.fields != NULL:
            for i in range(obj.numFields):
                if obj.fields[i].value != NULL:
                    _free_node(&obj.fields[i])
            cpython.PyMem_Free(obj.fields)
            obj.fields = NULL
        if obj.fieldNames != NULL:
            cpython.PyMem_Free(obj.fieldNames)
            obj.fieldNames = NULL
        if obj.fieldNameLengths != NULL:
            cpython.PyMem_Free(obj.fieldNameLengths)
            obj.fieldNameLengths = NULL
        if obj.fieldValues != NULL:
            cpython.PyMem_Free(obj.fieldValues)
            obj.fieldValues = NULL
