#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

# distutils: language = c++
# cython: language_level=3

from cpython.ref cimport PyObject
from cython.operator cimport dereference
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from pyarrow.includes.common cimport CResult, CStatus, GetResultValue
from pyarrow.includes.libarrow cimport (
    CBuffer,
    CInputStream,
    CIpcReadOptions,
    CRecordBatch,
    CRecordBatchReader,
    CRecordBatchStreamReader,
    FileInterface,
    FileMode,
    Readable,
    Seekable,
)

# todo: update when pyarrow >= 8
from pyarrow.includes.libarrow cimport PyReadableFile

from .constants import IterUnit
from .errorcode import (
    ER_FAILED_TO_CONVERT_ROW_TO_PYTHON_TYPE,
    ER_FAILED_TO_READ_ARROW_STREAM,
)
from .errors import Error, InterfaceError, OperationalError
from .snow_logging import getSnowLogger

snow_logger = getSnowLogger(__name__)


cdef extern from "cpp/ArrowIterator/CArrowIterator.hpp" namespace "sf":
    cdef cppclass ReturnVal:
        PyObject * successObj;

        PyObject * exception;

    cdef cppclass CArrowIterator:
        shared_ptr[ReturnVal] next() except +;


cdef extern from "cpp/ArrowIterator/CArrowChunkIterator.hpp" namespace "sf":
    cdef cppclass CArrowChunkIterator(CArrowIterator):
        CArrowChunkIterator(
                PyObject* context,
                vector[shared_ptr[CRecordBatch]]* batches,
                PyObject* use_numpy,
        ) except +

    cdef cppclass DictCArrowChunkIterator(CArrowChunkIterator):
        DictCArrowChunkIterator(
                PyObject* context,
                vector[shared_ptr[CRecordBatch]]* batches,
                PyObject* use_numpy
        ) except +


cdef extern from "cpp/ArrowIterator/CArrowTableIterator.hpp" namespace "sf":
    cdef cppclass CArrowTableIterator(CArrowIterator):
        CArrowTableIterator(
            PyObject* context,
            vector[shared_ptr[CRecordBatch]]* batches,
            bint number_to_decimal,
        ) except +


cdef class EmptyPyArrowIterator:

    def __iter__(self):
        return self

    def __next__(self):
       raise StopIteration

    def init(self, str iter_unit, bint number_to_decimal):
        pass


cdef class PyArrowIterator(EmptyPyArrowIterator):
    cdef object context
    cdef CArrowIterator* cIterator
    cdef str unit
    cdef shared_ptr[ReturnVal] cret
    cdef vector[shared_ptr[CRecordBatch]] batches
    cdef object use_dict_result
    cdef object cursor

    # this is the flag indicating whether fetch data as numpy datatypes or not. The flag
    # is passed from the constructor of SnowflakeConnection class. Note, only FIXED, REAL
    # and TIMESTAMP_NTZ will be converted into numpy data types, all other sql types will
    # still be converted into native python types.
    # https://docs.snowflake.com/en/user-guide/sqlalchemy.html#numpy-data-type-support
    cdef object use_numpy
    cdef object number_to_decimal

    def __cinit__(
            self,
            object cursor,
            object py_inputstream,
            object arrow_context,
            object use_dict_result,
            object numpy,
            object number_to_decimal,
    ):
        cdef shared_ptr[CInputStream] input_stream
        cdef shared_ptr[CRecordBatch] record_batch
        cdef CStatus ret
        input_stream.reset(new PyReadableFile(py_inputstream))
        cdef CResult[shared_ptr[CRecordBatchReader]] readerRet = CRecordBatchStreamReader.Open(
            input_stream,
            CIpcReadOptions.Defaults()
            )
        if not readerRet.ok():
            Error.errorhandler_wrapper(
                cursor.connection if cursor is not None else None,
                cursor,
                OperationalError,
                {
                    'msg': f'Failed to open arrow stream: {readerRet.status().message()}',
                    'errno': ER_FAILED_TO_READ_ARROW_STREAM
                })

        cdef shared_ptr[CRecordBatchReader] reader = dereference(readerRet)

        while True:
            ret = reader.get().ReadNext(&record_batch)
            if not ret.ok():
                Error.errorhandler_wrapper(
                    cursor.connection if cursor is not None else None,
                    cursor,
                    OperationalError,
                    {
                        'msg': f'Failed to read next arrow batch: {ret.message()}',
                        'errno': ER_FAILED_TO_READ_ARROW_STREAM
                    }
                )

            if record_batch.get() is NULL:
                break

            self.batches.push_back(record_batch)

        snow_logger.debug(msg=f"Batches read: {self.batches.size()}", path_name=__file__, func_name="__cinit__")

        self.context = arrow_context
        self.cIterator = NULL
        self.unit = ''
        self.use_dict_result = use_dict_result
        self.cursor = cursor
        self.use_numpy = numpy
        self.number_to_decimal = number_to_decimal

    def __dealloc__(self):
        del self.cIterator

    def __iter__(self):
        return self

    def __next__(self):
        if self.cIterator is NULL:
            self.init_row_unit()
        self.cret = self.cIterator.next()

        if not self.cret.get().successObj:
            Error.errorhandler_wrapper(
                self.cursor.connection if self.cursor is not None else None,
                self.cursor,
                InterfaceError,
                {
                    'msg': f'Failed to convert current row, cause: {<object>self.cret.get().exception}',
                    'errno': ER_FAILED_TO_CONVERT_ROW_TO_PYTHON_TYPE
                }
            )
            # it looks like this line can help us get into python and detect the global variable immediately
            # however, this log will not show up for unclear reason
        ret = <object>self.cret.get().successObj

        if ret is None:
            raise StopIteration
        else:
            return ret

    def init(self, str iter_unit):
        if iter_unit == IterUnit.ROW_UNIT.value:
            self.init_row_unit()
        elif iter_unit == IterUnit.TABLE_UNIT.value:
            self.init_table_unit()
        self.unit = iter_unit

    def init_row_unit(self) -> None:
        self.cIterator = new CArrowChunkIterator(
            <PyObject *> self.context,
            &self.batches,
            <PyObject *> self.use_numpy
        ) \
            if not self.use_dict_result \
            else new DictCArrowChunkIterator(
            <PyObject *> self.context,
            &self.batches,
            <PyObject *> self.use_numpy
            )

    def init_table_unit(self) -> None:
        self.cIterator = new CArrowTableIterator(
            <PyObject *> self.context,
            &self.batches,
            self.number_to_decimal,
        )
