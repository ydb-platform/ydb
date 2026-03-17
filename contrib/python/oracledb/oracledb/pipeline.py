# -----------------------------------------------------------------------------
# Copyright (c) 2024, Oracle and/or its affiliates.
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
# pipeline.py
#
# Contains the Pipeline class used for executing multiple operations.
# -----------------------------------------------------------------------------

from typing import Any, Callable, Union

from . import __name__ as MODULE_NAME
from . import utils
from .defaults import defaults
from .fetch_info import FetchInfo
from .base_impl import PipelineImpl, PipelineOpImpl, PipelineOpResultImpl
from .enums import PipelineOpType
from .errors import _Error


class PipelineOp:
    __module__ = MODULE_NAME

    def __repr__(self):
        typ = self.__class__
        cls_name = f"{typ.__module__}.{typ.__qualname__}"
        return f"<{cls_name} of type {self.op_type.name}>"

    def _create_result(self):
        """
        Internal method used for creating a result object that is returned when
        running a pipeline.
        """
        impl = PipelineOpResultImpl(self._impl)
        result = PipelineOpResult.__new__(PipelineOpResult)
        result._operation = self
        result._impl = impl
        return result

    @property
    def arraysize(self) -> int:
        """
        Returns the array size to use when fetching all of the rows in a query.
        For all other operations the value returned is 0.
        """
        return self._impl.arraysize

    @property
    def keyword_parameters(self) -> Any:
        """
        Returns the keyword parameters to the stored procedure or function
        being called by the operation, if applicable.
        """
        return self._impl.keyword_parameters

    @property
    def name(self) -> Union[str, None]:
        """
        Returns the name of the stored procedure or function being called by
        the operation, if applicable.
        """
        return self._impl.name

    @property
    def num_rows(self) -> int:
        """
        Returns the number of rows to fetch when performing a query of a
        specific number of rows. For all operations, the value returned is 0.
        """
        return self._impl.num_rows

    @property
    def op_type(self) -> PipelineOpType:
        """
        Returns the type of operation that is taking place.
        """
        return PipelineOpType(self._impl.op_type)

    @property
    def parameters(self) -> Any:
        """
        Returns the parameters to the stored procedure or function or the
        parameters bound to the statement being executed by the operation, if
        applicable.
        """
        return self._impl.parameters

    @property
    def return_type(self) -> Any:
        """
        Returns the return type of the stored function being called by the
        operation, if applicable.
        """
        return self._impl.return_type

    @property
    def rowfactory(self) -> Union[Callable, None]:
        """
        Returns the row factory callable function to be used in a query
        executed by the operation, if applicable.
        """
        return self._impl.rowfactory

    @property
    def statement(self) -> Union[str, None]:
        """
        Returns the statement being executed by the operation, if applicable.
        """
        return self._impl.statement


class PipelineOpResult:
    __module__ = MODULE_NAME

    def __repr__(self):
        typ = self.__class__
        cls_name = f"{typ.__module__}.{typ.__qualname__}"
        return (
            f"<{cls_name} for operation of type {self.operation.op_type.name}>"
        )

    @property
    def columns(self) -> Union[list, None]:
        """
        Returns a list of FetchInfo instances containing metadata about an
        executed query, or the value None, if no fetch operation took place.
        """
        if self._impl.fetch_info_impls is not None:
            return [
                FetchInfo._from_impl(i) for i in self._impl.fetch_info_impls
            ]

    @property
    def error(self) -> Union[_Error, None]:
        """
        Returns the error that occurred when running this operation, or the
        value None, if no error occurred.
        """
        return self._impl.error

    @property
    def operation(self) -> PipelineOp:
        """
        Returns the operation associated with the result.
        """
        return self._operation

    @property
    def return_value(self) -> Any:
        """
        Returns the return value of the called function, if a function was
        called for the operation.
        """
        return self._impl.return_value

    @property
    def rows(self) -> Union[list, None]:
        """
        Returns the rows that were fetched by the operation, if a query was
        executed.
        """
        return self._impl.rows

    @property
    def warning(self) -> Union[_Error, None]:
        """
        Returns the warning that was encountered when running this operation,
        or the value None, if no warning was encountered.
        """
        return self._impl.warning


class Pipeline:
    __module__ = MODULE_NAME

    def __repr__(self):
        typ = self.__class__
        cls_name = f"{typ.__module__}.{typ.__qualname__}"
        return f"<{cls_name} with {len(self._impl.operations)} operations>"

    def _add_op(self, op_impl):
        """
        Internal method for adding an PipelineOpImpl instance to the list of
        operations, creating an associated PipelineOp instance to correspond to
        it.
        """
        self._impl.operations.append(op_impl)
        op = PipelineOp.__new__(PipelineOp)
        op._impl = op_impl
        self._operations.append(op)
        return op

    def add_callfunc(
        self,
        name: str,
        return_type: Any,
        parameters: Union[list, tuple] = None,
        keyword_parameters: dict = None,
    ) -> PipelineOp:
        """
        Adds an operation that calls a stored function with the given
        parameters and return type. The PipelineOpResult object that is
        returned will have the "return_value" attribute populated with the
        return value of the function if the call completes successfully.
        """
        utils.verify_stored_proc_args(parameters, keyword_parameters)
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.CALL_FUNC,
            name=name,
            return_type=return_type,
            parameters=parameters,
            keyword_parameters=keyword_parameters,
        )
        return self._add_op(op_impl)

    def add_callproc(
        self,
        name: str,
        parameters: Union[list, tuple] = None,
        keyword_parameters: dict = None,
    ) -> PipelineOp:
        """
        Adds an operation that calls a stored procedure with the given
        parameters.
        """
        utils.verify_stored_proc_args(parameters, keyword_parameters)
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.CALL_PROC,
            name=name,
            parameters=parameters,
            keyword_parameters=keyword_parameters,
        )
        return self._add_op(op_impl)

    def add_commit(self) -> PipelineOp:
        """
        Adds an operation that performs a commit.
        """
        op_impl = PipelineOpImpl(op_type=PipelineOpType.COMMIT)
        return self._add_op(op_impl)

    def add_execute(
        self,
        statement: str,
        parameters: Union[list, tuple, dict] = None,
    ) -> PipelineOp:
        """
        Adds an operation that executes a statement with the given parameters.
        """
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.EXECUTE,
            statement=statement,
            parameters=parameters,
        )
        return self._add_op(op_impl)

    def add_executemany(
        self,
        statement: Union[str, None],
        parameters: Union[list, int],
    ) -> PipelineOp:
        """
        Adds an operation that executes a statement multiple times with the
        given list of parameters (or number of iterations).
        """
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.EXECUTE_MANY,
            statement=statement,
            parameters=parameters,
        )
        return self._add_op(op_impl)

    def add_fetchall(
        self,
        statement: str,
        parameters: Union[list, tuple, dict] = None,
        arraysize: int = None,
        rowfactory: Callable = None,
    ) -> PipelineOp:
        """
        Adds an operation that executes a query and returns up to the
        specified number of rows from the result set. The PipelineOpResult
        object that is returned will have the "return_value" attribute
        populated with the list of rows returned by the query.
        """
        if arraysize is None:
            arraysize = defaults.arraysize
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.FETCH_ALL,
            statement=statement,
            parameters=parameters,
            arraysize=arraysize,
            rowfactory=rowfactory,
        )
        return self._add_op(op_impl)

    def add_fetchmany(
        self,
        statement: str,
        parameters: Union[list, tuple, dict] = None,
        num_rows: int = None,
        rowfactory: Callable = None,
    ) -> PipelineOp:
        """
        Adds an operation that executes a query and returns up to the specified
        number of rows from the result set. The PipelineOpResult object that is
        returned will have the "return_value" attribute populated with the list
        of rows returned by the query.
        """
        if num_rows is None:
            num_rows = defaults.arraysize
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.FETCH_MANY,
            statement=statement,
            parameters=parameters,
            num_rows=num_rows,
            rowfactory=rowfactory,
        )
        return self._add_op(op_impl)

    def add_fetchone(
        self,
        statement: str,
        parameters: Union[list, tuple, dict] = None,
        rowfactory: Callable = None,
    ) -> PipelineOp:
        """
        Adds an operation that executes a query and returns the first row of
        the result set if one exists (or None, if no rows exist). The
        PipelineOpResult object that is returned will have the "return_value"
        attribute populated with this row if the query is performed
        successfully.
        """
        op_impl = PipelineOpImpl(
            op_type=PipelineOpType.FETCH_ONE,
            statement=statement,
            parameters=parameters,
            rowfactory=rowfactory,
        )
        return self._add_op(op_impl)

    @property
    def operations(self) -> list:
        """
        Returns the list of operations associated with the pipeline.
        """
        return self._operations


def create_pipeline() -> Pipeline:
    """
    Creates a pipeline object which can be used to process a set of operations
    against a database.
    """
    pipeline = Pipeline.__new__(Pipeline)
    pipeline._impl = PipelineImpl()
    pipeline._operations = []
    return pipeline
