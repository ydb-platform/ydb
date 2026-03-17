# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2023, Oracle and/or its affiliates.
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
# var.py
#
# Contains the Var class used for managing variables used during bind and
# fetch. These hold the metadata as well as any necessary buffers.
# -----------------------------------------------------------------------------

from typing import Any, Callable, Union
from .dbobject import DbObjectType
from .base_impl import DbType


class Var:
    def __repr__(self):
        value = self._impl.get_all_values()
        if not self._impl.is_array and len(value) == 1:
            value = value[0]
        typ = self._type
        return f"<oracledb.Var of type {typ.name} with value {repr(value)}>"

    @classmethod
    def _from_impl(cls, impl, typ=None):
        var = cls.__new__(cls)
        var._impl = impl
        if typ is not None:
            var._type = typ
        elif impl.objtype is not None:
            var._type = DbObjectType._from_impl(impl.objtype)
        else:
            var._type = impl.dbtype
        return var

    @property
    def actual_elements(self) -> int:
        """
        This read-only attribute returns the actual number of elements in the
        variable. This corresponds to the number of elements in a PL/SQL
        index-by table for variables that are created using the method
        Cursor.arrayvar(). For all other variables this value will be identical
        to the attribute num_elements.
        """
        if self._impl.is_array:
            return self._impl.num_elements_in_array
        return self._impl.num_elements

    @property
    def actualElements(self) -> int:
        """
        Deprecated. Use property actual_elements instead.
        """
        return self.actual_elements

    @property
    def buffer_size(self) -> int:
        """
        This read-only attribute returns the size of the buffer allocated for
        each element in bytes.
        """
        return self._impl.buffer_size

    @property
    def bufferSize(self) -> int:
        """
        Deprecated. Use property buffer_size intead().
        """
        return self.buffer_size

    @property
    def convert_nulls(self) -> bool:
        """
        This read-only attribute returns whether null values are converted
        using the supplied ``outconverter``.
        """
        return self._impl.convert_nulls

    def getvalue(self, pos: int = 0) -> Any:
        """
        Return the value at the given position in the variable. For variables
        created using the method Cursor.arrayvar() the value returned will be a
        list of each of the values in the PL/SQL index-by table. For variables
        bound to DML returning statements, the value returned will also be a
        list corresponding to the returned data for the given execution of the
        statement (as identified by the pos parameter).
        """
        return self._impl.get_value(pos)

    @property
    def inconverter(self) -> Callable:
        """
        This read-only attribute specifies the method used to convert data from
        Python to the Oracle database. The method signature is converter(value)
        and the expected return value is the value to bind to the database. If
        this attribute is None, the value is bound directly without any
        conversion.
        """
        return self._impl.inconverter

    @property
    def num_elements(self) -> int:
        """
        This read-only attribute returns the number of elements allocated in an
        array, or the number of scalar items that can be fetched into the
        variable or bound to the variable.
        """
        return self._impl.num_elements

    @property
    def numElements(self) -> int:
        """
        Deprecated. Use property num_elements instead.
        """
        return self.num_elements

    @property
    def outconverter(self) -> Callable:
        """
        This read-only attribute specifies the method used to convert data from
        the Oracle database to Python. The method signature is converter(value)
        and the expected return value is the value to return to Python. If this
        attribute is None, the value is returned directly without any
        conversion.
        """
        return self._impl.outconverter

    def setvalue(self, pos: int, value: Any) -> None:
        """
        Set the value at the given position in the variable.
        """
        self._impl.set_value(pos, value)

    @property
    def size(self) -> int:
        """
        This read-only attribute returns the size of the variable. For strings
        this value is the size in characters. For all others, this is same
        value as the attribute buffer_size.
        """
        return self._impl.size

    @property
    def type(self) -> Union[DbType, DbObjectType]:
        """
        This read-only attribute returns the type of the variable. This will be
        an Oracle Object Type if the variable binds Oracle objects; otherwise,
        it will be one of the database type constants.
        """
        return self._type

    @property
    def values(self) -> list:
        """
        This read-only attribute returns a copy of the value of all actual
        positions in the variable as a list. This is the equivalent of calling
        getvalue() for each valid position and the length will correspond to
        the value of the actual_elements attribute.
        """
        return self._impl.get_all_values()
