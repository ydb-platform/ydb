# -----------------------------------------------------------------------------
# Copyright (c) 2023, 2024, Oracle and/or its affiliates.
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
# fetch_info.py
#
# Contains the FetchInfo class which stores metadata about columns that are
# being fetched.
# -----------------------------------------------------------------------------

from typing import Union

import oracledb

from . import __name__ as MODULE_NAME
from . import constants
from .dbobject import DbObjectType
from .base_impl import (
    DbType,
    DB_TYPE_DATE,
    DB_TYPE_TIMESTAMP,
    DB_TYPE_TIMESTAMP_LTZ,
    DB_TYPE_TIMESTAMP_TZ,
    DB_TYPE_BINARY_FLOAT,
    DB_TYPE_BINARY_DOUBLE,
    DB_TYPE_BINARY_INTEGER,
    DB_TYPE_NUMBER,
    DB_TYPE_VECTOR,
)


class FetchInfo:
    """
    Identifies metadata of columns that are being fetched.
    """

    __module__ = MODULE_NAME

    def __eq__(self, other):
        return tuple(self) == other

    def __getitem__(self, index):
        """
        Return the parts mandated by the Python Database API.
        """
        if index == 0 or index == -7:
            return self.name
        elif index == 1 or index == -6:
            return self.type_code
        elif index == 2 or index == -5:
            return self.display_size
        elif index == 3 or index == -4:
            return self.internal_size
        elif index == 4 or index == -3:
            return self.precision
        elif index == 5 or index == -2:
            return self.scale
        elif index == 6 or index == -1:
            return self.null_ok
        elif isinstance(index, slice):
            return tuple(self).__getitem__(index)
        raise IndexError("list index out of range")

    def __len__(self):
        """
        Length mandated by the Python Database API.
        """
        return 7

    def __repr__(self):
        return repr(tuple(self))

    def __str__(self):
        return str(tuple(self))

    @classmethod
    def _from_impl(cls, impl):
        info = cls.__new__(cls)
        info._impl = impl
        info._type = None
        return info

    @property
    def annotations(self) -> Union[dict, None]:
        """
        Returns a dictionary of the annotations associated with the column, if
        applicable.
        """
        return self._impl.annotations

    @property
    def display_size(self) -> Union[int, None]:
        """
        Returns the display size of the column.
        """
        if self._impl.size > 0:
            return self._impl.size
        dbtype = self._impl.dbtype
        if (
            dbtype is DB_TYPE_DATE
            or dbtype is DB_TYPE_TIMESTAMP
            or dbtype is DB_TYPE_TIMESTAMP_LTZ
            or dbtype is DB_TYPE_TIMESTAMP_TZ
        ):
            return 23
        elif (
            dbtype is DB_TYPE_BINARY_FLOAT
            or dbtype is DB_TYPE_BINARY_DOUBLE
            or dbtype is DB_TYPE_BINARY_INTEGER
            or dbtype is DB_TYPE_NUMBER
        ):
            if self._impl.precision:
                display_size = self._impl.precision + 1
                if self._impl.scale > 0:
                    display_size += self._impl.scale + 1
            else:
                display_size = 127
            return display_size

    @property
    def domain_name(self) -> Union[str, None]:
        """
        Returns the name of the domain, if applicable.
        """
        return self._impl.domain_name

    @property
    def domain_schema(self) -> Union[str, None]:
        """
        Returns the name of the schema in which the domain is found, if
        applicable.
        """
        return self._impl.domain_schema

    @property
    def internal_size(self) -> Union[int, None]:
        """
        Returns the size in bytes of the column.
        """
        if self._impl.size > 0:
            return self._impl.buffer_size

    @property
    def is_json(self) -> bool:
        """
        Returns whether the column contains JSON.
        """
        return self._impl.is_json

    @property
    def is_oson(self) -> bool:
        """
        Returns whether the column contains OSON encoded bytes.
        """
        return self._impl.is_oson

    @property
    def name(self) -> str:
        """
        Returns the name of the column.
        """
        return self._impl.name

    @property
    def null_ok(self) -> bool:
        """
        Returns whether nulls or permitted or not in the column.
        """
        return self._impl.nulls_allowed

    @property
    def precision(self) -> Union[int, None]:
        """
        Returns the precision of the column.
        """
        if self._impl.precision or self._impl.scale:
            return self._impl.precision

    @property
    def scale(self) -> Union[int, None]:
        """
        Returns the scale of the column.
        """
        if self._impl.precision or self._impl.scale:
            return self._impl.scale

    @property
    def type(self) -> Union[DbType, DbObjectType]:
        """
        Returns the type of the column, as either a database object type or a
        database type.
        """
        if self._type is None:
            if self._impl.objtype is not None:
                self._type = DbObjectType._from_impl(self._impl.objtype)
            else:
                self._type = self._impl.dbtype
        return self._type

    @property
    def type_code(self) -> DbType:
        """
        Returns the type of the column.
        """
        return self._impl.dbtype

    @property
    def vector_dimensions(self) -> [int, None]:
        """
        Returns the number of dimensions required by vector columns. If the
        column is not a vector column or allows for any number of dimensions,
        the value returned is None.
        """
        if self._impl.dbtype is DB_TYPE_VECTOR:
            flags = self._impl.vector_flags
            if not (flags & constants.VECTOR_META_FLAG_FLEXIBLE_DIM):
                return self._impl.vector_dimensions

    @property
    def vector_format(self) -> [oracledb.VectorFormat, None]:
        """
        Returns the storage type required by vector columns. If the column is
        not a vector column or allows for any type of storage, the value
        returned is None.
        """
        if (
            self._impl.dbtype is DB_TYPE_VECTOR
            and self._impl.vector_format != 0
        ):
            return oracledb.VectorFormat(self._impl.vector_format)
