# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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
# dbobject.py
#
# Contains the classes used for managing database objects and the database
# object type metadata: DbObject, DbObjectType and DbObjectAttr.
# -----------------------------------------------------------------------------

from typing import Any, Sequence, Union

from . import errors
from . import __name__ as MODULE_NAME
from .base_impl import DbType


class DbObject:
    __module__ = MODULE_NAME

    def __getattr__(self, name):
        try:
            attr_impl = self._impl.type.attrs_by_name[name]
        except KeyError:
            return super().__getattribute__(name)
        return self._impl.get_attr_value(attr_impl)

    def __iter__(self):
        self._ensure_is_collection()
        ix = self._impl.get_first_index()
        while ix is not None:
            yield self._impl.get_element_by_index(ix)
            ix = self._impl.get_next_index(ix)

    def __repr__(self):
        return (
            f"<oracledb.DbObject {self.type._get_full_name()} at "
            f"{hex(id(self))}>"
        )

    def __setattr__(self, name, value):
        if name == "_impl" or name == "_type":
            super().__setattr__(name, value)
        else:
            attr_impl = self._impl.type.attrs_by_name[name]
            self._impl.set_attr_value(attr_impl, value)

    def _ensure_is_collection(self):
        """
        Ensures that the object refers to a collection. If not, an exception is
        raised.
        """
        if not self.type.iscollection:
            errors._raise_err(
                errors.ERR_OBJECT_IS_NOT_A_COLLECTION,
                name=self.type._get_full_name(),
            )

    @classmethod
    def _from_impl(cls, impl):
        obj = cls.__new__(cls)
        obj._impl = impl
        obj._type = None
        return obj

    def append(self, element: Any) -> None:
        """
        Append an element to the collection object. If no elements exist in the
        collection, this creates an element at index 0; otherwise, it creates
        an element immediately following the highest index available in the
        collection.
        """
        self._impl.append(element)

    def asdict(self) -> dict:
        """
        Return a dictionary where the collection’s indexes are the keys and the
        elements are its values.
        """
        self._ensure_is_collection()
        result = {}
        ix = self._impl.get_first_index()
        while ix is not None:
            result[ix] = self._impl.get_element_by_index(ix)
            ix = self._impl.get_next_index(ix)
        return result

    def aslist(self) -> list:
        """
        Return a list of each of the collection’s elements in index order.
        """
        return list(self)

    def copy(self) -> "DbObject":
        """
        Create a copy of the object and return it.
        """
        copied_impl = self._impl.copy()
        return DbObject._from_impl(copied_impl)

    def delete(self, index: int) -> None:
        """
        Delete the element at the specified index of the collection. If the
        element does not exist or is otherwise invalid, an error is raised.
        Note that the indices of the remaining elements in the collection are
        not changed. In other words, the delete operation creates holes in the
        collection.
        """
        self._ensure_is_collection()
        self._impl.delete_by_index(index)

    def exists(self, index: int) -> bool:
        """
        Return True or False indicating if an element exists in the collection
        at the specified index.
        """
        self._ensure_is_collection()
        return self._impl.exists_by_index(index)

    def extend(self, seq: list) -> None:
        """
        Append all of the elements in the sequence to the collection. This is
        the equivalent of performing append() for each element found in the
        sequence.
        """
        self._ensure_is_collection()
        for value in seq:
            self.append(value)

    def first(self) -> int:
        """
        Return the index of the first element in the collection. If the
        collection is empty, None is returned.
        """
        self._ensure_is_collection()
        return self._impl.get_first_index()

    def getelement(self, index: int) -> Any:
        """
        Return the element at the specified index of the collection. If no
        element exists at that index, an exception is raised.
        """
        self._ensure_is_collection()
        return self._impl.get_element_by_index(index)

    def last(self) -> int:
        """
        Return the index of the last element in the collection. If the
        collection is empty, None is returned.
        """
        self._ensure_is_collection()
        return self._impl.get_last_index()

    def next(self, index: int) -> int:
        """
        Return the index of the next element in the collection following the
        specified index. If there are no elements in the collection following
        the specified index, None is returned.
        """
        self._ensure_is_collection()
        return self._impl.get_next_index(index)

    def prev(self, index: int) -> int:
        """
        Return the index of the element in the collection preceding the
        specified index. If there are no elements in the collection preceding
        the specified index, None is returned.
        """
        self._ensure_is_collection()
        return self._impl.get_prev_index(index)

    def setelement(self, index: int, value: Any) -> None:
        """
        Set the value in the collection at the specified index to the given
        value.
        """
        self._ensure_is_collection()
        self._impl.set_element_by_index(index, value)

    def size(self) -> int:
        """
        Return the number of elements in the collection.
        """
        self._ensure_is_collection()
        return self._impl.get_size()

    def trim(self, num: int) -> None:
        """
        Remove the specified number of elements from the end of the collection.
        """
        self._ensure_is_collection()
        self._impl.trim(num)

    @property
    def type(self) -> "DbObjectType":
        """
        Returns an ObjectType corresponding to the type of the object.
        """
        if self._type is None:
            self._type = DbObjectType._from_impl(self._impl.type)
        return self._type


class DbObjectAttr:
    __module__ = MODULE_NAME

    def __repr__(self):
        return f"<oracledb.DbObjectAttr {self.name}>"

    @classmethod
    def _from_impl(cls, impl):
        attr = cls.__new__(cls)
        attr._impl = impl
        attr._type = None
        return attr

    @property
    def name(self) -> str:
        """
        This read-only attribute returns the name of the attribute.
        """
        return self._impl.name

    @property
    def type(self) -> Union["DbObjectType", DbType]:
        """
        This read-only attribute returns the type of the attribute. This will
        be an Oracle Object Type if the variable binds Oracle objects;
        otherwise, it will be one of the database type constants.
        """
        if self._type is None:
            if self._impl.objtype is not None:
                self._type = DbObjectType._from_impl(self._impl.objtype)
            else:
                self._type = self._impl.dbtype
        return self._type


class DbObjectType:
    __module__ = MODULE_NAME

    def __call__(self, value=None):
        return self.newobject(value)

    def __eq__(self, other):
        if isinstance(other, DbObjectType):
            return other._impl == self._impl
        return NotImplemented

    def __repr__(self):
        return f"<oracledb.DbObjectType {self._get_full_name()}>"

    @classmethod
    def _from_impl(cls, impl):
        typ = cls.__new__(cls)
        typ._impl = impl
        typ._attributes = None
        typ._element_type = None
        return typ

    def _get_full_name(self):
        """
        Returns the full name of the type.
        """
        return self._impl._get_fqn()

    @property
    def attributes(self) -> list:
        """
        This read-only attribute returns a list of the attributes that make up
        the object type.
        """
        if self._attributes is None:
            self._attributes = [
                DbObjectAttr._from_impl(i) for i in self._impl.attrs
            ]
        return self._attributes

    @property
    def iscollection(self) -> bool:
        """
        This read-only attribute returns a boolean indicating if the object
        type refers to a collection or not.
        """
        return self._impl.is_collection

    @property
    def name(self) -> str:
        """
        This read-only attribute returns the name of the type.
        """
        return self._impl.name

    @property
    def element_type(self) -> Union["DbObjectType", DbType]:
        """
        This read-only attribute returns the type of elements found in
        collections of this type, if iscollection is True; otherwise, it
        returns None. If the collection contains objects, this will be another
        object type; otherwise, it will be one of the database type constants.
        """
        if self._element_type is None:
            if self._impl.element_objtype is not None:
                typ_impl = self._impl.element_objtype
                self._element_type = DbObjectType._from_impl(typ_impl)
            else:
                self._element_type = self._impl.element_dbtype
        return self._element_type

    def newobject(self, value: Sequence = None) -> DbObject:
        """
        Return a new Oracle object of the given type. This object can then be
        modified by setting its attributes and then bound to a cursor for
        interaction with Oracle. If the object type refers to a collection, a
        sequence may be passed and the collection will be initialized with the
        items in that sequence.
        """
        obj_impl = self._impl.create_new_object()
        obj = DbObject._from_impl(obj_impl)
        if value is not None:
            obj.extend(value)
        return obj

    @property
    def package_name(self) -> str:
        """
        This read-only attribute returns the name of the package containing the
        PL/SQL type or None if the type is not a PL/SQL type.
        """
        return self._impl.package_name

    @property
    def schema(self) -> str:
        """
        This read-only attribute returns the name of the schema that owns the
        type.
        """
        return self._impl.schema
