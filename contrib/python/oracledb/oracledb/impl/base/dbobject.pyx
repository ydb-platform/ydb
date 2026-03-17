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
# dbobject.pyx
#
# Cython file defining the base DbObjectType, DbObjectAttr and DbObject
# implementation classes (embedded in base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseDbObjectImpl:

    cdef int _check_max_size(self, object value, uint32_t max_size,
                             ssize_t* actual_size, bint* violated) except -1:
        """
        Checks to see if the maximum size has been violated.
        """
        violated[0] = False
        if max_size > 0 and value is not None:
            if isinstance(value, str):
                actual_size[0] = len((<str> value).encode())
            else:
                actual_size[0] = len(<bytes> value)
            if actual_size[0] > max_size:
                violated[0] = True

    def append(self, object value):
        """
        Appends a value to the collection after first checking to see if the
        value is acceptable.
        """
        cdef:
            BaseConnImpl conn_impl = self.type._conn_impl
            ssize_t actual_size
            bint violated
        value = conn_impl._check_value(self.type.element_dbtype,
                                       self.type.element_objtype, value, NULL)
        self._check_max_size(value, self.type.element_max_size, &actual_size,
                             &violated)
        if violated:
            errors._raise_err(errors.ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED,
                              index=self.get_size(),
                              type_name=self.type._get_fqn(),
                              actual_size=actual_size,
                              max_size=self.type.element_max_size)
        self.append_checked(value)

    def append_checked(self, object value):
        errors._raise_not_supported("appending a value to a collection")

    def copy(self):
        errors._raise_not_supported("creating a copy of an object")

    def delete_by_index(self, int32_t index):
        errors._raise_not_supported("deleting an element in a collection")

    def exists_by_index(self, int32_t index):
        errors._raise_not_supported(
            "determining if an entry exists in a collection"
        )

    def get_attr_value(self, BaseDbObjectAttrImpl attr):
        errors._raise_not_supported("getting an attribute value")

    def get_element_by_index(self, int32_t index):
        errors._raise_not_supported("getting an element of a collection")

    def get_first_index(self):
        errors._raise_not_supported("getting the first index of a collection")

    def get_last_index(self):
        errors._raise_not_supported("getting the last index of a collection")

    def get_next_index(self, int32_t index):
        errors._raise_not_supported("getting the next index of a collection")

    def get_prev_index(self, int32_t index):
        errors._raise_not_supported(
            "getting the previous index of a collection"
        )

    def get_size(self):
        errors._raise_not_supported("getting the size of a collection")

    def set_attr_value(self, BaseDbObjectAttrImpl attr, object value):
        """
        Sets the attribute value after first checking to see if the value is
        acceptable.
        """
        cdef:
            BaseConnImpl conn_impl = self.type._conn_impl
            ssize_t actual_size
            bint violated
        value = conn_impl._check_value(attr.dbtype, attr.objtype, value, NULL)
        self._check_max_size(value, attr.max_size, &actual_size, &violated)
        if violated:
            errors._raise_err(errors.ERR_DBOBJECT_ATTR_MAX_SIZE_VIOLATED,
                              attr_name=attr.name,
                              type_name=self.type._get_fqn(),
                              actual_size=actual_size, max_size=attr.max_size)
        self.set_attr_value_checked(attr, value)

    def set_attr_value_checked(self, BaseDbObjectAttrImpl attr, object value):
        errors._raise_not_supported("setting an attribute value")

    def set_element_by_index(self, int32_t index, object value):
        """
        Sets the element value after first checking to see if the value is
        acceptable.
        """
        cdef:
            BaseConnImpl conn_impl = self.type._conn_impl
            ssize_t actual_size
            bint violated
        value = conn_impl._check_value(self.type.element_dbtype,
                                       self.type.element_objtype, value, NULL)
        self._check_max_size(value, self.type.element_max_size, &actual_size,
                             &violated)
        if violated:
            errors._raise_err(errors.ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED,
                              index=index, type_name=self.type._get_fqn(),
                              actual_size=actual_size,
                              max_size=self.type.element_max_size)
        self.set_element_by_index_checked(index, value)

    def set_element_by_index_checked(self, int32_t index, object value):
        errors._raise_not_supported("setting an element of a collection")

    def trim(self, int32_t num_to_trim):
        errors._raise_not_supported("trimming elements from a collection")


cdef class BaseDbObjectAttrImpl:
    pass


cdef class BaseDbObjectTypeImpl:

    def __eq__(self, other):
        if isinstance(other, BaseDbObjectTypeImpl):
            return other._conn_impl is self._conn_impl \
                    and other.schema == self.schema \
                    and other.name == self.name
        return NotImplemented

    cpdef str _get_fqn(self):
        """
        Return the fully qualified name of the type.
        """
        if self.package_name is not None:
            return f"{self.schema}.{self.package_name}.{self.name}"
        return f"{self.schema}.{self.name}"

    def create_new_object(self):
        errors._raise_not_supported("creating a new object")
