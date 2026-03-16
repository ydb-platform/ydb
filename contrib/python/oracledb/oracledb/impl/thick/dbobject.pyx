#------------------------------------------------------------------------------
# Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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
# Cython file defining the thick DbObjectType, DbObjectAttr and DbObject
# implementation classes (embedded in thick_impl.pyx).
#------------------------------------------------------------------------------

cdef class ThickDbObjectImpl(BaseDbObjectImpl):
    cdef:
        dpiObject *_handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiObject_release(self._handle)

    cdef int _convert_from_python(self, object value, DbType dbtype,
                                  ThickDbObjectTypeImpl objtype, dpiData *data,
                                  StringBuffer buf):
        """
        Internal method for converting a value from Python to the value
        required by ODPI-C.
        """
        if value is None:
            data.isNull = 1
        else:
            data.isNull = 0
            _convert_from_python(value, dbtype, objtype, &data.value, buf)

    def append_checked(self, object value):
        """
        Internal method for appending a value to a collection object.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            ThickDbObjectTypeImpl objtype
            dpiData data
        objtype = <ThickDbObjectTypeImpl> self.type
        self._convert_from_python(value, objtype.element_dbtype,
                                  objtype.element_objtype, &data, buf)
        if dpiObject_appendElement(self._handle,
                                   objtype.element_dbtype._native_num,
                                   &data) < 0:
            _raise_from_odpi()

    def copy(self):
        """
        Internal method for creating a copy of an object.
        """
        cdef ThickDbObjectImpl copied_impl
        copied_impl = ThickDbObjectImpl.__new__(ThickDbObjectImpl)
        if dpiObject_copy(self._handle, &copied_impl._handle) < 0:
            _raise_from_odpi()
        copied_impl.type = self.type
        return copied_impl

    def delete_by_index(self, int32_t index):
        """
        Internal method for deleting an entry from a collection that is indexed
        by integers.
        """
        if dpiObject_deleteElementByIndex(self._handle, index) < 0:
            _raise_from_odpi()

    def exists_by_index(self, int32_t index):
        """
        Internal method for determining if an entry exists in a collection that
        is indexed by integers.
        """
        cdef bint exists
        if dpiObject_getElementExistsByIndex(self._handle, index, &exists) < 0:
            _raise_from_odpi()
        return exists

    def get_attr_value(self, ThickDbObjectAttrImpl attr):
        """
        Internal method for getting an attribute value.
        """
        cdef:
            char number_as_string_buffer[200]
            ThickDbObjectTypeImpl type_impl
            dpiData data
        if attr.dbtype._native_num == DPI_NATIVE_TYPE_BYTES \
                and attr.dbtype.num == DPI_ORACLE_TYPE_NUMBER:
            data.value.asBytes.ptr = number_as_string_buffer
            data.value.asBytes.length = sizeof(number_as_string_buffer)
            data.value.asBytes.encoding = NULL
        if dpiObject_getAttributeValue(self._handle, attr._handle,
                                       attr.dbtype._native_num, &data) < 0:
            _raise_from_odpi()
        if data.isNull:
            return None
        type_impl = self.type
        try:
            return _convert_to_python(type_impl._conn_impl, attr.dbtype,
                                      attr.objtype, &data.value,
                                      attr._preferred_num_type)
        finally:
            if attr.objtype is not None:
                dpiObject_release(data.value.asObject)

    def get_element_by_index(self, int32_t index):
        """
        Internal method for getting an entry from a collection that is indexed
        by integers.
        """
        cdef:
            char number_as_string_buffer[200]
            ThickDbObjectTypeImpl objtype
            dpiData data
        objtype = self.type
        if objtype.element_dbtype._native_num == DPI_NATIVE_TYPE_BYTES \
                and objtype.element_dbtype.num == DPI_ORACLE_TYPE_NUMBER:
            data.value.asBytes.ptr = number_as_string_buffer
            data.value.asBytes.length = sizeof(number_as_string_buffer)
            data.value.asBytes.encoding = NULL
        if dpiObject_getElementValueByIndex(self._handle, index,
                                            objtype.element_dbtype._native_num,
                                            &data) < 0:
            _raise_from_odpi()
        if data.isNull:
            return None
        try:
            return _convert_to_python(objtype._conn_impl,
                                      objtype.element_dbtype,
                                      objtype.element_objtype, &data.value,
                                      objtype._element_preferred_num_type)
        finally:
            if objtype.element_objtype is not None:
                dpiObject_release(data.value.asObject)

    def get_first_index(self):
        """
        Internal method for getting the first index from a collection that is
        indexed by integers.
        """
        cdef:
            int32_t index
            bint exists
        if dpiObject_getFirstIndex(self._handle, &index, &exists) < 0:
            _raise_from_odpi()
        if exists:
            return index

    def get_last_index(self):
        """
        Internal method for getting the last index from a collection that is
        indexed by integers.
        """
        cdef:
            int32_t index
            bint exists
        if dpiObject_getLastIndex(self._handle, &index, &exists) < 0:
            _raise_from_odpi()
        if exists:
            return index

    def get_next_index(self, int32_t index):
        """
        Internal method for getting the next index from a collection that is
        indexed by integers.
        """
        cdef:
            int32_t next_index
            bint exists
        if dpiObject_getNextIndex(self._handle, index, &next_index,
                                  &exists) < 0:
            _raise_from_odpi()
        if exists:
            return next_index

    def get_prev_index(self, int32_t index):
        """
        Internal method for getting the next index from a collection that is
        indexed by integers.
        """
        cdef:
            int32_t prev_index
            bint exists
        if dpiObject_getPrevIndex(self._handle, index, &prev_index,
                                  &exists) < 0:
            _raise_from_odpi()
        if exists:
            return prev_index

    def get_size(self):
        """
        Internal method for getting the size of a collection.
        """
        cdef int32_t size
        if dpiObject_getSize(self._handle, &size) < 0:
            _raise_from_odpi()
        return size

    def set_attr_value_checked(self, ThickDbObjectAttrImpl attr, object value):
        """
        Internal method for setting an attribute value.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            uint32_t native_type_num
            dpiData data
        self._convert_from_python(value, attr.dbtype, attr.objtype, &data,
                                  buf)
        native_type_num = attr.dbtype._native_num
        if native_type_num == DPI_NATIVE_TYPE_LOB \
                and not isinstance(value, PY_TYPE_LOB):
            native_type_num = DPI_NATIVE_TYPE_BYTES
        if dpiObject_setAttributeValue(self._handle, attr._handle,
                                       native_type_num, &data) < 0:
            _raise_from_odpi()

    def set_element_by_index_checked(self, int32_t index, object value):
        """
        Internal method for setting an entry in a collection that is indexed by
        integers.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            ThickDbObjectTypeImpl objtype
            uint32_t native_type_num
            dpiData data
        objtype = self.type
        self._convert_from_python(value, objtype.element_dbtype,
                                  objtype.element_objtype, &data, buf)
        native_type_num = objtype.element_dbtype._native_num
        if native_type_num == DPI_NATIVE_TYPE_LOB \
                and not isinstance(value, PY_TYPE_LOB):
            native_typeNum = DPI_NATIVE_TYPE_BYTES
        if dpiObject_setElementValueByIndex(self._handle, index,
                                            native_type_num, &data) < 0:
            _raise_from_odpi()

    def trim(self, int32_t num_to_trim):
        """
        Internal method for trimming a number of entries from a collection.
        """
        if dpiObject_trim(self._handle, num_to_trim) < 0:
            _raise_from_odpi()


cdef class ThickDbObjectAttrImpl(BaseDbObjectAttrImpl):
    cdef:
        dpiObjectAttr *_handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiObjectAttr_release(self._handle)

    @staticmethod
    cdef ThickDbObjectAttrImpl _from_handle(ThickConnImpl conn_impl,
                                            dpiObjectAttr *handle):
        """
        Create a new DbObjectAttr implementation object given an ODPI-C handle.
        """
        cdef:
            ThickDbObjectAttrImpl impl
            dpiObjectType *typ_handle
            dpiObjectAttrInfo info
        impl = ThickDbObjectAttrImpl.__new__(ThickDbObjectAttrImpl)
        impl._handle = handle
        if dpiObjectAttr_getInfo(handle, &info) < 0:
            _raise_from_odpi()
        impl.name = info.name[:info.nameLength].decode()
        impl.dbtype = DbType._from_num(info.typeInfo.oracleTypeNum)
        impl.precision = info.typeInfo.precision
        impl.scale = info.typeInfo.scale
        impl.max_size = info.typeInfo.dbSizeInBytes
        impl._preferred_num_type = \
                get_preferred_num_type(info.typeInfo.precision,
                                       info.typeInfo.scale)
        if info.typeInfo.objectType:
            typ_handle = info.typeInfo.objectType
            impl.objtype = ThickDbObjectTypeImpl._from_handle(conn_impl,
                                                              typ_handle)
        return impl


cdef class ThickDbObjectTypeImpl(BaseDbObjectTypeImpl):
    cdef:
        dpiObjectType *_handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiObjectType_release(self._handle)

    @staticmethod
    cdef ThickDbObjectTypeImpl _from_handle(ThickConnImpl conn_impl,
                                            dpiObjectType *handle):
        """
        Create a new DbObjectType implementation object given an ODPI-C handle.
        """
        cdef:
            dpiObjectAttr **attributes = NULL
            ThickDbObjectTypeImpl impl, temp
            ThickDbObjectAttrImpl attr_impl
            dpiObjectTypeInfo info
            ssize_t num_bytes
            DbType dbtype
            uint16_t i
            object typ
        impl = ThickDbObjectTypeImpl.__new__(ThickDbObjectTypeImpl)
        if dpiObjectType_addRef(handle) < 0:
            _raise_from_odpi()
        impl._conn_impl = conn_impl
        impl._handle = handle
        if dpiObjectType_getInfo(impl._handle, &info) < 0:
            _raise_from_odpi()
        impl.schema = info.schema[:info.schemaLength].decode()
        impl.name = info.name[:info.nameLength].decode()
        if info.packageNameLength > 0:
            impl.package_name = \
                    info.packageName[:info.packageNameLength].decode()
        impl.is_collection = info.isCollection
        if impl.is_collection:
            dbtype = DbType._from_num(info.elementTypeInfo.oracleTypeNum)
            impl.element_dbtype = dbtype
            impl.element_precision = info.elementTypeInfo.precision
            impl.element_scale = info.elementTypeInfo.scale
            impl.element_max_size = info.elementTypeInfo.dbSizeInBytes
            impl._element_preferred_num_type = \
                get_preferred_num_type(info.elementTypeInfo.precision,
                                       info.elementTypeInfo.scale)
            if info.elementTypeInfo.objectType != NULL:
                handle = info.elementTypeInfo.objectType
                temp = ThickDbObjectTypeImpl._from_handle(conn_impl, handle)
                impl.element_objtype = temp
        impl.attrs_by_name = {}
        impl.attrs = [None] * info.numAttributes
        try:
            num_bytes = info.numAttributes * sizeof(dpiObjectAttr*)
            attributes = <dpiObjectAttr**> cpython.PyMem_Malloc(num_bytes)
            if dpiObjectType_getAttributes(impl._handle, info.numAttributes,
                                           attributes) < 0:
                _raise_from_odpi()
            for i in range(info.numAttributes):
                attr_impl = ThickDbObjectAttrImpl._from_handle(conn_impl,
                                                               attributes[i])
                impl.attrs[i] = attr_impl
                impl.attrs_by_name[attr_impl.name] = attr_impl
        finally:
            if attributes != NULL:
                cpython.PyMem_Free(attributes)
        return impl

    def create_new_object(self):
        """
        Internal method for creating a new object.
        """
        cdef ThickDbObjectImpl obj_impl
        obj_impl = ThickDbObjectImpl.__new__(ThickDbObjectImpl)
        obj_impl.type = self
        if dpiObjectType_createObject(self._handle, &obj_impl._handle) < 0:
            _raise_from_odpi()
        return obj_impl
