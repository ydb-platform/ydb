#------------------------------------------------------------------------------
# Copyright (c) 2022, 2024, Oracle and/or its affiliates.
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
# dbobject_cache.pyx
#
# Cython file defining the DbObject cache implementation class (embedded in
# thin_impl.pyx).
#------------------------------------------------------------------------------

# SQL statements used within the DbObject type cache
cdef str DBO_CACHE_SQL_GET_METADATA_FOR_NAME = """
        declare
            t_Instantiable              varchar2(3);
            t_SuperTypeOwner            varchar2(128);
            t_SuperTypeName             varchar2(128);
            t_SubTypeRefCursor          sys_refcursor;
            t_Pos                       pls_integer;
        begin
            :ret_val := dbms_pickler.get_type_shape(:full_name, :oid,
                :version, :tds, t_Instantiable, t_SuperTypeOwner,
                t_SuperTypeName, :attrs_rc, t_SubTypeRefCursor);
            :package_name := null;
            if substr(:full_name, length(:full_name) - 7) = '%ROWTYPE' then
                t_Pos := instr(:full_name, '.');
                :schema := substr(:full_name, 1, t_Pos - 1);
                :name := substr(:full_name, t_Pos + 1);
            else
                begin
                    select owner, type_name
                    into :schema, :name
                    from all_types
                    where type_oid = :oid;
                exception
                when no_data_found then
                    begin
                        select owner, package_name, type_name
                        into :schema, :package_name, :name
                        from all_plsql_types
                        where type_oid = :oid;
                    exception
                    when no_data_found then
                        null;
                    end;
                end;
            end if;
        end;"""

cdef str DBO_CACHE_SQL_GET_COLUMNS = """
        select
            column_name,
            data_type,
            data_type_owner,
            case
                when data_type in
                        ('CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'RAW')
                    then data_length
                else 0
            end,
            nvl(data_precision, 0),
            nvl(data_scale, 0)
        from all_tab_cols
        where owner = :owner
          and table_name = substr(:name, 1, length(:name) - 8)
          and hidden_column != 'YES'
        order by column_id"""

cdef str DBO_CACHE_SQL_GET_ELEM_TYPE_WITH_PACKAGE = """
        select elem_type_name
        from all_plsql_coll_types
        where owner = :owner
          and package_name = :package_name
          and type_name = :name"""

cdef str DBO_CACHE_SQL_GET_ELEM_TYPE_NO_PACKAGE = """
        select elem_type_name
        from all_coll_types
        where owner = :owner
          and type_name = :name"""

cdef str DBO_CACHE_SQL_GET_ELEM_OBJTYPE_WITH_PACKAGE = """
        select
            elem_type_owner,
            elem_type_package,
            elem_type_name
        from all_plsql_coll_types
        where owner = :owner
          and package_name = :package_name
          and type_name = :name"""

cdef str DBO_CACHE_SQL_GET_ELEM_OBJTYPE_NO_PACKAGE = """
        select
            elem_type_owner,
            elem_type_name
        from all_coll_types
        where owner = :owner
          and type_name = :name"""

cdef class ThinDbObjectTypeSuperCache:
    cdef:
        dict caches
        object lock
        int cache_num

    def __init__(self):
        self.caches = {}
        self.cache_num = 0
        self.lock = threading.Lock()


cdef class BaseThinDbObjectTypeCache:

    cdef:
        object meta_cursor, columns_cursor, attrs_ref_cursor_var, version_var
        object return_value_var, full_name_var, oid_var, tds_var
        object schema_var, package_name_var, name_var
        BaseThinConnImpl conn_impl
        dict types_by_oid
        dict types_by_name
        list partial_types

    cdef int _clear_cursors(self) except -1:
        """
        Clears the cursors used for searching metadata. This is needed when
        returning a connection to the pool since user-level objects are
        retained.
        """
        if self.meta_cursor is not None:
            self.meta_cursor.close()
            self.meta_cursor = None
            self.return_value_var = None
            self.full_name_var = None
            self.oid_var = None
            self.tds_var = None
            self.attrs_ref_cursor_var = None
            self.version_var = None
            self.schema_var = None
            self.package_name_var = None
            self.name_var = None
        if self.columns_cursor is not None:
            self.columns_cursor.close()
            self.columns_cursor = None

    cdef str _get_full_name(self, ThinDbObjectTypeImpl typ_impl):
        """
        Gets the full name of the type which is used for searching the database
        for the type information.
        """
        cdef str name, suffix = "%ROWTYPE"
        if typ_impl.name.endswith(suffix):
            name = typ_impl.name[:-len(suffix)]
        else:
            name = typ_impl.name
            suffix = ""
        if typ_impl.package_name is None:
            return f'"{typ_impl.schema}"."{name}"{suffix}'
        return f'"{typ_impl.schema}".' + \
               f'"{typ_impl.package_name}".' + \
               f'"{name}"{suffix}'

    cdef int _initialize(self, BaseThinConnImpl conn_impl) except -1:
        self.types_by_oid = {}
        self.types_by_name = {}
        self.partial_types = []
        self.conn_impl = conn_impl

    cdef int _init_columns_cursor(self, object conn) except -1:
        """
        Initializes the cursor that fetches the columns for a table or view.
        The input values come from the meta cursor that has been initialized
        and executed earlier.
        """
        cursor = conn.cursor()
        cursor.setinputsizes(owner=self.schema_var, name=self.name_var)
        cursor.prepare(DBO_CACHE_SQL_GET_COLUMNS)
        self.columns_cursor = cursor

    cdef int _init_meta_cursor(self, object conn) except -1:
        """
        Initializes the cursor that fetches the type metadata.
        """
        cursor = conn.cursor()
        self.return_value_var = cursor.var(DB_TYPE_BINARY_INTEGER)
        self.tds_var = cursor.var(bytes)
        self.full_name_var = cursor.var(str)
        self.schema_var = cursor.var(str)
        self.package_name_var = cursor.var(str)
        self.name_var = cursor.var(str)
        self.oid_var = cursor.var(bytes)
        self.version_var = cursor.var(DB_TYPE_BINARY_INTEGER)
        self.attrs_ref_cursor_var = cursor.var(DB_TYPE_CURSOR)
        cursor.setinputsizes(ret_val=self.return_value_var,
                             tds=self.tds_var,
                             full_name=self.full_name_var,
                             oid=self.oid_var,
                             schema=self.schema_var,
                             package_name=self.package_name_var,
                             name=self.name_var,
                             version=self.version_var,
                             attrs_rc=self.attrs_ref_cursor_var)
        cursor.prepare(DBO_CACHE_SQL_GET_METADATA_FOR_NAME)
        self.meta_cursor = cursor

    cdef object _parse_tds(self, ThinDbObjectTypeImpl typ_impl, bytes tds):
        """
        Parses the TDS for the type. This is only needed for collection types,
        so if the TDS is determined to be for an object type, the remaining
        information is skipped.
        """
        cdef:
            ThinDbObjectAttrImpl attr_impl
            int preferred_num_type
            uint16_t num_attrs, i
            uint8_t attr_type
            TDSBuffer buf
            uint32_t pos

        # parse initial TDS bytes
        buf = TDSBuffer.__new__(TDSBuffer)
        buf._populate_from_bytes(tds)
        buf.skip_raw_bytes(4)               # end offset
        buf.skip_raw_bytes(2)               # version op code and version
        buf.skip_raw_bytes(2)               # unknown
        buf.read_uint16(&num_attrs)         # number of attributes
        buf.skip_raw_bytes(1)               # TDS attributes?
        buf.skip_raw_bytes(1)               # start ADT op code
        buf.skip_raw_bytes(2)               # ADT number (always zero)
        buf.skip_raw_bytes(4)               # offset to index table

        # check to see if type refers to a collection (only one attribute is
        # present in that case)
        if num_attrs == 1:
            pos = buf._pos
            buf.read_ub1(&attr_type)
            if attr_type == TNS_OBJ_TDS_TYPE_COLL:
                typ_impl.is_collection = True
            else:
                buf.skip_to(pos)

        # handle collections
        if typ_impl.is_collection:
            buf.read_uint32(&pos)
            buf.read_uint32(&typ_impl.max_num_elements)
            buf.read_ub1(&typ_impl.collection_type)
            if typ_impl.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                typ_impl.collection_flags = TNS_OBJ_HAS_INDEXES
            buf.skip_to(pos)
            typ_impl.element_dbtype = self._parse_tds_attr(
                buf, &typ_impl.element_precision, &typ_impl.element_scale,
                &typ_impl.element_max_size,
                &typ_impl._element_preferred_num_type
            )
            if typ_impl.element_dbtype is DB_TYPE_CLOB:
                return self._get_element_type_clob(typ_impl)
            elif typ_impl.element_dbtype is DB_TYPE_OBJECT:
                return self._get_element_type_obj(typ_impl)

        # handle objects with attributes
        else:
            for i, attr_impl in enumerate(typ_impl.attrs):
                self._parse_tds_attr(buf, &attr_impl.precision,
                                     &attr_impl.scale, &attr_impl.max_size,
                                     &attr_impl._preferred_num_type)

    cdef DbType _parse_tds_attr(self, TDSBuffer buf, int8_t* precision,
                                int8_t* scale, uint32_t *max_size,
                                int* preferred_num_type):
        """
        Parses a TDS attribute from the buffer.
        """
        cdef:
            uint8_t attr_type, ora_type_num = 0, csfrm = 0
            int8_t temp_precision, temp_scale
            int temp_preferred_num_type
            uint32_t temp_max_size
            uint16_t temp16

        # skip until a type code that is of interest
        while True:
            buf.read_ub1(&attr_type)
            if attr_type == TNS_OBJ_TDS_TYPE_EMBED_ADT_INFO:
                buf.skip_raw_bytes(1)       # flags
            elif attr_type != TNS_OBJ_TDS_TYPE_SUBTYPE_MARKER:
                break

        # process the type code
        if attr_type == TNS_OBJ_TDS_TYPE_NUMBER:
            ora_type_num = TNS_DATA_TYPE_NUMBER
            buf.read_sb1(precision)
            buf.read_sb1(scale)
            preferred_num_type[0] = \
                    get_preferred_num_type(precision[0], scale[0])
        elif attr_type == TNS_OBJ_TDS_TYPE_FLOAT:
            ora_type_num = TNS_DATA_TYPE_NUMBER
            buf.skip_raw_bytes(1)           # precision
        elif attr_type in (TNS_OBJ_TDS_TYPE_VARCHAR, TNS_OBJ_TDS_TYPE_CHAR):
            buf.read_uint16(&temp16)        # maximum length
            max_size[0] = temp16
            buf.read_ub1(&csfrm)
            csfrm = csfrm & 0x7f
            buf.skip_raw_bytes(2)           # character set
            if attr_type == TNS_OBJ_TDS_TYPE_VARCHAR:
                ora_type_num = TNS_DATA_TYPE_VARCHAR
            else:
                ora_type_num = TNS_DATA_TYPE_CHAR
        elif attr_type == TNS_OBJ_TDS_TYPE_RAW:
            buf.read_uint16(&temp16)        # maximum length
            max_size[0] = temp16
            ora_type_num = TNS_DATA_TYPE_RAW
        elif attr_type == TNS_OBJ_TDS_TYPE_BINARY_FLOAT:
            ora_type_num = TNS_DATA_TYPE_BINARY_FLOAT
        elif attr_type == TNS_OBJ_TDS_TYPE_BINARY_DOUBLE:
            ora_type_num = TNS_DATA_TYPE_BINARY_DOUBLE
        elif attr_type == TNS_OBJ_TDS_TYPE_DATE:
            ora_type_num = TNS_DATA_TYPE_DATE
        elif attr_type == TNS_OBJ_TDS_TYPE_TIMESTAMP:
            buf.skip_raw_bytes(1)           # precision
            ora_type_num = TNS_DATA_TYPE_TIMESTAMP
        elif attr_type == TNS_OBJ_TDS_TYPE_TIMESTAMP_LTZ:
            buf.skip_raw_bytes(1)           # precision
            ora_type_num = TNS_DATA_TYPE_TIMESTAMP_LTZ
        elif attr_type == TNS_OBJ_TDS_TYPE_TIMESTAMP_TZ:
            buf.skip_raw_bytes(1)           # precision
            ora_type_num = TNS_DATA_TYPE_TIMESTAMP_TZ
        elif attr_type == TNS_OBJ_TDS_TYPE_BOOLEAN:
            ora_type_num = TNS_DATA_TYPE_BOOLEAN
        elif attr_type == TNS_OBJ_TDS_TYPE_CLOB:
            ora_type_num = TNS_DATA_TYPE_CLOB
            csfrm = CS_FORM_IMPLICIT
        elif attr_type == TNS_OBJ_TDS_TYPE_BLOB:
            ora_type_num = TNS_DATA_TYPE_BLOB
        elif attr_type == TNS_OBJ_TDS_TYPE_OBJ:
            ora_type_num = TNS_DATA_TYPE_INT_NAMED
            buf.skip_raw_bytes(5)           # offset and code
        elif attr_type == TNS_OBJ_TDS_TYPE_START_EMBED_ADT:
            ora_type_num = TNS_DATA_TYPE_INT_NAMED
            while self._parse_tds_attr(buf, &temp_precision, &temp_scale,
                                       &temp_max_size,
                                       &temp_preferred_num_type):
                pass
        elif attr_type == TNS_OBJ_TDS_TYPE_END_EMBED_ADT:
            return None
        else:
            errors._raise_err(errors.ERR_TDS_TYPE_NOT_SUPPORTED, num=attr_type)
        return DbType._from_ora_type_and_csfrm(ora_type_num, csfrm)

    cdef int _create_attr(self, ThinDbObjectTypeImpl typ_impl, str name,
                          str type_name, str type_owner,
                          str type_package_name=None, bytes oid=None,
                          int8_t precision=0, int8_t scale=0,
                          uint32_t max_size=0) except -1:
        """
        Creates an attribute from the supplied information and adds it to the
        list of attributes for the type.
        """
        cdef:
            ThinDbObjectTypeImpl attr_typ_impl
            ThinDbObjectAttrImpl attr_impl
        attr_impl = ThinDbObjectAttrImpl.__new__(ThinDbObjectAttrImpl)
        attr_impl.name = name
        if type_owner is not None:
            attr_typ_impl = self.get_type_for_info(oid, type_owner,
                                                   type_package_name,
                                                   type_name)
            if attr_typ_impl.is_xml_type:
                attr_impl.dbtype = DB_TYPE_XMLTYPE
            else:
                attr_impl.dbtype = DB_TYPE_OBJECT
                attr_impl.objtype = attr_typ_impl
        else:
            attr_impl.dbtype = DbType._from_ora_name(type_name)
            attr_impl.max_size = max_size
            if precision != 0 or scale != 0:
                attr_impl.precision = precision
                attr_impl.scale = scale
                attr_impl._preferred_num_type = \
                        get_preferred_num_type(precision, scale)
        typ_impl.attrs.append(attr_impl)
        typ_impl.attrs_by_name[name] = attr_impl

    cdef object _populate_type_info(self, str name, object attrs,
                                    ThinDbObjectTypeImpl typ_impl):
        """
        Populate the type information given the name of the type.
        """
        cdef:
            ssize_t start_pos, end_pos, name_length
            ThinDbObjectAttrImpl attr_impl
            str data_type
        typ_impl.version = self.version_var.getvalue()
        if typ_impl.oid is None:
            typ_impl.oid = self.oid_var.getvalue()
            self.types_by_oid[typ_impl.oid] = typ_impl
        if typ_impl.schema is None:
            typ_impl.schema = self.schema_var.getvalue()
            typ_impl.package_name = self.package_name_var.getvalue()
            typ_impl.name = self.name_var.getvalue()
            if typ_impl.name is None:
                errors._raise_err(errors.ERR_INVALID_OBJECT_TYPE_NAME,
                                  name=name)
            typ_impl.is_xml_type = \
                    (typ_impl.schema == "SYS" and typ_impl.name == "XMLTYPE")
        typ_impl.attrs = []
        typ_impl.attrs_by_name = {}
        if typ_impl.is_row_type:
            for name, data_type, data_type_owner, max_size, precision, \
                    scale in attrs:
                if data_type_owner is None:
                    start_pos = data_type.find("(")
                    if start_pos > 0:
                        end_pos = data_type.find(")")
                        if end_pos > start_pos:
                            data_type = data_type[:start_pos] + \
                                    data_type[end_pos + 1:]
                self._create_attr(typ_impl, name, data_type, data_type_owner,
                                  None, None, precision, scale, max_size)
        else:
            for cursor_version, attr_name, attr_num, attr_type_name, \
                    attr_type_owner, attr_type_package, attr_type_oid, \
                    attr_instantiable, attr_super_type_owner, \
                    attr_super_type_name in attrs:
                if attr_name is None:
                    continue
                self._create_attr(typ_impl, attr_name, attr_type_name,
                                  attr_type_owner, attr_type_package,
                                  attr_type_oid)
            return self._parse_tds(typ_impl, self.tds_var.getvalue())

    cdef ThinDbObjectTypeImpl get_type_for_info(self, bytes oid, str schema,
                                                str package_name, str name):
        """
        Returns a type for the specified fetch info, if one has already been
        cached. If not, a new type object is created and cached. It is also
        added to the partial_types list which will be fully populated once the
        current execute has completed.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl
            str full_name
        if package_name is not None:
            full_name = f"{schema}.{package_name}.{name}"
        else:
            full_name = f"{schema}.{name}"
        if oid is not None:
            typ_impl = self.types_by_oid.get(oid)
        else:
            typ_impl = self.types_by_name.get(full_name)
        if typ_impl is None:
            typ_impl = ThinDbObjectTypeImpl.__new__(ThinDbObjectTypeImpl)
            typ_impl._conn_impl = self.conn_impl
            typ_impl.oid = oid
            typ_impl.schema = schema
            typ_impl.package_name = package_name
            typ_impl.name = name
            typ_impl.is_xml_type = (schema == "SYS" and name == "XMLTYPE")
            if oid is not None:
                self.types_by_oid[oid] = typ_impl
            self.types_by_name[full_name] = typ_impl
            self.partial_types.append(typ_impl)
        return typ_impl


cdef class ThinDbObjectTypeCache(BaseThinDbObjectTypeCache):

    def _get_element_type_clob(self, ThinDbObjectTypeImpl typ_impl):
        """
        Determine if the element type refers to an NCLOB or CLOB value. This
        must be fetched from the data dictionary since it is not included in
        the TDS.
        """
        cursor = self.meta_cursor.connection.cursor()
        if typ_impl.package_name is not None:
            cursor.execute(DBO_CACHE_SQL_GET_ELEM_TYPE_WITH_PACKAGE,
                    owner=typ_impl.schema,
                    package_name=typ_impl.package_name,
                    name=typ_impl.name)
        else:
            cursor.execute(DBO_CACHE_SQL_GET_ELEM_TYPE_NO_PACKAGE,
                    owner=typ_impl.schema,
                    name=typ_impl.name)
        type_name, = cursor.fetchone()
        if type_name == "NCLOB":
            typ_impl.element_dbtype = DB_TYPE_NCLOB

    def _get_element_type_obj(self, ThinDbObjectTypeImpl typ_impl):
        """
        Determine the element type's object type. This is needed when
        processing collections with object as the element type since this
        information is not available in the TDS.
        """
        cdef:
            str schema, name, package_name = None
            object cursor
        cursor = self.meta_cursor.connection.cursor()
        if typ_impl.package_name is not None:
            cursor.execute(DBO_CACHE_SQL_GET_ELEM_OBJTYPE_WITH_PACKAGE,
                    owner=typ_impl.schema,
                    package_name=typ_impl.package_name,
                    name=typ_impl.name)
            schema, package_name, name = cursor.fetchone()
        else:
            cursor.execute(DBO_CACHE_SQL_GET_ELEM_OBJTYPE_NO_PACKAGE,
                    owner=typ_impl.schema,
                    name=typ_impl.name)
            schema, name = cursor.fetchone()
        typ_impl.element_objtype = self.get_type_for_info(None, schema,
                                                          package_name, name)

    cdef list _lookup_type(self, object conn, str name,
                           ThinDbObjectTypeImpl typ_impl):
        """
        Lookup the type given its name and return the list of attributes for
        further processing. The metadata cursor execution will populate the
        variables.
        """
        if self.meta_cursor is None:
            self._init_meta_cursor(conn)
        self.full_name_var.setvalue(0, name)
        self.meta_cursor.execute(None)
        if self.return_value_var.getvalue() != 0:
            errors._raise_err(errors.ERR_INVALID_OBJECT_TYPE_NAME, name=name)
        if name.endswith("%ROWTYPE"):
            typ_impl.is_row_type = True
            if self.columns_cursor is None:
                self._init_columns_cursor(conn)
            self.columns_cursor.execute(None)
            return self.columns_cursor.fetchall()
        else:
            attrs_rc = self.attrs_ref_cursor_var.getvalue()
            return attrs_rc.fetchall()

    cdef ThinDbObjectTypeImpl get_type(self, object conn, str name):
        """
        Returns the database object type given its name. The cache is first
        searched and if it is not found, the database is searched and the
        result stored in the cache.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl
            bint is_rowtype
        typ_impl = self.types_by_name.get(name)
        if typ_impl is None:
            typ_impl = ThinDbObjectTypeImpl.__new__(ThinDbObjectTypeImpl)
            typ_impl._conn_impl = self.conn_impl
            attrs = self._lookup_type(conn, name, typ_impl)
            self._populate_type_info(name, attrs, typ_impl)
            self.types_by_oid[typ_impl.oid] = typ_impl
            self.types_by_name[name] = typ_impl
            self.populate_partial_types(conn)
        return typ_impl

    def populate_partial_types(self, object conn):
        """
        Populate any partial types that were discovered earlier. Since
        populating an object type might result in additional object types being
        discovered, object types are popped from the partial types list until
        the list is empty.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl
            str full_name
            list attrs
        while self.partial_types:
            typ_impl = self.partial_types.pop()
            full_name = self._get_full_name(typ_impl)
            attrs = self._lookup_type(conn, full_name, typ_impl)
            self._populate_type_info(full_name, attrs, typ_impl)


cdef class AsyncThinDbObjectTypeCache(BaseThinDbObjectTypeCache):

    async def _get_element_type_clob(self, ThinDbObjectTypeImpl typ_impl):
        """
        Determine if the element type refers to an NCLOB or CLOB value. This
        must be fetched from the data dictionary since it is not included in
        the TDS.
        """
        cursor = self.meta_cursor.connection.cursor()
        if typ_impl.package_name is not None:
            await cursor.execute(DBO_CACHE_SQL_GET_ELEM_TYPE_WITH_PACKAGE,
                    owner=typ_impl.schema,
                    package_name=typ_impl.package_name,
                    name=typ_impl.name)
        else:
            await cursor.execute(DBO_CACHE_SQL_GET_ELEM_TYPE_NO_PACKAGE,
                    owner=typ_impl.schema,
                    name=typ_impl.name)
        type_name, = await cursor.fetchone()
        if type_name == "NCLOB":
            typ_impl.element_dbtype = DB_TYPE_NCLOB

    async def _get_element_type_obj(self, ThinDbObjectTypeImpl typ_impl):
        """
        Determine the element type's object type. This is needed when
        processing collections with object as the element type since this
        information is not available in the TDS.
        """
        cdef:
            str schema, name, package_name = None
            object cursor
        cursor = self.meta_cursor.connection.cursor()
        if typ_impl.package_name is not None:
            await cursor.execute(DBO_CACHE_SQL_GET_ELEM_OBJTYPE_WITH_PACKAGE,
                    owner=typ_impl.schema,
                    package_name=typ_impl.package_name,
                    name=typ_impl.name)
            schema, package_name, name = await cursor.fetchone()
        else:
            await cursor.execute(DBO_CACHE_SQL_GET_ELEM_OBJTYPE_NO_PACKAGE,
                    owner=typ_impl.schema,
                    name=typ_impl.name)
            schema, name = await cursor.fetchone()
        typ_impl.element_objtype = self.get_type_for_info(None, schema,
                                                          package_name, name)

    async def _lookup_type(self, object conn, str name,
                           ThinDbObjectTypeImpl typ_impl):
        """
        Lookup the type given its name and return the list of attributes for
        further processing. The metadata cursor execution will populate the
        variables.
        """
        if self.meta_cursor is None:
            self._init_meta_cursor(conn)
        self.full_name_var.setvalue(0, name)
        await self.meta_cursor.execute(None)
        if self.return_value_var.getvalue() != 0:
            errors._raise_err(errors.ERR_INVALID_OBJECT_TYPE_NAME, name=name)
        if name.endswith("%ROWTYPE"):
            typ_impl.is_row_type = True
            if self.columns_cursor is None:
                self._init_columns_cursor(conn)
            await self.columns_cursor.execute(None)
            return await self.columns_cursor.fetchall()
        else:
            attrs_rc = self.attrs_ref_cursor_var.getvalue()
            return await attrs_rc.fetchall()

    async def get_type(self, object conn, str name):
        """
        Returns the database object type given its name. The cache is first
        searched and if it is not found, the database is searched and the
        result stored in the cache.
        """
        cdef ThinDbObjectTypeImpl typ_impl
        typ_impl = self.types_by_name.get(name)
        if typ_impl is None:
            typ_impl = ThinDbObjectTypeImpl.__new__(ThinDbObjectTypeImpl)
            typ_impl._conn_impl = self.conn_impl
            attrs = await self._lookup_type(conn, name, typ_impl)
            coroutine = self._populate_type_info(name, attrs, typ_impl)
            if coroutine is not None:
                await coroutine
            self.types_by_oid[typ_impl.oid] = typ_impl
            self.types_by_name[name] = typ_impl
            await self.populate_partial_types(conn)
        return typ_impl

    async def populate_partial_types(self, object conn):
        """
        Populate any partial types that were discovered earlier. Since
        populating an object type might result in additional object types being
        discovered, object types are popped from the partial types list until
        the list is empty.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl
            str full_name
            list attrs
        while self.partial_types:
            typ_impl = self.partial_types.pop()
            full_name = self._get_full_name(typ_impl)
            attrs = await self._lookup_type(conn, full_name, typ_impl)
            coroutine = self._populate_type_info(full_name, attrs, typ_impl)
            if coroutine is not None:
                await coroutine


# global cache of database object types
# since the database object types require a reference to the connection (in
# order to be able to manage LOBs), storing the cache on the connection would
# involve creating a circular reference
cdef ThinDbObjectTypeSuperCache DB_OBJECT_TYPE_SUPER_CACHE = \
        ThinDbObjectTypeSuperCache()


cdef int create_new_dbobject_type_cache(BaseThinConnImpl conn_impl) except -1:
    """
    Creates a new database object type cache and returns its identifier.
    """
    cdef:
        BaseThinDbObjectTypeCache cache
        bint is_async
        int cache_num
    with DB_OBJECT_TYPE_SUPER_CACHE.lock:
        DB_OBJECT_TYPE_SUPER_CACHE.cache_num += 1
        cache_num = DB_OBJECT_TYPE_SUPER_CACHE.cache_num
    is_async = conn_impl._protocol._transport._is_async
    cls = AsyncThinDbObjectTypeCache if is_async else ThinDbObjectTypeCache
    cache = cls.__new__(cls)
    cache._initialize(conn_impl)
    DB_OBJECT_TYPE_SUPER_CACHE.caches[cache_num] = cache
    return cache_num


cdef BaseThinDbObjectTypeCache get_dbobject_type_cache(int cache_num):
    """
    Returns the database object type cache given its identifier.
    """
    return DB_OBJECT_TYPE_SUPER_CACHE.caches[cache_num]


cdef int remove_dbobject_type_cache(int cache_num) except -1:
    """
    Removes the sub cache given its identifier.
    """
    del DB_OBJECT_TYPE_SUPER_CACHE.caches[cache_num]
