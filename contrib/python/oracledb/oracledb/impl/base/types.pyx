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
# types.pyx
#
# Cython file defining the API types mandated by the Python Database API as
# well as the database types specific to Oracle (embedded in base_impl.pyx).
#------------------------------------------------------------------------------

cdef class ApiType:

    def __init__(self, name, *dbtypes):
        self.name = name
        self.dbtypes = dbtypes

    def __eq__(self, other):
        if isinstance(other, DbType):
            return other in self.dbtypes
        return NotImplemented

    def __hash__(self):
        return hash(self.name)

    def __reduce__(self):
        return self.name

    def __repr__(self):
        return f"<ApiType {self.name}>"


cdef list db_type_by_num = [None] * (DB_TYPE_NUM_MAX - DB_TYPE_NUM_MIN + 1)
cdef dict db_type_by_ora_name = {}
cdef dict db_type_by_ora_type_num = {}

cdef class DbType:

    def __init__(self, num, name, ora_name, native_num=0, ora_type_num=0,
                 default_size=0, csfrm=0, buffer_size_factor=0):
        cdef uint16_t ora_type_key = csfrm * 256 + ora_type_num
        self.num = num
        self.name = name
        self.default_size = default_size
        self._native_num = native_num
        self._ora_name = ora_name
        self._ora_type_num = ora_type_num
        self._csfrm = csfrm
        self._buffer_size_factor = buffer_size_factor
        if num != 0:
            num -= DB_TYPE_NUM_MIN
        db_type_by_num[num] = self
        db_type_by_ora_name[ora_name] = self
        db_type_by_ora_type_num[ora_type_key] = self

    def __reduce__(self):
        return self.name

    def __repr__(self):
        return f"<DbType {self.name}>"

    @staticmethod
    cdef DbType _from_num(uint32_t num):
        try:
            if num != 0:
                num -= DB_TYPE_NUM_MIN
            return db_type_by_num[num]
        except KeyError:
            pass
        errors._raise_err(errors.ERR_ORACLE_TYPE_NOT_SUPPORTED, num=num)

    @staticmethod
    cdef DbType _from_ora_name(str name):
        try:
            return db_type_by_ora_name[name]
        except KeyError:
            pass
        errors._raise_err(errors.ERR_ORACLE_TYPE_NAME_NOT_SUPPORTED, name=name)

    @staticmethod
    cdef DbType _from_ora_type_and_csfrm(uint8_t ora_type_num, uint8_t csfrm):
        cdef uint16_t ora_type_key = csfrm * 256 + ora_type_num
        try:
            return db_type_by_ora_type_num[ora_type_key]
        except KeyError:
            pass
        errors._raise_err(errors.ERR_ORACLE_TYPE_NOT_SUPPORTED,
                          num=ora_type_num)


# database types
DB_TYPE_BFILE = DbType(DB_TYPE_NUM_BFILE, "DB_TYPE_BFILE", "BFILE",
                       NATIVE_TYPE_NUM_LOB, 114, buffer_size_factor=4000)
DB_TYPE_BINARY_DOUBLE = DbType(DB_TYPE_NUM_BINARY_DOUBLE,
                               "DB_TYPE_BINARY_DOUBLE", "BINARY_DOUBLE",
                               NATIVE_TYPE_NUM_DOUBLE, 101,
                               buffer_size_factor=8)
DB_TYPE_BINARY_FLOAT = DbType(DB_TYPE_NUM_BINARY_FLOAT, "DB_TYPE_BINARY_FLOAT",
                              "BINARY_FLOAT", NATIVE_TYPE_NUM_FLOAT, 100,
                              buffer_size_factor=4)
DB_TYPE_BINARY_INTEGER = DbType(DB_TYPE_NUM_BINARY_INTEGER,
                                "DB_TYPE_BINARY_INTEGER", "BINARY_INTEGER",
                                NATIVE_TYPE_NUM_INT64, 3,
                                buffer_size_factor=22)
DB_TYPE_BLOB = DbType(DB_TYPE_NUM_BLOB, "DB_TYPE_BLOB", "BLOB",
                      NATIVE_TYPE_NUM_LOB, 113,
                      buffer_size_factor=112)
DB_TYPE_BOOLEAN = DbType(DB_TYPE_NUM_BOOLEAN, "DB_TYPE_BOOLEAN", "BOOLEAN",
                         NATIVE_TYPE_NUM_BOOLEAN, 252, buffer_size_factor=4)
DB_TYPE_CHAR = DbType(DB_TYPE_NUM_CHAR, "DB_TYPE_CHAR", "CHAR",
                      NATIVE_TYPE_NUM_BYTES, 96, 2000, csfrm=1,
                      buffer_size_factor=4)
DB_TYPE_CLOB = DbType(DB_TYPE_NUM_CLOB, "DB_TYPE_CLOB", "CLOB",
                      NATIVE_TYPE_NUM_LOB, 112, csfrm=1,
                      buffer_size_factor=112)
DB_TYPE_CURSOR = DbType(DB_TYPE_NUM_CURSOR, "DB_TYPE_CURSOR", "CURSOR",
                        NATIVE_TYPE_NUM_STMT, 102, buffer_size_factor=4)
DB_TYPE_DATE = DbType(DB_TYPE_NUM_DATE, "DB_TYPE_DATE", "DATE",
                      NATIVE_TYPE_NUM_TIMESTAMP, 12, buffer_size_factor=7)
DB_TYPE_INTERVAL_DS = DbType(DB_TYPE_NUM_INTERVAL_DS, "DB_TYPE_INTERVAL_DS",
                             "INTERVAL DAY TO SECOND",
                             NATIVE_TYPE_NUM_INTERVAL_DS, 183,
                             buffer_size_factor=11)
DB_TYPE_INTERVAL_YM = DbType(DB_TYPE_NUM_INTERVAL_YM, "DB_TYPE_INTERVAL_YM",
                             "INTERVAL YEAR TO MONTH",
                             NATIVE_TYPE_NUM_INTERVAL_YM, 182,
                             buffer_size_factor=5)
DB_TYPE_JSON = DbType(DB_TYPE_NUM_JSON, "DB_TYPE_JSON", "JSON",
                      NATIVE_TYPE_NUM_JSON, 119)
DB_TYPE_LONG = DbType(DB_TYPE_NUM_LONG_VARCHAR, "DB_TYPE_LONG", "LONG",
                      NATIVE_TYPE_NUM_BYTES, 8, csfrm=1,
                      buffer_size_factor=2147483647)
DB_TYPE_LONG_NVARCHAR = DbType(DB_TYPE_NUM_LONG_NVARCHAR,
                               "DB_TYPE_LONG_NVARCHAR", "LONG NVARCHAR",
                               NATIVE_TYPE_NUM_BYTES, 8, csfrm=2,
                               buffer_size_factor=2147483647)
DB_TYPE_LONG_RAW = DbType(DB_TYPE_NUM_LONG_RAW, "DB_TYPE_LONG_RAW", "LONG RAW",
                          NATIVE_TYPE_NUM_BYTES, 24,
                          buffer_size_factor=2147483647)
DB_TYPE_NCHAR = DbType(DB_TYPE_NUM_NCHAR, "DB_TYPE_NCHAR", "NCHAR",
                       NATIVE_TYPE_NUM_BYTES, 96, 2000, csfrm=2,
                       buffer_size_factor=4)
DB_TYPE_NCLOB = DbType(DB_TYPE_NUM_NCLOB, "DB_TYPE_NCLOB", "NCLOB",
                       NATIVE_TYPE_NUM_LOB, 112, csfrm=2,
                       buffer_size_factor=112)
DB_TYPE_NUMBER = DbType(DB_TYPE_NUM_NUMBER, "DB_TYPE_NUMBER", "NUMBER",
                        NATIVE_TYPE_NUM_BYTES, 2, buffer_size_factor=22)
DB_TYPE_NVARCHAR = DbType(DB_TYPE_NUM_NVARCHAR, "DB_TYPE_NVARCHAR",
                          "NVARCHAR2", NATIVE_TYPE_NUM_BYTES, 1, 4000, csfrm=2,
                          buffer_size_factor=4)
DB_TYPE_OBJECT = DbType(DB_TYPE_NUM_OBJECT, "DB_TYPE_OBJECT", "OBJECT",
                        NATIVE_TYPE_NUM_OBJECT, 109)
DB_TYPE_RAW = DbType(DB_TYPE_NUM_RAW, "DB_TYPE_RAW", "RAW",
                     NATIVE_TYPE_NUM_BYTES, 23, 4000, buffer_size_factor=1)
DB_TYPE_ROWID = DbType(DB_TYPE_NUM_ROWID, "DB_TYPE_ROWID", "ROWID",
                       NATIVE_TYPE_NUM_ROWID, 11, buffer_size_factor=18)
DB_TYPE_TIMESTAMP = DbType(DB_TYPE_NUM_TIMESTAMP, "DB_TYPE_TIMESTAMP",
                           "TIMESTAMP", NATIVE_TYPE_NUM_TIMESTAMP, 180,
                           buffer_size_factor=11)
DB_TYPE_TIMESTAMP_LTZ = DbType(DB_TYPE_NUM_TIMESTAMP_LTZ,
                               "DB_TYPE_TIMESTAMP_LTZ",
                               "TIMESTAMP WITH LOCAL TIME ZONE",
                               NATIVE_TYPE_NUM_TIMESTAMP, 231,
                               buffer_size_factor=11)
DB_TYPE_TIMESTAMP_TZ = DbType(DB_TYPE_NUM_TIMESTAMP_TZ, "DB_TYPE_TIMESTAMP_TZ",
                              "TIMESTAMP WITH TIME ZONE",
                              NATIVE_TYPE_NUM_TIMESTAMP, 181,
                              buffer_size_factor=13)
DB_TYPE_UNKNOWN = DbType(DB_TYPE_NUM_UNKNOWN, "DB_TYPE_UNKNOWN", "UNKNOWN")
DB_TYPE_UROWID = DbType(DB_TYPE_NUM_UROWID, "DB_TYPE_UROWID", "UROWID",
                        NATIVE_TYPE_NUM_BYTES, 208)
DB_TYPE_VARCHAR = DbType(DB_TYPE_NUM_VARCHAR, "DB_TYPE_VARCHAR", "VARCHAR2",
                         NATIVE_TYPE_NUM_BYTES, 1, 4000, csfrm=1,
                         buffer_size_factor=4)
DB_TYPE_VECTOR = DbType(DB_TYPE_NUM_VECTOR, "DB_TYPE_VECTOR", "VECTOR",
                        NATIVE_TYPE_NUM_VECTOR, 127)
DB_TYPE_XMLTYPE = DbType(DB_TYPE_NUM_XMLTYPE, "DB_TYPE_XMLTYPE", "XMLTYPE",
                         NATIVE_TYPE_NUM_BYTES, 109, csfrm=1,
                         buffer_size_factor=2147483647)

# additional aliases
db_type_by_ora_name["DOUBLE PRECISION"] = DB_TYPE_NUMBER
db_type_by_ora_name["FLOAT"] = DB_TYPE_NUMBER
db_type_by_ora_name["INTEGER"] = DB_TYPE_NUMBER
db_type_by_ora_name["PL/SQL BOOLEAN"] = DB_TYPE_BOOLEAN
db_type_by_ora_name["PL/SQL BINARY INTEGER"] = DB_TYPE_BINARY_INTEGER
db_type_by_ora_name["PL/SQL PLS INTEGER"] = DB_TYPE_BINARY_INTEGER
db_type_by_ora_name["REAL"] = DB_TYPE_NUMBER
db_type_by_ora_name["SMALLINT"] = DB_TYPE_NUMBER
db_type_by_ora_name["TIMESTAMP WITH TZ"] = DB_TYPE_TIMESTAMP_TZ
db_type_by_ora_name["TIMESTAMP WITH LOCAL TZ"] = DB_TYPE_TIMESTAMP_LTZ

# DB API types
BINARY = ApiType("BINARY", DB_TYPE_RAW, DB_TYPE_LONG_RAW)
DATETIME = ApiType("DATETIME", DB_TYPE_DATE, DB_TYPE_TIMESTAMP,
                   DB_TYPE_TIMESTAMP_LTZ, DB_TYPE_TIMESTAMP_TZ)
NUMBER = ApiType("NUMBER", DB_TYPE_NUMBER, DB_TYPE_BINARY_DOUBLE,
                 DB_TYPE_BINARY_FLOAT, DB_TYPE_BINARY_INTEGER)
ROWID = ApiType("ROWID", DB_TYPE_ROWID, DB_TYPE_UROWID)
STRING = ApiType("STRING", DB_TYPE_VARCHAR, DB_TYPE_NVARCHAR, DB_TYPE_CHAR,
                 DB_TYPE_NCHAR, DB_TYPE_LONG)
