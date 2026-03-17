//-----------------------------------------------------------------------------
// Copyright (c) 2016, 2024, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpiOracleType.c
//   Implementation of variable types.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// definition of Oracle types (MUST be in same order as enumeration)
//-----------------------------------------------------------------------------
static const dpiOracleType
        dpiAllOracleTypes[DPI_ORACLE_TYPE_MAX - DPI_ORACLE_TYPE_NONE - 1] = {
    {
        DPI_ORACLE_TYPE_VARCHAR,            // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_CHR,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        0,                                  // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NVARCHAR,           // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_CHR,                       // internal Oracle type
        DPI_SQLCS_NCHAR,                    // charset form
        0,                                  // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_CHAR,               // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_AFC,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        0,                                  // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NCHAR,              // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_AFC,                       // internal Oracle type
        DPI_SQLCS_NCHAR,                    // charset form
        0,                                  // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_ROWID,              // public Oracle type
        DPI_NATIVE_TYPE_ROWID,              // default native type
        DPI_SQLT_RDD,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_RAW,                // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_BIN,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        0,                                  // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NATIVE_FLOAT,       // public Oracle type
        DPI_NATIVE_TYPE_FLOAT,              // default native type
        DPI_SQLT_BFLOAT,                    // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(float),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NATIVE_DOUBLE,      // public Oracle type
        DPI_NATIVE_TYPE_DOUBLE,             // default native type
        DPI_SQLT_BDOUBLE,                   // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(double),                     // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NATIVE_INT,         // public Oracle type
        DPI_NATIVE_TYPE_INT64,              // default native type
        DPI_SQLT_INT,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(int64_t),                    // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NUMBER,             // public Oracle type
        DPI_NATIVE_TYPE_DOUBLE,             // default native type
        DPI_SQLT_VNU,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        DPI_OCI_NUMBER_SIZE,                // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_DATE,               // public Oracle type
        DPI_NATIVE_TYPE_TIMESTAMP,          // default native type
        DPI_SQLT_ODT,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(dpiOciDate),                 // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_TIMESTAMP,          // public Oracle type
        DPI_NATIVE_TYPE_TIMESTAMP,          // default native type
        DPI_SQLT_TIMESTAMP,                 // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_TIMESTAMP_TZ,       // public Oracle type
        DPI_NATIVE_TYPE_TIMESTAMP,          // default native type
        DPI_SQLT_TIMESTAMP_TZ,              // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_TIMESTAMP_LTZ,      // public Oracle type
        DPI_NATIVE_TYPE_TIMESTAMP,          // default native type
        DPI_SQLT_TIMESTAMP_LTZ,             // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_INTERVAL_DS,        // public Oracle type
        DPI_NATIVE_TYPE_INTERVAL_DS,        // default native type
        DPI_SQLT_INTERVAL_DS,               // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_INTERVAL_YM,        // public Oracle type
        DPI_NATIVE_TYPE_INTERVAL_YM,        // default native type
        DPI_SQLT_INTERVAL_YM,               // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_CLOB,               // public Oracle type
        DPI_NATIVE_TYPE_LOB,                // default native type
        DPI_SQLT_CLOB,                      // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        1,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NCLOB,              // public Oracle type
        DPI_NATIVE_TYPE_LOB,                // default native type
        DPI_SQLT_CLOB,                      // internal Oracle type
        DPI_SQLCS_NCHAR,                    // charset form
        sizeof(void*),                      // buffer size
        1,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_BLOB,               // public Oracle type
        DPI_NATIVE_TYPE_LOB,                // default native type
        DPI_SQLT_BLOB,                      // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_BFILE,              // public Oracle type
        DPI_NATIVE_TYPE_LOB,                // default native type
        DPI_SQLT_BFILE,                     // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_STMT,               // public Oracle type
        DPI_NATIVE_TYPE_STMT,               // default native type
        DPI_SQLT_RSET,                      // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_BOOLEAN,            // public Oracle type
        DPI_NATIVE_TYPE_BOOLEAN,            // default native type
        DPI_SQLT_BOL,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(int),                        // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_OBJECT,             // public Oracle type
        DPI_NATIVE_TYPE_OBJECT,             // default native type
        DPI_SQLT_NTY,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_LONG_VARCHAR,       // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_CHR,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        DPI_MAX_BASIC_BUFFER_SIZE + 1,      // buffer size
        1,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_LONG_RAW,           // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_BIN,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        DPI_MAX_BASIC_BUFFER_SIZE + 1,      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_NATIVE_UINT,        // public Oracle type
        DPI_NATIVE_TYPE_UINT64,             // default native type
        DPI_SQLT_UIN,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(uint64_t),                   // buffer size
        0,                                  // is character data
        1,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_JSON,               // public Oracle type
        DPI_NATIVE_TYPE_JSON,               // default native type
        DPI_SQLT_JSON,                      // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_JSON_OBJECT,        // public Oracle type
        DPI_NATIVE_TYPE_JSON_OBJECT,        // default native type
        0,                                  // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(dpiJsonObject),              // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_JSON_ARRAY,         // public Oracle type
        DPI_NATIVE_TYPE_JSON_ARRAY,         // default native type
        0,                                  // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(dpiJsonArray),               // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_UROWID,             // public Oracle type
        DPI_NATIVE_TYPE_ROWID,              // default native type
        DPI_SQLT_RDD,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        1,                                  // is character data
        1,                                  // can be in array
        1                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_LONG_NVARCHAR,      // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_CHR,                       // internal Oracle type
        DPI_SQLCS_NCHAR,                    // charset form
        DPI_MAX_BASIC_BUFFER_SIZE + 1,      // buffer size
        1,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_XMLTYPE,            // public Oracle type
        DPI_NATIVE_TYPE_BYTES,              // default native type
        DPI_SQLT_CHR,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        DPI_MAX_BASIC_BUFFER_SIZE + 1,      // buffer size
        1,                                  // is character data
        0,                                  // can be in array
        0                                   // requires pre-fetch
    },
    {
        DPI_ORACLE_TYPE_VECTOR,             // public Oracle type
        DPI_NATIVE_TYPE_VECTOR,             // default native type
        DPI_SQLT_VEC,                       // internal Oracle type
        DPI_SQLCS_IMPLICIT,                 // charset form
        sizeof(void*),                      // buffer size
        0,                                  // is character data
        0,                                  // can be in array
        1                                   // requires pre-fetch
    },
};


//-----------------------------------------------------------------------------
// dpiOracleType__convertFromOracle() [INTERNAL]
//   Return a value from the dpiOracleTypeNum enumeration for the OCI data type
// and charset form. If the OCI data type is not supported, 0 is returned.
//-----------------------------------------------------------------------------
static dpiOracleTypeNum dpiOracleType__convertFromOracle(uint16_t typeCode,
        uint8_t charsetForm)
{
    switch(typeCode) {
        case DPI_SQLT_CHR:
        case DPI_SQLT_VCS:
            if (charsetForm == DPI_SQLCS_NCHAR)
                return DPI_ORACLE_TYPE_NVARCHAR;
            return DPI_ORACLE_TYPE_VARCHAR;
        case DPI_SQLT_INT:
        case DPI_SQLT_FLT:
        case DPI_SQLT_NUM:
        case DPI_SQLT_PDN:
        case DPI_SQLT_VNU:
        case DPI_SQLT_BFLOAT:
        case DPI_SQLT_BDOUBLE:
        case DPI_OCI_TYPECODE_SMALLINT:
            return DPI_ORACLE_TYPE_NUMBER;
        case DPI_SQLT_DAT:
        case DPI_SQLT_ODT:
            return DPI_ORACLE_TYPE_DATE;
        case DPI_SQLT_BIN:
        case DPI_SQLT_LVB:
            return DPI_ORACLE_TYPE_RAW;
        case DPI_SQLT_AFC:
            if (charsetForm == DPI_SQLCS_NCHAR)
                return DPI_ORACLE_TYPE_NCHAR;
            return DPI_ORACLE_TYPE_CHAR;
        case DPI_OCI_TYPECODE_BINARY_INTEGER:
        case DPI_OCI_TYPECODE_PLS_INTEGER:
            return DPI_ORACLE_TYPE_NATIVE_INT;
        case DPI_SQLT_IBFLOAT:
            return DPI_ORACLE_TYPE_NATIVE_FLOAT;
        case DPI_SQLT_IBDOUBLE:
            return DPI_ORACLE_TYPE_NATIVE_DOUBLE;
        case DPI_SQLT_DATE:
        case DPI_SQLT_TIMESTAMP:
            return DPI_ORACLE_TYPE_TIMESTAMP;
        case DPI_SQLT_TIMESTAMP_TZ:
            return DPI_ORACLE_TYPE_TIMESTAMP_TZ;
        case DPI_SQLT_TIMESTAMP_LTZ:
            return DPI_ORACLE_TYPE_TIMESTAMP_LTZ;
        case DPI_SQLT_NTY:
        case DPI_SQLT_REC:
        case DPI_SQLT_NCO:
            return DPI_ORACLE_TYPE_OBJECT;
        case DPI_SQLT_BOL:
            return DPI_ORACLE_TYPE_BOOLEAN;
        case DPI_SQLT_CLOB:
            if (charsetForm == DPI_SQLCS_NCHAR)
                return DPI_ORACLE_TYPE_NCLOB;
            return DPI_ORACLE_TYPE_CLOB;
        case DPI_SQLT_BLOB:
            return DPI_ORACLE_TYPE_BLOB;
        case DPI_SQLT_BFILE:
            return DPI_ORACLE_TYPE_BFILE;
        case DPI_SQLT_RDD:
        case DPI_OCI_TYPECODE_ROWID:
            return DPI_ORACLE_TYPE_ROWID;
        case DPI_SQLT_RSET:
            return DPI_ORACLE_TYPE_STMT;
        case DPI_SQLT_INTERVAL_DS:
            return DPI_ORACLE_TYPE_INTERVAL_DS;
        case DPI_SQLT_INTERVAL_YM:
            return DPI_ORACLE_TYPE_INTERVAL_YM;
        case DPI_SQLT_LNG:
        case DPI_OCI_TYPECODE_LONG:
            return DPI_ORACLE_TYPE_LONG_VARCHAR;
        case DPI_SQLT_LBI:
        case DPI_OCI_TYPECODE_LONG_RAW:
            return DPI_ORACLE_TYPE_LONG_RAW;
        case DPI_SQLT_JSON:
            return DPI_ORACLE_TYPE_JSON;
        case DPI_SQLT_VEC:
            return DPI_ORACLE_TYPE_VECTOR;
    }
    return (dpiOracleTypeNum) 0;
}


//-----------------------------------------------------------------------------
// dpiOracleType__getFromNum() [INTERNAL]
//   Return the type associated with the type number.
//-----------------------------------------------------------------------------
const dpiOracleType *dpiOracleType__getFromNum(dpiOracleTypeNum typeNum,
        dpiError *error)
{
    if (typeNum > DPI_ORACLE_TYPE_NONE && typeNum < DPI_ORACLE_TYPE_MAX)
        return &dpiAllOracleTypes[typeNum - DPI_ORACLE_TYPE_NONE - 1];
    dpiError__set(error, "check type", DPI_ERR_INVALID_ORACLE_TYPE, typeNum);
    return NULL;
}


//-----------------------------------------------------------------------------
// dpiOracleType__populateTypeInfo() [INTERNAL]
//   Populate dpiDataTypeInfo structure given an Oracle descriptor. Note that
// no error is raised by this function if the data type is not supported. This
// method is called for both implicit and explicit describes (which behave
// slightly differently).
//-----------------------------------------------------------------------------
int dpiOracleType__populateTypeInfo(dpiConn *conn, void *handle,
        uint32_t handleType, dpiDataTypeInfo *info, dpiError *error)
{
    const dpiOracleType *oracleType = NULL;
    uint8_t charsetForm, isJson, isOson;
    dpiNativeTypeNum nativeTypeNum;
    uint32_t dataTypeAttribute, i;
    void *listParam, *itemParam;
    dpiAnnotation *annotation;
    uint16_t ociSize;

    // acquire data type
    if (handleType == DPI_OCI_DTYPE_PARAM)
        dataTypeAttribute = DPI_OCI_ATTR_TYPECODE;
    else dataTypeAttribute = DPI_OCI_ATTR_DATA_TYPE;
    if (dpiOci__attrGet(handle, handleType, (void*) &info->ociTypeCode, 0,
            dataTypeAttribute, "get data type", error) < 0)
        return DPI_FAILURE;

    // acquire character set form
    if (info->ociTypeCode != DPI_SQLT_CHR &&
            info->ociTypeCode != DPI_SQLT_AFC &&
            info->ociTypeCode != DPI_SQLT_VCS &&
            info->ociTypeCode != DPI_SQLT_CLOB)
        charsetForm = DPI_SQLCS_IMPLICIT;
    else if (dpiOci__attrGet(handle, handleType, (void*) &charsetForm, 0,
            DPI_OCI_ATTR_CHARSET_FORM, "get charset form", error) < 0)
        return DPI_FAILURE;

    // convert Oracle type to ODPI-C enumerations, if possible
    info->oracleTypeNum = dpiOracleType__convertFromOracle(info->ociTypeCode,
            charsetForm);
    if (!info->oracleTypeNum)
        info->defaultNativeTypeNum = (dpiNativeTypeNum) 0;
    else {
        oracleType = dpiOracleType__getFromNum(info->oracleTypeNum, error);
        if (!oracleType)
            return DPI_FAILURE;
        info->defaultNativeTypeNum = oracleType->defaultNativeTypeNum;
    }

    // determine precision/scale
    nativeTypeNum = info->defaultNativeTypeNum;
    switch (nativeTypeNum) {
        case DPI_NATIVE_TYPE_DOUBLE:
        case DPI_NATIVE_TYPE_FLOAT:
        case DPI_NATIVE_TYPE_INT64:
        case DPI_NATIVE_TYPE_TIMESTAMP:
        case DPI_NATIVE_TYPE_INTERVAL_YM:
        case DPI_NATIVE_TYPE_INTERVAL_DS:
            if (dpiOci__attrGet(handle, handleType, (void*) &info->scale, 0,
                    DPI_OCI_ATTR_SCALE, "get scale", error) < 0)
                return DPI_FAILURE;
            if (dpiOci__attrGet(handle, handleType, (void*) &info->precision,
                    0, DPI_OCI_ATTR_PRECISION, "get precision", error) < 0)
                return DPI_FAILURE;
            if (nativeTypeNum == DPI_NATIVE_TYPE_TIMESTAMP ||
                    nativeTypeNum == DPI_NATIVE_TYPE_INTERVAL_DS) {
                info->fsPrecision = (uint8_t) info->scale;
                info->scale = 0;
            }
            break;
        default:
            info->precision = 0;
            info->fsPrecision = 0;
            info->scale = 0;
            break;
    }

    // change default type to integer if precision/scale supports it
    if (info->oracleTypeNum == DPI_ORACLE_TYPE_NUMBER && info->scale == 0 &&
            info->precision > 0 && info->precision <= DPI_MAX_INT64_PRECISION)
        info->defaultNativeTypeNum = DPI_NATIVE_TYPE_INT64;

    // acquire size (in bytes) of item
    info->sizeInChars = 0;
    if (oracleType && oracleType->sizeInBytes == 0) {
        if (dpiOci__attrGet(handle, handleType, (void*) &ociSize, 0,
                DPI_OCI_ATTR_DATA_SIZE, "get size (bytes)", error) < 0)
            return DPI_FAILURE;
        info->dbSizeInBytes = ociSize;
        info->clientSizeInBytes = ociSize;
    } else {
        info->dbSizeInBytes = 0;
        info->clientSizeInBytes = 0;
    }

    // acquire size (in characters) of item, if applicable
    if (oracleType && oracleType->isCharacterData &&
            oracleType->sizeInBytes == 0) {
        if (dpiOci__attrGet(handle, handleType, (void*) &ociSize, 0,
                DPI_OCI_ATTR_CHAR_SIZE, "get size (chars)", error) < 0)
            return DPI_FAILURE;
        info->sizeInChars = ociSize;
        if (charsetForm == DPI_SQLCS_NCHAR)
            info->clientSizeInBytes = info->sizeInChars *
                    conn->env->nmaxBytesPerCharacter;
        else if (conn->charsetId != conn->env->charsetId)
            info->clientSizeInBytes = info->sizeInChars *
                    conn->env->maxBytesPerCharacter;
    }

    // acquire object type, if applicable
    if (info->oracleTypeNum == DPI_ORACLE_TYPE_OBJECT) {
        if (dpiObjectType__allocate(conn, handle, handleType,
                &info->objectType, error) < 0)
            return DPI_FAILURE;
        if (dpiObjectType__isXmlType(info->objectType)) {
            dpiObjectType__free(info->objectType, error);
            info->objectType = NULL;
            info->ociTypeCode = DPI_SQLT_CHR;
            info->oracleTypeNum = DPI_ORACLE_TYPE_XMLTYPE;
            info->defaultNativeTypeNum = DPI_NATIVE_TYPE_BYTES;
        }
    }

    // determine if the data refers to a JSON column
    if (handleType == DPI_OCI_HTYPE_DESCRIBE &&
            conn->env->versionInfo->versionNum >= 19) {
        if (dpiOci__attrGet(handle, handleType, (void*) &isJson, 0,
                DPI_OCI_ATTR_JSON_COL, "get is JSON column", error) < 0)
            return DPI_FAILURE;
        info->isJson = isJson;
    }

    // determine if the data refers to an OSON column
    if (handleType == DPI_OCI_HTYPE_DESCRIBE &&
            conn->env->versionInfo->versionNum >= 21) {
        if (dpiOci__attrGet(handle, handleType, (void*) &isOson, 0,
                DPI_OCI_ATTR_OSON_COL, "get is OSON column", error) < 0)
            return DPI_FAILURE;
        info->isOson = isOson;
    }

    // get domain and annotations, if applicable
    if (handleType == DPI_OCI_HTYPE_DESCRIBE &&
            conn->env->versionInfo->versionNum >= 23) {

        // check for domain
        if (dpiOci__attrGet(handle, handleType, (void*) &info->domainSchema,
                &info->domainSchemaLength, DPI_OCI_ATTR_DOMAIN_SCHEMA,
                "get domain schema", error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrGet(handle, handleType, (void*) &info->domainName,
                &info->domainNameLength, DPI_OCI_ATTR_DOMAIN_NAME,
                "get domain name", error) < 0)
            return DPI_FAILURE;

        // check for annotations
        if (dpiOci__attrGet(handle, handleType, (void*) &info->numAnnotations,
                0, DPI_OCI_ATTR_NUM_ANNOTATIONS, "get number of annotations",
                error) < 0)
            return DPI_FAILURE;
        if (info->numAnnotations > 0) {

            // allocate memory for the array
            if (dpiUtils__allocateMemory(info->numAnnotations,
                    sizeof(dpiAnnotation), 1, "allocate annotation array",
                    (void**) &info->annotations, error) < 0)
                return DPI_FAILURE;

            // get the list parameter
            if (dpiOci__attrGet(handle, handleType,
                    (void*) &listParam, 0, DPI_OCI_ATTR_LIST_ANNOTATIONS,
                    "get annotation list param", error) < 0)
                return DPI_FAILURE;

            // populate the arrays
            for (i = 0 ; i < info->numAnnotations; i++) {
                annotation = &info->annotations[i];
                if (dpiOci__paramGet(listParam, DPI_OCI_DTYPE_PARAM,
                        &itemParam, (uint32_t) i + 1, "get annotation",
                        error) < 0)
                    return DPI_FAILURE;
                if (dpiOci__attrGet(itemParam, DPI_OCI_DTYPE_PARAM,
                        (void*) &annotation->key, &annotation->keyLength,
                        DPI_OCI_ATTR_ANNOTATION_KEY,
                        "get annotation key", error) < 0)
                    return DPI_FAILURE;
                if (dpiOci__attrGet(itemParam, DPI_OCI_DTYPE_PARAM,
                        (void*) &annotation->value, &annotation->valueLength,
                        DPI_OCI_ATTR_ANNOTATION_VALUE,
                        "get annotation value", error) < 0)
                    return DPI_FAILURE;
            }

        }

    }

    // get vector metadata, if applicable
    if (info->oracleTypeNum == DPI_ORACLE_TYPE_VECTOR) {

        // get vector format
        if (dpiOci__attrGet(handle, handleType, &info->vectorFormat, 0,
                DPI_OCI_ATTR_VECTOR_DATA_FORMAT, "get vector column format",
                error) < 0)
            return DPI_FAILURE;

        // get number of dimensions
        if (dpiOci__attrGet(handle, handleType, &info->vectorDimensions, 0,
                DPI_OCI_ATTR_VECTOR_DIMENSION,
                "get number of vector dimensions in column", error) < 0)
            return DPI_FAILURE;

        // get vector column flags
        if (dpiOci__attrGet(handle, handleType, &info->vectorFlags, 0,
                DPI_OCI_ATTR_VECTOR_PROPERTY, "get vector column flags",
                error) < 0)
            return DPI_FAILURE;

    }

    return DPI_SUCCESS;
}
