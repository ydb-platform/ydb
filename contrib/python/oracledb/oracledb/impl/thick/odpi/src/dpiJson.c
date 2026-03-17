//-----------------------------------------------------------------------------
// Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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
// dpiJson.c
//   Implementation of JSON manipulation routines.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

// define number of nodes which are processed in each batch
#define DPI_JSON_BATCH_NODES            64

// define batch size of space allocated for temporary buffers used for
// converting numbers to text
#define DPI_JSON_TEMP_BUFFER_SIZE       1024

// forward declarations of internal functions only used in this file
static int dpiJsonNode__fromOracleArrayToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error);
static int dpiJsonNode__fromOracleObjectToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error);
static int dpiJsonNode__fromOracleScalarToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error);
static int dpiJsonNode__fromOracleToNative(dpiJson *json, dpiJsonNode *node,
        dpiJznDomDoc *domDoc, void *oracleNode, uint32_t options,
        dpiError *error);
static int dpiJsonNode__toOracleFromNative(dpiJson *json,
        const dpiJsonNode *node, dpiJznDomDoc *domDoc, void **oracleNode,
        dpiError *error);


//-----------------------------------------------------------------------------
// dpiJson__allocate() [INTERNAL]
//   Allocate and initialize a JSON object.
//-----------------------------------------------------------------------------
int dpiJson__allocate(dpiConn *conn, void *handle, dpiJson **json,
        dpiError *error)
{
    dpiJson *tempJson;

    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 21, 0, error) < 0)
        return DPI_FAILURE;
    if (dpiGen__allocate(DPI_HTYPE_JSON, conn->env, (void**) &tempJson,
            error) < 0)
        return DPI_FAILURE;
    dpiGen__setRefCount(conn, error, 1);
    tempJson->conn = conn;
    if (handle) {
        tempJson->handle = handle;
    } else {
        if (dpiOci__descriptorAlloc(conn->env->handle, &tempJson->handle,
                DPI_OCI_DTYPE_JSON, "allocate JSON descriptor", error) < 0) {
            dpiJson__free(tempJson, error);
            return DPI_FAILURE;
        }
        tempJson->handleIsOwned = 1;
    }
    tempJson->topNode.value = &tempJson->topNodeBuffer;
    tempJson->topNode.oracleTypeNum = DPI_ORACLE_TYPE_NONE;
    tempJson->topNode.nativeTypeNum = DPI_NATIVE_TYPE_NULL;

    *json = tempJson;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__fromOracleArrayToNative() [INTERNAL]
//   Populate an array node from the Oracle JSON node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__fromOracleArrayToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error)
{
    void *oracleElementNodes[DPI_JSON_BATCH_NODES];
    uint32_t pos, i, numInBatch;
    dpiJsonNode *elementNode;
    dpiJsonArray *array;

    // define node types
    node->oracleTypeNum = DPI_ORACLE_TYPE_JSON_ARRAY;
    node->nativeTypeNum = DPI_NATIVE_TYPE_JSON_ARRAY;

    // determine number of elements in the array; if no elements exist in the
    // array, nothing further needs to be done at this point
    array = &node->value->asJsonArray;
    array->numElements = (*domDoc->methods->fnGetArraySize)(domDoc,
            oracleNode);
    if (array->numElements == 0)
        return DPI_SUCCESS;

    // allocate memory
    if (dpiUtils__allocateMemory(array->numElements, sizeof(dpiJsonNode), 1,
            "allocate JSON array element nodes", (void**) &array->elements,
            error) < 0)
        return DPI_FAILURE;
    if (dpiUtils__allocateMemory(array->numElements, sizeof(dpiDataBuffer), 1,
            "allocate JSON array element values",
            (void**) &array->elementValues, error) < 0)
        return DPI_FAILURE;

    // process all of the nodes in the array in batches
    pos = 0;
    while (pos < array->numElements) {
        numInBatch = (*domDoc->methods->fnGetArrayElemBatch)(domDoc,
                oracleNode, pos, DPI_JSON_BATCH_NODES,
                oracleElementNodes);
        for (i = 0; i < numInBatch; i++) {
            elementNode = &array->elements[pos + i];
            elementNode->value = &array->elementValues[pos + i];
            if (dpiJsonNode__fromOracleToNative(json, elementNode, domDoc,
                    oracleElementNodes[i], options, error) < 0)
                return DPI_FAILURE;
        }
        pos += numInBatch;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__fromOracleObjectToNative() [INTERNAL]
//   Populate an object node from the Oracle JSON node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__fromOracleObjectToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error)
{
    dpiJznDomNameValuePair nameValuePairs[DPI_JSON_BATCH_NODES];
    uint32_t pos, i, numInBatch;
    dpiJsonNode *fieldNode;
    dpiJsonObject *obj;

    // define node types
    node->oracleTypeNum = DPI_ORACLE_TYPE_JSON_OBJECT;
    node->nativeTypeNum = DPI_NATIVE_TYPE_JSON_OBJECT;

    // determine number of fields in the object; if no fields exist in the
    // object, nothing further needs to be done at this point
    obj = &node->value->asJsonObject;
    obj->numFields = (*domDoc->methods->fnGetNumObjField)(domDoc, oracleNode);
    if (obj->numFields == 0)
        return DPI_SUCCESS;

    // allocate memory
    if (dpiUtils__allocateMemory(obj->numFields, sizeof(char*), 1,
            "allocate JSON object field names", (void**) &obj->fieldNames,
            error) < 0)
        return DPI_FAILURE;
    if (dpiUtils__allocateMemory(obj->numFields, sizeof(uint32_t), 1,
            "allocate JSON object field name lengths",
            (void**) &obj->fieldNameLengths, error) < 0)
        return DPI_FAILURE;
    if (dpiUtils__allocateMemory(obj->numFields, sizeof(dpiJsonNode), 1,
            "allocate JSON object field nodes", (void**) &obj->fields,
            error) < 0)
        return DPI_FAILURE;
    if (dpiUtils__allocateMemory(obj->numFields, sizeof(dpiDataBuffer), 1,
            "allocate JSON object field values", (void**) &obj->fieldValues,
            error) < 0)
        return DPI_FAILURE;

    // process all of the nodes in the object in batches
    pos = 0;
    while (pos < obj->numFields) {
        numInBatch = (*domDoc->methods->fnGetFieldNamesAndValsBatch)(domDoc,
                oracleNode, pos, DPI_JSON_BATCH_NODES, nameValuePairs);
        for (i = 0; i < numInBatch; i++) {
            obj->fieldNames[pos + i] = nameValuePairs[i].name.ptr;
            obj->fieldNameLengths[pos + i] = nameValuePairs[i].name.length;
            fieldNode = &obj->fields[pos + i];
            fieldNode->value = &obj->fieldValues[pos + i];
            if (dpiJsonNode__fromOracleToNative(json, fieldNode, domDoc,
                    nameValuePairs[i].value, options, error) < 0)
                return DPI_FAILURE;
        }
        pos += numInBatch;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__fromOracleNumberAsText() [INTERNAL]
//   Populate a scalar number as a text buffer. Temporary buffers are allocated
// as needed and an array of these are stored in the JSON object. Each
// temporary buffer contains as many numbers as can be fit before a new
// temporary buffer is allocated.
//-----------------------------------------------------------------------------
static int dpiJsonNode__fromOracleNumberAsText(dpiJson *json,
        dpiJsonNode *node, uint8_t *numBuffer, dpiError *error)
{
    char **tempBuffers;

    // if there is no room in the current temporary buffer, allocate a new one
    if (json->tempBufferUsed + DPI_NUMBER_AS_TEXT_CHARS <
            DPI_JSON_TEMP_BUFFER_SIZE) {

        // if no room is available in the temp buffer array, increase the size
        // of the array
        if (json->numTempBuffers == json->allocatedTempBuffers) {
            json->allocatedTempBuffers += 16;
            if (dpiUtils__allocateMemory(json->allocatedTempBuffers,
                    sizeof(char*), 0, "allocate JSON temp buffer array",
                    (void**) &tempBuffers, error) < 0)
                return DPI_FAILURE;
            if (json->numTempBuffers > 0) {
                memcpy(tempBuffers, json->tempBuffers,
                        json->numTempBuffers * sizeof(char*));
                dpiUtils__freeMemory(json->tempBuffers);
            }
            json->tempBuffers = tempBuffers;
        }

        // allocate a new temporary buffer
        if (dpiUtils__allocateMemory(1, DPI_JSON_TEMP_BUFFER_SIZE, 0,
                "allocate JSON temp buffer",
                (void**) &json->tempBuffers[json->numTempBuffers], error) < 0)
            return DPI_FAILURE;
        json->numTempBuffers++;
        json->tempBufferUsed = 0;

    }

    // perform the conversion
    node->value->asBytes.ptr = json->tempBuffers[json->numTempBuffers - 1] +
            json->tempBufferUsed;
    node->value->asBytes.length = DPI_NUMBER_AS_TEXT_CHARS;
    return dpiDataBuffer__fromOracleNumberAsText(node->value, json->env,
            error, numBuffer);
}


//-----------------------------------------------------------------------------
// dpiJsonNode__fromOracleScalarToNative() [INTERNAL]
//   Populate a scalar node from the Oracle JSON node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__fromOracleScalarToNative(dpiJson *json,
        dpiJsonNode *node, dpiJznDomDoc *domDoc, void *oracleNode,
        uint32_t options, dpiError *error)
{
    dpiJznDomScalar scalar;
    dpiJsonOciVal ociVal;

    (*domDoc->methods->fnGetScalarInfoOci)(domDoc, oracleNode, &scalar,
            &ociVal);
    switch (scalar.valueType) {
        case DPI_JZNVAL_STRING:
            node->oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR;
            node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
            node->value->asBytes.ptr = scalar.value.asBytes.value;
            node->value->asBytes.length = scalar.value.asBytes.valueLength;
            return DPI_SUCCESS;
        case DPI_JZNVAL_BINARY:
            node->oracleTypeNum = DPI_ORACLE_TYPE_RAW;
            node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
            node->value->asBytes.ptr = scalar.value.asBytes.value;
            node->value->asBytes.length = scalar.value.asBytes.valueLength;
            return DPI_SUCCESS;
        case DPI_JZNVAL_ID:
            node->oracleTypeNum = (json->env->context->useJsonId) ?
                    DPI_ORACLE_TYPE_JSON_ID : DPI_ORACLE_TYPE_RAW;
            node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
            node->value->asBytes.ptr = scalar.value.asBytes.value;
            node->value->asBytes.length = scalar.value.asBytes.valueLength;
            return DPI_SUCCESS;
        case DPI_JZNVAL_FLOAT:
            node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
            node->nativeTypeNum = DPI_NATIVE_TYPE_FLOAT;
            node->value->asFloat = scalar.value.asFloat.value;
            return DPI_SUCCESS;
        case DPI_JZNVAL_DOUBLE:
            node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
            node->nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE;
            node->value->asDouble = scalar.value.asDouble.value;
            return DPI_SUCCESS;
        case DPI_JZNVAL_ORA_SIGNED_INT:
        case DPI_JZNVAL_ORA_SIGNED_LONG:
        case DPI_JZNVAL_ORA_DECIMAL128:
        case DPI_JZNVAL_ORA_NUMBER:
            if (scalar.valueType != DPI_JZNVAL_ORA_NUMBER) {
                ociVal.asJsonNumber[0] =
                        (uint8_t) scalar.value.asOciVal.valueLength;
                memcpy(&ociVal.asJsonNumber[1], scalar.value.asOciVal.value,
                        scalar.value.asOciVal.valueLength);
            }
            node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
            if (options & DPI_JSON_OPT_NUMBER_AS_STRING) {
                node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
                return dpiJsonNode__fromOracleNumberAsText(json, node,
                        ociVal.asJsonNumber, error);
            }
            node->nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE;
            return dpiDataBuffer__fromOracleNumberAsDouble(node->value, error,
                    ociVal.asJsonNumber);
        case DPI_JZNVAL_ORA_DATE:
        case DPI_JZNVAL_ORA_TIMESTAMP:
        case DPI_JZNVAL_ORA_TIMESTAMPTZ:
            node->oracleTypeNum = (scalar.valueType == DPI_JZNVAL_ORA_DATE) ?
                    DPI_ORACLE_TYPE_DATE : DPI_ORACLE_TYPE_TIMESTAMP;
            if (options & DPI_JSON_OPT_DATE_AS_DOUBLE) {
                node->nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE;
                if (dpiDataBuffer__fromOracleDateAsDouble(node->value,
                        json->env, error,
                        (dpiOciDate*) &ociVal.asJsonDateTime) < 0)
                    return DPI_FAILURE;
                node->value->asDouble +=
                        (ociVal.asJsonDateTime.fsecond / 1000000);
                return DPI_SUCCESS;
            }
            node->nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP;
            node->value->asTimestamp.year = ociVal.asJsonDateTime.year;
            node->value->asTimestamp.month = ociVal.asJsonDateTime.month;
            node->value->asTimestamp.day = ociVal.asJsonDateTime.day;
            node->value->asTimestamp.hour = ociVal.asJsonDateTime.hour;
            node->value->asTimestamp.minute = ociVal.asJsonDateTime.minute;
            node->value->asTimestamp.second = ociVal.asJsonDateTime.second;
            node->value->asTimestamp.fsecond = ociVal.asJsonDateTime.fsecond;
            node->value->asTimestamp.tzHourOffset =
                    ociVal.asJsonDateTime.tzHourOffset;
            node->value->asTimestamp.tzMinuteOffset =
                    ociVal.asJsonDateTime.tzMinuteOffset;
            return DPI_SUCCESS;
        case DPI_JZNVAL_ORA_DAYSECOND_DUR:
            node->oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_DS;
            node->nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_DS;
            node->value->asIntervalDS.days = ociVal.asJsonDayInterval.days;
            node->value->asIntervalDS.hours = ociVal.asJsonDayInterval.hours;
            node->value->asIntervalDS.minutes =
                    ociVal.asJsonDayInterval.minutes;
            node->value->asIntervalDS.seconds =
                    ociVal.asJsonDayInterval.seconds;
            node->value->asIntervalDS.fseconds =
                    ociVal.asJsonDayInterval.fseconds;
            return DPI_SUCCESS;
        case DPI_JZNVAL_ORA_YEARMONTH_DUR:
            node->oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_YM;
            node->nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_YM;
            node->value->asIntervalYM.years = ociVal.asJsonYearInterval.years;
            node->value->asIntervalYM.months =
                    ociVal.asJsonYearInterval.months;
            return DPI_SUCCESS;
        case DPI_JZNVAL_FALSE:
        case DPI_JZNVAL_TRUE:
            node->oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN;
            node->nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN;
            node->value->asBoolean = (scalar.valueType == DPI_JZNVAL_TRUE);
            return DPI_SUCCESS;
        case DPI_JZNVAL_NULL:
            node->oracleTypeNum = DPI_ORACLE_TYPE_NONE;
            node->nativeTypeNum = DPI_NATIVE_TYPE_NULL;
            return DPI_SUCCESS;
        case DPI_JZNVAL_VECTOR:
            node->oracleTypeNum = DPI_ORACLE_TYPE_VECTOR;
            node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
            node->value->asBytes.ptr = scalar.value.asBytes.value;
            node->value->asBytes.length = scalar.value.asBytes.valueLength;
            return DPI_SUCCESS;
        default:
            break;
    }

    return dpiError__set(error, "populate scalar node from Oracle",
            DPI_ERR_UNHANDLED_JSON_SCALAR_TYPE, scalar.valueType);
}


//-----------------------------------------------------------------------------
// dpiJsonNode__fromOracleToNative() [INTERNAL]
//   Populate the JSON node structure from the Oracle JSON node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__fromOracleToNative(dpiJson *json, dpiJsonNode *node,
        dpiJznDomDoc *domDoc, void *oracleNode, uint32_t options,
        dpiError *error)
{
    int nodeType;

    nodeType = (*domDoc->methods->fnGetNodeType)(domDoc, oracleNode);
    switch (nodeType) {
        case DPI_JZNDOM_ARRAY:
            return dpiJsonNode__fromOracleArrayToNative(json, node, domDoc,
                    oracleNode, options, error);
        case DPI_JZNDOM_OBJECT:
            return dpiJsonNode__fromOracleObjectToNative(json, node, domDoc,
                    oracleNode, options, error);
        case DPI_JZNDOM_SCALAR:
            return dpiJsonNode__fromOracleScalarToNative(json, node, domDoc,
                    oracleNode, options, error);
        default:
            break;
    }
    return dpiError__set(error, "from Oracle to native node",
            DPI_ERR_UNHANDLED_JSON_NODE_TYPE, nodeType);
}


//-----------------------------------------------------------------------------
// dpiJsonNode__toOracleArrayFromNative() [INTERNAL]
//   Populate an Oracle array node from the JSON array node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__toOracleArrayFromNative(dpiJson *json,
        dpiJsonArray *array, dpiJznDomDoc *domDoc, void **oracleNode,
        dpiError *error)
{
    dpiJsonNode *childNode;
    void *oracleChildNode;
    uint32_t i;

    *oracleNode = domDoc->methods->fnNewArray(domDoc, array->numElements);
    for (i = 0; i < array->numElements; i++) {
        childNode = &array->elements[i];
        if (dpiJsonNode__toOracleFromNative(json, childNode, domDoc,
                &oracleChildNode, error) < 0) {
            domDoc->methods->fnFreeNode(domDoc, *oracleNode);
            return DPI_FAILURE;
        }
        domDoc->methods->fnAppendItem(domDoc, *oracleNode, oracleChildNode);
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__toOracleObjectFromNative() [INTERNAL]
//   Populate an Oracle object node from the JSON object node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__toOracleObjectFromNative(dpiJson *json,
        dpiJsonObject *obj, dpiJznDomDoc *domDoc, void **oracleNode,
        dpiError *error)
{
    dpiJsonNode *childNode;
    void *oracleChildNode;
    uint32_t i;

    *oracleNode = domDoc->methods->fnNewObject(domDoc, obj->numFields);
    for (i = 0; i < obj->numFields; i++) {
        childNode = &obj->fields[i];
        if (dpiJsonNode__toOracleFromNative(json, childNode, domDoc,
                &oracleChildNode, error) < 0) {
            domDoc->methods->fnFreeNode(domDoc, *oracleNode);
            return DPI_FAILURE;
        }
        domDoc->methods->fnPutFieldValue(domDoc, *oracleNode,
                obj->fieldNames[i], (uint16_t) obj->fieldNameLengths[i],
                oracleChildNode);
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__toOracleFromNative() [INTERNAL]
//   Create an Oracle node from the native node.
//-----------------------------------------------------------------------------
static int dpiJsonNode__toOracleFromNative(dpiJson *json,
        const dpiJsonNode *node, dpiJznDomDoc *domDoc, void **oracleNode,
        dpiError *error)
{
    dpiOracleDataBuffer dataBuffer;
    int scalarType;

    switch (node->oracleTypeNum) {
        case DPI_ORACLE_TYPE_JSON_ARRAY:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_JSON_ARRAY) {
                return dpiJsonNode__toOracleArrayFromNative(json,
                        &node->value->asJsonArray, domDoc, oracleNode, error);
            }
            break;
        case DPI_ORACLE_TYPE_JSON_OBJECT:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_JSON_OBJECT) {
                return dpiJsonNode__toOracleObjectFromNative(json,
                        &node->value->asJsonObject, domDoc, oracleNode, error);
            }
            break;
        case DPI_ORACLE_TYPE_NUMBER:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                if (dpiDataBuffer__toOracleNumberFromDouble(node->value,
                        error, &dataBuffer.asNumber) < 0)
                    return DPI_FAILURE;
            } else if (node->nativeTypeNum == DPI_NATIVE_TYPE_INT64) {
                if (dpiDataBuffer__toOracleNumberFromInteger(node->value,
                        error, &dataBuffer.asNumber) < 0)
                    return DPI_FAILURE;
            } else if (node->nativeTypeNum == DPI_NATIVE_TYPE_UINT64) {
                if (dpiDataBuffer__toOracleNumberFromUnsignedInteger(
                        node->value, error, &dataBuffer.asNumber) < 0)
                    return DPI_FAILURE;
            } else if (node->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                if (dpiDataBuffer__toOracleNumberFromText(node->value,
                        json->env, error, &dataBuffer.asNumber) < 0)
                    return DPI_FAILURE;
            } else {
                break;
            }
            *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                    DPI_JZNVAL_OCI_NUMBER, &dataBuffer.asNumber);
            return DPI_SUCCESS;
        case DPI_ORACLE_TYPE_NATIVE_DOUBLE:
            if (node->nativeTypeNum != DPI_NATIVE_TYPE_DOUBLE)
                break;
            *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                    DPI_JZNVAL_DOUBLE, node->value->asDouble);
            return DPI_SUCCESS;
        case DPI_ORACLE_TYPE_NATIVE_FLOAT:
            if (node->nativeTypeNum != DPI_NATIVE_TYPE_FLOAT)
                break;
            *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                    DPI_JZNVAL_FLOAT, node->value->asFloat);
            return DPI_SUCCESS;
        case DPI_ORACLE_TYPE_RAW:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_BINARY, node->value->asBytes.ptr,
                        node->value->asBytes.length);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_JSON_ID:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_ID, node->value->asBytes.ptr,
                        node->value->asBytes.length);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_VARCHAR:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_STRING, node->value->asBytes.ptr,
                        node->value->asBytes.length);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_DATE:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_TIMESTAMP) {
                if (dpiDataBuffer__toOracleDate(node->value,
                        &dataBuffer.asDate) < 0)
                    return DPI_FAILURE;
            } else if (node->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                if (dpiDataBuffer__toOracleDateFromDouble(node->value,
                        json->env, error, &dataBuffer.asDate) < 0)
                    return DPI_FAILURE;
            } else {
                break;
            }
            *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                    DPI_JZNVAL_OCI_DATE, &dataBuffer.asDate);
            return DPI_SUCCESS;
        case DPI_ORACLE_TYPE_TIMESTAMP:
            if (!json->convTimestamp) {
                if (dpiOci__descriptorAlloc(json->env->handle,
                        &json->convTimestamp, DPI_OCI_DTYPE_TIMESTAMP,
                        "alloc timestamp for JSON", error) < 0)
                    return DPI_FAILURE;
            }
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_TIMESTAMP) {
                if (dpiDataBuffer__toOracleTimestamp(node->value, json->env,
                        error, json->convTimestamp, 0) < 0)
                    return DPI_FAILURE;
            } else if (node->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                if (dpiDataBuffer__toOracleTimestampFromDouble(node->value,
                        node->oracleTypeNum, json->env, error,
                        json->convTimestamp) < 0)
                    return DPI_FAILURE;
            } else {
                break;
            }
            *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                    DPI_JZNVAL_OCI_DATETIME, json->convTimestamp);
            return DPI_SUCCESS;
        case DPI_ORACLE_TYPE_INTERVAL_DS:
            if (!json->convIntervalDS) {
                if (dpiOci__descriptorAlloc(json->env->handle,
                        &json->convIntervalDS, DPI_OCI_DTYPE_INTERVAL_DS,
                        "alloc interval DS for JSON", error) < 0)
                    return DPI_FAILURE;
            }
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_INTERVAL_DS) {
                if (dpiDataBuffer__toOracleIntervalDS(node->value, json->env,
                        error, json->convIntervalDS) < 0)
                    return DPI_FAILURE;
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_OCI_INTERVAL, json->convIntervalDS);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_INTERVAL_YM:
            if (!json->convIntervalYM) {
                if (dpiOci__descriptorAlloc(json->env->handle,
                        &json->convIntervalYM, DPI_OCI_DTYPE_INTERVAL_YM,
                        "alloc interval YM for JSON", error) < 0)
                    return DPI_FAILURE;
            }
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_INTERVAL_YM) {
                if (dpiDataBuffer__toOracleIntervalYM(node->value, json->env,
                        error, json->convIntervalYM) < 0)
                    return DPI_FAILURE;
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_OCI_INTERVAL, json->convIntervalYM);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_BOOLEAN:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_BOOLEAN) {
                scalarType = (node->value->asBoolean) ?
                        DPI_JZNVAL_TRUE: DPI_JZNVAL_FALSE;
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        scalarType, NULL);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_VECTOR:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_VECTOR, node->value->asBytes.ptr,
                        node->value->asBytes.length);
                return DPI_SUCCESS;
            }
            break;
        case DPI_ORACLE_TYPE_NONE:
            if (node->nativeTypeNum == DPI_NATIVE_TYPE_NULL) {
                *oracleNode = domDoc->methods->fnNewScalarVal(domDoc,
                        DPI_JZNVAL_NULL, NULL);
                return DPI_SUCCESS;
            }
            break;
    }
    *oracleNode = NULL;
    return dpiError__set(error, "from native to Oracle node",
            DPI_ERR_UNHANDLED_CONVERSION_TO_JSON, node->nativeTypeNum,
            node->oracleTypeNum);
}


//-----------------------------------------------------------------------------
// dpiJson__setValue() [INTERNAL]
//   Sets the value of the JSON object, given a hierarchy of nodes.
//-----------------------------------------------------------------------------
int dpiJson__setValue(dpiJson *json, const dpiJsonNode *topNode,
        dpiError *error)
{
    const char *dummyValue = "0";
    dpiJznDomDoc *domDoc;
    void *oracleTopNode;
    int mutable = 1;

    // first, set the JSON descriptor as mutable
    if (dpiOci__attrSet(json->handle, DPI_OCI_DTYPE_JSON,
            (void*) &mutable, 0, DPI_OCI_ATTR_JSON_DOM_MUTABLE,
            "set JSON descriptor mutable", error) < 0)
        return DPI_FAILURE;

    // write a dummy value to the JSON descriptor
    if (dpiOci__jsonTextBufferParse(json, dummyValue, strlen(dummyValue), 0,
            error) < 0)
        return DPI_FAILURE;

    // acquire the DOM doc which will be used to create the Oracle nodes
    if (dpiOci__jsonDomDocGet(json, &domDoc, error) < 0)
        return DPI_FAILURE;

    // convert the top node (and all of the child nodes to Oracle nodes)
    if (dpiJsonNode__toOracleFromNative(json, topNode, domDoc, &oracleTopNode,
            error) < 0)
        return DPI_FAILURE;

    // set the top node
    domDoc->methods->fnSetRootNode(domDoc, oracleTopNode);

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiJsonNode__free() [INTERNAL]
//   Free the buffers allocated for the JSON node.
//-----------------------------------------------------------------------------
static void dpiJsonNode__free(dpiJsonNode *node)
{
    dpiJsonArray *array;
    dpiJsonObject *obj;
    uint32_t i;

    if (node->oracleTypeNum == DPI_ORACLE_TYPE_JSON_ARRAY) {
        array = &node->value->asJsonArray;
        if (array->elements) {
            for (i = 0; i < array->numElements; i++) {
                if (array->elements[i].value)
                    dpiJsonNode__free(&array->elements[i]);
            }
            dpiUtils__freeMemory(array->elements);
            array->elements = NULL;
        }
        if (array->elementValues) {
            dpiUtils__freeMemory(array->elementValues);
            array->elementValues = NULL;
        }
    } else if (node->oracleTypeNum == DPI_ORACLE_TYPE_JSON_OBJECT) {
        obj = &node->value->asJsonObject;
        if (obj->fields) {
            for (i = 0; i < obj->numFields; i++) {
                if (obj->fields[i].value)
                    dpiJsonNode__free(&obj->fields[i]);
            }
            dpiUtils__freeMemory(obj->fields);
            obj->fields = NULL;
        }
        if (obj->fieldNames) {
            dpiUtils__freeMemory(obj->fieldNames);
            obj->fieldNames = NULL;
        }
        if (obj->fieldNameLengths) {
            dpiUtils__freeMemory(obj->fieldNameLengths);
            obj->fieldNameLengths = NULL;
        }
        if (obj->fieldValues) {
            dpiUtils__freeMemory(obj->fieldValues);
            obj->fieldValues = NULL;
        }
    }
}


//-----------------------------------------------------------------------------
// dpiJson__free() [INTERNAL]
//   Free the buffers allocated for the JSON value and all of its nodes, if
// applicable.
//-----------------------------------------------------------------------------
void dpiJson__free(dpiJson *json, dpiError *error)
{
    uint32_t i;

    if (json->handle && json->handleIsOwned) {
        dpiOci__descriptorFree(json->handle, DPI_OCI_DTYPE_JSON);
        json->handle = NULL;
    }
    if (json->conn) {
        dpiGen__setRefCount(json->conn, error, -1);
        json->conn = NULL;
    }
    if (json->tempBuffers) {
        for (i = 0; i < json->numTempBuffers; i++)
            dpiUtils__freeMemory(json->tempBuffers[i]);
        dpiUtils__freeMemory(json->tempBuffers);
        json->tempBuffers = NULL;
    }
    if (json->convTimestamp) {
        dpiOci__descriptorFree(json->convTimestamp, DPI_OCI_DTYPE_TIMESTAMP);
        json->convTimestamp = NULL;
    }
    if (json->convIntervalDS) {
        dpiOci__descriptorFree(json->convIntervalDS,
                DPI_OCI_DTYPE_INTERVAL_DS);
        json->convIntervalDS = NULL;
    }
    if (json->convIntervalYM) {
        dpiOci__descriptorFree(json->convIntervalYM,
                DPI_OCI_DTYPE_INTERVAL_YM);
        json->convIntervalYM = NULL;
    }
    dpiJsonNode__free(&json->topNode);
    dpiUtils__freeMemory(json);
}


//-----------------------------------------------------------------------------
// dpiJson_addRef() [PUBLIC]
//   Add a reference to the JSON object.
//-----------------------------------------------------------------------------
int dpiJson_addRef(dpiJson *json)
{
    return dpiGen__addRef(json, DPI_HTYPE_JSON, __func__);
}


//-----------------------------------------------------------------------------
// dpiJson_getValue() [PUBLIC]
//   Gets the value of the JSON object as a hierarchy of nodes.
//-----------------------------------------------------------------------------
int dpiJson_getValue(dpiJson *json, uint32_t options, dpiJsonNode **topNode)
{
    dpiJznDomDoc *domDoc;
    void *oracleNode;
    dpiError error;

    if (dpiGen__startPublicFn(json, DPI_HTYPE_JSON, __func__, &error) < 0)
        return dpiGen__endPublicFn(json, DPI_FAILURE, &error);
    dpiJsonNode__free(&json->topNode);
    json->topNode.value = &json->topNodeBuffer;
    json->topNode.oracleTypeNum = DPI_ORACLE_TYPE_NONE;
    json->topNode.nativeTypeNum = DPI_NATIVE_TYPE_NULL;
    if (dpiOci__jsonDomDocGet(json, &domDoc, &error) < 0)
        return dpiGen__endPublicFn(json, DPI_FAILURE, &error);
    if (domDoc) {
        oracleNode = (*domDoc->methods->fnGetRootNode)(domDoc);
        if (dpiJsonNode__fromOracleToNative(json, &json->topNode, domDoc,
                oracleNode, options, &error) < 0)
            return dpiGen__endPublicFn(json, DPI_FAILURE, &error);
    }

    *topNode = &json->topNode;
    return dpiGen__endPublicFn(json, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiJson_release() [PUBLIC]
//   Release a reference to the JSON object.
//-----------------------------------------------------------------------------
int dpiJson_release(dpiJson *json)
{
    return dpiGen__release(json, DPI_HTYPE_JSON, __func__);
}


//-----------------------------------------------------------------------------
// dpiJson_setFromText() [PUBLIC]
//   Sets the value of the JSON handle, given a JSON string.
//-----------------------------------------------------------------------------
int dpiJson_setFromText(dpiJson *json, const char *value, uint64_t valueLength,
        uint32_t flags)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(json, DPI_HTYPE_JSON, __func__, &error) < 0)
        return dpiGen__endPublicFn(json, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(json, value)
    status = dpiOci__jsonTextBufferParse(json, value, valueLength,
                flags, &error);
    return dpiGen__endPublicFn(json, status, &error);
}


//-----------------------------------------------------------------------------
// dpiJson_setValue() [PUBLIC]
//   Sets the value of the JSON object, given a hierarchy of nodes.
//-----------------------------------------------------------------------------
int dpiJson_setValue(dpiJson *json, dpiJsonNode *topNode)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(json, DPI_HTYPE_JSON, __func__, &error) < 0)
        return dpiGen__endPublicFn(json, DPI_FAILURE, &error);
    status = dpiJson__setValue(json, topNode, &error);
    return dpiGen__endPublicFn(json, status, &error);
}
