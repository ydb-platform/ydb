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
// dpiMsgProps.c
//   Implementation of AQ message properties.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiMsgProps__allocate() [INTERNAL]
//   Create a new message properties structure and return it. In case of error
// NULL is returned.
//-----------------------------------------------------------------------------
int dpiMsgProps__allocate(dpiConn *conn, dpiMsgProps **props, dpiError *error)
{
    dpiMsgProps *tempProps;

    if (dpiGen__allocate(DPI_HTYPE_MSG_PROPS, conn->env, (void**) &tempProps,
            error) < 0)
        return DPI_FAILURE;
    dpiGen__setRefCount(conn, error, 1);
    tempProps->conn = conn;
    if (dpiOci__descriptorAlloc(conn->env->handle, &tempProps->handle,
            DPI_OCI_DTYPE_AQMSG_PROPERTIES, "allocate descriptor",
            error) < 0) {
        dpiMsgProps__free(tempProps, error);
        return DPI_FAILURE;
    }

    *props = tempProps;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiMsgProps__extractMsgId() [INTERNAL]
//   Extract bytes from the OCIRaw value containing the message id.
//-----------------------------------------------------------------------------
void dpiMsgProps__extractMsgId(dpiMsgProps *props, const char **msgId,
        uint32_t *msgIdLength)
{
    dpiOci__rawPtr(props->env->handle, props->msgIdRaw, (void**) msgId);
    dpiOci__rawSize(props->env->handle, props->msgIdRaw, msgIdLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps__free() [INTERNAL]
//   Free the memory for a message properties structure.
//-----------------------------------------------------------------------------
void dpiMsgProps__free(dpiMsgProps *props, dpiError *error)
{
    if (props->handle) {
        dpiOci__descriptorFree(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES);
        props->handle = NULL;
    }
    if (props->payloadObj) {
        dpiGen__setRefCount(props->payloadObj, error, -1);
        props->payloadObj = NULL;
    }
    if (props->payloadJson) {
        dpiGen__setRefCount(props->payloadJson, error, -1);
        props->payloadJson = NULL;
    }
    if (props->payloadRaw) {
        dpiOci__rawResize(props->env->handle, &props->payloadRaw, 0, error);
        props->payloadRaw = NULL;
    }
    if (props->msgIdRaw) {
        dpiOci__rawResize(props->env->handle, &props->msgIdRaw, 0, error);
        props->msgIdRaw = NULL;
    }
    if (props->conn) {
        dpiGen__setRefCount(props->conn, error, -1);
        props->conn = NULL;
    }
    dpiUtils__freeMemory(props);
}


//-----------------------------------------------------------------------------
// dpiMsgProps__getAttrValue() [INTERNAL]
//   Get the attribute value in OCI.
//-----------------------------------------------------------------------------
static int dpiMsgProps__getAttrValue(dpiMsgProps *props, uint32_t attribute,
        const char *fnName, void *value, uint32_t *valueLength)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, fnName, &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    DPI_CHECK_PTR_NOT_NULL(props, valueLength)
    status = dpiOci__attrGet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            value, valueLength, attribute, "get attribute value", &error);
    return dpiGen__endPublicFn(props, status, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps__setAttrValue() [INTERNAL]
//   Set the attribute value in OCI.
//-----------------------------------------------------------------------------
static int dpiMsgProps__setAttrValue(dpiMsgProps *props, uint32_t attribute,
        const char *fnName, const void *value, uint32_t valueLength)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, fnName, &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    status = dpiOci__attrSet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            (void*) value, valueLength, attribute, "set attribute value",
            &error);
    return dpiGen__endPublicFn(props, status, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps__setRecipients() [INTERNAL]
//   Set the recipients value in OCI.
//-----------------------------------------------------------------------------
int dpiMsgProps__setRecipients(dpiMsgProps *props,
        dpiMsgRecipient *recipients, uint32_t numRecipients,
        void **aqAgents, dpiError *error)
{
    uint32_t i;

    for (i = 0; i < numRecipients; i++) {
        if (dpiOci__descriptorAlloc(props->env->handle, &aqAgents[i],
                DPI_OCI_DTYPE_AQAGENT, "allocate agent descriptor",
                error) < 0)
            return DPI_FAILURE;
        if (recipients[i].name && recipients[i].nameLength > 0) {
            if (dpiOci__attrSet(aqAgents[i], DPI_OCI_DTYPE_AQAGENT,
                    (void*) recipients[i].name, recipients[i].nameLength,
                    DPI_OCI_ATTR_AGENT_NAME, "set agent name", error) < 0)
                return DPI_FAILURE;
        }
    }
    if (dpiOci__attrSet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            aqAgents, numRecipients, DPI_OCI_ATTR_RECIPIENT_LIST,
            "set recipient list", error) < 0)
        return DPI_FAILURE;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiMsgProps_addRef() [PUBLIC]
//   Add a reference to the message properties.
//-----------------------------------------------------------------------------
int dpiMsgProps_addRef(dpiMsgProps *props)
{
    return dpiGen__addRef(props, DPI_HTYPE_MSG_PROPS, __func__);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getCorrelation() [PUBLIC]
//   Return correlation associated with the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getCorrelation(dpiMsgProps *props, const char **value,
        uint32_t *valueLength)
{
    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_CORRELATION, __func__,
            (void*) value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getDelay() [PUBLIC]
//   Return the number of seconds the message was delayed.
//-----------------------------------------------------------------------------
int dpiMsgProps_getDelay(dpiMsgProps *props, int32_t *value)
{
    uint32_t valueLength = sizeof(uint32_t);

    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_DELAY, __func__,
            value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getDeliveryMode() [PUBLIC]
//   Return the mode used for delivering the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getDeliveryMode(dpiMsgProps *props,
        dpiMessageDeliveryMode *value)
{
    uint32_t valueLength = sizeof(uint16_t);

    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_MSG_DELIVERY_MODE,
            __func__, value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getEnqTime() [PUBLIC]
//   Return the time the message was enqueued.
//-----------------------------------------------------------------------------
int dpiMsgProps_getEnqTime(dpiMsgProps *props, dpiTimestamp *value)
{
    dpiOciDate ociValue;
    dpiError error;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    if (dpiOci__attrGet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            &ociValue, NULL, DPI_OCI_ATTR_ENQ_TIME, "get attribute value",
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    value->year = ociValue.year;
    value->month = ociValue.month;
    value->day = ociValue.day;
    value->hour = ociValue.hour;
    value->minute = ociValue.minute;
    value->second = ociValue.second;
    value->fsecond = 0;
    value->tzHourOffset = 0;
    value->tzMinuteOffset = 0;
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getExceptionQ() [PUBLIC]
//   Return the name of the exception queue associated with the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getExceptionQ(dpiMsgProps *props, const char **value,
        uint32_t *valueLength)
{
    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_EXCEPTION_QUEUE,
            __func__, (void*) value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getExpiration() [PUBLIC]
//   Return the number of seconds until the message expires.
//-----------------------------------------------------------------------------
int dpiMsgProps_getExpiration(dpiMsgProps *props, int32_t *value)
{
    uint32_t valueLength = sizeof(uint32_t);

    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_EXPIRATION, __func__,
            value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getNumAttempts() [PUBLIC]
//   Return the number of attempts made to deliver the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getNumAttempts(dpiMsgProps *props, int32_t *value)
{
    uint32_t valueLength = sizeof(uint32_t);

    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_ATTEMPTS, __func__,
            value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getMsgId() [PUBLIC]
//   Return the message id for the message (available after enqueuing or
// dequeuing a message).
//-----------------------------------------------------------------------------
int dpiMsgProps_getMsgId(dpiMsgProps *props, const char **value,
        uint32_t *valueLength)
{
    dpiError error;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    DPI_CHECK_PTR_NOT_NULL(props, valueLength)
    dpiMsgProps__extractMsgId(props, value, valueLength);
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getOriginalMsgId() [PUBLIC]
//   Return the original message id for the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getOriginalMsgId(dpiMsgProps *props, const char **value,
        uint32_t *valueLength)
{
    dpiError error;
    void *rawValue;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    DPI_CHECK_PTR_NOT_NULL(props, valueLength)
    if (dpiOci__attrGet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            &rawValue, NULL, DPI_OCI_ATTR_ORIGINAL_MSGID,
            "get attribute value", &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    dpiOci__rawPtr(props->env->handle, rawValue, (void**) value);
    dpiOci__rawSize(props->env->handle, rawValue, valueLength);
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getPayload() [PUBLIC]
//   Get the payload for the message (as an object or a series of bytes).
//-----------------------------------------------------------------------------
int dpiMsgProps_getPayload(dpiMsgProps *props, dpiObject **obj,
        const char **value, uint32_t *valueLength)
{
    dpiError error;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    if (obj)
        *obj = props->payloadObj;
    if (value && valueLength) {
        if (props->payloadRaw) {
            dpiOci__rawPtr(props->env->handle, props->payloadRaw,
                    (void**) value);
            dpiOci__rawSize(props->env->handle, props->payloadRaw,
                    valueLength);
        } else {
            *value = NULL;
            *valueLength = 0;
        }
    }

    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getPayloadJson() [PUBLIC]
//   Get the JSON payload for the message (as a dpiJson).
//-----------------------------------------------------------------------------
int dpiMsgProps_getPayloadJson(dpiMsgProps *props, dpiJson **json)
{
    dpiError error;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, json)
    *json = props->payloadJson;
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getPriority() [PUBLIC]
//   Return the priority of the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getPriority(dpiMsgProps *props, int32_t *value)
{
    uint32_t valueLength = sizeof(uint32_t);

    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_PRIORITY, __func__,
            value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_getState() [PUBLIC]
//   Return the state of the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_getState(dpiMsgProps *props, dpiMessageState *value)
{
    uint32_t valueLength = sizeof(uint32_t);


    return dpiMsgProps__getAttrValue(props, DPI_OCI_ATTR_MSG_STATE, __func__,
            value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_release() [PUBLIC]
//   Release a reference to the message properties.
//-----------------------------------------------------------------------------
int dpiMsgProps_release(dpiMsgProps *props)
{
    return dpiGen__release(props, DPI_HTYPE_MSG_PROPS, __func__);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setCorrelation() [PUBLIC]
//   Set correlation associated with the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setCorrelation(dpiMsgProps *props, const char *value,
        uint32_t valueLength)
{
    return dpiMsgProps__setAttrValue(props, DPI_OCI_ATTR_CORRELATION, __func__,
            value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setDelay() [PUBLIC]
//   Set the number of seconds to delay the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setDelay(dpiMsgProps *props, int32_t value)
{
    return dpiMsgProps__setAttrValue(props, DPI_OCI_ATTR_DELAY, __func__,
            &value, 0);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setExceptionQ() [PUBLIC]
//   Set the name of the exception queue associated with the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setExceptionQ(dpiMsgProps *props, const char *value,
        uint32_t valueLength)
{
    return dpiMsgProps__setAttrValue(props, DPI_OCI_ATTR_EXCEPTION_QUEUE,
            __func__, value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setExpiration() [PUBLIC]
//   Set the number of seconds until the message expires.
//-----------------------------------------------------------------------------
int dpiMsgProps_setExpiration(dpiMsgProps *props, int32_t value)
{
    return dpiMsgProps__setAttrValue(props, DPI_OCI_ATTR_EXPIRATION, __func__,
            &value, 0);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setOriginalMsgId() [PUBLIC]
//   Set the original message id for the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setOriginalMsgId(dpiMsgProps *props, const char *value,
        uint32_t valueLength)
{
    void *rawValue = NULL;
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    if (dpiOci__rawAssignBytes(props->env->handle, value, valueLength,
            &rawValue, &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    status = dpiOci__attrSet(props->handle, DPI_OCI_DTYPE_AQMSG_PROPERTIES,
            (void*) rawValue, 0, DPI_OCI_ATTR_ORIGINAL_MSGID, "set value",
            &error);
    dpiOci__rawResize(props->env->handle, &rawValue, 0, &error);
    return dpiGen__endPublicFn(props, status, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setPayloadBytes() [PUBLIC]
//   Set the payload for the message (as a series of bytes).
//-----------------------------------------------------------------------------
int dpiMsgProps_setPayloadBytes(dpiMsgProps *props, const char *value,
        uint32_t valueLength)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, value)
    if (props->payloadRaw) {
        dpiOci__rawResize(props->env->handle, &props->payloadRaw, 0, &error);
        props->payloadRaw = NULL;
    }
    status = dpiOci__rawAssignBytes(props->env->handle, value, valueLength,
            &props->payloadRaw, &error);
    return dpiGen__endPublicFn(props, status, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setPayloadJson() [PUBLIC]
//   Set the payload for the message (as a JSON object).
//-----------------------------------------------------------------------------
int dpiMsgProps_setPayloadJson(dpiMsgProps *props, dpiJson *json)
{
    dpiError error;
    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(json, DPI_HTYPE_JSON, "check json object",
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    if (props->payloadJson)
        dpiGen__setRefCount(props->payloadJson, &error, -1);
    dpiGen__setRefCount(json, &error, 1);
    props->payloadJson = json;
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setPayloadObject() [PUBLIC]
//   Set the payload for the message (as an object).
//-----------------------------------------------------------------------------
int dpiMsgProps_setPayloadObject(dpiMsgProps *props, dpiObject *obj)
{
    dpiError error;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(obj, DPI_HTYPE_OBJECT, "check object", &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    if (props->payloadObj)
        dpiGen__setRefCount(props->payloadObj, &error, -1);
    dpiGen__setRefCount(obj, &error, 1);
    props->payloadObj = obj;
    return dpiGen__endPublicFn(props, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setPriority() [PUBLIC]
//   Set the priority of the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setPriority(dpiMsgProps *props, int32_t value)
{
    return dpiMsgProps__setAttrValue(props, DPI_OCI_ATTR_PRIORITY, __func__,
            &value, 0);
}


//-----------------------------------------------------------------------------
// dpiMsgProps_setRecipients() [PUBLIC]
//   Set recipients associated with the message.
//-----------------------------------------------------------------------------
int dpiMsgProps_setRecipients(dpiMsgProps *props,
        dpiMsgRecipient *recipients, uint32_t numRecipients)
{
    void **aqAgents;
    dpiError error;
    uint32_t i;
    int status;

    if (dpiGen__startPublicFn(props, DPI_HTYPE_MSG_PROPS, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(props, recipients)
    if (dpiUtils__allocateMemory(numRecipients, sizeof(void*), 1,
            "allocate memory for agents", (void**) &aqAgents, &error) < 0)
        return dpiGen__endPublicFn(props, DPI_FAILURE, &error);
    status = dpiMsgProps__setRecipients(props, recipients, numRecipients,
            aqAgents, &error);
    for (i = 0; i < numRecipients; i++) {
        if (aqAgents[i])
            dpiOci__descriptorFree(aqAgents[i], DPI_OCI_DTYPE_AQAGENT);
    }
    dpiUtils__freeMemory(aqAgents);
    return dpiGen__endPublicFn(props, status, &error);
}
