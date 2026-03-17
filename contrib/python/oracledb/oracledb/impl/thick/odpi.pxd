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
# odpi.pxd
#
# Cython definition file for the constants, functions and structures found in
# ODPI-C used by the thick implementation classes (embedded in thick_impl.pyx).
#------------------------------------------------------------------------------

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t

cdef enum:
    PYO_OCI_ATTR_TYPE_STRING = 1
    PYO_OCI_ATTR_TYPE_BOOLEAN = 2
    PYO_OCI_ATTR_TYPE_UINT8 = 8
    PYO_OCI_ATTR_TYPE_UINT16 = 16
    PYO_OCI_ATTR_TYPE_UINT32 = 32
    PYO_OCI_ATTR_TYPE_UINT64 = 64

cdef extern from "impl/thick/odpi/embed/dpi.c":

    # version constants
    unsigned int DPI_MAJOR_VERSION
    unsigned int DPI_MINOR_VERSION

    # success/failure constants
    int DPI_SUCCESS
    int DPI_FAILURE

    # defaults
    enum:
        DPI_DEFAULT_PREFETCH_ROWS

    # execution modes
    enum:
        DPI_MODE_EXEC_ARRAY_DML_ROWCOUNTS
        DPI_MODE_EXEC_BATCH_ERRORS
        DPI_MODE_EXEC_COMMIT_ON_SUCCESS
        DPI_MODE_EXEC_DEFAULT
        DPI_MODE_EXEC_DESCRIBE_ONLY
        DPI_MODE_EXEC_PARSE_ONLY

    # connection/pool creation modes
    enum:
        DPI_MODE_CREATE_DEFAULT
        DPI_MODE_CREATE_THREADED
        DPI_MODE_CREATE_EVENTS

    # connection close modes
    enum:
        DPI_MODE_CONN_CLOSE_DEFAULT
        DPI_MODE_CONN_CLOSE_DROP
        DPI_MODE_CONN_CLOSE_RETAG

    # pool close modes
    enum:
        DPI_MODE_POOL_CLOSE_DEFAULT
        DPI_MODE_POOL_CLOSE_FORCE

    # native types
    enum:
        DPI_NATIVE_TYPE_BOOLEAN
        DPI_NATIVE_TYPE_BYTES
        DPI_NATIVE_TYPE_DOUBLE
        DPI_NATIVE_TYPE_FLOAT
        DPI_NATIVE_TYPE_INT64
        DPI_NATIVE_TYPE_INTERVAL_DS
        DPI_NATIVE_TYPE_JSON
        DPI_NATIVE_TYPE_JSON_ARRAY
        DPI_NATIVE_TYPE_JSON_OBJECT
        DPI_NATIVE_TYPE_LOB
        DPI_NATIVE_TYPE_NULL
        DPI_NATIVE_TYPE_OBJECT
        DPI_NATIVE_TYPE_ROWID
        DPI_NATIVE_TYPE_STMT
        DPI_NATIVE_TYPE_TIMESTAMP
        DPI_NATIVE_TYPE_VECTOR

    # Oracle types
    enum:
        DPI_ORACLE_TYPE_BFILE
        DPI_ORACLE_TYPE_BLOB
        DPI_ORACLE_TYPE_BOOLEAN
        DPI_ORACLE_TYPE_CHAR
        DPI_ORACLE_TYPE_CLOB
        DPI_ORACLE_TYPE_DATE
        DPI_ORACLE_TYPE_INTERVAL_DS
        DPI_ORACLE_TYPE_INTERVAL_YM
        DPI_ORACLE_TYPE_JSON
        DPI_ORACLE_TYPE_JSON_ARRAY
        DPI_ORACLE_TYPE_JSON_ID
        DPI_ORACLE_TYPE_JSON_OBJECT
        DPI_ORACLE_TYPE_LONG_NVARCHAR
        DPI_ORACLE_TYPE_LONG_RAW
        DPI_ORACLE_TYPE_LONG_VARCHAR
        DPI_ORACLE_TYPE_NATIVE_DOUBLE
        DPI_ORACLE_TYPE_NATIVE_FLOAT
        DPI_ORACLE_TYPE_NATIVE_INT
        DPI_ORACLE_TYPE_NCHAR
        DPI_ORACLE_TYPE_NCLOB
        DPI_ORACLE_TYPE_NONE
        DPI_ORACLE_TYPE_NUMBER
        DPI_ORACLE_TYPE_NVARCHAR
        DPI_ORACLE_TYPE_OBJECT
        DPI_ORACLE_TYPE_RAW
        DPI_ORACLE_TYPE_ROWID
        DPI_ORACLE_TYPE_STMT
        DPI_ORACLE_TYPE_TIMESTAMP
        DPI_ORACLE_TYPE_TIMESTAMP_LTZ
        DPI_ORACLE_TYPE_TIMESTAMP_TZ
        DPI_ORACLE_TYPE_VARCHAR
        DPI_ORACLE_TYPE_VECTOR
        DPI_ORACLE_TYPE_XMLTYPE

    # SODA flags
    enum:
        DPI_SODA_FLAGS_DEFAULT
        DPI_SODA_FLAGS_ATOMIC_COMMIT
        DPI_SODA_FLAGS_CREATE_COLL_MAP
        DPI_SODA_FLAGS_INDEX_DROP_FORCE

    # database startup modes
    enum:
        DPI_MODE_STARTUP_DEFAULT
        DPI_MODE_STARTUP_FORCE
        DPI_MODE_STARTUP_RESTRICT

    # fetch modes (for scrolling cursors)
    enum:
        DPI_MODE_FETCH_FIRST
        DPI_MODE_FETCH_LAST
        DPI_MODE_FETCH_ABSOLUTE
        DPI_MODE_FETCH_RELATIVE

    # vector formats
    enum:
        DPI_VECTOR_FORMAT_BINARY
        DPI_VECTOR_FORMAT_FLOAT32
        DPI_VECTOR_FORMAT_FLOAT64
        DPI_VECTOR_FORMAT_INT8

    # subscription constants
    uint32_t DPI_SUBSCR_QOS_QUERY
    uint32_t DPI_EVENT_OBJCHANGE
    uint32_t DPI_EVENT_QUERYCHANGE

    # JSON flags
    uint32_t DPI_JSON_OPT_NUMBER_AS_STRING

    # opaque handles
    ctypedef struct dpiConn:
        pass

    ctypedef struct dpiContext:
        pass

    ctypedef struct dpiDeqOptions:
        pass

    ctypedef struct dpiEnqOptions:
        pass

    ctypedef struct dpiJson:
        pass

    ctypedef struct dpiLob:
        pass

    ctypedef struct dpiMsgProps:
        pass

    ctypedef struct dpiObject:
        pass

    ctypedef struct dpiObjectAttr:
        pass

    ctypedef struct dpiObjectType:
        pass

    ctypedef struct dpiPool:
        pass

    ctypedef struct dpiQueue:
        pass

    ctypedef struct dpiRowid:
        pass

    ctypedef struct dpiSodaColl:
        pass

    ctypedef struct dpiSodaDb:
        pass

    ctypedef struct dpiSodaDoc:
        pass

    ctypedef struct dpiSodaDocCursor:
        pass

    ctypedef struct dpiStmt:
        pass

    ctypedef struct dpiSubscr:
        pass

    ctypedef struct dpiVar:
        pass

    ctypedef struct dpiVector:
        pass

    # function pointer types
    ctypedef void (*dpiSubscrCallback)(void* context,
            dpiSubscrMessage *message) except * nogil

    ctypedef int (*dpiAccessTokenCallback)(void *context,
            dpiAccessToken *accessToken) except * nogil

    # complex native data types
    ctypedef struct dpiBytes:
        char *ptr
        uint32_t length
        const char *encoding

    ctypedef struct dpiIntervalDS:
        int32_t days
        int32_t hours
        int32_t minutes
        int32_t seconds
        int32_t fseconds

    ctypedef struct dpiIntervalYM:
        int32_t years
        int32_t months

    ctypedef struct dpiJsonNode:
        uint32_t oracleTypeNum
        uint32_t nativeTypeNum
        dpiDataBuffer *value

    ctypedef struct dpiJsonArray:
        uint32_t numElements
        dpiJsonNode *elements
        dpiDataBuffer *elementValues

    ctypedef struct dpiJsonObject:
        uint32_t numFields
        char **fieldNames
        uint32_t *fieldNameLengths
        dpiJsonNode *fields
        dpiDataBuffer *fieldValues

    ctypedef struct dpiTimestamp:
        int16_t year
        uint8_t month
        uint8_t day
        uint8_t hour
        uint8_t minute
        uint8_t second
        uint32_t fsecond
        int8_t tzHourOffset
        int8_t tzMinuteOffset

    # public structures
    ctypedef struct dpiAnnotation:
        const char *key
        uint32_t keyLength
        const char *value
        uint32_t valueLength

    ctypedef struct dpiAppContext:
        const char *namespaceName
        uint32_t namespaceNameLength
        const char *name
        uint32_t nameLength
        const char *value
        uint32_t valueLength

    ctypedef struct dpiCommonCreateParams:
        uint32_t createMode
        const char *encoding
        const char *nencoding
        const char *edition
        uint32_t editionLength
        const char *driverName
        uint32_t driverNameLength
        bint sodaMetadataCache
        uint32_t stmtCacheSize
        dpiAccessToken *accessToken

    ctypedef struct dpiConnCreateParams:
        uint32_t authMode
        const char *connectionClass
        uint32_t connectionClassLength
        uint32_t purity
        const char *newPassword
        uint32_t newPasswordLength
        dpiAppContext *appContext
        uint32_t numAppContext
        bint externalAuth
        void *externalHandle
        dpiPool *pool
        const char *tag
        uint32_t tagLength
        bint matchAnyTag
        const char *outTag
        uint32_t outTagLength
        bint outTagFound
        dpiShardingKeyColumn *shardingKeyColumns
        uint8_t numShardingKeyColumns
        dpiShardingKeyColumn *superShardingKeyColumns
        uint8_t numSuperShardingKeyColumns
        bint outNewSession

    ctypedef struct dpiConnInfo:
        const char *dbDomain
        uint32_t dbDomainLength
        const char *dbName
        uint32_t dbNameLength
        const char *instanceName
        uint32_t instanceNameLength
        const char *serviceName
        uint32_t serviceNameLength
        uint32_t maxIdentifierLength
        uint32_t maxOpenCursors
        uint8_t serverType

    ctypedef struct dpiContextCreateParams:
        const char *defaultDriverName
        const char *defaultEncoding
        const char *loadErrorUrl
        const char *oracleClientLibDir
        const char *oracleClientConfigDir
        bint sodaUseJsonDesc
        bint useJsonId

    ctypedef union dpiDataBuffer:
        bint asBoolean
        uint8_t asUint8
        uint16_t asUint16
        uint32_t asUint32
        int64_t asInt64
        uint64_t asUint64
        float asFloat
        double asDouble
        char *asString
        void *asRaw
        dpiBytes asBytes
        dpiTimestamp asTimestamp
        dpiIntervalDS asIntervalDS
        dpiIntervalYM asIntervalYM
        dpiJson *asJson
        dpiJsonObject asJsonObject
        dpiJsonArray asJsonArray
        dpiLob *asLOB
        dpiObject *asObject
        dpiStmt *asStmt
        dpiRowid *asRowid
        dpiVector *asVector

    ctypedef struct dpiData:
        bint isNull
        dpiDataBuffer value

    ctypedef struct dpiDataTypeInfo:
        uint32_t oracleTypeNum
        uint32_t defaultNativeTypeNum
        uint16_t ociTypeCode
        uint32_t dbSizeInBytes
        uint32_t clientSizeInBytes
        uint32_t sizeInChars
        int16_t precision
        int8_t scale
        uint8_t fsPrecision
        dpiObjectType *objectType
        bint isJson
        const char *domainSchema
        uint32_t domainSchemaLength
        const char *domainName
        uint32_t domainNameLength
        uint32_t numAnnotations
        dpiAnnotation *annotations
        bint isOson
        uint32_t vectorDimensions
        uint8_t vectorFormat
        uint8_t vectorFlags

    ctypedef struct dpiAccessToken:
        const char *token
        uint32_t tokenLength
        const char *privateKey
        uint32_t privateKeyLength

    ctypedef struct dpiErrorInfo:
        int32_t code
        uint16_t offset16
        const char *message
        uint32_t messageLength
        const char *encoding
        const char *fnName
        const char *action
        const char *sqlState
        bint isRecoverable
        bint isWarning
        uint32_t offset

    ctypedef struct dpiObjectAttrInfo:
        const char *name
        uint32_t nameLength
        dpiDataTypeInfo typeInfo

    ctypedef struct dpiObjectTypeInfo:
        const char *schema
        uint32_t schemaLength
        const char *name
        uint32_t nameLength
        bint isCollection
        dpiDataTypeInfo elementTypeInfo
        uint16_t numAttributes
        const char *packageName
        uint32_t packageNameLength

    ctypedef struct dpiPoolCreateParams:
        uint32_t minSessions
        uint32_t maxSessions
        uint32_t sessionIncrement
        int pingInterval
        int pingTimeout
        bint homogeneous
        bint externalAuth
        uint32_t getMode
        const char *outPoolName
        uint32_t outPoolNameLength
        uint32_t timeout
        uint32_t waitTimeout
        uint32_t maxLifetimeSession
        const char *plsqlFixupCallback
        uint32_t plsqlFixupCallbackLength
        uint32_t maxSessionsPerShard
        dpiAccessTokenCallback accessTokenCallback
        void *accessTokenCallbackContext

    ctypedef struct dpiQueryInfo:
        const char *name
        uint32_t nameLength
        dpiDataTypeInfo typeInfo
        bint nullOk

    ctypedef struct dpiShardingKeyColumn:
        uint32_t oracleTypeNum
        uint32_t nativeTypeNum
        dpiDataBuffer value

    ctypedef struct dpiSodaOperOptions:
        uint32_t numKeys
        const char **keys
        uint32_t *keyLengths
        const char *key
        uint32_t keyLength
        const char *version
        uint32_t versionLength
        const char *filter
        uint32_t filterLength
        const char *hint;
        uint32_t hintLength;
        uint32_t skip
        uint32_t limit
        uint32_t fetchArraySize
        bint lock

    ctypedef struct dpiStmtInfo:
        bint isQuery
        bint isPLSQL
        bint isDDL
        bint isDML
        uint16_t statementType
        bint isReturning

    ctypedef struct dpiStringList:
        uint32_t numStrings
        const char **strings
        uint32_t *stringLengths

    ctypedef struct dpiSubscrCreateParams:
        uint32_t subscrNamespace
        uint32_t protocol
        uint32_t qos
        uint32_t operations
        uint32_t portNumber
        uint32_t timeout
        const char *name
        uint32_t nameLength
        dpiSubscrCallback callback
        void *callbackContext
        const char *recipientName
        uint32_t recipientNameLength
        const char *ipAddress
        uint32_t ipAddressLength
        uint8_t groupingClass
        uint32_t groupingValue
        uint8_t groupingType
        uint64_t outRegId
        bint clientInitiated

    ctypedef struct dpiSubscrMessage:
        uint32_t eventType
        const char *dbName
        uint32_t dbNameLength
        dpiSubscrMessageTable *tables
        uint32_t numTables
        dpiSubscrMessageQuery *queries
        uint32_t numQueries
        dpiErrorInfo *errorInfo
        const void *txId
        uint32_t txIdLength
        bint registered
        const char *queueName
        uint32_t queueNameLength
        const char *consumerName
        uint32_t consumerNameLength
        const void *aqMsgId
        uint32_t aqMsgIdLength

    ctypedef struct dpiSubscrMessageQuery:
        uint64_t id
        uint32_t operation
        dpiSubscrMessageTable *tables
        uint32_t numTables

    ctypedef struct dpiSubscrMessageRow:
        uint32_t operation
        const char *rowid
        uint32_t rowidLength

    ctypedef struct dpiSubscrMessageTable:
        uint32_t operation
        const char *name
        uint32_t nameLength
        dpiSubscrMessageRow *rows
        uint32_t numRows

    ctypedef union dpiVectorDimensionBuffer:
        void* asPtr
        int8_t* asInt8
        float* asFloat
        double* asDouble

    ctypedef struct dpiVectorInfo:
        uint8_t format
        uint32_t numDimensions
        uint8_t dimensionSize
        dpiVectorDimensionBuffer dimensions

    ctypedef struct dpiVersionInfo:
        int versionNum
        int releaseNum
        int updateNum
        int portReleaseNum
        int portUpdateNum
        uint32_t fullVersionNum

    ctypedef struct dpiXid:
        long formatId
        const char *globalTransactionId
        uint32_t globalTransactionIdLength
        const char *branchQualifier
        uint32_t branchQualifierLength

    ctypedef struct dpiMsgRecipient:
        const char *name
        uint32_t nameLength

    # functions
    int dpiConn_breakExecution(dpiConn *conn) nogil

    int dpiConn_changePassword(dpiConn *conn, const char *userName,
            uint32_t userNameLength, const char *oldPassword,
            uint32_t oldPasswordLength, const char *newPassword,
            uint32_t newPasswordLength) nogil

    int dpiConn_close(dpiConn *conn, uint32_t mode, const char *tag,
            uint32_t tagLength) nogil

    int dpiConn_commit(dpiConn *conn) nogil

    int dpiConn_create(const dpiContext *context, const char *userName,
            uint32_t userNameLength, const char *password,
            uint32_t passwordLength, const char *connectString,
            uint32_t connectStringLength,
            const dpiCommonCreateParams *commonParams,
            dpiConnCreateParams *createParams, dpiConn **conn) nogil

    int dpiConn_getCurrentSchema(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getDbDomain(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getDbName(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getEdition(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getExternalName(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getHandle(dpiConn *conn, void **handle) nogil

    int dpiConn_getInfo(dpiConn *conn, dpiConnInfo *info) nogil

    int dpiConn_getInstanceName(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getInternalName(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getIsHealthy(dpiConn *conn, bint *isHealthy) nogil

    int dpiConn_getLTXID(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getMaxOpenCursors(dpiConn *conn,
            uint32_t *maxOpenCursors) nogil

    int dpiConn_getOciAttr(dpiConn *conn, uint32_t handleType,
            uint32_t attribute, dpiDataBuffer *value,
            uint32_t *valueLength) nogil

    int dpiConn_getObjectType(dpiConn *conn, const char *name,
            uint32_t nameLength, dpiObjectType **objType) nogil

    int dpiConn_getServerVersion(dpiConn *conn, const char **releaseString,
            uint32_t *releaseStringLength, dpiVersionInfo *versionInfo) nogil

    int dpiConn_getServiceName(dpiConn *conn, const char **value,
            uint32_t *valueLength) nogil

    int dpiConn_getSodaDb(dpiConn *conn, dpiSodaDb **db) nogil

    int dpiConn_getStmtCacheSize(dpiConn *conn, uint32_t *cacheSize) nogil

    int dpiConn_getTransactionInProgress(dpiConn *conn,
            bint *txnInProgress) nogil

    int dpiConn_getCallTimeout(dpiConn *conn, uint32_t *value) nogil

    int dpiConn_newMsgProps(dpiConn *conn, dpiMsgProps **props) nogil

    int dpiConn_newQueue(dpiConn *conn, const char *name,
            uint32_t nameLength, dpiObjectType *payloadType,
            dpiQueue **queue) nogil

    int dpiConn_newJson(dpiConn *conn, dpiJson **json) nogil

    int dpiConn_newJsonQueue(dpiConn *conn, const char *name,
            uint32_t nameLength, dpiQueue **queue) nogil

    int dpiConn_newTempLob(dpiConn *conn, uint32_t lobType, dpiLob **lob) nogil

    int dpiConn_newVar(dpiConn *conn, uint32_t oracleTypeNum,
            uint32_t nativeTypeNum, uint32_t maxArraySize, uint32_t size,
            int sizeIsBytes, bint isArray, dpiObjectType *objType, dpiVar **var,
            dpiData **data) nogil

    int dpiConn_newVector(dpiConn *conn, dpiVectorInfo *info,
            dpiVector **vector) nogil

    int dpiConn_ping(dpiConn *conn) nogil

    int dpiConn_prepareStmt(dpiConn *conn, bint scrollable, const char *sql,
            uint32_t sqlLength, const char *tag, uint32_t tagLength,
            dpiStmt **stmt) nogil

    int dpiConn_release(dpiConn *conn) nogil

    int dpiConn_rollback(dpiConn *conn) nogil

    int dpiConn_setAction(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setCallTimeout(dpiConn *conn, uint32_t value) nogil

    int dpiConn_setClientIdentifier(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setClientInfo(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setCurrentSchema(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setDbOp(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setEcontextId(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setExternalName(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setInternalName(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setModule(dpiConn *conn, const char *value,
            uint32_t valueLength) nogil

    int dpiConn_setOciAttr(dpiConn *conn, uint32_t handleType,
            uint32_t attribute, void *value, uint32_t valueLength) nogil

    int dpiConn_setStmtCacheSize(dpiConn *conn, uint32_t cacheSize) nogil

    int dpiConn_shutdownDatabase(dpiConn *conn, uint32_t mode) nogil

    int dpiConn_startupDatabaseWithPfile(dpiConn *conn, const char *pfile,
            uint32_t pfileLength, uint32_t mode) nogil

    int dpiConn_subscribe(dpiConn *conn, dpiSubscrCreateParams *params,
            dpiSubscr **subscr) nogil

    int dpiConn_tpcBegin(dpiConn *conn, dpiXid *xid,
            uint32_t transactionTimeout, uint32_t flags) nogil

    int dpiConn_tpcCommit(dpiConn *conn, dpiXid *xid, bint onePhase) nogil

    int dpiConn_tpcEnd(dpiConn *conn, dpiXid *xid, uint32_t flags) nogil

    int dpiConn_tpcForget(dpiConn *conn, dpiXid *xid) nogil

    int dpiConn_tpcPrepare(dpiConn *conn, dpiXid *xid,
            bint *commitNeeded) nogil

    int dpiConn_tpcRollback(dpiConn *conn, dpiXid *xid) nogil

    int dpiConn_unsubscribe(dpiConn *conn, dpiSubscr *subscr) nogil

    int dpiContext_createWithParams(unsigned int majorVersion,
            unsigned int minorVersion, dpiContextCreateParams *params,
            dpiContext **context, dpiErrorInfo *errorInfo) nogil

    int dpiContext_freeStringList(dpiContext *context,
            dpiStringList *stringList) nogil

    int dpiContext_getClientVersion(const dpiContext *context,
            dpiVersionInfo *versionInfo) nogil

    void dpiContext_getError(const dpiContext *context,
            dpiErrorInfo *errorInfo) nogil

    int dpiContext_initCommonCreateParams(const dpiContext *context,
            dpiCommonCreateParams *params) nogil

    int dpiContext_initConnCreateParams(const dpiContext *context,
            dpiConnCreateParams *params) nogil

    int dpiContext_initPoolCreateParams(const dpiContext *context,
            dpiPoolCreateParams *params) nogil

    int dpiContext_initSodaOperOptions(const dpiContext *context,
            dpiSodaOperOptions *options) nogil

    int dpiContext_initSubscrCreateParams(const dpiContext *context,
            dpiSubscrCreateParams *params) nogil

    int dpiDeqOptions_getCondition(dpiDeqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiDeqOptions_getConsumerName(dpiDeqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiDeqOptions_getCorrelation(dpiDeqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiDeqOptions_getMode(dpiDeqOptions *options, uint32_t *value) nogil

    int dpiDeqOptions_getMsgId(dpiDeqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiDeqOptions_getNavigation(dpiDeqOptions *options,
            uint32_t *value) nogil

    int dpiDeqOptions_getTransformation(dpiDeqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiDeqOptions_getVisibility(dpiDeqOptions *options,
            uint32_t *value) nogil

    int dpiDeqOptions_getWait(dpiDeqOptions *options, uint32_t *value) nogil

    int dpiDeqOptions_release(dpiDeqOptions *options) nogil

    int dpiDeqOptions_setCondition(dpiDeqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiDeqOptions_setConsumerName(dpiDeqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiDeqOptions_setCorrelation(dpiDeqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiDeqOptions_setDeliveryMode(dpiDeqOptions *options,
            uint16_t value) nogil

    int dpiDeqOptions_setMode(dpiDeqOptions *options, uint32_t value) nogil

    int dpiDeqOptions_setMsgId(dpiDeqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiDeqOptions_setNavigation(dpiDeqOptions *options,
            uint32_t value) nogil

    int dpiDeqOptions_setTransformation(dpiDeqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiDeqOptions_setVisibility(dpiDeqOptions *options,
            uint32_t value) nogil

    int dpiDeqOptions_setWait(dpiDeqOptions *options, uint32_t value) nogil

    int dpiEnqOptions_getTransformation(dpiEnqOptions *options,
            const char **value, uint32_t *valueLength) nogil

    int dpiEnqOptions_getVisibility(dpiEnqOptions *options,
            uint32_t *value) nogil

    int dpiEnqOptions_release(dpiEnqOptions *options) nogil

    int dpiEnqOptions_setDeliveryMode(dpiEnqOptions *options,
            uint16_t value) nogil

    int dpiEnqOptions_setTransformation(dpiEnqOptions *options,
            const char *value, uint32_t valueLength) nogil

    int dpiEnqOptions_setVisibility(dpiEnqOptions *options,
            uint32_t value) nogil

    int dpiJson_getValue(dpiJson *json, uint32_t options,
            dpiJsonNode **topNode) nogil

    int dpiJson_release(dpiJson *json) nogil

    int dpiJson_setValue(dpiJson *json, dpiJsonNode *topNode)

    int dpiLob_addRef(dpiLob *lob) nogil

    int dpiLob_closeResource(dpiLob *lob) nogil

    int dpiLob_getBufferSize(dpiLob *lob, uint64_t sizeInChars,
            uint64_t *sizeInBytes) nogil

    int dpiLob_getChunkSize(dpiLob *lob, uint32_t *size) nogil

    int dpiLob_getDirectoryAndFileName(dpiLob *lob,
            const char **directoryAlias, uint32_t *directoryAliasLength,
            const char **fileName, uint32_t *fileNameLength) nogil

    int dpiLob_getFileExists(dpiLob *lob, bint *exists) nogil

    int dpiLob_getIsResourceOpen(dpiLob *lob, bint *isOpen) nogil

    int dpiLob_getSize(dpiLob *lob, uint64_t *size) nogil

    int dpiLob_openResource(dpiLob *lob) nogil

    int dpiLob_readBytes(dpiLob *lob, uint64_t offset, uint64_t amount,
            char *value, uint64_t *valueLength) nogil

    int dpiLob_release(dpiLob *lob) nogil

    int dpiLob_setDirectoryAndFileName(dpiLob *lob,
            const char *directoryAlias, uint32_t directoryAliasLength,
            const char *fileName, uint32_t fileNameLength) nogil

    int dpiLob_setFromBytes(dpiLob *lob, const char *value,
            uint64_t valueLength) nogil

    int dpiLob_trim(dpiLob *lob, uint64_t newSize) nogil

    int dpiLob_writeBytes(dpiLob *lob, uint64_t offset,
            const char *value, uint64_t valueLength) nogil

    int dpiMsgProps_getNumAttempts(dpiMsgProps *props, int32_t *value) nogil

    int dpiMsgProps_getCorrelation(dpiMsgProps *props,
            const char **value, uint32_t *valueLength) nogil

    int dpiMsgProps_getDelay(dpiMsgProps *props, int32_t *value) nogil

    int dpiMsgProps_getDeliveryMode(dpiMsgProps *props, uint16_t *value) nogil

    int dpiMsgProps_getEnqTime(dpiMsgProps *props, dpiTimestamp *value) nogil

    int dpiMsgProps_getExceptionQ(dpiMsgProps *props,
            const char **value, uint32_t *valueLength) nogil

    int dpiMsgProps_getExpiration(dpiMsgProps *props, int32_t *value) nogil

    int dpiMsgProps_getMsgId(dpiMsgProps *props, const char **value,
            uint32_t *valueLength) nogil

    int dpiMsgProps_getOriginalMsgId(dpiMsgProps *props,
            const char **value, uint32_t *valueLength) nogil

    int dpiMsgProps_getPayload(dpiMsgProps *props, dpiObject **obj,
            const char **value, uint32_t *valueLength) nogil

    int dpiMsgProps_getPayloadJson(dpiMsgProps *props,
        dpiJson **json) nogil

    int dpiMsgProps_getPriority(dpiMsgProps *props, int32_t *value) nogil

    int dpiMsgProps_getState(dpiMsgProps *props, uint32_t *value) nogil

    int dpiMsgProps_release(dpiMsgProps *props) nogil

    int dpiMsgProps_setCorrelation(dpiMsgProps *props,
            const char *value, uint32_t valueLength) nogil

    int dpiMsgProps_setDelay(dpiMsgProps *props, int32_t value) nogil

    int dpiMsgProps_setExceptionQ(dpiMsgProps *props, const char *value,
            uint32_t valueLength) nogil

    int dpiMsgProps_setExpiration(dpiMsgProps *props, int32_t value) nogil

    int dpiMsgProps_setOriginalMsgId(dpiMsgProps *props,
        const char *value, uint32_t valueLength) nogil

    int dpiMsgProps_setPayloadBytes(dpiMsgProps *props,
            const char *value, uint32_t valueLength) nogil

    int dpiMsgProps_setPayloadObject(dpiMsgProps *props, dpiObject *obj) nogil

    int dpiMsgProps_setPayloadJson(dpiMsgProps *props, dpiJson *json) nogil

    int dpiMsgProps_setPriority(dpiMsgProps *props, int32_t value) nogil

    int dpiMsgProps_setRecipients(dpiMsgProps *props,
            dpiMsgRecipient *recipients, uint32_t numRecipients) nogil

    int dpiObject_addRef(dpiObject *obj) nogil

    int dpiObject_appendElement(dpiObject *obj, uint32_t nativeTypeNum,
            dpiData *value) nogil

    int dpiObject_copy(dpiObject *obj, dpiObject **copiedObj) nogil

    int dpiObject_deleteElementByIndex(dpiObject *obj, int32_t index) nogil

    int dpiObject_getAttributeValue(dpiObject *obj, dpiObjectAttr *attr,
            uint32_t nativeTypeNum, dpiData *value) nogil

    int dpiObject_getElementExistsByIndex(dpiObject *obj, int32_t index,
            bint *exists) nogil

    int dpiObject_getElementValueByIndex(dpiObject *obj, int32_t index,
            uint32_t nativeTypeNum, dpiData *value) nogil

    int dpiObject_getFirstIndex(dpiObject *obj, int32_t *index,
            bint *exists) nogil

    int dpiObject_getLastIndex(dpiObject *obj, int32_t *index,
            bint *exists) nogil

    int dpiObject_getNextIndex(dpiObject *obj, int32_t index,
            int32_t *nextIndex, bint *exists) nogil

    int dpiObject_getPrevIndex(dpiObject *obj, int32_t index,
            int32_t *prevIndex, bint *exists) nogil

    int dpiObject_getSize(dpiObject *obj, int32_t *size) nogil

    int dpiObject_release(dpiObject *obj) nogil

    int dpiObject_setAttributeValue(dpiObject *obj, dpiObjectAttr *attr,
            uint32_t nativeTypeNum, dpiData *value) nogil

    int dpiObject_setElementValueByIndex(dpiObject *obj, int32_t index,
            uint32_t nativeTypeNum, dpiData *value) nogil

    int dpiObject_trim(dpiObject *obj, uint32_t numToTrim) nogil

    int dpiObjectAttr_getInfo(dpiObjectAttr *attr,
            dpiObjectAttrInfo *info) nogil

    int dpiObjectAttr_release(dpiObjectAttr *attr) nogil

    int dpiObjectType_addRef(dpiObjectType *objType) nogil

    int dpiObjectType_createObject(dpiObjectType *objType,
            dpiObject **obj) nogil

    int dpiObjectType_getAttributes(dpiObjectType *objType,
            uint16_t numAttributes, dpiObjectAttr **attributes) nogil

    int dpiObjectType_getInfo(dpiObjectType *objType,
            dpiObjectTypeInfo *info) nogil

    int dpiObjectType_release(dpiObjectType *objType) nogil

    int dpiPool_close(dpiPool *pool, uint32_t closeMode) nogil

    int dpiPool_create(const dpiContext *context, const char *userName,
            uint32_t userNameLength, const char *password,
            uint32_t passwordLength, const char *connectString,
            uint32_t connectStringLength,
            const dpiCommonCreateParams *commonParams,
            dpiPoolCreateParams *createParams, dpiPool **pool) nogil

    int dpiPool_getBusyCount(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_getGetMode(dpiPool *pool, uint8_t *value) nogil

    int dpiPool_getMaxLifetimeSession(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_getMaxSessionsPerShard(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_getOpenCount(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_getPingInterval(dpiPool *pool, int *value) nogil

    int dpiPool_getSodaMetadataCache(dpiPool *pool, bint *enabled) nogil

    int dpiPool_getStmtCacheSize(dpiPool *pool, uint32_t *cacheSize) nogil

    int dpiPool_getTimeout(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_getWaitTimeout(dpiPool *pool, uint32_t *value) nogil

    int dpiPool_release(dpiPool *pool) nogil

    int dpiPool_reconfigure(dpiPool *pool, uint32_t minSessions,
            uint32_t maxSessions, uint32_t sessionIncrement) nogil

    int dpiPool_setAccessToken(dpiPool *pool, dpiAccessToken *params) nogil

    int dpiPool_setGetMode(dpiPool *pool, uint8_t value) nogil

    int dpiPool_setMaxLifetimeSession(dpiPool *pool, uint32_t value) nogil

    int dpiPool_setMaxSessionsPerShard(dpiPool *pool, uint32_t value) nogil

    int dpiPool_setPingInterval(dpiPool *pool, int value) nogil

    int dpiPool_setSodaMetadataCache(dpiPool *pool, bint enabled) nogil

    int dpiPool_setStmtCacheSize(dpiPool *pool, uint32_t cacheSize) nogil

    int dpiPool_setTimeout(dpiPool *pool, uint32_t value) nogil

    int dpiPool_setWaitTimeout(dpiPool *pool, uint32_t value) nogil

    int dpiQueue_deqMany(dpiQueue *queue, uint32_t *numProps,
            dpiMsgProps **props) nogil

    int dpiQueue_deqOne(dpiQueue *queue, dpiMsgProps **props) nogil

    int dpiQueue_enqMany(dpiQueue *queue, uint32_t numProps,
            dpiMsgProps **props) nogil

    int dpiQueue_enqOne(dpiQueue *queue, dpiMsgProps *props) nogil

    int dpiQueue_getDeqOptions(dpiQueue *queue,
            dpiDeqOptions **options) nogil

    int dpiQueue_getEnqOptions(dpiQueue *queue,
            dpiEnqOptions **options) nogil

    int dpiQueue_release(dpiQueue *queue) nogil

    int dpiRowid_getStringValue(dpiRowid *rowid, const char **value,
            uint32_t *valueLength)

    int dpiSodaColl_createIndex(dpiSodaColl *coll,
            const char *indexSpec, uint32_t indexSpecLength,
            uint32_t flags) nogil

    int dpiSodaColl_drop(dpiSodaColl *coll, uint32_t flags,
            bint *isDropped) nogil

    int dpiSodaColl_dropIndex(dpiSodaColl *coll, const char *name,
            uint32_t nameLength, uint32_t flags, bint *isDropped) nogil

    int dpiSodaColl_find(dpiSodaColl *coll,
            const dpiSodaOperOptions *options, uint32_t flags,
            dpiSodaDocCursor **cursor) nogil

    int dpiSodaColl_findOne(dpiSodaColl *coll,
            const dpiSodaOperOptions *options, uint32_t flags,
            dpiSodaDoc **doc) nogil

    int dpiSodaColl_getDataGuide(dpiSodaColl *coll, uint32_t flags,
            dpiSodaDoc **doc) nogil

    int dpiSodaColl_getDocCount(dpiSodaColl *coll,
            const dpiSodaOperOptions *options, uint32_t flags,
            uint64_t *count) nogil

    int dpiSodaColl_getMetadata(dpiSodaColl *coll, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaColl_getName(dpiSodaColl *coll, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaColl_insertManyWithOptions(dpiSodaColl *coll, uint32_t numDocs,
            dpiSodaDoc **docs, dpiSodaOperOptions *options, uint32_t flags,
            dpiSodaDoc **insertedDocs) nogil

    int dpiSodaColl_insertOneWithOptions(dpiSodaColl *coll, dpiSodaDoc *doc,
            dpiSodaOperOptions *options, uint32_t flags,
            dpiSodaDoc **insertedDoc) nogil

    int dpiSodaColl_listIndexes(dpiSodaColl *coll, uint32_t flags,
            dpiStringList *lst) nogil

    int dpiSodaColl_release(dpiSodaColl *coll) nogil

    int dpiSodaColl_remove(dpiSodaColl *coll,
            const dpiSodaOperOptions *options, uint32_t flags,
            uint64_t *count) nogil

    int dpiSodaColl_replaceOne(dpiSodaColl *coll,
            const dpiSodaOperOptions *options, dpiSodaDoc *doc, uint32_t flags,
            bint *replaced, dpiSodaDoc **replacedDoc) nogil

    int dpiSodaColl_saveWithOptions(dpiSodaColl *coll, dpiSodaDoc *doc,
            dpiSodaOperOptions *options, uint32_t flags,
            dpiSodaDoc **savedDoc) nogil

    int dpiSodaColl_truncate(dpiSodaColl *coll) nogil

    int dpiSodaDb_createCollection(dpiSodaDb *db, const char *name,
            uint32_t nameLength, const char *metadata, uint32_t metadataLength,
            uint32_t flags, dpiSodaColl **coll) nogil

    int dpiSodaDb_createDocument(dpiSodaDb *db, const char *key,
            uint32_t keyLength, const char *content, uint32_t contentLength,
            const char *mediaType, uint32_t mediaTypeLength, uint32_t flags,
            dpiSodaDoc **doc) nogil

    int dpiSodaDb_createJsonDocument(dpiSodaDb *db, const char *key,
            uint32_t keyLength, const dpiJsonNode *content, uint32_t flags,
            dpiSodaDoc **doc) nogil

    int dpiSodaDb_getCollectionNames(dpiSodaDb *db,
            const char *startName, uint32_t startNameLength, uint32_t limit,
            uint32_t flags, dpiStringList *names) nogil

    int dpiSodaDb_openCollection(dpiSodaDb *db, const char *name,
            uint32_t nameLength, uint32_t flags, dpiSodaColl **coll) nogil

    int dpiSodaDb_release(dpiSodaDb *db) nogil

    int dpiSodaDoc_getContent(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength, const char **encoding) nogil

    int dpiSodaDoc_getCreatedOn(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaDoc_getIsJson(dpiSodaDoc *doc, bint *isJson) nogil

    int dpiSodaDoc_getJsonContent(dpiSodaDoc *doc, dpiJson **value) nogil

    int dpiSodaDoc_getKey(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaDoc_getLastModified(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaDoc_getMediaType(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaDoc_getVersion(dpiSodaDoc *doc, const char **value,
            uint32_t *valueLength) nogil

    int dpiSodaDoc_release(dpiSodaDoc *doc) nogil

    int dpiSodaDocCursor_close(dpiSodaDocCursor *cursor) nogil

    int dpiSodaDocCursor_getNext(dpiSodaDocCursor *cursor,
            uint32_t flags, dpiSodaDoc **doc) nogil

    int dpiSodaDocCursor_release(dpiSodaDocCursor *cursor) nogil

    int dpiStmt_addRef(dpiStmt *stmt) nogil

    int dpiStmt_bindByName(dpiStmt *stmt, const char *name,
            uint32_t nameLength, dpiVar *var) nogil

    int dpiStmt_bindByPos(dpiStmt *stmt, uint32_t pos, dpiVar *var) nogil

    int dpiStmt_close(dpiStmt *stmt, const char *tag, uint32_t tagLength)

    int dpiStmt_define(dpiStmt *stmt, uint32_t pos, dpiVar *var) nogil

    int dpiStmt_deleteFromCache(dpiStmt *stmt) nogil

    int dpiStmt_execute(dpiStmt *stmt, uint32_t mode,
            uint32_t *numQueryColumns) nogil

    int dpiStmt_executeMany(dpiStmt *stmt, uint32_t mode,
            uint32_t numIters) nogil

    int dpiStmt_fetchRows(dpiStmt *stmt, uint32_t maxRows,
            uint32_t *bufferRowIndex, uint32_t *numRowsFetched,
            bint *moreRows) nogil

    int dpiStmt_getBatchErrorCount(dpiStmt *stmt, uint32_t *count) nogil

    int dpiStmt_getBatchErrors(dpiStmt *stmt, uint32_t numErrors,
            dpiErrorInfo *errors) nogil

    int dpiStmt_getBindCount(dpiStmt *stmt, uint32_t *count) nogil

    int dpiStmt_getBindNames(dpiStmt *stmt, uint32_t *numBindNames,
            const char **bindNames, uint32_t *bindNameLengths) nogil

    int dpiStmt_getImplicitResult(dpiStmt *stmt,
            dpiStmt **implicitResult) nogil

    int dpiStmt_getNumQueryColumns(dpiStmt *stmt,
            uint32_t *numQueryColumns) nogil

    int dpiStmt_getInfo(dpiStmt *stmt, dpiStmtInfo *info) nogil

    int dpiStmt_getLastRowid(dpiStmt *stmt, dpiRowid **rowid) nogil

    int dpiStmt_getOciAttr(dpiStmt *stmt, uint32_t attribute,
            dpiDataBuffer *value, uint32_t *valueLength) nogil

    int dpiStmt_getQueryInfo(dpiStmt *stmt, uint32_t pos,
            dpiQueryInfo *info) nogil

    int dpiStmt_getRowCount(dpiStmt *stmt, uint64_t *count) nogil

    int dpiStmt_getRowCounts(dpiStmt *stmt, uint32_t *numRowCounts,
            uint64_t **rowCounts) nogil

    int dpiStmt_getSubscrQueryId(dpiStmt *stmt, uint64_t *queryId) nogil

    int dpiStmt_release(dpiStmt *stmt) nogil

    int dpiStmt_scroll(dpiStmt *stmt, uint32_t mode, int32_t offset,
            int32_t rowCountOffset) nogil

    int dpiStmt_setFetchArraySize(dpiStmt *stmt, uint32_t arraySize) nogil

    int dpiStmt_setOciAttr(dpiStmt *stmt, uint32_t attribute, void *value,
            uint32_t valueLength) nogil

    int dpiStmt_setPrefetchRows(dpiStmt *stmt, uint32_t numRows) nogil

    int dpiSubscr_prepareStmt(dpiSubscr *subscr, const char *sql,
            uint32_t sqlLength, dpiStmt **stmt) nogil

    int dpiSubscr_release(dpiSubscr *subscr) nogil

    int dpiVar_getNumElementsInArray(dpiVar *var, uint32_t *numElements) nogil

    int dpiVar_getReturnedData(dpiVar *var, uint32_t pos,
            uint32_t *numElements, dpiData **data) nogil

    int dpiVar_getSizeInBytes(dpiVar *var, uint32_t *sizeInBytes) nogil

    int dpiVar_release(dpiVar *var) nogil

    int dpiVar_setNumElementsInArray(dpiVar *var, uint32_t numElements) nogil

    int dpiVar_setFromBytes(dpiVar *var, uint32_t pos, const char *value,
            uint32_t valueLength) nogil

    int dpiVar_setFromLob(dpiVar *var, uint32_t pos, dpiLob *lob) nogil

    int dpiVar_setFromObject(dpiVar *var, uint32_t pos, dpiObject *obj) nogil

    int dpiVar_setFromStmt(dpiVar *var, uint32_t pos, dpiStmt *stmt) nogil

    int dpiVector_getValue(dpiVector *vector, dpiVectorInfo *info) nogil

    int dpiVector_setValue(dpiVector *vector, dpiVectorInfo *info) nogil
