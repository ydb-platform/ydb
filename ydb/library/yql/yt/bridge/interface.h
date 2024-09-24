#pragma once

#include <unistd.h>

////////////////////////////////////////////////////////////////////////////////

// This is a plain C version of an interface described in yt/yql/plugin/plugin.h.
// All strings without separate length field are assumed to be null-terminated.

////////////////////////////////////////////////////////////////////////////////

// NB(mpereskokova): Do not forget to update EYqlPluginAbiVersion and supported versions
// during ABI changes.
using TFuncBridgeGetAbiVersion = ssize_t();

////////////////////////////////////////////////////////////////////////////////

struct TBridgeYqlPluginOptions
{
    const char* SingletonsConfig = nullptr;
    ssize_t SingletonsConfigLength = 0;

    const char* GatewayConfig = nullptr;
    size_t GatewayConfigLength = 0;

    const char* DqGatewayConfig = nullptr;
    size_t DqGatewayConfigLength = 0;

    const char* DqManagerConfig = nullptr;
    size_t DqManagerConfigLength = 0;

    const char* FileStorageConfig = nullptr;
    size_t FileStorageConfigLength = 0;

    const char* OperationAttributes = nullptr;
    size_t OperationAttributesLength = 0;

    const char* YTTokenPath = nullptr;

    // TODO(max42): passing C++ objects across shared libraries is incredibly
    // fragile. This is a temporary mean until we come up with something more
    // convenient; get rid of this ASAP.
    using TLogBackendHolder = void;
    TLogBackendHolder* LogBackend = nullptr;
};

// Opaque type representing a YQL plugin.
using TBridgeYqlPlugin = void;

using TFuncBridgeCreateYqlPlugin = TBridgeYqlPlugin*(const TBridgeYqlPluginOptions* options);
using TFuncBridgeStartYqlPlugin = void(TBridgeYqlPlugin* plugin);
using TFuncBridgeFreeYqlPlugin = void(TBridgeYqlPlugin* plugin);

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): consider making structure an opaque type with accessors a-la
// const char* BridgeGetYsonResult(const TBridgeQueryResult*). This would remove the need
// to manually free string data.
struct TBridgeQueryResult
{
    const char* YsonResult = nullptr;
    ssize_t YsonResultLength = 0;
    const char* Plan = nullptr;
    ssize_t PlanLength = 0;
    const char* Statistics = nullptr;
    ssize_t StatisticsLength = 0;
    const char* Progress = nullptr;
    ssize_t ProgressLength = 0;
    const char* TaskInfo = nullptr;
    ssize_t TaskInfoLength = 0;

    const char* YsonError = nullptr;
    ssize_t YsonErrorLength = 0;
};

#define FOR_EACH_QUERY_RESULT_STRING_FIELD(XX) \
    XX(YsonResult) \
    XX(Plan) \
    XX(Statistics) \
    XX(Progress) \
    XX(TaskInfo) \
    XX(YsonError)

struct TBridgeClustersResult
{
    const char** Clusters = nullptr;
    ssize_t ClusterCount = 0;

    const char* YsonError = nullptr;
    ssize_t YsonErrorLength = 0;
};

#define FOR_EACH_BRIDGE_RESULT_STRING_FIELD(XX) \
    XX(YsonError)

enum EQueryFileContentType
{
    RawInlineData,
    Url,
};

struct TBridgeQueryFile
{
    const char* Name = nullptr;
    size_t NameLength = 0;

    const char* Content = nullptr;
    size_t ContentLength = 0;

    EQueryFileContentType Type;
};

struct TBridgeAbortResult
{
    const char* YsonError = nullptr;
    ssize_t YsonErrorLength = 0;
};

#define FOR_EACH_ABORT_RESULT_STRING_FIELD(XX) \
    XX(YsonError)

using TFuncBridgeFreeQueryResult = void(TBridgeQueryResult* result);
using TFuncBridgeFreeClustersResult = void(TBridgeClustersResult* result);
using TFuncBridgeRun = TBridgeQueryResult*(
    TBridgeYqlPlugin* plugin,
    const char* queryId,
    const char* user,
    const char* token,
    const char* queryText,
    const char* settings,
    int settingsLength,
    const TBridgeQueryFile* files,
    int fileCount,
    int executeMode);
using TFuncBridgeGetUsedClusters = TBridgeClustersResult*(
    TBridgeYqlPlugin* plugin,
    const char* queryText,
    const char* settings,
    int settingsLength,
    const TBridgeQueryFile* files,
    int fileCount);
using TFuncBridgeGetProgress = TBridgeQueryResult*(TBridgeYqlPlugin* plugin, const char* queryId);
using TFuncBridgeAbort = TBridgeAbortResult*(TBridgeYqlPlugin* plugin, const char* queryId);
using TFuncBridgeFreeAbortResult = void(TBridgeAbortResult* result);

////////////////////////////////////////////////////////////////////////////////

#define FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX) \
    XX(BridgeCreateYqlPlugin) \
    XX(BridgeStartYqlPlugin) \
    XX(BridgeFreeYqlPlugin) \
    XX(BridgeFreeQueryResult) \
    XX(BridgeFreeClustersResult) \
    XX(BridgeRun) \
    XX(BridgeGetUsedClusters) \
    XX(BridgeGetProgress) \
    XX(BridgeGetAbiVersion) \
    XX(BridgeAbort) \
    XX(BridgeFreeAbortResult)

////////////////////////////////////////////////////////////////////////////////
