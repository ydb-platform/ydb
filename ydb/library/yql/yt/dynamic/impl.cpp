#include <ydb/library/yql/yt/bridge/interface.h>
#include <ydb/library/yql/yt/native/plugin.h>

#include <type_traits>

using namespace NYT::NYqlPlugin;
using namespace NYT::NYson;

extern "C" {

#define FREE_STRING_FIELD(StringField) delete[] result->StringField;
#define FILL_STRING_FIELD(StringField) FillString(bridgeResult->StringField, bridgeResult->StringField##Length, result.StringField);

////////////////////////////////////////////////////////////////////////////////

ssize_t BridgeGetAbiVersion()
{
    return 4; // EYqlPluginAbiVersion::TemporaryTokens
}

TBridgeYqlPlugin* BridgeCreateYqlPlugin(const TBridgeYqlPluginOptions* bridgeOptions)
{
    static const TYsonString EmptyMap = TYsonString(TString("{}"));

    auto operationAttributes = bridgeOptions->OperationAttributes
        ? TYsonString(TString(bridgeOptions->OperationAttributes, bridgeOptions->OperationAttributesLength))
        : EmptyMap;

    auto singletonsConfig = bridgeOptions->SingletonsConfig
        ? TYsonString(TString(bridgeOptions->SingletonsConfig, bridgeOptions->SingletonsConfigLength))
        : EmptyMap;

    TYqlPluginOptions options{
        .SingletonsConfig = singletonsConfig,
        .GatewayConfig = TYsonString(TStringBuf(bridgeOptions->GatewayConfig, bridgeOptions->GatewayConfigLength)),
        .DqGatewayConfig = bridgeOptions->DqGatewayConfigLength ? TYsonString(TStringBuf(bridgeOptions->DqGatewayConfig, bridgeOptions->DqGatewayConfigLength)) : TYsonString(),
        .DqManagerConfig = bridgeOptions->DqGatewayConfigLength ? TYsonString(TStringBuf(bridgeOptions->DqManagerConfig, bridgeOptions->DqManagerConfigLength)) : TYsonString(),
        .FileStorageConfig = TYsonString(TStringBuf(bridgeOptions->FileStorageConfig, bridgeOptions->FileStorageConfigLength)),
        .OperationAttributes = TYsonString(TStringBuf(bridgeOptions->OperationAttributes, bridgeOptions->OperationAttributesLength)),
        .YTTokenPath = TString(bridgeOptions->YTTokenPath),
        .LogBackend = std::move(*reinterpret_cast<THolder<TLogBackend>*>(bridgeOptions->LogBackend)),
    };
    auto nativePlugin = CreateYqlPlugin(std::move(options));
    return nativePlugin.release();
}

void BridgeStartYqlPlugin(TBridgeYqlPlugin* plugin)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    nativePlugin->Start();
}

void BridgeFreeYqlPlugin(TBridgeYqlPlugin* plugin)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    delete nativePlugin;
}

void BridgeFreeClustersResult(TBridgeClustersResult* result)
{
    for (ssize_t i = 0; i < result->ClusterCount; i++) {
        delete[] result->Clusters[i];
    }
    delete[] result->Clusters;

    FOR_EACH_BRIDGE_RESULT_STRING_FIELD(FREE_STRING_FIELD);
    delete result;
}

void BridgeFreeQueryResult(TBridgeQueryResult* result)
{
    FOR_EACH_QUERY_RESULT_STRING_FIELD(FREE_STRING_FIELD);
    delete result;
}

void FillString(const char*& str, ssize_t& strLength, const std::optional<TString>& original)
{
    if (!original) {
        str = nullptr;
        strLength = 0;
        return;
    }
    char* copy = new char[original->size() + 1];
    memcpy(copy, original->data(), original->size() + 1);
    str = copy;
    strLength = original->size();
}

TBridgeClustersResult* BridgeGetUsedClusters(
    TBridgeYqlPlugin* plugin,
    const char* queryText,
    const char* settings,
    int settingsLength,
    const TBridgeQueryFile* bridgeFiles,
    int bridgeFileCount)
{
    static const auto EmptyMap = TYsonString(TString("{}"));

    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeClustersResult;

    std::vector<TQueryFile> files(bridgeFileCount);
    for (int index = 0; index < bridgeFileCount; index++) {
        const auto& file = bridgeFiles[index];
        files.push_back(TQueryFile {
            .Name = TStringBuf(file.Name, file.NameLength),
            .Content = TStringBuf(file.Content, file.ContentLength),
            .Type = file.Type,
        });
    }

    auto result = nativePlugin->GetUsedClusters(
        TString(queryText),
        settings ? TYsonString(TString(settings, settingsLength)) : EmptyMap,
        files);

    bridgeResult->Clusters = new const char*[result.Clusters.size()];
    for (size_t i = 0; i < result.Clusters.size(); i++) {
        ssize_t clusterLength;
        FillString(bridgeResult->Clusters[i], clusterLength, result.Clusters[i]);
    }
    bridgeResult->ClusterCount = result.Clusters.size();

    FOR_EACH_BRIDGE_RESULT_STRING_FIELD(FILL_STRING_FIELD);

    return bridgeResult;
}

TBridgeQueryResult* BridgeRun(
    TBridgeYqlPlugin* plugin,
    const char* queryId,
    const char* user,
    const char* token,
    const char* queryText,
    const char* settings,
    int settingsLength,
    const TBridgeQueryFile* bridgeFiles,
    int bridgeFileCount,
    int executeMode)
{
    static const auto EmptyMap = TYsonString(TString("{}"));

    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeQueryResult;

    std::vector<TQueryFile> files(bridgeFileCount);
    for (int index = 0; index < bridgeFileCount; index++) {
        const auto& file = bridgeFiles[index];
        files.push_back(TQueryFile {
            .Name = TStringBuf(file.Name, file.NameLength),
            .Content = TStringBuf(file.Content, file.ContentLength),
            .Type = file.Type,
        });
    }

    auto result = nativePlugin->Run(
        NYT::TGuid::FromString(queryId),
        TString(user),
        TString(token),
        TString(queryText),
        settings ? TYsonString(TString(settings, settingsLength)) : EmptyMap,
        files,
        executeMode);
    FOR_EACH_QUERY_RESULT_STRING_FIELD(FILL_STRING_FIELD);

    return bridgeResult;
}

TBridgeQueryResult* BridgeGetProgress(TBridgeYqlPlugin* plugin, const char* queryId)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeQueryResult;

    auto result = nativePlugin->GetProgress(NYT::TGuid::FromString(queryId));
    FOR_EACH_QUERY_RESULT_STRING_FIELD(FILL_STRING_FIELD);

    return bridgeResult;
}

TBridgeAbortResult* BridgeAbort(TBridgeYqlPlugin* plugin, const char* queryId)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeAbortResult;

    auto result = nativePlugin->Abort(NYT::TGuid::FromString(queryId));
    FOR_EACH_ABORT_RESULT_STRING_FIELD(FILL_STRING_FIELD);

    return bridgeResult;
}

void BridgeFreeAbortResult(TBridgeAbortResult* result)
{
    FOR_EACH_ABORT_RESULT_STRING_FIELD(FREE_STRING_FIELD);
    delete result;
}

////////////////////////////////////////////////////////////////////////////////

// Validate that the all functions from the bridge interface are implemented with proper signatures.

#define XX(function) static_assert(std::is_same_v<decltype(&(function)), TFunc ## function*>);
FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)
#undef XX

////////////////////////////////////////////////////////////////////////////////

} // extern "C"
