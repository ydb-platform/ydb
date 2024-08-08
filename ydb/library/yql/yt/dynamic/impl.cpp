#include <ydb/library/yql/yt/bridge/interface.h>
#include <ydb/library/yql/yt/native/plugin.h>

#include <type_traits>

using namespace NYT::NYqlPlugin;
using namespace NYT::NYson;

extern "C" {

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
    delete[] result->YsonError;
    delete result;
}

void BridgeFreeQueryResult(TBridgeQueryResult* result)
{
    delete[] result->TaskInfo;
    delete[] result->Statistics;
    delete[] result->Plan;
    delete[] result->Progress;
    delete[] result->YsonResult;
    delete[] result->YsonError;
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
    auto* bridgeClustersResult = new TBridgeClustersResult;

    std::vector<TQueryFile> files(bridgeFileCount);
    for (int index = 0; index < bridgeFileCount; index++) {
        const auto& file = bridgeFiles[index];
        files.push_back(TQueryFile {
            .Name = TStringBuf(file.Name, file.NameLength),
            .Content = TStringBuf(file.Content, file.ContentLength),
            .Type = file.Type,
        });
    }

    auto clustersResult = nativePlugin->GetUsedClusters(
        TString(queryText),
        settings ? TYsonString(TString(settings, settingsLength)) : EmptyMap,
        files);

    bridgeClustersResult->Clusters = new const char*[clustersResult.Clusters.size()];
    for (size_t i = 0; i < clustersResult.Clusters.size(); i++) {
        ssize_t clusterLength;
        FillString(bridgeClustersResult->Clusters[i], clusterLength, clustersResult.Clusters[i]);
    }
    bridgeClustersResult->ClusterCount = clustersResult.Clusters.size();

    FillString(bridgeClustersResult->YsonError, bridgeClustersResult->YsonErrorLength, clustersResult.YsonError);

    return bridgeClustersResult;
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
    FillString(bridgeResult->YsonResult, bridgeResult->YsonResultLength, result.YsonResult);
    FillString(bridgeResult->Plan, bridgeResult->PlanLength, result.Plan);
    FillString(bridgeResult->Statistics, bridgeResult->StatisticsLength, result.Statistics);
    FillString(bridgeResult->Progress, bridgeResult->ProgressLength, result.Progress);
    FillString(bridgeResult->TaskInfo, bridgeResult->TaskInfoLength, result.TaskInfo);
    FillString(bridgeResult->YsonError, bridgeResult->YsonErrorLength, result.YsonError);

    return bridgeResult;
}

TBridgeQueryResult* BridgeGetProgress(TBridgeYqlPlugin* plugin, const char* queryId)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeQueryResult;

    auto result = nativePlugin->GetProgress(NYT::TGuid::FromString(queryId));
    FillString(bridgeResult->Plan, bridgeResult->PlanLength, result.Plan);
    FillString(bridgeResult->Progress, bridgeResult->ProgressLength, result.Progress);

    return bridgeResult;
}

TBridgeAbortResult* BridgeAbort(TBridgeYqlPlugin* plugin, const char* queryId)
{
    auto* nativePlugin = reinterpret_cast<IYqlPlugin*>(plugin);
    auto* bridgeResult = new TBridgeAbortResult;

    auto result = nativePlugin->Abort(NYT::TGuid::FromString(queryId));
    FillString(bridgeResult->YsonError, bridgeResult->YsonErrorLength, result.YsonError);

    return bridgeResult;
}

void BridgeFreeAbortResult(TBridgeAbortResult* result)
{
    delete[] result->YsonError;
    delete result;
}

////////////////////////////////////////////////////////////////////////////////

// Validate that the all functions from the bridge interface are implemented with proper signatures.

#define XX(function) static_assert(std::is_same_v<decltype(&(function)), TFunc ## function*>);
FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)
#undef XX

////////////////////////////////////////////////////////////////////////////////

} // extern "C"
