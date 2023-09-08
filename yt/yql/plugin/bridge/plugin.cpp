#include "plugin.h"

#include "interface.h"

#include <yt/yql/plugin/plugin.h>
#include <util/system/dynlib.h>

#include <vector>
#include <optional>

namespace NYT::NYqlPlugin {
namespace NBridge {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<TString> ToString(const char* str, size_t strLength)
{
    if (!str) {
        return std::nullopt;
    }
    return TString(str, strLength);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDynamicYqlPlugin
{
public:
    TDynamicYqlPlugin(std::optional<TString> yqlPluginSharedLibrary)
    {
        const TString DefaultYqlPluginLibraryName = "./libyqlplugin.so";
        auto sharedLibraryPath = yqlPluginSharedLibrary.value_or(DefaultYqlPluginLibraryName);
        Library_.Open(sharedLibraryPath.data());
        #define XX(function) function = reinterpret_cast<TFunc ## function*>(Library_.Sym(#function));
        FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX);
        #undef XX
    }

protected:
    #define XX(function) TFunc ## function* function;
    FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)
    #undef XX

    TDynamicLibrary Library_;
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public TDynamicYqlPlugin
    , public IYqlPlugin
{
public:
    explicit TYqlPlugin(TYqlPluginOptions& options)
        : TDynamicYqlPlugin(options.YqlPluginSharedLibrary)
    {
        std::vector<TBridgeYqlPluginOptions::TBridgeCluster> bridgeClusters;
        for (const auto& [cluster, proxy]: options.Clusters) {
            bridgeClusters.push_back({
               .Cluster = cluster.data(),
               .Proxy = proxy.data(),
           });
        }

        auto operationAttributesString = options.OperationAttributes ? options.OperationAttributes.ToString() : "{}";

        const char* defaultCluster = options.DefaultCluster
            ? options.DefaultCluster->data()
            : nullptr;

        TBridgeYqlPluginOptions bridgeOptions {
            .MRJobBinary = options.MRJobBinary.data(),
            .UdfDirectory = options.UdfDirectory.data(),
            .ClusterCount = ssize(bridgeClusters),
            .Clusters = bridgeClusters.data(),
            .DefaultCluster = defaultCluster,
            .OperationAttributes = operationAttributesString.data(),
            .OperationAttributesLength = static_cast<int>(operationAttributesString.size()),
            .MaxFilesSizeMb = options.MaxFilesSizeMb,
            .MaxFileCount = options.MaxFileCount,
            .DownloadFileRetryCount = options.DownloadFileRetryCount,
            .YTTokenPath = options.YTTokenPath.data(),
            .LogBackend = &options.LogBackend,
        };

        BridgePlugin_ = BridgeCreateYqlPlugin(&bridgeOptions);
    }

    TQueryResult Run(TQueryId queryId, TString impersonationUser, TString queryText, NYson::TYsonString settings, std::vector<TQueryFile> files) noexcept override
    {
        auto settingsString = settings ? settings.ToString() : "{}";
        auto queryIdStr = ToString(queryId);

        std::vector<TBridgeQueryFile> filesData;
        filesData.reserve(files.size());
        for (const auto& file : files) {
            filesData.push_back(TBridgeQueryFile {
                .Name = file.Name.data(),
                .NameLength = file.Name.size(),
                .Content = file.Content.data(),
                .ContentLength = file.Content.size(),
                .Type = file.Type,
            });
        }

        auto* bridgeQueryResult = BridgeRun(BridgePlugin_, queryIdStr.data(), impersonationUser.data(), queryText.data(), settingsString.data(), filesData.data(), filesData.size());
        TQueryResult queryResult = {
            .YsonResult = ToString(bridgeQueryResult->YsonResult, bridgeQueryResult->YsonResultLength),
            .Plan = ToString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Statistics = ToString(bridgeQueryResult->Statistics, bridgeQueryResult->StatisticsLength),
            .Progress = ToString(bridgeQueryResult->Progress, bridgeQueryResult->ProgressLength),
            .TaskInfo = ToString(bridgeQueryResult->TaskInfo, bridgeQueryResult->TaskInfoLength),
            .YsonError = ToString(bridgeQueryResult->YsonError, bridgeQueryResult->YsonErrorLength),
        };
        BridgeFreeQueryResult(bridgeQueryResult);
        return queryResult;
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
    {
        auto queryIdStr = ToString(queryId);
        auto* bridgeQueryResult = BridgeGetProgress(BridgePlugin_, queryIdStr.data());
        TQueryResult queryResult = {
            .Plan = ToString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Progress = ToString(bridgeQueryResult->Progress, bridgeQueryResult->ProgressLength),
        };
        BridgeFreeQueryResult(bridgeQueryResult);
        return queryResult;
    }

    ~TYqlPlugin() override
    {
        BridgeFreeYqlPlugin(BridgePlugin_);
    }

private:
    TBridgeYqlPlugin* BridgePlugin_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBridge

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& options) noexcept
{
    return std::make_unique<NBridge::TYqlPlugin>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NBridge
