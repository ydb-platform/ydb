#include "plugin.h"

#include "interface.h"

#include <yt/yql/plugin/plugin.h>
#include <util/system/dynlib.h>

#include <vector>
#include <optional>

namespace NYT::NYqlPlugin {
namespace NBridge {

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

        const char* operationAttributes = options.OperationAttributes
            ? options.OperationAttributes.ToString().data()
            : nullptr;

        const char* defaultCluster = options.DefaultCluster
            ? options.DefaultCluster->data()
            : nullptr;

        TBridgeYqlPluginOptions bridgeOptions {
            .MRJobBinary = options.MRJobBinary.data(),
            .UdfDirectory = options.UdfDirectory.data(),
            .ClusterCount = static_cast<int>(bridgeClusters.size()),
            .Clusters = bridgeClusters.data(),
            .DefaultCluster = defaultCluster,
            .OperationAttributes = operationAttributes,
            .YTTokenPath = options.YTTokenPath.data(),
            .LogBackend = &options.LogBackend,
        };

        BridgePlugin_ = BridgeCreateYqlPlugin(&bridgeOptions);
    }

    TQueryResult Run(TString impersonationUser, TString queryText, NYson::TYsonString settings) noexcept override
    {
        const char* settingsData = settings ? settings.ToString().data() : nullptr;
        auto* bridgeQueryResult = BridgeRun(BridgePlugin_, impersonationUser.data(), queryText.data(), settingsData);
        auto toString = [] (const char* str, size_t strLength) -> std::optional<TString> {
            if (!str) {
                return std::nullopt;
            }
            return TString(str, strLength);
        };
        TQueryResult queryResult = {
            .YsonResult = toString(bridgeQueryResult->YsonResult, bridgeQueryResult->YsonResultLength),
            .Plan = toString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Statistics = toString(bridgeQueryResult->Statistics, bridgeQueryResult->StatisticsLength),
            .TaskInfo = toString(bridgeQueryResult->TaskInfo, bridgeQueryResult->TaskInfoLength),
            .YsonError = toString(bridgeQueryResult->YsonError, bridgeQueryResult->YsonErrorLength),
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
