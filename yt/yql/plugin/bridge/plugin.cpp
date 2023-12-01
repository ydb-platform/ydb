#include "plugin.h"

#include "interface.h"

#include <yt/yql/plugin/plugin.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/system/dynlib.h>

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

// Each YQL plugin ABI change should be listed here. Either a compat should be added
// or MinSupportedYqlPluginAbiVersion should be promoted.
DEFINE_ENUM(EYqlPluginAbiVersion,
    ((Invalid)            (-1))
    ((TheBigBang)          (0))
    ((AbortQuery)          (1)) // gritukan: Added BridgeAbort; no breaking changes.
);

constexpr auto MinSupportedYqlPluginAbiVersion = EYqlPluginAbiVersion::TheBigBang;
constexpr auto MaxSupportedYqlPluginAbiVersion = EYqlPluginAbiVersion::AbortQuery;

////////////////////////////////////////////////////////////////////////////////

class TDynamicYqlPlugin
{
public:
    TDynamicYqlPlugin(std::optional<TString> yqlPluginSharedLibrary)
    {
        static const TString DefaultYqlPluginLibraryName = "./libyqlplugin.so";
        auto sharedLibraryPath = yqlPluginSharedLibrary.value_or(DefaultYqlPluginLibraryName);
        Library_.Open(sharedLibraryPath.data());
        #define XX(function) \
        { \
            if constexpr(#function == TStringBuf("BridgeAbort")) { \
                if (AbiVersion_ < EYqlPluginAbiVersion::AbortQuery) { \
                    function = reinterpret_cast<TFunc ## function*>(AbortQueryStub); \
                } else { \
                    function = reinterpret_cast<TFunc ## function*>(Library_.Sym(#function)); \
                } \
            } else { \
                function = reinterpret_cast<TFunc ## function*>(Library_.Sym(#function)); \
            } \
        } \

        // Firstly we need to get ABI version of the plugin to make it possible to
        // add compats for other functions.
        XX(BridgeGetAbiVersion);
        GetYqlPluginAbiVersion();

        FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX);
        // COMPAT(gritukan): Remove after commit in YDB repository.
        XX(BridgeAbort)
        #undef XX
    }

protected:
    TDynamicLibrary Library_;

    EYqlPluginAbiVersion AbiVersion_ = EYqlPluginAbiVersion::Invalid;

    #define XX(function) TFunc ## function* function;
    FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)

    // COMPAT(gritukan): Remove after commit in YDB repository.
    XX(BridgeAbort)
    #undef XX

    // COMPAT(gritukan): AbortQuery
    static void AbortQueryStub(TBridgeYqlPlugin* /*plugin*/, const char* /*queryId*/)
    {
        // Just do nothing. It is not worse than in used to be before.
    }

    void GetYqlPluginAbiVersion()
    {
        if (!TryEnumCast(BridgeGetAbiVersion(), &AbiVersion_)) {
            THROW_ERROR_EXCEPTION(
                "YQL plugin ABI version %v is not supported",
                BridgeGetAbiVersion());
        }

        if (AbiVersion_ < MinSupportedYqlPluginAbiVersion ||
            AbiVersion_ > MaxSupportedYqlPluginAbiVersion)
        {
            THROW_ERROR_EXCEPTION(
                "YQL plugin ABI version %Qv is not supported",
                AbiVersion_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public TDynamicYqlPlugin
    , public IYqlPlugin
{
public:
    explicit TYqlPlugin(TYqlPluginOptions options)
        : TDynamicYqlPlugin(options.YqlPluginSharedLibrary)
    {
        TString singletonsConfig = options.SingletonsConfig ? options.SingletonsConfig.ToString() : "{}";

        TBridgeYqlPluginOptions bridgeOptions {
            .SingletonsConfig = singletonsConfig.data(),
            .SingletonsConfigLength = static_cast<int>(singletonsConfig.size()),
            .GatewayConfig = options.GatewayConfig.AsStringBuf().Data(),
            .GatewayConfigLength = options.GatewayConfig.AsStringBuf().Size(),
            .FileStorageConfig = options.FileStorageConfig.AsStringBuf().Data(),
            .FileStorageConfigLength = options.FileStorageConfig.AsStringBuf().Size(),
            .OperationAttributes = options.OperationAttributes.AsStringBuf().Data(),
            .OperationAttributesLength = options.OperationAttributes.AsStringBuf().Size(),
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
            filesData.push_back(TBridgeQueryFile{
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

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions options) noexcept
{
    return std::make_unique<NBridge::TYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
