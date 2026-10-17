#include "minirun_lib.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/pure/yql_pure_provider.h>

namespace NYql {

TMiniRunTool::TMiniRunTool(TString toolName)
    : TFacadeRunner(std::move(toolName))
{
    GetRunOptions().UseRepeatableRandomAndTimeProviders = true;
    GetRunOptions().ResultsFormat = NYson::EYsonFormat::Pretty;
    GetRunOptions().OptimizeLibs = false;
    GetRunOptions().CustomTests = true;
    GetRunOptions().EnableCredentials = true;

    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
        opts.AddLongOption("ndebug", "Do not show debug info in error output").NoArgument().SetFlag(&GetRunOptions().NoDebug);
    });

    GetRunOptions().SetSupportedGateways({TString{PureProviderName}});
    GetRunOptions().GatewayTypes.emplace(PureProviderName);
    AddProviderFactory([]() -> TDataProviderInitializer {
        return GetPureDataProviderInitializer();
    });
}

} // namespace NYql
