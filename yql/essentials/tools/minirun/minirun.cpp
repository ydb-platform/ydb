#include <util/generic/yexception.h>
#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/providers/pure/yql_pure_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

class TMiniRunTool: public TFacadeRunner {
public:
    TMiniRunTool()
        : TFacadeRunner("minirun")
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

        AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
            THashMap<TString, TString> secureParams;
            GetRunOptions().Credentials->ForEach([&](const TString& key, const TCredential& cred) {
                secureParams["token:" + key] = cred.Content;
            });
            return GetPureDataProviderInitializer({.SecureParams = std::move(secureParams)});
        });
    }
};

} // namespace NYql

int main(int argc, const char** argv) {
    try {
        return NYql::TMiniRunTool().Main(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
