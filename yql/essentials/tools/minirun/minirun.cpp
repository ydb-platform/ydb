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

        GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
            opts.AddLongOption("ndebug", "Do not show debug info in error output").NoArgument().SetFlag(&GetRunOptions().NoDebug);
        });
        GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
            opts.AddLongOption("test-format", "Compare formatted query's AST with the original query's AST (only syntaxVersion=1 is supported)").NoArgument().SetFlag(&GetRunOptions().TestSqlFormat);
        });
        GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
            opts.AddLongOption("validate-result-format", "Check that result-format can parse Result").NoArgument().SetFlag(&GetRunOptions().ValidateResultFormat);
        });

        GetRunOptions().SetSupportedGateways({TString{PureProviderName}});
        GetRunOptions().GatewayTypes.emplace(PureProviderName);

        AddProviderFactory([]() -> NYql::TDataProviderInitializer {
            return GetPureDataProviderInitializer();
        });
    }
};

} // NYql

int main(int argc, const char *argv[]) {
    try {
        return NYql::TMiniRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
