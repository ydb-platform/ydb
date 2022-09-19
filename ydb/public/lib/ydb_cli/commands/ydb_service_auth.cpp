#include "ydb_service_auth.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include "ydb_sdk_core_access.h"

namespace NYdb {
namespace NConsoleClient {

TCommandAuth::TCommandAuth()
    : TClientCommandTree("auth", {}, "Auth service operations")
{
    AddCommand(std::make_unique<TCommandGetToken>());
}

TCommandGetToken::TCommandGetToken()
    : TYdbSimpleCommand("get-token", {}, "Get token from authentication parameters")
{}

void TCommandGetToken::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);
    config.Opts->AddLongOption('f', "force", "Print token without prompt").NoArgument().StoreTrue(&ForceMode);
    config.SetFreeArgsNum(0);
}

int TCommandGetToken::Run(TConfig& config) {
    auto credentialsProviderFactory = config.CredentialsGetter(config);

    if (!ForceMode) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        Cout << colors.RedColor() << "Caution: Your auth token will be printed to console." << colors.OldColor()
            << " Use \"--force\" (\"-f\") option to print without prompting." << Endl
            << "Do you want to proceed (y/n)? : ";
        if (!AskYesOrNo()) {
            return EXIT_FAILURE;
        }
    }

    if (credentialsProviderFactory) {
        auto driver = CreateDriver(config);
        TDummyClient client(driver);

        auto authInfo = credentialsProviderFactory->CreateProvider(client.GetCoreFacility())->GetAuthInfo();
        if (authInfo) {
            Cout << authInfo << Endl;
            return EXIT_SUCCESS;
        }
    }
    Cerr << "No authentication provided" << Endl;
    return EXIT_FAILURE;
}

}
}
