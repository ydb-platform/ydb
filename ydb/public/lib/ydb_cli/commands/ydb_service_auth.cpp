#include "ydb_service_auth.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include "ydb_sdk_core_access.h"

namespace NYdb::NConsoleClient {

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
    config.Opts->AddLongOption('f', "force", "Print token without prompt").NoArgument().StoreTrue(&config.AssumeYes);
    config.SetFreeArgsNum(0);
}

bool TCommandGetToken::Prompt(TConfig& config) {
    if (!config.AssumeYes) {
        NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
        Cout << colors.RedColor() << "Caution: Your auth token will be printed to console." << colors.OldColor()
            << " Use \"--force\" (\"-f\") option to print without prompting." << Endl;

        if (!AskYesOrNo("Do you want to proceed?", /* defaultAnswer */ false)) {
            return false;
        }
    }

    return true;
}

int TCommandGetToken::Run(TConfig& config) {
    auto credentialsProviderFactory = config.GetSingletonCredentialsProviderFactory();
    if (credentialsProviderFactory) {
        auto driver = CreateDriver(config);
        TDummyClient client(driver);

        auto authInfo = credentialsProviderFactory->CreateProvider(client.GetCoreFacility())->GetAuthInfo();
        if (!authInfo.empty()) {
            Cout << authInfo << Endl;
            return EXIT_SUCCESS;
        }
    }
    Cerr << "No authentication provided" << Endl;
    return EXIT_FAILURE;
}

} // namespace NConsoleClient::NYdb
