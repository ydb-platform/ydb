#include "mock_env.h"

const TString TEST_DATABASE = "/test_database";

TString TCliTestFixture::RunCliWithStderr(TList<TString> args, const THashMap<TString, TString>& env, const TString& profileFileContent, TString* stderrOutput) {
    ClearFailures();

    if (profileFileContent) {
        TString profileFile = EnvFile(profileFileContent, "profile.yaml");
        args.emplace_front(profileFile);
        args.emplace_front("--profile-file");
    }
    TString output = RunYdbWithStderr(
        args,
        {},
        true,
        false,
        GetEndEnv(env),
        ExpectedExitCode,
        stderrOutput
    );
    CheckExpectations();
    // reset
    ClearExpectations();
    return output;
}
