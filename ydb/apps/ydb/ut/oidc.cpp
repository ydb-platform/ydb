#include "mock_env.h"

#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <util/stream/file.h>
#include <library/cpp/string_utils/base64/base64.h>

namespace {

TString TokenProfile(const TString& endpoint, const TString& database, const TString& tokenFile);
TString ClientProfile(const TString& issuer, const TString& secretFile);
TString ConfigProfile(const TString& configFile);

TString TokenProfile(const TString& endpoint, const TString& database, const TString& tokenFile) {
    return TStringBuilder()
        << "profiles:\n"
        << "  oidc:\n"
        << "    endpoint: " << endpoint << "\n"
        << "    database: " << database << "\n"
        << "    authentication:\n"
        << "      method: token-file\n"
        << "      data: " << tokenFile << "\n"
        << "active_profile: oidc\n";
}

TString ClientProfile(const TString& issuer, const TString& secretFile) {
    return TStringBuilder()
        << "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
        << "        issuer: " << issuer << "\n"
        << "        flow: client\n        client_id: client\n"
        << "        client_secret_file: " << secretFile << "\nactive_profile: oidc\n";
}

TString ConfigProfile(const TString& configFile) {
    return TStringBuilder()
        << "profiles:\n  oidc:\n    authentication:\n      method: oidc-config\n"
        << "      data: " << configFile << "\nactive_profile: oidc\n";
}

} // namespace

Y_UNIT_TEST_SUITE(ParseOidcOptionsTest) {

    Y_UNIT_TEST_F(UpdateProfileCanSelectOidcAndRejectsNoAuthConflict, TCliTestFixture) {
        const auto profile = EnvFile("profiles:\n  test:\n    authentication:\n      method: anonymous-auth\n", "profiles.yaml");
        const auto token = EnvFile("updated-token", "token");
        RunCliWithInput({"--profile-file", profile, "config", "profile", "update", "test",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-access-token-file", token}, "");
        const auto saved = TFileInput(profile).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(saved, "method: oidc");
        UNIT_ASSERT(!saved.Contains("updated-token"));
        ExpectToken("Bearer updated-token");
        RunCli({"--profile-file", profile, "--profile", "test", "scheme", "ls"});
        ExpectFail();
        RunCliWithInput({"--profile-file", profile, "config", "profile", "update", "test",
            "--oidc-issuer", "https://issuer.example", "--oidc-access-token-file", token, "--no-auth"}, "");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(profile).ReadAll(), saved);
    }

    Y_UNIT_TEST_F(RejectsMalformedOidcProfileFields, TCliTestFixture) {
        for (const TString& data : {TString("[]"), TString("{issuer: []}"),
                TString("{issuer: https://issuer.example, client_id: {nested: value}}")}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "config", "info"}, {},
                "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data: " + data + "\nactive_profile: oidc\n");
        }
    }

    Y_UNIT_TEST_F(RejectsEmptyOidcProfileOptions, TCliTestFixture) {
        for (const TString& option : {TString("--oidc-config"), TString("--oidc-issuer"),
                TString("--oidc-client-id"), TString("--oidc-scope"), TString("--oidc-access-token-file"),
                TString("--oidc-client-secret-file"), TString("--oidc-cache-path")}) {
            const auto profile = EnvFile("", "profiles.yaml");
            ExpectFail();
            RunCliWithInput({"--profile-file", profile, "config", "profile", "create", "invalid", option, ""}, "");
            UNIT_ASSERT(!TFileInput(profile).ReadAll().Contains("invalid"));
        }
    }

    Y_UNIT_TEST_F(RejectsEmptyOidcScopeFromStdin, TCliTestFixture) {
        const auto profile = EnvFile("", "profiles.yaml");
        ExpectFail();
        RunCliWithInput({"--profile-file", profile, "config", "profile", "create", "invalid",
            "--oidc-issuer", "https://issuer.example", "--oidc-client-id", "cli"}, "oidc-scope:   \n");
        UNIT_ASSERT(!TFileInput(profile).ReadAll().Contains("invalid"));
    }

    Y_UNIT_TEST_F(RejectsConflictingProfileAuthentication, TCliTestFixture) {
        const auto profile = EnvFile("", "profiles.yaml");
        ExpectFail();
        RunCliWithInput({"--profile-file", profile, "config", "profile", "create", "invalid",
            "--oidc-issuer", "https://issuer.example", "--oidc-client-id", "cli", "--anonymous-auth"}, "");
        UNIT_ASSERT(!TFileInput(profile).ReadAll().Contains("invalid"));
    }

    Y_UNIT_TEST_F(StaticOidcConfigFromCommandLine, TCliTestFixture) {
        TTempDir dir;
        TFileOutput((dir.Path() / "token").GetPath()).Write("Bearer config-token\n");
        const auto config = (dir.Path() / "oidc.yaml").GetPath();
        TFileOutput(config).Write("issuer: https://issuer.example\nstatic_credentials:\n  access_token_file: token\n");
        ExpectToken("Bearer config-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, "scheme", "ls"}, {
            {"YDB_TOKEN", "ignored-token"}, {"YDB_OIDC_ISSUER", "ignored-issuer"},
            {"YDB_OIDC_FLOW", "ignored-flow"}, {"YDB_OIDC_CLIENT_ID", "ignored-client"},
            {"YDB_OIDC_SCOPE", "ignored-scope"}, {"YDB_OIDC_ACCESS_TOKEN", "ignored-access-token"},
        });
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, "config", "info"});
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-config: " + config);
        UNIT_ASSERT(!output.Contains("config-token"));
    }

    Y_UNIT_TEST_F(ClientOidcConfigUsesLiteralEnvironmentSecret, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-config-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        const auto config = EnvFile(TStringBuilder() << "issuer: " << idp.Issuer()
            << "\nclient_credentials_grant:\n  client_id: yaml-client\n", "oidc.yaml");
        ExpectToken("Bearer client-config-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, "scheme", "ls"}, {
            {"YDB_OIDC_CLIENT_SECRET", "literal-secret"}, {"YDB_OIDC_CLIENT_ID", "ignored-client"},
        });
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("yaml-client:literal-secret"));
    }

    Y_UNIT_TEST_F(CreatesAndUsesOidcConfigProfile, TCliTestFixture) {
        const auto token = EnvFile("profile-config-token", "token");
        const auto config = EnvFile(TStringBuilder() << "issuer: https://issuer.example\nstatic_credentials:\n  access_token_file: " << token << "\n", "oidc.yaml");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-config-profile",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config}, "");
        const auto stored = TFileInput(profileFile).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(stored, "method: oidc-config");
        UNIT_ASSERT_STRING_CONTAINS(stored, config);
        UNIT_ASSERT(!stored.Contains("profile-config-token"));
        ExpectToken("Bearer profile-config-token");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-config-profile", "scheme", "ls"}, {
            {"YDB_TOKEN", "ignored-token"}, {"YDB_OIDC_ISSUER", "invalid-ignored-issuer"},
        });
        const auto output = RunCli({"--profile-file", profileFile, "config", "profile", "get", "oidc-config-profile"});
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-config: " + config);
        UNIT_ASSERT(!output.Contains("profile-config-token"));
    }

    Y_UNIT_TEST_F(OidcConfigAuthenticationPriority, TCliTestFixture) {
        const auto token = EnvFile("profile-config-token", "token");
        const auto config = EnvFile(TStringBuilder() << "issuer: https://issuer.example\nstatic_credentials:\n  access_token_file: " << token << "\n", "oidc.yaml");
        const auto profile = ConfigProfile(config);
        ExpectToken("Bearer profile-config-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {}, profile);
        ExpectToken("legacy-env-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {{"YDB_TOKEN", "legacy-env-token"}}, profile);
        ExpectToken("Bearer profile-config-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--profile", "oidc", "scheme", "ls"}, {{"YDB_TOKEN", "ignored-env-token"}}, profile);
        const auto explicitToken = EnvFile("explicit-token", "token");
        ExpectToken("explicit-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--profile", "oidc", "--token-file", explicitToken, "scheme", "ls"}, {}, ConfigProfile("/nonexistent/oidc.yaml"));
        ExpectToken("Bearer profile-config-token");
        RunCli({"--profile", "oidc", "--oidc-config", config, "scheme", "ls"}, {{"YDB_TOKEN", "ignored-env-token"}},
            TokenProfile(GetEndpoint(), GetDatabase(), "/nonexistent/old-token"));
    }

    Y_UNIT_TEST_F(DirectOidcOptionsOverrideConfigProfileWithoutOpeningFile, TCliTestFixture) {
        const auto profile = ConfigProfile("/nonexistent/oidc.yaml");
        ExpectToken("Bearer fresh-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {
            {"YDB_OIDC_ISSUER", "https://issuer.example"}, {"YDB_OIDC_ACCESS_TOKEN", "fresh-token"},
        }, profile);
        const auto token = EnvFile("fresh-token", "token");
        ExpectToken("Bearer fresh-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--profile", "oidc", "--oidc-issuer", "https://issuer.example",
            "--oidc-access-token-file", token, "scheme", "ls"}, {}, profile);
    }

    Y_UNIT_TEST_F(RejectsConflictingOidcConfigOptions, TCliTestFixture) {
        const auto token = EnvFile("token", "token");
        const auto config = EnvFile(TStringBuilder() << "issuer: https://issuer.example\nstatic_credentials:\n  access_token_file: " << token << "\n", "oidc.yaml");
        for (const auto& [option, value] : std::initializer_list<std::pair<TString, TString>>{
                {"--oidc-issuer", "https://issuer.example"}, {"--oidc-flow", "static"}, {"--oidc-scope", "read"},
                {"--oidc-access-token-file", token}, {"--oidc-client-secret-file", token}, {"--token-file", token}})
        {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, option, value, "config", "info"});
            ExpectFail();
            RunCliWithInput({"config", "profile", "create", "conflicting", "--oidc-config", config, option, value}, "");
        }
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-scope", "read", "config", "info"}, {}, ConfigProfile(config));
    }

    Y_UNIT_TEST_F(RejectsMissingEmptyAndInvalidOidcConfig, TCliTestFixture) {
        const auto empty = EnvFile("", "empty.yaml");
        const auto invalid = EnvFile("{broken yaml", "invalid.yaml");
        const auto inlineSecret = EnvFile("issuer: https://issuer.example\nstatic_credentials:\n  access_token: forbidden-inline-token\n", "inline.yaml");
        for (const TString& config : {TString(), TString("/nonexistent/oidc.yaml"), empty, invalid, inlineSecret}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, "config", "info"}, {{"YDB_TOKEN", "must-not-fall-back"}});
        }
        ExpectFail();
        RunCliWithInput({"config", "profile", "create", "empty-config", "--oidc-config", ""}, "");
    }

    Y_UNIT_TEST_F(DeviceOidcConfigReusesRelativeCache, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(TStringBuilder() << R"({"device_code":"code","user_code":"USER","verification_uri":")"
            << idp.Issuer() << R"(/verify","expires_in":60,"interval":1})", HTTP_OK);
        idp.Enqueue(R"({"access_token":"device-config-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        TTempDir dir;
        const auto config = (dir.Path() / "oidc.yaml").GetPath();
        TFileOutput(config).Write(TStringBuilder() << "issuer: " << idp.Issuer()
            << "\ncache_path: tokens.json\ndevice_authorization_grant:\n  client_id: cli\n");
        const TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", config, "scheme", "ls"};
        ExpectToken("Bearer device-config-token");
        RunCli(args);
        UNIT_ASSERT_VALUES_EQUAL(idp.Requests().size(), 2);
        UNIT_ASSERT((dir.Path() / "tokens.json").Exists());
        ExpectToken("Bearer device-config-token");
        RunCli(args);
        UNIT_ASSERT_VALUES_EQUAL(idp.Requests().size(), 2);
    }

    Y_UNIT_TEST_F(OidcTokenFileFromCommandLine, TCliTestFixture) {
        const auto file = EnvFile("raw-oidc-token\n", "token");
        ExpectToken("Bearer raw-oidc-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-access-token-file", file, "scheme", "ls"}, {{"YDB_TOKEN", "ignored-env-token"}});
    }

    Y_UNIT_TEST_F(CreatesAndUsesOidcTokenFileProfile, TCliTestFixture) {
        const auto tokenFile = EnvFile("Bearer private-profile-token", "token");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-static",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-access-token-file", tokenFile}, "");
        const auto stored = TFileInput(profileFile).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(stored, "access_token_file:");
        UNIT_ASSERT_STRING_CONTAINS(stored, tokenFile);
        UNIT_ASSERT(!stored.Contains("private-profile-token"));
        ExpectToken("Bearer private-profile-token");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-static", "scheme", "ls"});
        const auto output = RunCli({"--profile-file", profileFile, "config", "profile", "get", "oidc-static"});
        UNIT_ASSERT_STRING_CONTAINS(output, "access_token_file: " + tokenFile);
        UNIT_ASSERT(!output.Contains("private-profile-token"));
    }

    Y_UNIT_TEST_F(StaticOidcFlowUsesOidcTokenEnvironment, TCliTestFixture) {
        ExpectToken("Bearer environment-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "static", "scheme", "ls"}, {{"YDB_OIDC_ACCESS_TOKEN", "environment-token"}});
    }

    Y_UNIT_TEST_F(StaticOidcEnvironmentTokenIsLiteralAndMasked, TCliTestFixture) {
        const auto token = EnvFile("must-not-read-file", "token");
        ExpectToken("Bearer " + token);
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example", "scheme", "ls"},
            {{"YDB_OIDC_ACCESS_TOKEN", token}});
        const auto output = RunCli({"-v", "-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", "https://issuer.example", "config", "info"},
            {{"YDB_OIDC_ACCESS_TOKEN", token}});
        UNIT_ASSERT_STRING_CONTAINS(output, "YDB_OIDC_ACCESS_TOKEN: ***");
        UNIT_ASSERT_STRING_CONTAINS(output, "Value: ***. Got from: YDB_OIDC_ACCESS_TOKEN");
        UNIT_ASSERT(!output.Contains(token));
    }

    Y_UNIT_TEST_F(StaticProfileIgnoresEnvironmentFieldsForOtherFlows, TCliTestFixture) {
        const auto tokenFile = EnvFile("profile-token", "token");
        const TString profile = TStringBuilder()
            << "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
            << "        issuer: https://issuer.example\n        flow: static\n"
            << "        access_token_file: " << tokenFile << "\nactive_profile: oidc\n";
        for (const bool explicitProfile : {false, true}) {
            TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase()};
            if (explicitProfile) {
                args.insert(args.end(), {"--profile", "oidc"});
            }
            args.insert(args.end(), {"scheme", "ls"});
            ExpectToken("Bearer profile-token");
            RunCli(args, {{"YDB_OIDC_SCOPE", "read"}, {"YDB_OIDC_CLIENT_ID", "other-client"},
                {"YDB_OIDC_CLIENT_SECRET", "unused-secret"}}, profile);
        }
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-scope", "read", "config", "info"}, {}, profile);
    }

    Y_UNIT_TEST_F(OidcTokenSourcesFollowAuthenticationPriority, TCliTestFixture) {
        const auto tokenFile = EnvFile("profile-token", "token");
        const TString profile = TStringBuilder()
            << "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
            << "        issuer: https://issuer.example\n        flow: static\n"
            << "        access_token_file: " << tokenFile << "\nactive_profile: oidc\n";
        const THashMap<TString, TString> env = {{"YDB_OIDC_ACCESS_TOKEN", "environment-token"}};
        ExpectToken("Bearer environment-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, env, profile);
        ExpectToken("Bearer profile-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--profile", "oidc", "scheme", "ls"}, env, profile);
        const auto cliFile = EnvFile("cli-token", "token");
        ExpectToken("Bearer cli-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--profile", "oidc", "--oidc-access-token-file", cliFile, "scheme", "ls"}, env, profile);
    }

    Y_UNIT_TEST_F(RejectsMissingOrEmptyOidcTokenFile, TCliTestFixture) {
        const auto empty = EnvFile("", "empty-token");
        for (const TString& file : {TString("/nonexistent/token"), empty}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
                "--oidc-access-token-file", file, "scheme", "ls"}, {{"YDB_OIDC_ACCESS_TOKEN", "must-not-fall-back"}});
        }
    }

    Y_UNIT_TEST_F(OidcTokenFileOverrideDoesNotReadStaleProfilePath, TCliTestFixture) {
        const TString profile = "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
            "        issuer: https://issuer.example\n        flow: static\n"
            "        access_token_file: /nonexistent/old-token\nactive_profile: oidc\n";
        const auto file = EnvFile("fresh-token", "token");
        ExpectToken("Bearer fresh-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-access-token-file", file, "scheme", "ls"}, {}, profile);
        ExpectToken("Bearer fresh-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {{"YDB_TOKEN", "Bearer fresh-token"}}, profile);
    }

    Y_UNIT_TEST_F(StaticTokenFromCommandLine, TCliTestFixture) {
        const auto file = EnvFile("Bearer cli-token\n", "token");
        ExpectToken("Bearer cli-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", file, "scheme", "ls"});
    }

    Y_UNIT_TEST_F(StaticTokenFromEnvironment, TCliTestFixture) {
        ExpectToken("Bearer env-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {{"YDB_TOKEN", "Bearer env-token"}});
    }

    Y_UNIT_TEST_F(TokenFilePreservesLegacySemantics, TCliTestFixture) {
        const auto file = EnvFile("legacy-token", "token");
        ExpectToken("legacy-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", file, "scheme", "ls"}, {
            {"YDB_TOKEN", "ignored-token"},
            {"YDB_OIDC_ISSUER", "invalid-ignored-issuer"},
            {"YDB_OIDC_FLOW", "invalid-ignored-flow"},
        });
    }

    Y_UNIT_TEST_F(RejectsRemovedOidcKeyFileOption, TCliTestFixture) {
        const auto file = EnvFile("issuer: https://issuer.example\ndevice_authorization_grant:\n  client_id: cli\n", "oidc.yaml");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-key-file", file, "config", "info"});
        ExpectFail();
        RunCliWithInput({"config", "profile", "create", "oidc-file", "--oidc-key-file", file}, "");
        const auto help = RunCli({"--help"});
        UNIT_ASSERT(!help.Contains("--oidc-key-file"));
        UNIT_ASSERT_STRING_CONTAINS(help, "--oidc-config");
        UNIT_ASSERT_STRING_CONTAINS(help, "--oidc-access-token-file");
        UNIT_ASSERT(!help.Contains("--oidc-token-file"));
        UNIT_ASSERT_STRING_CONTAINS(help, "Device flow requires browser sign-in");
        UNIT_ASSERT(!help.Contains("For OIDC, include the Bearer prefix"));
        UNIT_ASSERT_STRING_CONTAINS(help, "--oidc-client-secret-file");
    }

    Y_UNIT_TEST_F(RemovedConfigEnvironmentVariableIsIgnored, TCliTestFixture) {
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "config", "info"}, {
            {"YDB_OIDC_CONFIG", "/nonexistent/oidc.yaml"},
        });
        UNIT_ASSERT(!output.Contains("oidc-key-file:"));
        UNIT_ASSERT(!output.Contains("must-not-be-used"));
    }

    Y_UNIT_TEST_F(ActiveProfileAndEnvironmentPriority, TCliTestFixture) {
        const auto file = EnvFile("Bearer profile-token", "token");
        const auto profile = TokenProfile(GetEndpoint(), GetDatabase(), file);
        ExpectToken("Bearer profile-token");
        RunCli({"scheme", "ls"}, {}, profile);
        ExpectToken("Bearer env-token");
        RunCli({"scheme", "ls"}, {{"YDB_TOKEN", "Bearer env-token"}}, profile);
    }

    Y_UNIT_TEST_F(ExplicitProfileOverridesEnvironment, TCliTestFixture) {
        const auto file = EnvFile("Bearer profile-token", "token");
        ExpectToken("Bearer profile-token");
        RunCli({"--profile", "oidc", "scheme", "ls"}, {
            {"YDB_TOKEN", "ignored-token"},
            {"YDB_OIDC_ISSUER", "https://other-issuer.example"},
            {"YDB_OIDC_FLOW", "client"},
        }, TokenProfile(GetEndpoint(), GetDatabase(), file));
    }

    Y_UNIT_TEST_F(ExplicitTokenFileOverridesProfile, TCliTestFixture) {
        const auto file = EnvFile("Bearer override-token", "token");
        ExpectToken("Bearer override-token");
        RunCli({"--token-file", file, "scheme", "ls"}, {},
            TokenProfile(GetEndpoint(), GetDatabase(), "/nonexistent/profile-token"));
    }

    Y_UNIT_TEST_F(DoesNotReuseSecretFromDifferentClientProfile, TCliTestFixture) {
        const auto profile = ClientProfile("https://issuer.example", "/nonexistent/old-client-secret");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "new-client", "config", "info"}, {}, profile);
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "new-client", "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "new-client-secret"}}, profile);
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-client-id: new-client");
        UNIT_ASSERT(!output.Contains("oidc-client-secret-file:"));
        UNIT_ASSERT(!output.Contains("old-client-secret"));
        UNIT_ASSERT(!output.Contains("new-client-secret"));
    }

    Y_UNIT_TEST_F(ExplicitAuthenticationIgnoresMissingProfileSecretFile, TCliTestFixture) {
        const auto tokenFile = EnvFile("Bearer cli-token", "token");
        const auto profile = ClientProfile("https://issuer.example", "/nonexistent/old-secret");
        ExpectToken("Bearer cli-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", tokenFile, "scheme", "ls"}, {}, profile);
    }

    Y_UNIT_TEST_F(NewIssuerIgnoresMissingProfileSecretFile, TCliTestFixture) {
        const auto profile = ClientProfile("https://old-issuer.example", "/nonexistent/old-secret");
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://new-issuer.example",
            "--oidc-flow", "client", "--oidc-client-id", "new-client", "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "new-secret"}}, profile);
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-issuer: https://new-issuer.example");
        UNIT_ASSERT(!output.Contains("old-secret"));
        UNIT_ASSERT(!output.Contains("new-secret"));
    }

    Y_UNIT_TEST_F(RejectsInlineProfileSecrets, TCliTestFixture) {
        for (const char* field : {"client_secret", "access_token"}) {
            const auto profile = TStringBuilder()
                << "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
                << "        issuer: https://issuer.example\n        flow: client\n        client_id: client\n"
                << "        " << field << ": do-not-print-secret\nactive_profile: oidc\n";
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "config", "info"}, {}, profile);
        }
    }

    Y_UNIT_TEST_F(RejectsSecretArguments, TCliTestFixture) {
        const auto file = EnvFile("private-secret", "secret");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "client", "--oidc-client-id", "client", "--oidc-client-secret", file, "config", "info"});
        for (const char* option : {"--oidc-access-token", "--oidc-client-secret"}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), option, "secret", "config", "info"});
            ExpectFail();
            RunCliWithInput({"config", "profile", "create", "inline-secret", option, "secret"}, "");
        }
    }

    Y_UNIT_TEST_F(RejectsEmptyProfileOptions, TCliTestFixture) {
        ExpectFail();
        RunCliWithInput({"config", "profile", "create", "empty-secret", "--oidc-client-secret-file", ""}, "");
    }

    Y_UNIT_TEST_F(RejectsConflictingAuthenticationMethods, TCliTestFixture) {
        const auto tokenFile = EnvFile("legacy-token", "token");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", tokenFile,
            "--oidc-issuer", "https://issuer.example", "--oidc-client-id", "client", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(RejectsMissingIssuerAndIncompleteClientFlow, TCliTestFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "client", "scheme", "ls"});
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "client", "--oidc-client-id", "client", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(RejectsMissingAndEmptySecretFile, TCliTestFixture) {
        const auto empty = EnvFile("", "empty");
        for (const TString& file : {TString("/nonexistent/secret"), empty}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
                "--oidc-flow", "client", "--oidc-client-id", "client", "--oidc-client-secret-file", file, "config", "info"},
                {{"YDB_OIDC_CLIENT_SECRET", "must-not-fall-back"}});
        }
    }

    Y_UNIT_TEST_F(MasksSecretsInVerboseConnectionInfo, TCliTestFixture) {
        const auto file = EnvFile("private-cli-secret", "secret");
        const auto output = RunCli({"-v", "-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", "https://issuer.example", "--oidc-flow", "client",
            "--oidc-client-id", "client", "--oidc-client-secret-file", file, "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "private-env-secret"}});
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-client-secret-file: " + file);
        UNIT_ASSERT_STRING_CONTAINS(output, "\"oidc-client-secret-file\" sources:");
        UNIT_ASSERT(!output.Contains("\"oidc-client-secret\" sources:"));
        UNIT_ASSERT_STRING_CONTAINS(output, "Value: ***. Got from: explicit --oidc-client-secret-file option");
        UNIT_ASSERT_STRING_CONTAINS(output, "Value: ***. Got from: YDB_OIDC_CLIENT_SECRET");
        UNIT_ASSERT(!output.Contains("private-cli-secret"));
        UNIT_ASSERT(!output.Contains("private-env-secret"));
    }

    Y_UNIT_TEST_F(CreatesAndUsesTokenFileProfile, TCliTestFixture) {
        const auto tokenFile = EnvFile("Bearer profile-secret", "token");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-static",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", tokenFile}, "");
        const auto stored = TFileInput(profileFile).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(stored, tokenFile);
        UNIT_ASSERT(!stored.Contains("profile-secret"));
        ExpectToken("Bearer profile-secret");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-static", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(CreatesAndUsesClientSecretFileProfile, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        const auto secretFile = EnvFile("profile-secret\n", "secret");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-client",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", TString(idp.Issuer()),
            "--oidc-flow", "client", "--oidc-client-id", "client", "--oidc-client-secret-file", secretFile}, "");
        const auto stored = TFileInput(profileFile).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(stored, "client_secret_file:");
        UNIT_ASSERT_STRING_CONTAINS(stored, secretFile);
        UNIT_ASSERT(!stored.Contains("profile-secret"));
        ExpectToken("Bearer client-token");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-client", "scheme", "ls"},
            {{"YDB_OIDC_CLIENT_SECRET", "ignored-env-secret"}});
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:profile-secret"));
        const auto output = RunCli({"--profile-file", profileFile, "config", "profile", "get", "oidc-client"});
        UNIT_ASSERT_STRING_CONTAINS(output, secretFile);
        UNIT_ASSERT(!output.Contains("profile-secret"));
    }

    Y_UNIT_TEST_F(AccumulatesOidcScopesFromStdin, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"scoped-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        const auto secretFile = EnvFile("secret", "secret");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "scoped",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", TString(idp.Issuer()),
            "--oidc-flow", "client", "--oidc-client-id", "client", "--oidc-client-secret-file", secretFile,
            "--oidc-scope", "read"}, "oidc-scope: write\noidc-scope: offline_access\n");
        UNIT_ASSERT_STRING_CONTAINS(TFileInput(profileFile).ReadAll(), "scope: write offline_access read");
        ExpectToken("Bearer scoped-token");
        RunCli({"--profile-file", profileFile, "--profile", "scoped", "scheme", "ls"});
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("scope"), "write offline_access read openid");
    }

    Y_UNIT_TEST_F(ClientCredentialsGrantFromFlagsAndEnvironment, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        ExpectToken("Bearer client-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", TString(idp.Issuer()),
            "--oidc-flow", "client", "--oidc-client-id", "client",
            "--oidc-scope", "read", "--oidc-scope", "write", "scheme", "ls"},
            {{"YDB_OIDC_CLIENT_SECRET", "client-secret"}, {"YDB_TOKEN", "ignored-token"}});
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("grant_type"), "client_credentials");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "openid");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "read");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "write");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:client-secret"));
    }

    Y_UNIT_TEST_F(ClientCredentialsGrantFromSecretFile, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        const auto secretFile = EnvFile("file-secret\n", "secret");
        ExpectToken("Bearer client-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", TString(idp.Issuer()),
            "--oidc-flow", "client", "--oidc-client-id", "client", "--oidc-client-secret-file", secretFile, "scheme", "ls"},
            {{"YDB_OIDC_CLIENT_SECRET", "ignored-env-secret"}});
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:file-secret"));
    }

    Y_UNIT_TEST_F(EnvironmentSecretOverridesActiveProfileFile, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        const auto file = EnvFile("ignored-profile-secret", "secret");
        ExpectToken("Bearer client-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"},
            {{"YDB_OIDC_CLIENT_SECRET", "env-secret"}}, ClientProfile(TString(idp.Issuer()), file));
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:env-secret"));
    }

    Y_UNIT_TEST_F(ProfileCreationDoesNotPersistEnvironmentSecret, TCliTestFixture) {
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-client",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "client", "--oidc-client-id", "client"}, "",
            {{"YDB_OIDC_CLIENT_SECRET", "environment-secret"}});
        const auto stored = TFileInput(profileFile).ReadAll();
        UNIT_ASSERT(!stored.Contains("environment-secret"));
        UNIT_ASSERT(!stored.Contains("client_secret"));
        const auto output = RunCli({"--profile-file", profileFile, "--profile", "oidc-client", "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "another-secret"}});
        UNIT_ASSERT_STRING_CONTAINS(output, "YDB_OIDC_CLIENT_SECRET: ***");
        UNIT_ASSERT(!output.Contains("oidc-client-secret:"));
        UNIT_ASSERT(!output.Contains("another-secret"));
    }

    Y_UNIT_TEST_F(RejectsProfileWithoutIssuer, TCliTestFixture) {
        const TString profile = "profiles:\n  oidc:\n    authentication:\n      method: oidc\n"
            "      data:\n        client_id: client\nactive_profile: oidc\n";
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {}, profile);
    }

    Y_UNIT_TEST_F(DeviceGrantWaitsAndReusesCacheBetweenRuns, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(TStringBuilder() << R"({"device_code":"device-code","user_code":"CODE","verification_uri":")"
            << idp.Issuer() << R"(/verify","expires_in":120,"interval":1})", HTTP_OK);
        for (size_t i = 0; i < 11; ++i) {
            idp.Enqueue(R"({"error":"authorization_pending"})", HTTP_BAD_REQUEST);
        }
        idp.Enqueue(R"({"access_token":"device-token","refresh_token":"refresh-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        TTempDir cacheDir;
        const auto cachePath = (cacheDir.Path() / "tokens.json").GetPath();
        const TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", TString(idp.Issuer()), "--oidc-client-id", "device-client",
            "--oidc-cache-path", cachePath, "scheme", "ls"};
        ExpectToken("Bearer device-token");
        const auto output = RunCli(args);
        UNIT_ASSERT(!output.Contains("To sign in"));
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 13);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("client_id"), "device-client");
        UNIT_ASSERT_VALUES_EQUAL(requests[1].Form.Get("grant_type"), "urn:ietf:params:oauth:grant-type:device_code");
        ExpectToken("Bearer device-token");
        RunCli(args);
        UNIT_ASSERT_VALUES_EQUAL(idp.Requests().size(), requests.size());
    }
}
