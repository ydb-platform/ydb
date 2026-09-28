#include "mock_env.h"

#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <util/stream/file.h>
#include <library/cpp/string_utils/base64/base64.h>

namespace {

TString TokenProfile(const TString& endpoint, const TString& database, const TString& tokenFile);
TString ClientProfile(const TString& issuer, const TString& secretFile);

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

} // namespace

Y_UNIT_TEST_SUITE(ParseOidcOptionsTest) {
    Y_UNIT_TEST_F(OidcTokenFileFromCommandLine, TCliTestFixture) {
        const auto file = EnvFile("raw-oidc-token\n", "token");
        ExpectToken("Bearer raw-oidc-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-token-file", file, "scheme", "ls"}, {{"YDB_TOKEN", "ignored-env-token"}});
    }

    Y_UNIT_TEST_F(CreatesAndUsesOidcTokenFileProfile, TCliTestFixture) {
        const auto tokenFile = EnvFile("Bearer private-profile-token", "token");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-static",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-token-file", tokenFile}, "");
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

    Y_UNIT_TEST_F(StaticOidcFlowUsesYdbTokenEnvironment, TCliTestFixture) {
        ExpectToken("Bearer environment-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "static", "scheme", "ls"}, {{"YDB_TOKEN", "environment-token"}});
    }

    Y_UNIT_TEST_F(RejectsMissingOrEmptyOidcTokenFile, TCliTestFixture) {
        const auto empty = EnvFile("", "empty-token");
        for (const TString& file : {TString("/nonexistent/token"), empty}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
                "--oidc-token-file", file, "scheme", "ls"}, {{"YDB_TOKEN", "must-not-fall-back"}});
        }
    }

    Y_UNIT_TEST_F(OidcTokenFileOverrideDoesNotReadStaleProfilePath, TCliTestFixture) {
        const TString profile = "profiles:\n  oidc:\n    authentication:\n      method: oidc\n      data:\n"
            "        issuer: https://issuer.example\n        flow: static\n"
            "        access_token_file: /nonexistent/old-token\nactive_profile: oidc\n";
        const auto file = EnvFile("fresh-token", "token");
        ExpectToken("Bearer fresh-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-token-file", file, "scheme", "ls"}, {}, profile);
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
        UNIT_ASSERT_STRING_CONTAINS(help, "--oidc-token-file");
        UNIT_ASSERT(!help.Contains("--oidc-access-token-file"));
        UNIT_ASSERT(!help.Contains("For OIDC, include the Bearer prefix"));
        UNIT_ASSERT_STRING_CONTAINS(help, "--oidc-client-secret-file");
    }

    Y_UNIT_TEST_F(RemovedEnvironmentVariablesAreIgnored, TCliTestFixture) {
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "config", "info"}, {
            {"YDB_OIDC_CONFIG", "/nonexistent/oidc.yaml"},
            {"YDB_OIDC_ACCESS_TOKEN", "must-not-be-used"},
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
