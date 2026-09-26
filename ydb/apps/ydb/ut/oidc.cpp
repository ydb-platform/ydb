#include "mock_env.h"

#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

namespace {

TString StaticProfile(const TString& endpoint, const TString& database);

TString StaticProfile(const TString& endpoint, const TString& database) {
    return TStringBuilder()
        << "profiles:\n"
        << "  oidc:\n"
        << "    endpoint: " << endpoint << "\n"
        << "    database: " << database << "\n"
        << "    authentication:\n"
        << "      method: oidc\n"
        << "      data:\n"
        << "        issuer: https://issuer.example\n"
        << "        flow: static\n"
        << "        access_token: profile-token\n"
        << "active_profile: oidc\n";
}

} // namespace

Y_UNIT_TEST_SUITE(ParseOidcOptionsTest) {
    Y_UNIT_TEST_F(StaticTokenFromCommandLine, TCliTestFixture) {
        ExpectToken("Bearer cli-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", "https://issuer.example", "--oidc-flow", "static",
            "--oidc-access-token", "cli-token", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(StaticTokenFromEnvironment, TCliTestFixture) {
        ExpectToken("Bearer env-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {
            {"YDB_OIDC_ISSUER", "https://issuer.example"},
            {"YDB_OIDC_FLOW", "static"},
            {"YDB_OIDC_ACCESS_TOKEN", "env-token"},
        });
    }

    Y_UNIT_TEST_F(ExplicitOidcOverridesLegacyEnvironment, TCliTestFixture) {
        ExpectToken("Bearer cli-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", "https://issuer.example", "--oidc-flow", "static",
            "--oidc-access-token", "cli-token", "scheme", "ls"}, {
            {"YDB_TOKEN", "legacy-token"},
            {"YDB_OIDC_ACCESS_TOKEN", "env-token"},
        });
    }

    Y_UNIT_TEST_F(ExplicitLegacyAuthIgnoresOidcEnvironment, TCliTestFixture) {
        const auto tokenFile = EnvFile("legacy-token", "token");
        ExpectToken("legacy-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", tokenFile, "scheme", "ls"}, {
            {"YDB_OIDC_ISSUER", "invalid-ignored-issuer"},
            {"YDB_OIDC_FLOW", "invalid-ignored-flow"},
        });
    }

    Y_UNIT_TEST_F(FileConfigFromCommandLineAndEnvironment, TCliTestFixture) {
        const auto file = EnvFile("issuer: https://issuer.example\nstatic_credentials:\n  access_token: file-token\n", "oidc.yaml");
        ExpectToken("Bearer file-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", file, "scheme", "ls"});
        ExpectToken("Bearer file-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {{"YDB_OIDC_CONFIG", file}});
    }

    Y_UNIT_TEST_F(ExplicitFileConfigIgnoresDirectEnvironment, TCliTestFixture) {
        const auto file = EnvFile("issuer: https://issuer.example\nstatic_credentials:\n  access_token: file-token\n", "oidc.yaml");
        ExpectToken("Bearer file-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", file, "scheme", "ls"}, {
            {"YDB_OIDC_ISSUER", "invalid-ignored-issuer"},
            {"YDB_OIDC_FLOW", "invalid-ignored-flow"},
            {"YDB_OIDC_CLIENT_SECRET", "ignored-secret"},
        });
    }

    Y_UNIT_TEST_F(ActiveProfileAndEnvironmentPriority, TCliTestFixture) {
        const auto profile = StaticProfile(GetEndpoint(), GetDatabase());
        ExpectToken("Bearer profile-token");
        RunCli({"scheme", "ls"}, {}, profile);
        ExpectToken("Bearer env-token");
        RunCli({"scheme", "ls"}, {
            {"YDB_OIDC_ISSUER", "https://other-issuer.example"},
            {"YDB_OIDC_FLOW", "static"},
            {"YDB_OIDC_ACCESS_TOKEN", "env-token"},
        }, profile);
    }

    Y_UNIT_TEST_F(ExplicitProfileOverridesEnvironment, TCliTestFixture) {
        ExpectToken("Bearer profile-token");
        RunCli({"--profile", "oidc", "scheme", "ls"}, {
            {"YDB_TOKEN", "ignored-legacy-token"},
            {"YDB_OIDC_ISSUER", "https://other-issuer.example"},
            {"YDB_OIDC_FLOW", "client"},
            {"YDB_OIDC_ACCESS_TOKEN", "ignored-env-token"},
        }, StaticProfile(GetEndpoint(), GetDatabase()));
    }

    Y_UNIT_TEST_F(ExplicitTokenOverridesProfileValue, TCliTestFixture) {
        ExpectToken("Bearer override-token");
        RunCli({"--oidc-access-token", "override-token", "scheme", "ls"}, {}, StaticProfile(GetEndpoint(), GetDatabase()));
    }

    Y_UNIT_TEST_F(TokenOverrideDoesNotReuseExpiredProfileDeadline, TCliTestFixture) {
        auto profile = StaticProfile(GetEndpoint(), GetDatabase());
        profile.insert(profile.find("active_profile:"), "        expires_at: '1'\n");
        ExpectToken("Bearer fresh-token");
        RunCli({"--oidc-access-token", "fresh-token", "scheme", "ls"}, {}, profile);
    }

    Y_UNIT_TEST_F(DoesNotReuseTokenFromDifferentIssuerProfile, TCliTestFixture) {
        ExpectFail();
        RunCli({"--oidc-issuer", "https://other-issuer.example", "--oidc-flow", "static", "scheme", "ls"},
            {}, StaticProfile(GetEndpoint(), GetDatabase()));
    }

    Y_UNIT_TEST_F(DoesNotReuseSecretFromDifferentClientProfile, TCliTestFixture) {
        const TString profile = R"(
profiles:
  oidc:
    authentication:
      method: oidc
      data:
        issuer: https://issuer.example
        flow: client
        client_id: old-client
        client_secret: old-client-secret
active_profile: oidc
)";
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "new-client", "config", "info"}, {}, profile);
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "new-client", "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "new-client-secret"}}, profile);
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-client-id: new-client");
        UNIT_ASSERT(!output.Contains("old-client-secret"));
        UNIT_ASSERT(!output.Contains("new-client-secret"));
    }

    Y_UNIT_TEST_F(RejectsProfileWithoutIssuer, TCliTestFixture) {
        const TString profile = R"(
profiles:
  oidc:
    authentication:
      method: oidc
      data:
        flow: static
        access_token: profile-token
active_profile: oidc
)";
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "scheme", "ls"}, {}, profile);
    }

    Y_UNIT_TEST_F(RejectsEmptyProfileOptions, TCliTestFixture) {
        ExpectFail();
        RunCliWithInput({"config", "profile", "create", "empty-oidc", "--oidc-config", ""}, "");
    }

    Y_UNIT_TEST_F(RejectsConflictingAuthenticationMethods, TCliTestFixture) {
        const auto tokenFile = EnvFile("legacy-token", "token");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--token-file", tokenFile,
            "--oidc-issuer", "https://issuer.example", "--oidc-flow", "static",
            "--oidc-access-token", "oidc-token", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(RejectsFileAndDirectOptions, TCliTestFixture) {
        const auto file = EnvFile("issuer: https://issuer.example\nstatic_credentials:\n  access_token: file-token\n", "oidc.yaml");
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", file,
            "--oidc-client-id", "client", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(RejectsMissingIssuerAndIncompleteClientFlow, TCliTestFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-client-id", "client", "scheme", "ls"});
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "client", "--oidc-client-id", "client", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(MasksSecretsInVerboseConnectionInfo, TCliTestFixture) {
        const auto output = RunCli({"-v", "-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", "https://issuer.example", "--oidc-flow", "client",
            "--oidc-client-id", "client", "--oidc-client-secret", "private-cli-secret", "config", "info"},
            {{"YDB_OIDC_CLIENT_SECRET", "private-env-secret"}});
        UNIT_ASSERT_STRING_CONTAINS(output, "oidc-client-secret: ***");
        UNIT_ASSERT(!output.Contains("private-cli-secret"));
        UNIT_ASSERT(!output.Contains("private-env-secret"));
    }

    Y_UNIT_TEST_F(CreatesAndUsesDirectProfile, TCliTestFixture) {
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-static",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", "https://issuer.example",
            "--oidc-flow", "static", "--oidc-access-token", "profile-secret"}, "");
        ExpectToken("Bearer profile-secret");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-static", "scheme", "ls"});
        const auto output = RunCli({"--profile-file", profileFile, "config", "profile", "get", "oidc-static"});
        UNIT_ASSERT_STRING_CONTAINS(output, "access_token: ***");
        UNIT_ASSERT(!output.Contains("profile-secret"));
    }

    Y_UNIT_TEST_F(CreatesAndUsesFileProfile, TCliTestFixture) {
        const auto file = EnvFile("issuer: https://issuer.example\nstatic_credentials:\n  access_token: file-token\n", "oidc.yaml");
        const auto profileFile = EnvFile("", "profiles.yaml");
        RunCliWithInput({"--profile-file", profileFile, "config", "profile", "create", "oidc-file",
            "-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-config", file}, "");
        ExpectToken("Bearer file-token");
        RunCli({"--profile-file", profileFile, "--profile", "oidc-file", "scheme", "ls"});
    }

    Y_UNIT_TEST_F(ClientCredentialsGrantFromFlagsAndEnvironment, TCliTestFixture) {
        TOidcTestServer idp;
        idp.Enqueue(R"({"access_token":"client-token","token_type":"Bearer","expires_in":600})", HTTP_OK);
        ExpectToken("Bearer client-token");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "--oidc-issuer", TString(idp.Issuer()),
            "--oidc-flow", "client", "--oidc-client-id", "client",
            "--oidc-scope", "read", "--oidc-scope", "write", "scheme", "ls"},
            {{"YDB_OIDC_CLIENT_SECRET", "client-secret"}});
        const auto requests = idp.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("grant_type"), "client_credentials");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "openid");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "read");
        UNIT_ASSERT_STRING_CONTAINS(requests[0].Form.Get("scope"), "write");
        UNIT_ASSERT(requests[0].Authorization.StartsWith("Basic "));
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
