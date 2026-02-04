#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/simple_server.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_defines.h>

#include <ydb/core/security/ldap_auth_provider/test_utils/test_settings.h>
#include "ydb_common_ut.h"

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

namespace {

class TLoginClientConnection {
public:
    TLoginClientConnection(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)> initLdapSettings, const TLdapClientOptions& ldapClientOptions = {})
        : LdapClientOptions(ldapClientOptions)
        , Server(InitAuthSettings(std::move(initLdapSettings)))
        , Connection(GetDriverConfig(Server.GetPort()))
        , Client(Connection)
    {
        Server.GetRuntime()->SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NLog::PRI_TRACE);
    }

    ui16 GetLdapPort() const {
        return LdapPort;
    }

    void Stop() {
        Connection.Stop(true);
    }

    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility() {
        return Client.GetCoreFacility();
    }

private:
    NKikimrConfig::TAppConfig InitAuthSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)>&& initLdapSettings) {
        TPortManager tp;
        LdapPort = tp.GetPort(389);

        NKikimrConfig::TAppConfig appConfig;
        auto authConfig = appConfig.MutableAuthConfig();

        authConfig->SetUseBlackBox(false);
        authConfig->SetUseLoginProvider(true);
        authConfig->SetEnableLoginAuthentication(LdapClientOptions.IsLoginAuthenticationEnabled);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);

        initLdapSettings(authConfig->MutableLdapAuthentication(), LdapPort, LdapClientOptions);
        return appConfig;
    }

    TDriverConfig GetDriverConfig(ui16 grpcPort) {
        TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(grpcPort));
        return config;
    }

private:
    const TLdapClientOptions LdapClientOptions;
    ui16 LdapPort;
    TKikimrWithGrpcAndRootSchemaWithAuth Server;
    NYdb::TDriver Connection;
    NConsoleClient::TDummyClient Client;
};

TCertStorage CertStorage;

void LdapAuthWithValidCredentials(const ESecurityConnectionType& secureType) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});
    responses.BindResponses.push_back({{{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo fetchUserSearchRequestInfo {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {"1.1"}
        }
    };

    std::vector<LdapMock::TSearchEntry> fetchUserSearchResponseEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"
        }
    };

    LdapMock::TSearchResponseInfo fetchUserSearchResponseInfo {
        .ResponseEntries = fetchUserSearchResponseEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({fetchUserSearchRequestInfo, fetchUserSearchResponseInfo});


    TLoginClientConnection loginConnection(InitLdapSettings, {
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .Type = secureType
    });
    LdapMock::TSimpleServer ldapServer({
        .Port = loginConnection.GetLdapPort(),
        .CertFile = CertStorage.GetServerCertFileName(),
        .KeyFile = CertStorage.GetServerKeyFileName(),
        .UseTls = secureType == ESecurityConnectionType::LDAPS_SCHEME
    }, responses);

    ldapServer.Start();
    auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
    auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
    TString token;
    UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
    UNIT_ASSERT(!token.empty());

    loginConnection.Stop();
    ldapServer.Stop();
}

} // namespace


// These tests check to create token for ldap authenticated users
Y_UNIT_TEST_SUITE(TGRpcLdapAuthentication) {
    Y_UNIT_TEST(CanAuthWithValidCredentialsNonSecure) {
        LdapAuthWithValidCredentials(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(CanAuthWithValidCredentialsStartTls) {
        LdapAuthWithValidCredentials(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(CanAuthWithValidCredentialsLdaps) {
        LdapAuthWithValidCredentials(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapAuthWithInvalidRobouserLogin) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=invalidRobouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidRobotUserLogin);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidRobouserPassword) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "invalidPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidRobotUserPassword);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidSearchFilter) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidFilter);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    void CheckRequiredLdapSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)> initLdapSettings, const TString& expectedErrorMessage) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        TLoginClientConnection loginConnection(initLdapSettings);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthServerIsUnavailable) {
        CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "Could not login via LDAP");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyHosts) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyHost, "Could not login via LDAP");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBaseDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBaseDn, "Could not login via LDAP");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBindDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindDn, "Could not login via LDAP");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBindPassword) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindPassword, "Could not login via LDAP");
    }

    Y_UNIT_TEST(LdapAuthWithInvalidLogin) {
        TString nonExistentUser = "nonexistentldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        LdapMock::TSearchRequestInfo fetchUserSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = nonExistentUser},
                .Attributes = {"1.1"}
            }
        };

        LdapMock::TSearchResponseInfo fetchUserSearchResponseInfo {
            .ResponseEntries = {}, // User does not exist. Return empty entries list
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({fetchUserSearchRequestInfo, fetchUserSearchResponseInfo});

        TLoginClientConnection loginConnection(InitLdapSettings);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = nonExistentUser + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidPassword) {
        TString login = "ldapUser";
        TString password = "wrongLdapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});
        responses.BindResponses.push_back({{{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        LdapMock::TSearchRequestInfo fetchUserSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                .Attributes = {"1.1"}
            }
        };

        std::vector<LdapMock::TSearchEntry> fetchUserSearchResponseEntries {
            {
                .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"
            }
        };

        LdapMock::TSearchResponseInfo fetchUserSearchResponseInfo {
            .ResponseEntries = fetchUserSearchResponseEntries,
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({fetchUserSearchRequestInfo, fetchUserSearchResponseInfo});

        TLoginClientConnection loginConnection(InitLdapSettings);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithEmptyPassword) {
        TString login = "ldapUser";
        TString password = "";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        LdapMock::TSearchRequestInfo fetchUserSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                .Attributes = {"1.1"}
            }
        };

        std::vector<LdapMock::TSearchEntry> fetchUserSearchResponseEntries {
            {
                .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"
            }
        };

        LdapMock::TSearchResponseInfo fetchUserSearchResponseInfo {
            .ResponseEntries = fetchUserSearchResponseEntries,
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({fetchUserSearchRequestInfo, fetchUserSearchResponseInfo});

        TLoginClientConnection loginConnection(InitLdapSettings);
        LdapMock::TSimpleServer ldapServer({.Port = loginConnection.GetLdapPort()}, responses);

        ldapServer.Start();
        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Could not login via LDAP");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthSetIncorrectDomain) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";
        const TString incorrectLdapDomain = "@ldap.domain"; // Correct domain is AuthConfig.LdapAuthenticationDomain: "ldap"

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + incorrectLdapDomain, .Password = password});
        TLoginClientConnection loginConnection(InitLdapSettings);
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Cannot find user: ldapuser@ldap.domain");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(DisableBuiltinAuthMechanism) {
        TString login = "builtinUser";
        TString password = "builtinUserPassword";

        TLoginClientConnection loginConnection(InitLdapSettings, {
            .IsLoginAuthenticationEnabled = false
        });

        auto factory = CreateLoginCredentialsProviderFactory({.User = login, .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TStringBuilder expectedErrorMessage;
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Login authentication is disabled");

        loginConnection.Stop();
    }
}
} //namespace NKikimr
