#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>

#include "ydb_common_ut.h"

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

namespace {
void InitLdapSettings(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort) {
    ldapSettings->SetHost("localhost");
    ldapSettings->SetPort(ldapPort);
    ldapSettings->SetBaseDn("dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindPassword("robouserPassword");
    ldapSettings->SetSearchFilter("uid=$username");
}

void InitLdapSettingsWithInvalidRobotUserLogin(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort) {
    InitLdapSettings(ldapSettings, ldapPort);
    ldapSettings->SetBindDn("cn=invalidRobouser,dc=search,dc=yandex,dc=net");
}

void InitLdapSettingsWithInvalidRobotUserPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort) {
    InitLdapSettings(ldapSettings, ldapPort);
    ldapSettings->SetBindPassword("invalidPassword");
}

void InitLdapSettingsWithInvalidFilter(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort) {
    InitLdapSettings(ldapSettings, ldapPort);
    ldapSettings->SetSearchFilter("&(uid=$username)()");
}

void InitLdapSettingsWithUnavaliableHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort) {
    InitLdapSettings(ldapSettings, ldapPort);
    ldapSettings->SetHost("unavaliablehost");
}

class TLoginClientConnection {
public:
    TLoginClientConnection(std::function<void(NKikimrProto::TLdapAuthentication*, ui16)> initLdapSettings)
        : Server(InitAuthSettings(initLdapSettings))
        , Connection(GetDriverConfig(Server.GetPort()))
        , Client(Connection)
    {}

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
    NKikimrConfig::TAppConfig InitAuthSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16)> initLdapSettings) {
        TPortManager tp;
        LdapPort = tp.GetPort(389);

        NKikimrConfig::TAppConfig appConfig;
        auto authConfig = appConfig.MutableAuthConfig();

        authConfig->SetUseBlackBox(false);
        authConfig->SetUseLoginProvider(true);

        initLdapSettings(authConfig->MutableLdapAuthentication(), LdapPort);
        return appConfig;
    }

    TDriverConfig GetDriverConfig(ui16 grpcPort) {
        TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(grpcPort));
        return config;
    }

private:
    ui16 LdapPort;
    TBasicKikimrWithGrpcAndRootSchema<TKikimrTestSettings> Server;
    NYdb::TDriver Connection;
    NConsoleClient::TDummyClient Client;
};

} // namespace


// These tests check to create token for ldap authenticated users
Y_UNIT_TEST_SUITE(TGRpcLdapAuthentication) {
    Y_UNIT_TEST(LdapAuthWithValidCredentials) {
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


        TLoginClientConnection loginConnection(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString token;
        UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
        UNIT_ASSERT(!token.empty());

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidRobouserLogin) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=invalidRobouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidRobotUserLogin);
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "Could not perform initial LDAP bind for dn cn=invalidRobouser,dc=search,dc=yandex,dc=net on server localhost\nInvalid credentials";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidRobouserPassword) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "invalidPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidRobotUserPassword);
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server localhost\nInvalid credentials";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidSearchFilter) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        TLoginClientConnection loginConnection(InitLdapSettingsWithInvalidFilter);
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "Could not search for filter &(uid=" + login + ")() on server localhost\nBad search filter";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthServerIsUnavaliable) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLoginClientConnection loginConnection(InitLdapSettingsWithUnavaliableHost);
        LdapMock::TLdapMockResponses responses;
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server unavaliablehost\nCan't contact LDAP server";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthWithInvalidLogin) {
        TString nonExistenUser = "nonexistenldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        LdapMock::TSearchRequestInfo fetchUserSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = nonExistenUser},
                .Attributes = {"1.1"}
            }
        };

        LdapMock::TSearchResponseInfo fetchUserSearchResponseInfo {
            .ResponseEntries = {}, // User does not exist. Return empty entries list
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({fetchUserSearchRequestInfo, fetchUserSearchResponseInfo});

        TLoginClientConnection loginConnection(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = nonExistenUser + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "LDAP user " + nonExistenUser + " does not exist. LDAP search for filter uid=" + nonExistenUser + " on server localhost return no entries";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

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
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString expectedErrorMessage = "LDAP login failed for user uid=" + login + ",dc=search,dc=yandex,dc=net on server localhost\nInvalid credentials";
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }
}
} //namespace NKikimr
