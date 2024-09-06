#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>

#include <util/system/tempfile.h>
#include "ydb_common_ut.h"

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

namespace {

TString certificateContent = R"___(-----BEGIN CERTIFICATE-----
MIIDjTCCAnWgAwIBAgIURt5IBx0J3xgEaQvmyrFH2A+NkpMwDQYJKoZIhvcNAQEL
BQAwVjELMAkGA1UEBhMCUlUxDzANBgNVBAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9z
Y293MQ8wDQYDVQQKDAZZYW5kZXgxFDASBgNVBAMMC3Rlc3Qtc2VydmVyMB4XDTE5
MDkyMDE3MTQ0MVoXDTQ3MDIwNDE3MTQ0MVowVjELMAkGA1UEBhMCUlUxDzANBgNV
BAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFDAS
BgNVBAMMC3Rlc3Qtc2VydmVyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAs0WY6HTuwKntcEcjo+pBuoNp5/GRgMX2qOJi09Iw021ZLK4Vf4drN7pXS5Ba
OVqzUPFmXvoiG13hS7PLTuobJc63qPbIodiB6EXB+Sp0v+mE6lYUUyW9YxNnTPDc
GG8E4vk9j3tBawT4yJIFTudIALWJfQvn3O9ebmYkilvq0ZT+TqBU8Mazo4lNu0T2
YxWMlivcEyNRLPbka5W2Wy5eXGOnStidQFYka2mmCgljtulWzj1i7GODg93vmVyH
NzjAs+mG9MJkT3ietG225BnyPDtu5A3b+vTAFhyJtMmDMyhJ6JtXXHu6zUDQxKiX
6HLGCLIPhL2sk9ckPSkwXoMOywIDAQABo1MwUTAdBgNVHQ4EFgQUDv/xuJ4CvCgG
fPrZP3hRAt2+/LwwHwYDVR0jBBgwFoAUDv/xuJ4CvCgGfPrZP3hRAt2+/LwwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAinKpMYaA2tjLpAnPVbjy
/ZxSBhhB26RiQp3Re8XOKyhTWqgYE6kldYT0aXgK9x9mPC5obQannDDYxDc7lX+/
qP/u1X81ZcDRo/f+qQ3iHfT6Ftt/4O3qLnt45MFM6Q7WabRm82x3KjZTqpF3QUdy
tumWiuAP5DMd1IRDtnKjFHO721OsEsf6NLcqdX89bGeqXDvrkwg3/PNwTyW5E7cj
feY8L2eWtg6AJUnIBu11wvfzkLiH3QKzHvO/SIZTGf5ihDsJ3aKEE9UNauTL3bVc
CRA/5XcX13GJwHHj6LCoc3sL7mt8qV9HKY2AOZ88mpObzISZxgPpdKCfjsrdm63V
6g==
-----END CERTIFICATE-----)___";

TTempFileHandle certificateFile;

void InitLdapSettings(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    ldapSettings->SetHost("localhost");
    ldapSettings->SetPort(ldapPort);
    ldapSettings->SetBaseDn("dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindPassword("robouserPassword");
    ldapSettings->SetSearchFilter("uid=$username");

    auto useTls = ldapSettings->MutableUseTls();
    useTls->SetEnable(true);
    certificateFile.Write(certificateContent.data(), certificateContent.size());
    useTls->SetCaCertFile(certificateFile.Name());
    useTls->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::ALLOW); // Enable TLS connection if server certificate is untrusted
}

void InitLdapSettingsWithInvalidRobotUserLogin(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetBindDn("cn=invalidRobouser,dc=search,dc=yandex,dc=net");
}

void InitLdapSettingsWithInvalidRobotUserPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetBindPassword("invalidPassword");
}

void InitLdapSettingsWithInvalidFilter(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetSearchFilter("&(uid=$username)()");
}

void InitLdapSettingsWithUnavailableHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetHost("unavailablehost");
}

void InitLdapSettingsWithEmptyHosts(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetHost("");
}

void InitLdapSettingsWithEmptyBaseDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetBaseDn("");
}

void InitLdapSettingsWithEmptyBindDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetBindDn("");
}

void InitLdapSettingsWithEmptyBindPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetBindPassword("");
}

class TLoginClientConnection {
public:
    TLoginClientConnection(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)> initLdapSettings, bool isLoginAuthenticationEnabled = true)
        : CaCertificateFile()
        , Server(InitAuthSettings(std::move(initLdapSettings), isLoginAuthenticationEnabled))
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
    NKikimrConfig::TAppConfig InitAuthSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)>&& initLdapSettings, bool isLoginAuthenticationEnabled = true) {
        TPortManager tp;
        LdapPort = tp.GetPort(389);

        NKikimrConfig::TAppConfig appConfig;
        auto authConfig = appConfig.MutableAuthConfig();

        authConfig->SetUseBlackBox(false);
        authConfig->SetUseLoginProvider(true);
        authConfig->SetEnableLoginAuthentication(isLoginAuthenticationEnabled);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);

        initLdapSettings(authConfig->MutableLdapAuthentication(), LdapPort, CaCertificateFile);
        return appConfig;
    }

    TDriverConfig GetDriverConfig(ui16 grpcPort) {
        TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(grpcPort));
        return config;
    }

private:
    TTempFileHandle CaCertificateFile;
    ui16 LdapPort;
    TKikimrWithGrpcAndRootSchemaWithAuth Server;
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "User is unauthorized in LDAP server");

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
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "User is unauthorized in LDAP server");

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
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "User is unauthorized in LDAP server");

        loginConnection.Stop();
        ldapServer.Stop();
    }

    void CheckRequiredLdapSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)> initLdapSettings, const TString& expectedErrorMessage) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLoginClientConnection loginConnection(initLdapSettings);
        LdapMock::TLdapMockResponses responses;
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, expectedErrorMessage);

        loginConnection.Stop();
        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapAuthServerIsUnavailable) {
        CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "User is unauthorized in LDAP server");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyHosts) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyHosts, "List of ldap server hosts is empty");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBaseDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBaseDn, "Parameter BaseDn is empty");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBindDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindDn, "Parameter BindDn is empty");
    }

    Y_UNIT_TEST(LdapAuthSettingsWithEmptyBindPassword) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindPassword, "Parameter BindPassword is empty");
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
        LdapMock::TLdapSimpleServer ldapServer(loginConnection.GetLdapPort(), responses);

        auto factory = CreateLoginCredentialsProviderFactory({.User = nonExistentUser + "@ldap", .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "User is unauthorized in LDAP server");

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
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "User is unauthorized in LDAP server");

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
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Invalid user");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(DisableBuiltinAuthMechanism) {
        TString login = "builtinUser";
        TString password = "builtinUserPassword";

        TLoginClientConnection loginConnection(InitLdapSettings, false);

        auto factory = CreateLoginCredentialsProviderFactory({.User = login, .Password = password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TStringBuilder expectedErrorMessage;
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Login authentication is disabled");

        loginConnection.Stop();
    }
}
} //namespace NKikimr
