#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/system/tempfile.h>

#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>

#include "ldap_auth_provider.h"

namespace NKikimr {

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

enum class ESecurityConnectionType {
    NON_SECURE,
    START_TLS,
    LDAPS_SCHEME,
};

void InitLdapSettings(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    ldapSettings->SetHost("localhost");
    ldapSettings->SetPort(ldapPort);
    ldapSettings->SetBaseDn("dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindPassword("robouserPassword");
    ldapSettings->SetSearchFilter("uid=$username");

    const auto setCertificate = [&ldapSettings] (bool useStartTls, TTempFileHandle& certificateFile) {
        auto useTls = ldapSettings->MutableUseTls();
        useTls->SetEnable(useStartTls);
        certificateFile.Write(certificateContent.data(), certificateContent.size());
        useTls->SetCaCertFile(certificateFile.Name());
        useTls->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::ALLOW); // Enable TLS connection if server certificate is untrusted
    };

    if (securityConnectionType == ESecurityConnectionType::START_TLS) {
        setCertificate(true, certificateFile);
    } else if (securityConnectionType == ESecurityConnectionType::LDAPS_SCHEME) {
        ldapSettings->SetScheme("ldaps");
        setCertificate(false, certificateFile);
    }
}

void InitLdapSettingsWithInvalidRobotUserLogin(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetBindDn("cn=invalidRobouser,dc=search,dc=yandex,dc=net");
}

void InitLdapSettingsWithInvalidRobotUserPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetBindPassword("invalidPassword");
}

void InitLdapSettingsWithInvalidFilter(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetSearchFilter("&(uid=$username)()");
}

void InitLdapSettingsWithUnavailableHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetHost("unavailablehost");
}

void InitLdapSettingsWithEmptyHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetHost("");
}

void InitLdapSettingsWithEmptyBaseDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetBaseDn("");
}

void InitLdapSettingsWithEmptyBindDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetBindDn("");
}

void InitLdapSettingsWithEmptyBindPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetBindPassword("");
}

void InitLdapSettingsWithCustomGroupAttribute(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->SetRequestedGroupAttribute("groupDN");
}

void InitLdapSettingsWithListOfHosts(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile, const ESecurityConnectionType& securityConnectionType) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile, securityConnectionType);
    ldapSettings->AddHosts("qqq");
    ldapSettings->AddHosts("localhost");
    ldapSettings->AddHosts("localhost:11111");
}

class TLdapKikimrServer {
public:
    TLdapKikimrServer(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&, const ESecurityConnectionType&)> initLdapSettings, const ESecurityConnectionType& securityConnectionType)
        : CaCertificateFile()
        , SecurityConnectionType(securityConnectionType)
        , Server(InitSettings(std::move(initLdapSettings))) {
        Server.EnableGRpc(GrpcPort);
        Server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        Server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
    }

    TTestActorRuntime* GetRuntime() const {
        return Server.GetRuntime();
    }

    ui16 GetLdapPort() const {
        return LdapPort;
    }

private:
    Tests::TServerSettings InitSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&, const ESecurityConnectionType&)>&& initLdapSettings) {
        using namespace Tests;
        TPortManager tp;
        LdapPort = tp.GetPort(389);
        ui16 kikimrPort = tp.GetPort(2134);
        GrpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");

        initLdapSettings(authConfig.MutableLdapAuthentication(), LdapPort, CaCertificateFile, SecurityConnectionType);

        Tests::TServerSettings settings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

private:
    TTempFileHandle CaCertificateFile;
    ESecurityConnectionType SecurityConnectionType = ESecurityConnectionType::NON_SECURE;
    Tests::TServer Server;
    ui16 LdapPort;
    ui16 GrpcPort;
};

NLogin::TLoginProvider::TLoginUserResponse GetLoginResponse(TLdapKikimrServer& server, const TString& login, const TString& password) {
    TTestActorRuntime* runtime = server.GetRuntime();
    NLogin::TLoginProvider provider;
    provider.Audience = "/Root";
    provider.RotateKeys();
    TActorId sender = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);
    return provider.LoginUser({.User = login, .Password = password, .ExternalAuth = "ldap"});
}

TAutoPtr<IEventHandle> LdapAuthenticate(TLdapKikimrServer& server, const TString& login, const TString& password) {
    auto loginResponse = GetLoginResponse(server, login, password);
    TTestActorRuntime* runtime = server.GetRuntime();
    TActorId sender = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
    return handle;
}

class TCorrectLdapResponse {
public:
    static std::vector<TString> Groups;
    static LdapMock::TLdapMockResponses GetResponses(const TString& login, const TString& groupAttribute = "memberOf");
};

std::vector<TString> TCorrectLdapResponse::Groups {
    "ou=groups,dc=search,dc=yandex,dc=net",
    "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
    "cn=developers,ou=groups,dc=search,dc=yandex,dc=net"
};

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetResponses(const TString& login, const TString& groupAttribute) {
    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo fetchGroupsSearchRequestInfo {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> fetchGroupsSearchResponseEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
            .AttributeList = {
                                {groupAttribute, TCorrectLdapResponse::Groups}
                            }
        }
    };

    LdapMock::TSearchResponseInfo fetchGroupsSearchResponseInfo {
        .ResponseEntries = fetchGroupsSearchResponseEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({fetchGroupsSearchRequestInfo, fetchGroupsSearchResponseInfo});
    return responses;
}

void CheckRequiredLdapSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&, const ESecurityConnectionType&)> initLdapSettings,
                               const TString& expectedErrorMessage,
                               const ESecurityConnectionType& securityConnectionType = ESecurityConnectionType::NON_SECURE) {
    TLdapKikimrServer server(initLdapSettings, securityConnectionType);

    LdapMock::TLdapMockResponses responses;
    LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, securityConnectionType == ESecurityConnectionType::LDAPS_SCHEME);

    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);

    ldapServer.Stop();
}

} // namespace


Y_UNIT_TEST_SUITE(LdapAuthProviderTest) {

void LdapFetchGroupsWithDefaultGroupAttributeGood(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login), secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups;
        std::transform(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end(), std::inserter(expectedGroups, expectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood_nonSecure) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood_StartTls) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood_LdapsScheme) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettingsWithListOfHosts, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login), secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups;
        std::transform(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end(), std::inserter(expectedGroups, expectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts_nonSecure) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts_StartTls) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts_LdapsScheme) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithCustomGroupAttributeGood(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettingsWithCustomGroupAttribute, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login, "groupDN"), secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups;
        std::transform(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end(), std::inserter(expectedGroups, expectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood_nonSecure) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood_StartTls) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood_LdapsScheme) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithDontExistGroupAttribute(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettingsWithCustomGroupAttribute, secureType);

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        LdapMock::TSearchRequestInfo fetchGroupsSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                .Attributes = {"groupDN"}
            }
        };

        std::vector<LdapMock::TSearchEntry> fetchGroupsSearchResponseEntries {
            {
                .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
                .AttributeList = {} // Return empty group list, attribute 'groupDN' not found
            }
        };

        LdapMock::TSearchResponseInfo fetchGroupsSearchResponseInfo {
            .ResponseEntries = fetchGroupsSearchResponseEntries,
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({fetchGroupsSearchRequestInfo, fetchGroupsSearchResponseInfo});

        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        UNIT_ASSERT_EQUAL(fetchedGroups.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(fetchedGroups.front(), "all-users@well-known");

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute_nonSecure) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute_StartTls) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute_LdapsScheme) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithInvalidRobotUserLoginBad(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=invalidRobouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidRobotUserLogin, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        TStringBuilder expectedErrorMessage;
        expectedErrorMessage << "Could not perform initial LDAP bind for dn cn=invalidRobouser,dc=search,dc=yandex,dc=net on server "
                             << (secureType == ESecurityConnectionType::LDAPS_SCHEME ? "ldaps://" : "ldap://") << "localhost:"
                             << server.GetLdapPort() << "\nInvalid credentials";
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad_nonSecure) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad_StartTls) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad_LdapsScheme) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithInvalidRobotUserPasswordBad(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "invalidPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidRobotUserPassword, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        TStringBuilder expectedErrorMessage;
        expectedErrorMessage << "Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server "
                             << (secureType == ESecurityConnectionType::LDAPS_SCHEME ? "ldaps://" : "ldap://") << "localhost:"
                             << server.GetLdapPort() << "\nInvalid credentials";
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad_nonSecure) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad_StartTls) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad_LdapsScheme) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsWithRemovedUserCredentialsBad(const ESecurityConnectionType& secureType) {
        TString removedUserLogin = "ldapuser";
        TString removedUserPassword = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        LdapMock::TSearchRequestInfo removedUserSearchRequestInfo {
            {
                .BaseDn = "dc=search,dc=yandex,dc=net",
                .Scope = 2,
                .DerefAliases = 0,
                .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = removedUserLogin},
                .Attributes = {"memberOf"}
            }
        };

        LdapMock::TSearchResponseInfo removedUserSearchResponseInfo {
            .ResponseEntries = {}, // Removed user was not found. Return empty list of entries
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };
        responses.SearchResponses.push_back({removedUserSearchRequestInfo, removedUserSearchResponseInfo});

        TLdapKikimrServer server(InitLdapSettings, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, removedUserLogin, removedUserPassword);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        const TString expectedErrorMessage = "LDAP user " + removedUserLogin + " does not exist. "
                                             "LDAP search for filter uid=" + removedUserLogin + " on server " +
                                             (secureType == ESecurityConnectionType::LDAPS_SCHEME ? "ldaps://" : "ldap://") + "localhost:" +
                                             ToString(server.GetLdapPort()) + " return no entries";
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad_nonSecure) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad_StartTls) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad_LdapsScheme) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapFetchGroupsUseInvalidSearchFilterBad(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidFilter, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        const TString expectedErrorMessage = "Could not search for filter &(uid=" + login + ")() on server " +
                                              (secureType == ESecurityConnectionType::LDAPS_SCHEME ? "ldaps://" : "ldap://") + "localhost:" +
                                              ToString(server.GetLdapPort()) + "\nBad search filter";
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad_nonSecure) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad_StartTls) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad_LdapsScheme) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapServerIsUnavailable) {
        CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "Could not start TLS\nCan't contact LDAP server", ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapRequestWithEmptyHost) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyHost, "List of ldap server hosts is empty");
    }

    Y_UNIT_TEST(LdapRequestWithEmptyBaseDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBaseDn, "Parameter BaseDn is empty");
    }

    Y_UNIT_TEST(LdapRequestWithEmptyBindDn) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindDn, "Parameter BindDn is empty");
    }

    Y_UNIT_TEST(LdapRequestWithEmptyBindPassword) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindPassword, "Parameter BindPassword is empty");
    }

    void LdapRefreshGroupsInfoGood(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";


        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapMockResponses updatedResponses = responses;

        std::vector<TString> newLdapGroups {
            "ou=groups,dc=search,dc=yandex,dc=net",
            "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            "cn=designers,ou=groups,dc=search,dc=yandex,dc=net"
        };
        std::vector<LdapMock::TSearchEntry> newFetchGroupsSearchResponseEntries {
            {
                .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
                .AttributeList = {
                                    {"memberOf", newLdapGroups}
                                }
            }
        };

        const TString ldapDomain = "@ldap";
        THashSet<TString> newExpectedGroups;
        std::transform(newLdapGroups.begin(), newLdapGroups.end(), std::inserter(newExpectedGroups, newExpectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        newExpectedGroups.insert("all-users@well-known");

        LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
            .ResponseEntries = newFetchGroupsSearchResponseEntries,
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };

        auto& searchResponse = updatedResponses.SearchResponses.front();
        searchResponse.second = newFetchGroupsSearchResponseInfo;

        TLdapKikimrServer server(InitLdapSettings, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), {responses, updatedResponses}, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        auto loginResponse = GetLoginResponse(server, login, password);
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups;
        std::transform(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end(), std::inserter(expectedGroups, expectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.UpdateResponses();
        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + "@ldap");
        const auto& newFetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> newGroups(newFetchedGroups.begin(), newFetchedGroups.end());
        UNIT_ASSERT_VALUES_EQUAL(newFetchedGroups.size(), newExpectedGroups.size());
        for (const auto& expectedGroup : newExpectedGroups) {
            UNIT_ASSERT_C(newGroups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood_nonSecure) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood_StartTls) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood_LdapsScheme) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    void LdapRefreshRemoveUserBad(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings, secureType);
        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapMockResponses updatedResponses = responses;
        LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
            .ResponseEntries = {}, // User has been removed. Return empty entries list
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };

        auto& searchResponse = updatedResponses.SearchResponses.front();
        searchResponse.second = newFetchGroupsSearchResponseInfo;
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), {responses, updatedResponses}, secureType == ESecurityConnectionType::LDAPS_SCHEME);

        auto loginResponse = GetLoginResponse(server, login, password);
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups;
        std::transform(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end(), std::inserter(expectedGroups, expectedGroups.end()), [&ldapDomain](TString& group) {
            return group.append(ldapDomain);
        });
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.UpdateResponses();
        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);
        const TString expectedErrorMessage = "LDAP user " + login + " does not exist. "
                                             "LDAP search for filter uid=" + login + " on server " +
                                             (secureType == ESecurityConnectionType::LDAPS_SCHEME ? "ldaps://" : "ldap://") + "localhost:" +
                                             ToString(server.GetLdapPort()) + " return no entries";
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);
        UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, false);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad_nonSecure) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad_StartTls) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad_LdapsScheme) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

}

} // NKikimr
