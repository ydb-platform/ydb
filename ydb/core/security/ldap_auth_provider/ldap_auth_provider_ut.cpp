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
        Server.GetRuntime()->SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NLog::PRI_TRACE);
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

// Scheme of groups
// *-> cn=people,ou=groups,dc=search,dc=yandex,dc=net
//   |
//   |*-> cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net
//   |  |
//   |  |*-> cn=managerOfProject1,cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net
//   |     |
//   |     |*-> uid=ldapuser,dc=search,dc=yandex,dc=net
//   |
//   |*-> cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net
//      |
//      |*-> cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net
//         |
//         |*-> uid=ldapuser,dc=search,dc=yandex,dc=net

class TCorrectLdapResponse {
public:
    static std::vector<TString> DirectGroups;
    static std::vector<TString> UpdatedDirectGroups;
    static std::vector<TString> ManagerGroups;
    static std::vector<TString> DevelopersGroups;
    static std::vector<TString> PeopleGroups;
    static LdapMock::TLdapMockResponses GetResponses(const TString& login, const TString& groupAttribute = "memberOf");
    static LdapMock::TLdapMockResponses GetAdResponses(const TString& login, const TString& groupAttribute = "memberOf");
    static LdapMock::TLdapMockResponses GetUpdatedResponses(const TString& login, const TString& groupAttribute = "memberOf");
    static THashSet<TString> GetAllGroups(const TString& domain) {
        THashSet<TString> result;
        auto AddGroups = [&result, &domain] (const std::vector<TString>& groups) {
            std::transform(groups.begin(), groups.end(), std::inserter(result, result.end()), [&domain](const TString& group) {
                return TString(group).append(domain);
            });
        };
        AddGroups(DirectGroups);
        AddGroups(ManagerGroups);
        AddGroups(DevelopersGroups);
        AddGroups(PeopleGroups);
        return result;
    }

    static THashSet<TString> GetAllUpdatedGroups(const TString& domain) {
        THashSet<TString> result;
        auto AddGroups = [&result, &domain] (const std::vector<TString>& groups) {
            std::transform(groups.begin(), groups.end(), std::inserter(result, result.end()), [&domain](const TString& group) {
                return TString(group).append(domain);
            });
        };
        AddGroups(UpdatedDirectGroups);
        AddGroups(DevelopersGroups);
        AddGroups(PeopleGroups);
        return result;
    }
};

std::vector<TString> TCorrectLdapResponse::DirectGroups {
    "cn=managerOfProject1,cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
    "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net"
};

std::vector<TString> TCorrectLdapResponse::UpdatedDirectGroups {
    "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net"
};

std::vector<TString> TCorrectLdapResponse::ManagerGroups {
    "cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
};

std::vector<TString> TCorrectLdapResponse::DevelopersGroups {
    "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
};

std::vector<TString> TCorrectLdapResponse::PeopleGroups {
    "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
};

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetResponses(const TString& login, const TString& groupAttribute) {
    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo requestDirectedUserGroups {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseDirectedUserGroupsEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
            .AttributeList = {
                                {groupAttribute, TCorrectLdapResponse::DirectGroups}
                            }
        },
    };

    LdapMock::TSearchResponseInfo responseDirectedUserGroups {
        .ResponseEntries = responseDirectedUserGroupsEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestDirectedUserGroups, responseDirectedUserGroups});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupOfManagers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupOfManagers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupOfManagers->Attribute = "entryDn";
    filterToGetGroupOfManagers->Value = "cn=managerOfProject1,cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupOfDevelopers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupOfDevelopers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupOfDevelopers->Attribute = "entryDn";
    filterToGetGroupOfDevelopers->Value = "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetGroupsOfManagersAndDevelopers = {
        filterToGetGroupOfManagers,
        filterToGetGroupOfDevelopers
    };
    LdapMock::TSearchRequestInfo requestToGetGroupsOfManagersAndDevelopers {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetGroupsOfManagersAndDevelopers},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseEntriesWithGroupsOfManagersAndDevelopers {
        {
            .Dn = "cn=managerOfProject1,cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::ManagerGroups}
            }
        },
        {
            .Dn = "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::DevelopersGroups}
            }
        },
    };

    LdapMock::TSearchResponseInfo responseWithGroupsOfManagersAndDevelopers {
        .ResponseEntries = responseEntriesWithGroupsOfManagersAndDevelopers,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetGroupsOfManagersAndDevelopers, responseWithGroupsOfManagersAndDevelopers});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupPeopleFromManagers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupPeopleFromManagers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupPeopleFromManagers->Attribute = "entryDn";
    filterToGetGroupPeopleFromManagers->Value = "cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupPeopleFromDevelopers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupPeopleFromDevelopers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupPeopleFromDevelopers->Attribute = "entryDn";
    filterToGetGroupPeopleFromDevelopers->Value = "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetGroupOfPeople = {
        filterToGetGroupPeopleFromManagers, filterToGetGroupPeopleFromDevelopers
    };
    LdapMock::TSearchRequestInfo requestToGetGroupOfPeople {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetGroupOfPeople},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseWithGroupOfPeopleEntries {
        {
            .Dn = "cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::PeopleGroups}
            }
        },
        {
            .Dn = "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::PeopleGroups}
            }
        },
    };

    LdapMock::TSearchResponseInfo responseWithGroupOfPeople {
        .ResponseEntries = responseWithGroupOfPeopleEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetGroupOfPeople, responseWithGroupOfPeople});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetParentGroupOfPeople = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetParentGroupOfPeople->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetParentGroupOfPeople->Attribute = "entryDn";
    filterToGetParentGroupOfPeople->Value = "cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetParentGroupOfPeople = {
        filterToGetParentGroupOfPeople
    };
    LdapMock::TSearchRequestInfo requestToGetParentGroupOfPeople {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetParentGroupOfPeople},
            .Attributes = {groupAttribute}
        }
    };

    LdapMock::TSearchResponseInfo responseWithParentGroupOfPeople {
        .ResponseEntries = {},
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetParentGroupOfPeople, responseWithParentGroupOfPeople});

    LdapMock::TSearchRequestInfo requestToGetAllNestedGroupsFromAd {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EXT,
                       .Attribute = "member",
                       .Value = "uid=ldapuser,dc=search,dc=yandex,dc=net",
                       .MatchingRule = "1.2.840.113556.1.4.1941",
                       .DnAttributes = false,
                       .NestedFilters = {}},
            .Attributes = {"1.1"}
        }
    };

    LdapMock::TSearchResponseInfo responseWithAllNestedGroupsFromAd {
        .ResponseEntries = {}, // LDAP server is not Active Directory. Return empty entries
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetAllNestedGroupsFromAd, responseWithAllNestedGroupsFromAd});

    return responses;
}

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetUpdatedResponses(const TString& login, const TString& groupAttribute) {
    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo requestDirectedUserGroups {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseDirectedUserGroupsEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
            .AttributeList = {
                                {groupAttribute, UpdatedDirectGroups}
                            }
        },
    };

    LdapMock::TSearchResponseInfo responseDirectedUserGroups {
        .ResponseEntries = responseDirectedUserGroupsEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestDirectedUserGroups, responseDirectedUserGroups});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupOfDevelopers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupOfDevelopers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupOfDevelopers->Attribute = "entryDn";
    filterToGetGroupOfDevelopers->Value = "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetGroupsOfDevelopers = {
        filterToGetGroupOfDevelopers
    };
    LdapMock::TSearchRequestInfo requestToGetGroupsOfDevelopers {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetGroupsOfDevelopers},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseEntriesWithGroupsOfDevelopers {
        {
            .Dn = "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::DevelopersGroups}
            }
        },
    };

    LdapMock::TSearchResponseInfo responseWithGroupsOfDevelopers {
        .ResponseEntries = responseEntriesWithGroupsOfDevelopers,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetGroupsOfDevelopers, responseWithGroupsOfDevelopers});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetGroupPeopleFromDevelopers = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetGroupPeopleFromDevelopers->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetGroupPeopleFromDevelopers->Attribute = "entryDn";
    filterToGetGroupPeopleFromDevelopers->Value = "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetGroupOfPeople = {
        filterToGetGroupPeopleFromDevelopers
    };
    LdapMock::TSearchRequestInfo requestToGetGroupOfPeople {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetGroupOfPeople},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseWithGroupOfPeopleEntries {
        {
            .Dn = "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {
                {groupAttribute, TCorrectLdapResponse::PeopleGroups}
            }
        },
    };

    LdapMock::TSearchResponseInfo responseWithGroupOfPeople {
        .ResponseEntries = responseWithGroupOfPeopleEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetGroupOfPeople, responseWithGroupOfPeople});

    std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter> filterToGetParentGroupOfPeople = std::make_shared<LdapMock::TSearchRequestInfo::TSearchFilter>();
    filterToGetParentGroupOfPeople->Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY;
    filterToGetParentGroupOfPeople->Attribute = "entryDn";
    filterToGetParentGroupOfPeople->Value = "cn=people,ou=groups,dc=search,dc=yandex,dc=net";

    std::vector<std::shared_ptr<LdapMock::TSearchRequestInfo::TSearchFilter>> nestedFiltersToGetParentGroupOfPeople = {
        filterToGetParentGroupOfPeople
    };
    LdapMock::TSearchRequestInfo requestToGetParentGroupOfPeople {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_OR, .NestedFilters = nestedFiltersToGetParentGroupOfPeople},
            .Attributes = {groupAttribute}
        }
    };

    LdapMock::TSearchResponseInfo responseWithParentGroupOfPeople {
        .ResponseEntries = {},
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetParentGroupOfPeople, responseWithParentGroupOfPeople});

    LdapMock::TSearchRequestInfo requestToGetAllNestedGroupsFromAd {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EXT,
                       .Attribute = "member",
                       .Value = "uid=ldapuser,dc=search,dc=yandex,dc=net",
                       .MatchingRule = "1.2.840.113556.1.4.1941",
                       .DnAttributes = false,
                       .NestedFilters = {}},
            .Attributes = {"1.1"}
        }
    };

    LdapMock::TSearchResponseInfo responseWithAllNestedGroupsFromAd {
        .ResponseEntries = {}, // LDAP server is not Active Directory. Return empty entries
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetAllNestedGroupsFromAd, responseWithAllNestedGroupsFromAd});

    return responses;
}

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetAdResponses(const TString& login, const TString& groupAttribute) {
    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo requestDirectedUserGroups {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {groupAttribute}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseDirectedUserGroupsEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
            .AttributeList = {
                                {groupAttribute, TCorrectLdapResponse::DirectGroups}
                            }
        },
    };

    LdapMock::TSearchResponseInfo responseDirectedUserGroups {
        .ResponseEntries = responseDirectedUserGroupsEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestDirectedUserGroups, responseDirectedUserGroups});

    LdapMock::TSearchRequestInfo requestToGetAllNestedGroupsFromAd {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EXT,
                       .Attribute = "member",
                       .Value = "uid=ldapuser,dc=search,dc=yandex,dc=net",
                       .MatchingRule = "1.2.840.113556.1.4.1941",
                       .DnAttributes = false,
                       .NestedFilters = {}},
            .Attributes = {"1.1"}
        }
    };

    std::vector<LdapMock::TSearchEntry> responseWithAllNestedGroupsFromAdEntries {
        {
            .Dn = "cn=managerOfProject1,cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {}
        },
        {
            .Dn = "cn=project1,cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {}
        },
        {
            .Dn = "cn=managers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {}
        },
        {
            .Dn = "cn=developers,cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {}
        },
        {
            .Dn = "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            .AttributeList = {}
        },
    };

    LdapMock::TSearchResponseInfo responseWithAllNestedGroupsFromAd {
        .ResponseEntries = responseWithAllNestedGroupsFromAdEntries,
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };
    responses.SearchResponses.push_back({requestToGetAllNestedGroupsFromAd, responseWithAllNestedGroupsFromAd});

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

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

    void LdapFetchGroupsFromAdLdapServer(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings, secureType);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetAdResponses(login), secureType == ESecurityConnectionType::LDAPS_SCHEME);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        const TString ldapDomain = "@ldap";
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
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

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
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

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
        expectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "User is unauthorized in LDAP server");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "User is unauthorized in LDAP server");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "User is unauthorized in LDAP server");

        ldapServer.Stop();
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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "User is unauthorized in LDAP server");

        ldapServer.Stop();
    }

    void LdapRefreshGroupsInfoGood(const ESecurityConnectionType& secureType) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";


        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapMockResponses updatedResponses = TCorrectLdapResponse::GetUpdatedResponses(login);
        const TString ldapDomain = "@ldap";

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

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
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

        THashSet<TString> newExpectedGroups = TCorrectLdapResponse::GetAllUpdatedGroups(ldapDomain);
        newExpectedGroups.insert("all-users@well-known");

        UNIT_ASSERT_VALUES_EQUAL(newFetchedGroups.size(), newExpectedGroups.size());
        for (const auto& expectedGroup : newExpectedGroups) {
            UNIT_ASSERT_C(newGroups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
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

        THashSet<TString> expectedGroups = TCorrectLdapResponse::GetAllGroups(ldapDomain);
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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "User is unauthorized in LDAP server");
        UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, false);

        ldapServer.Stop();
    }

Y_UNIT_TEST_SUITE(LdapAuthProviderTest) {
    Y_UNIT_TEST(LdapServerIsUnavailable) {
        CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "User is unauthorized in LDAP server", ESecurityConnectionType::START_TLS);
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
}

Y_UNIT_TEST_SUITE(LdapAuthProviderTest_LdapsScheme) {
    Y_UNIT_TEST(LdapFetchGroupsFromAdLdapServer) {
        LdapFetchGroupsFromAdLdapServer(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::LDAPS_SCHEME);
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::LDAPS_SCHEME);
    }
}

Y_UNIT_TEST_SUITE(LdapAuthProviderTest_StartTls) {
    Y_UNIT_TEST(LdapFetchGroupsFromAdLdapServer) {
        LdapFetchGroupsFromAdLdapServer(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::START_TLS);
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::START_TLS);
    }
}

Y_UNIT_TEST_SUITE(LdapAuthProviderTest_nonSecure) {
    Y_UNIT_TEST(LdapFetchGroupsFromAdLdapServer) {
        LdapFetchGroupsFromAdLdapServer(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood) {
        LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts) {
        LdapFetchGroupsWithDefaultGroupAttributeGoodUseListOfHosts(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood) {
        LdapFetchGroupsWithCustomGroupAttributeGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute) {
        LdapFetchGroupsWithDontExistGroupAttribute(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad) {
        LdapFetchGroupsWithInvalidRobotUserLoginBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad) {
        LdapFetchGroupsWithInvalidRobotUserPasswordBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad) {
        LdapFetchGroupsWithRemovedUserCredentialsBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad) {
        LdapFetchGroupsUseInvalidSearchFilterBad(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood) {
        LdapRefreshGroupsInfoGood(ESecurityConnectionType::NON_SECURE);
    }

    Y_UNIT_TEST(LdapRefreshRemoveUserBad) {
        LdapRefreshRemoveUserBad(ESecurityConnectionType::NON_SECURE);
    }
}

} // NKikimr
