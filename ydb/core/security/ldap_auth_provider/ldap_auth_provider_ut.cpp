#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/system/tempfile.h>

#include <ydb/library/testlib/service_mocks/ldap_mock/simple_server.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_defines.h>

#include <ydb/core/security/ldap_auth_provider/test_utils/test_settings.h>

namespace NKikimr {

namespace {

class TLdapKikimrServer {
public:
    TLdapKikimrServer(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)> initLdapSettings, const TLdapClientOptions& ldapClientOptions = {})
        : LdapClientOptions(ldapClientOptions)
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
    Tests::TServerSettings InitSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)>&& initLdapSettings) {
        using namespace Tests;
        TPortManager tp;
        LdapPort = tp.GetPort(389);
        ui16 kikimrPort = tp.GetPort(2134);
        GrpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");

        initLdapSettings(authConfig.MutableLdapAuthentication(), LdapPort, LdapClientOptions);

        Tests::TServerSettings settings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

private:
    const TLdapClientOptions LdapClientOptions;
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
    static LdapMock::TLdapMockResponses GetResponses(const TString& login, bool doReturnDirectedGroups = false, const TString& groupAttribute = "memberOf");
    static LdapMock::TLdapMockResponses GetAdResponses(const TString& login, bool doReturnDirectedGroups = false, const TString& groupAttribute = "memberOf");
    static LdapMock::TLdapMockResponses GetUpdatedResponses(const TString& login, bool doReturnDirectedGroups = false, const TString& groupAttribute = "memberOf");
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

    static THashSet<TString> GetDirectedGroups(const TString& domain) {
        THashSet<TString> result;
        auto AddGroups = [&result, &domain] (const std::vector<TString>& groups) {
            std::transform(groups.begin(), groups.end(), std::inserter(result, result.end()), [&domain](const TString& group) {
                return TString(group).append(domain);
            });
        };
        AddGroups(DirectGroups);
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

    static THashSet<TString> GetUpdatedDirectedGroups(const TString& domain) {
        THashSet<TString> result;
        auto AddGroups = [&result, &domain] (const std::vector<TString>& groups) {
            std::transform(groups.begin(), groups.end(), std::inserter(result, result.end()), [&domain](const TString& group) {
                return TString(group).append(domain);
            });
        };
        AddGroups(UpdatedDirectGroups);
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

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetResponses(const TString& login, bool doReturnDirectedGroups, const TString& groupAttribute) {
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

    if (doReturnDirectedGroups) {
        return responses;
    }

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

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetUpdatedResponses(const TString& login, bool doReturnDirectedGroups, const TString& groupAttribute) {
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

    if (doReturnDirectedGroups) {
        return responses;
    }

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

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetAdResponses(const TString& login, bool doReturnDirectedGroups, const TString& groupAttribute) {
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

    if (doReturnDirectedGroups) {
        return responses;
    }

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

TCertStorage CertStorage;

void CheckRequiredLdapSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, const TLdapClientOptions&)> initLdapSettings,
                               const TString& expectedErrorMessage,
                               const ESecurityConnectionType& securityConnectionType = ESecurityConnectionType::NON_SECURE) {
    TLdapKikimrServer ydbServer(initLdapSettings, {
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .Type = securityConnectionType
    });

    LdapMock::TSimpleServer ldapServer({
        .Port = ydbServer.GetLdapPort(),
        .CertFile = CertStorage.GetServerCertFileName(),
        .KeyFile = CertStorage.GetServerKeyFileName(),
        .UseTls = securityConnectionType == ESecurityConnectionType::LDAPS_SCHEME
    }, LdapMock::TLdapMockResponses());

    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);

    ldapServer.Stop();
}

void LdapFetchGroupsWithDefaultGroupAttributeGood(const ESecurityConnectionType& secureType) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings, {
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .Type = secureType
    });

    LdapMock::TSimpleServer ldapServer({
        .Port = ydbServer.GetLdapPort(),
        .CertFile = CertStorage.GetServerCertFileName(),
        .KeyFile = CertStorage.GetServerKeyFileName(),
        .UseTls = secureType == ESecurityConnectionType::LDAPS_SCHEME
    }, TCorrectLdapResponse::GetResponses(login));
    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

} // namespace

Y_UNIT_TEST_SUITE(LdapAuthProviderTests) {

Y_UNIT_TEST(CanFetchGroupsWithDefaultGroupAttributeStartTls) {
    LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::START_TLS);
}

Y_UNIT_TEST(CanFetchGroupsWithDefaultGroupAttributeLdaps) {
    LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::LDAPS_SCHEME);
}

Y_UNIT_TEST(CanFetchGroupsWithDefaultGroupAttributeNonSecure) {
    LdapFetchGroupsWithDefaultGroupAttributeGood(ESecurityConnectionType::NON_SECURE);
}

Y_UNIT_TEST(CanFetchGroupsWithDefaultGroupAttributeDisableNestedGroups) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsDisableSearchNestedGroups);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login, true));

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
    UNIT_ASSERT(ticketParserResult->Token != nullptr);
    const TString ldapDomain = "@ldap";
    UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
    const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
    THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

    THashSet<TString> expectedGroups = TCorrectLdapResponse::GetDirectedGroups(ldapDomain);
    expectedGroups.insert("all-users@well-known");

    UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
    for (const auto& expectedGroup : expectedGroups) {
        UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
    }
    ldapServer.Stop();
}

Y_UNIT_TEST(CanFetchGroupsFromAdServer) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login));

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

Y_UNIT_TEST(CanFetchGroupsWithDisabledRequestToAD) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsDisableSearchNestedGroups);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login, true));

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
    UNIT_ASSERT(ticketParserResult->Token != nullptr);
    const TString ldapDomain = "@ldap";
    UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
    const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
    THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

    THashSet<TString> expectedGroups = TCorrectLdapResponse::GetDirectedGroups(ldapDomain);
    expectedGroups.insert("all-users@well-known");

    UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
    for (const auto& expectedGroup : expectedGroups) {
        UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
    }
    ldapServer.Stop();
}

Y_UNIT_TEST(CanFetchGroupsWithDefaultGroupAttributeUseListOfHosts) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsWithListOfHosts);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login));

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

Y_UNIT_TEST(CanFetchGroupsWithCustomGroupAttribute) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsWithCustomGroupAttribute);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login, false, "groupDN"));

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

Y_UNIT_TEST(CanFetchGroupsWithDontExistGroupAttribute) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsWithCustomGroupAttribute);

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

    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

Y_UNIT_TEST(CanNotFetchGroupsWithInvalidRobotUserLogin) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=invalidRobouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

    TLdapKikimrServer ydbServer(InitLdapSettingsWithInvalidRobotUserLogin);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    ldapServer.Stop();
}

Y_UNIT_TEST(CanNotFetchGroupsWithInvalidRobotUserPassword) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "invalidPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

    TLdapKikimrServer ydbServer(InitLdapSettingsWithInvalidRobotUserPassword);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    ldapServer.Stop();
}

Y_UNIT_TEST(CanNotFetchGroupsWithRemovedUserCredentials) {
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

    TLdapKikimrServer ydbServer(InitLdapSettings);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, removedUserLogin, removedUserPassword);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    ldapServer.Stop();
}

Y_UNIT_TEST(CanNotFetchGroupsUseInvalidSearchFilter) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    TLdapKikimrServer ydbServer(InitLdapSettingsWithInvalidFilter);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    ldapServer.Stop();
}

Y_UNIT_TEST(CanRefreshGroupsInfo) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    auto responses = TCorrectLdapResponse::GetResponses(login);
    LdapMock::TLdapMockResponses updatedResponses = TCorrectLdapResponse::GetUpdatedResponses(login);
    const TString ldapDomain = "@ldap";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login));
    ldapServer.Start();

    auto loginResponse = GetLoginResponse(ydbServer, login, password);
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
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

    ldapServer.ReplaceResponses(std::move(updatedResponses));
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

Y_UNIT_TEST(CanRefreshGroupsInfoWithDisabledNestedGroups) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    auto responses = TCorrectLdapResponse::GetResponses(login, true);
    LdapMock::TLdapMockResponses updatedResponses = TCorrectLdapResponse::GetUpdatedResponses(login, true);
    const TString ldapDomain = "@ldap";

    TLdapKikimrServer ydbServer(InitLdapSettingsDisableSearchNestedGroups);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort(),}, responses);

    ldapServer.Start();
    auto loginResponse = GetLoginResponse(ydbServer, login, password);
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
    TActorId sender = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    TAutoPtr<IEventHandle> handle;
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

    UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
    UNIT_ASSERT(ticketParserResult->Token != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + ldapDomain);
    const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
    THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

    THashSet<TString> expectedGroups = TCorrectLdapResponse::GetDirectedGroups(ldapDomain);
    expectedGroups.insert("all-users@well-known");

    UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
    for (const auto& expectedGroup : expectedGroups) {
        UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
    }

    ldapServer.ReplaceResponses(std::move(updatedResponses));
    Sleep(TDuration::Seconds(10));

    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

    UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
    UNIT_ASSERT(ticketParserResult->Token != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + "@ldap");
    const auto& newFetchedGroups = ticketParserResult->Token->GetGroupSIDs();
    THashSet<TString> newGroups(newFetchedGroups.begin(), newFetchedGroups.end());

    THashSet<TString> newExpectedGroups = TCorrectLdapResponse::GetUpdatedDirectedGroups(ldapDomain);
    newExpectedGroups.insert("all-users@well-known");

    UNIT_ASSERT_VALUES_EQUAL(newFetchedGroups.size(), newExpectedGroups.size());
    for (const auto& expectedGroup : newExpectedGroups) {
        UNIT_ASSERT_C(newGroups.contains(expectedGroup), "Can not find " + expectedGroup);
    }
    ldapServer.Stop();
}

Y_UNIT_TEST(CanNotRefreshRemovedUser) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    auto responses = TCorrectLdapResponse::GetResponses(login);
    LdapMock::TLdapMockResponses updatedResponses = responses;
    LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
        .ResponseEntries = {}, // User has been removed. Return empty entries list
        .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
    };

    auto& searchResponse = updatedResponses.SearchResponses.front();
    searchResponse.second = newFetchGroupsSearchResponseInfo;
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    auto loginResponse = GetLoginResponse(ydbServer, login, password);
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
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

    ldapServer.ReplaceResponses(std::move(updatedResponses));
    Sleep(TDuration::Seconds(10));

    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, false);
    ldapServer.Stop();
}

Y_UNIT_TEST(CanRefreshGroupsInfoWithError) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    auto responses = TCorrectLdapResponse::GetResponses(login);
    LdapMock::TLdapMockResponses updatedResponses = responses;
    LdapMock::TSearchResponseInfo responseServerBusy {
        .ResponseEntries = {}, // Server is busy, can retry attempt
        .ResponseDone = {.Status = LdapMock::EStatus::BUSY}
    };

    auto& searchResponse = responses.SearchResponses.front();
    searchResponse.second = responseServerBusy;
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, responses);

    ldapServer.Start();
    auto loginResponse = GetLoginResponse(ydbServer, login, password);
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
    TActorId sender = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    TAutoPtr<IEventHandle> handle;
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

    // Server is busy, return retryable error
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not login via LDAP");
    UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, true);

    Sleep(TDuration::Seconds(3));
    ldapServer.ReplaceResponses(std::move(updatedResponses));
    Sleep(TDuration::Seconds(7));

    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

    // After refresh ticket, server return success
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


Y_UNIT_TEST(LdapServerIsUnavailable) {
    CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "Could not login via LDAP", ESecurityConnectionType::START_TLS);
}

Y_UNIT_TEST(CanNotRequestWithEmptyHost) {
    CheckRequiredLdapSettings(InitLdapSettingsWithEmptyHost, "Could not login via LDAP");
}

Y_UNIT_TEST(CanNotRequestWithEmptyBaseDn) {
    CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBaseDn, "Could not login via LDAP");
}

Y_UNIT_TEST(CanNotRequestWithEmptyBindDn) {
    CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindDn, "Could not login via LDAP");
}

Y_UNIT_TEST(CanNotRequestWithEmptyBindPassword) {
    CheckRequiredLdapSettings(InitLdapSettingsWithEmptyBindPassword, "Could not login via LDAP");
}

Y_UNIT_TEST(CanFetchGroupsWithDelayUpdateSecurityState) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login));

    ldapServer.Start();
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
    NLogin::TLoginProvider provider;
    provider.Audience = "/Root";
    provider.RotateKeys();
    TActorId sender = runtime->AllocateEdgeActor();

    auto loginResponse = provider.LoginUser({.User = login, .Password = password, .ExternalAuth = "ldap"});
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    Sleep(TDuration::Seconds(1));
    // Send update security state in 1 second after send TEvAuthorizeTicket
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
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

Y_UNIT_TEST(CanGetErrorIfAppropriateLoginProviderIsAbsent) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettings);
    LdapMock::TSimpleServer ldapServer({.Port = ydbServer.GetLdapPort()}, TCorrectLdapResponse::GetResponses(login));

    ldapServer.Start();
    TTestActorRuntime* runtime = ydbServer.GetRuntime();
    NLogin::TLoginProvider provider;
    provider.Audience = "/Root";
    provider.RotateKeys();
    TActorId sender = runtime->AllocateEdgeActor();

    auto loginResponse = provider.LoginUser({.User = login, .Password = password, .ExternalAuth = "ldap"});
    runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
    Sleep(TDuration::Seconds(1));
    // Do no send update security state
    // runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT(!ticketParserResult->Error.empty());
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    UNIT_ASSERT_EQUAL_C(ticketParserResult->Error.Message, "Login state is not available", ticketParserResult->Error);
    UNIT_ASSERT_EQUAL_C(ticketParserResult->Error.Retryable, false, ticketParserResult->Error.Retryable);
    ldapServer.Stop();
}

Y_UNIT_TEST(CanFetchGroupsWithValidCredentialsUseExternalSaslAuth) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsWithMtlsAuth, {
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .CertFile = CertStorage.GetClientCertFileName(),
        .KeyFile = CertStorage.GetClientKeyFileName(),
        .Type = ESecurityConnectionType::LDAPS_SCHEME
    });

    LdapMock::TLdapMockResponses responses = TCorrectLdapResponse::GetResponses(login);
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "", .Mechanism = LdapMock::ESaslMechanism::EXTERNAL}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSimpleServer ldapServer({
        .Port = ydbServer.GetLdapPort(),
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .CertFile = CertStorage.GetServerCertFileName(),
        .KeyFile = CertStorage.GetServerKeyFileName(),
        .UseTls = true,
        .RequireClientCert = true,
        .ExternalAuthMap = {
            {"/C=RU/ST=MSK/L=MSK/O=YA/OU=UtTest/CN=localhost", "cn=robouser,dc=search,dc=yandex,dc=net"}
        }
    }, responses);
    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
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

Y_UNIT_TEST(CanNotFetchGroupsWithInvalidCredentialsUseExternalSaslAuth) {
    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TLdapKikimrServer ydbServer(InitLdapSettingsWithMtlsAuth, {
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .CertFile = CertStorage.GetClientCertFileName(),
        .KeyFile = CertStorage.GetClientKeyFileName(),
        .Type = ESecurityConnectionType::LDAPS_SCHEME
    });

    LdapMock::TLdapMockResponses responses = TCorrectLdapResponse::GetResponses(login);
    responses.BindResponses.push_back({{{.Login = "cn=unauthenticatedrobot,dc=search,dc=yandex,dc=net", .Password = "", .Mechanism = LdapMock::ESaslMechanism::EXTERNAL}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

    LdapMock::TSimpleServer ldapServer({
        .Port = ydbServer.GetLdapPort(),
        .CaCertFile = CertStorage.GetCaCertFileName(),
        .CertFile = CertStorage.GetServerCertFileName(),
        .KeyFile = CertStorage.GetServerKeyFileName(),
        .UseTls = true,
        .RequireClientCert = true,
        .ExternalAuthMap = {
            {"/C=RU/ST=MSK/L=MSK/O=YA/OU=UtTest/CN=localhost", "cn=unauthenticatedrobot,dc=search,dc=yandex,dc=net"}
        }
    }, responses);
    ldapServer.Start();
    TAutoPtr<IEventHandle> handle = LdapAuthenticate(ydbServer, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Should be error");
    UNIT_ASSERT(ticketParserResult->Token == nullptr);
    UNIT_ASSERT_EQUAL_C(ticketParserResult->Error.Message, "Could not login via LDAP", ticketParserResult->Error);
    ldapServer.Stop();
}

} // LdapAuthProviderTests

} // NKikimr
