#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/testlib/service_mocks/user_account_service_mock.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include "ldap_auth_provider.h"
#include "ticket_parser.h"

namespace NKikimr {

using TAccessServiceMock = TTicketParserAccessServiceMock;

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

class TLdapKikimrServer {
public:
    TLdapKikimrServer(std::function<void(NKikimrProto::TLdapAuthentication*, ui16)> initLdapSettings)
        : InitLdapSettings(std::move(initLdapSettings))
        , Server(InitSettings()) {
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
    Tests::TServerSettings InitSettings() {
        using namespace Tests;
        TPortManager tp;
        LdapPort = tp.GetPort(389);
        ui16 kikimrPort = tp.GetPort(2134);
        GrpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");

        InitLdapSettings(authConfig.MutableLdapAuthentication(), LdapPort);

        Tests::TServerSettings settings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

private:
    std::function<void(NKikimrProto::TLdapAuthentication*, ui16)> InitLdapSettings;
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
    static LdapMock::TLdapMockResponses GetResponses(const TString& login);
};

std::vector<TString> TCorrectLdapResponse::Groups {
    "ou=groups,dc=search,dc=yandex,dc=net",
    "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
    "cn=developers,ou=groups,dc=search,dc=yandex,dc=net"
};

LdapMock::TLdapMockResponses TCorrectLdapResponse::GetResponses(const TString& login) {
    LdapMock::TLdapMockResponses responses;
    responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

    LdapMock::TSearchRequestInfo fetchGroupsSearchRequestInfo {
        {
            .BaseDn = "dc=search,dc=yandex,dc=net",
            .Scope = 2,
            .DerefAliases = 0,
            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
            .Attributes = {"memberOf"}
        }
    };

    std::vector<LdapMock::TSearchEntry> fetchGroupsSearchResponseEntries {
        {
            .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
            .AttributeList = {
                                {"memberOf", TCorrectLdapResponse::Groups}
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

Y_UNIT_TEST_SUITE(TTicketParserTest) {

    Y_UNIT_TEST(LoginGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        provider.CreateUser({.User = "user1", .Password = "password1"});
        auto loginResponse = provider.LoginUser({.User = "user1", .Password = "password1"});

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
    }

    Y_UNIT_TEST(LoginGoodWithGroups) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();

        provider.CreateGroup({.Group = "group1"});
        provider.CreateUser({.User = "user1", .Password = "password1"});
        provider.AddGroupMembership({.Group = "group1", .Member = "user1"});

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        auto loginResponse = provider.LoginUser({.User = "user1", .Password = "password1"});

        UNIT_ASSERT_VALUES_EQUAL(loginResponse.Error, "");

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
        UNIT_ASSERT(result->Token->IsExist("group1"));
    }

    Y_UNIT_TEST(LoginBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("Login bad-token")), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Token is not in correct format");
    }

    Y_UNIT_TEST(LoginRefreshGroupsGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();

        provider.CreateGroup({.Group = "group1"});
        provider.CreateUser({.User = "user1", .Password = "password1"});
        provider.AddGroupMembership({.Group = "group1", .Member = "user1"});

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        auto loginResponse = provider.LoginUser({.User = "user1", .Password = "password1"});

        UNIT_ASSERT_VALUES_EQUAL(loginResponse.Error, "");

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
        UNIT_ASSERT(result->Token->IsExist("group1"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetGroupSIDs().size(), 2);

        provider.CreateGroup({.Group = "group2"});
        provider.AddGroupMembership({.Group = "group2", .Member = "group1"});
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
        UNIT_ASSERT(result->Token->IsExist("group1"));
        UNIT_ASSERT(result->Token->IsExist("group2"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetGroupSIDs().size(), 3);

        provider.RemoveGroup({.Group = "group2"});
        provider.CreateGroup({.Group = "group3"});
        provider.AddGroupMembership({.Group = "group3", .Member = "user1"});
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
        UNIT_ASSERT(result->Token->IsExist("group1"));
        UNIT_ASSERT(result->Token->IsExist("group3"));
        UNIT_ASSERT(!result->Token->IsExist("group2"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetGroupSIDs().size(), 3);
    }

    Y_UNIT_TEST(LoginCheckRemovedUser) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();

        provider.CreateGroup({.Group = "group1"});
        provider.CreateUser({.User = "user1", .Password = "password1"});
        provider.AddGroupMembership({.Group = "group1", .Member = "user1"});

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        auto loginResponse = provider.LoginUser({.User = "user1", .Password = "password1"});

        UNIT_ASSERT_VALUES_EQUAL(loginResponse.Error, "");

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1");
        UNIT_ASSERT(result->Token->IsExist("group1"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetGroupSIDs().size(), 2);

        provider.RemoveUser({.User = "user1"});

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT_EQUAL(result->Error.Message, "User not found");
        UNIT_ASSERT(result->Token == nullptr);
    }

    Y_UNIT_TEST(LoginEmptyTicketBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        NLogin::TLoginProvider provider;

        provider.Audience = "/Root";
        provider.RotateKeys();

        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvUpdateLoginSecurityState(provider.GetSecurityState())), 0);

        provider.CreateUser({.User = "user1", .Password = "password1"});
        auto loginResponse = provider.LoginUser({.User = "user1", .Password = "password1"});

        TString emptyUserToken = "";

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(emptyUserToken)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Token == nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Ticket is empty");
    }

    Y_UNIT_TEST(LdapFetchGroupsGood) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login));

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + "@ldap");
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end());
        expectedGroups.insert("all-users@well-known");
        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        ldapServer.Stop();
    }

     Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserLoginBad) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=invalidRobouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidRobotUserLogin);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not perform initial LDAP bind for dn cn=invalidRobouser,dc=search,dc=yandex,dc=net on server localhost\nInvalid credentials");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithInvalidRobotUserPasswordBad) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "invalidPassword"}}, {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidRobotUserPassword);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server localhost\nInvalid credentials");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsWithRemovedUserCredentialsBad) {
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

        TLdapKikimrServer server(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, removedUserLogin, removedUserPassword);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "LDAP user " + removedUserLogin + " does not exist. "
                                                                     "LDAP search for filter uid=" + removedUserLogin + " on server localhost return no entries");

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapFetchGroupsUseInvalidSearchFilterBad) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        LdapMock::TLdapMockResponses responses;
        responses.BindResponses.push_back({{{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}}, {.Status = LdapMock::EStatus::SUCCESS}});

        TLdapKikimrServer server(InitLdapSettingsWithInvalidFilter);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not search for filter &(uid=" + login + ")() on server localhost\nBad search filter");

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapServerIsUnavaliable) {
        TLdapKikimrServer server(InitLdapSettingsWithUnavaliableHost);

        LdapMock::TLdapMockResponses responses;
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server unavaliablehost\nCan't contact LDAP server");

        ldapServer.Stop();
    }

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings);
        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

        auto loginResponse = GetLoginResponse(server, login, password);
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + "@ldap");
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end());
        expectedGroups.insert("all-users@well-known");
        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        THashSet<TString> newExpectedGroups {
            "ou=groups,dc=search,dc=yandex,dc=net",
            "cn=people,ou=groups,dc=search,dc=yandex,dc=net",
            "cn=desiners,ou=groups,dc=search,dc=yandex,dc=net"
        };
        std::vector<LdapMock::TSearchEntry> newFetchGroupsSearchResponseEntries {
            {
                .Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net",
                .AttributeList = {
                                    {"memberOf", std::vector(newExpectedGroups.begin(), newExpectedGroups.end())}
                                }
            }
        };
        newExpectedGroups.insert("all-users@well-known");

        LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
            .ResponseEntries = newFetchGroupsSearchResponseEntries,
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };

        auto& searchresponse = responses.SearchResponses.front();
        searchresponse.second = newFetchGroupsSearchResponseInfo;
        ldapServer.SetSearchReasponse(searchresponse);
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

    Y_UNIT_TEST(LdapRefreshRemoveUserBad) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings);
        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);


        auto loginResponse = GetLoginResponse(server, login, password);
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(ticketParserResult->Error.empty(), ticketParserResult->Error);
        UNIT_ASSERT(ticketParserResult->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(ticketParserResult->Token->GetUserSID(), login + "@ldap");
        const auto& fetchedGroups = ticketParserResult->Token->GetGroupSIDs();
        THashSet<TString> groups(fetchedGroups.begin(), fetchedGroups.end());

        THashSet<TString> expectedGroups(TCorrectLdapResponse::Groups.begin(), TCorrectLdapResponse::Groups.end());
        expectedGroups.insert("all-users@well-known");
        UNIT_ASSERT_VALUES_EQUAL(fetchedGroups.size(), expectedGroups.size());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groups.contains(expectedGroup), "Can not find " + expectedGroup);
        }

        LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
            .ResponseEntries = {}, // User has been removed. Return empty entries list
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };

        auto& searchresponse = responses.SearchResponses.front();
        searchresponse.second = newFetchGroupsSearchResponseInfo;
        ldapServer.SetSearchReasponse(searchresponse);
        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);
        ticketParserResult = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);

        UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
        UNIT_ASSERT(ticketParserResult->Token == nullptr);
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "LDAP user " + login + " does not exist. "
                                                                     "LDAP search for filter uid=" + login + " on server localhost return no entries");
        UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, false);

        ldapServer.Stop();
    }

    Y_UNIT_TEST(AccessServiceAuthenticationOk) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 accessServicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket("Bearer " + userToken)), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
    }

    Y_UNIT_TEST(AccessServiceAuthenticationApiKeyOk) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 accessServicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceApiKey(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        TString userToken = "ApiKey ApiKey-value-valid";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(userToken)), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
    }

    Y_UNIT_TEST(AuthenticationWithUserAccount) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        TString accessServiceEndpoint = "localhost:" + ToString(tp.GetPort(4284));
        TString userAccountServiceEndpoint = "localhost:" + ToString(tp.GetPort(4285));
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseStaff(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseUserAccountService(true);
        authConfig.SetUseUserAccountServiceTLS(false);
        authConfig.SetUserAccountServiceEndpoint(userAccountServiceEndpoint);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder1;
        builder1.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder1.BuildAndStart());

        // User Account Service Mock
        TUserAccountServiceMock userAccountServiceMock;
        auto& user1 = userAccountServiceMock.UserAccountData["user1"];
        user1.mutable_yandex_passport_user_account()->set_login("login1");
        grpc::ServerBuilder builder2;
        builder2.AddListeningPort(userAccountServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&userAccountServiceMock);
        std::unique_ptr<grpc::Server> userAccountServer(builder2.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(userToken)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "login1@passport");
    }

    Y_UNIT_TEST(AuthenticationUnavailable) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.UnavailableTokens.insert(userToken);
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(userToken)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");
    }

    Y_UNIT_TEST(AuthenticationRetryError) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetMinErrorRefreshTime("300ms");
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateRetryableError = true;
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "keyId"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", {})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));
        accessServiceMock.ShouldGenerateRetryableError = false;
        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", {})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
    }

    Y_UNIT_TEST(AuthenticationRetryErrorImmediately) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetRefreshPeriod("5s");
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateOneRetryableError = true;
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "keyId"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", {})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", {})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
    }

    Y_UNIT_TEST(AuthorizationRetryError) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetMinErrorRefreshTime("300ms");
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateRetryableError = true;
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "keyId"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;
        const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                                                                        TEvTicketParser::TEvAuthorizeTicket::ToPermissions({"something.read"}),
                                                                        {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}}
                                                                    }};
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", entries)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));
        accessServiceMock.ShouldGenerateRetryableError = false;
        Sleep(TDuration::Seconds(10));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", entries)), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
    }

    Y_UNIT_TEST(AuthorizationRetryErrorImmediately) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetRefreshPeriod("5s");
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateOneRetryableError = true;
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "keyId"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;
        const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                                                                        TEvTicketParser::TEvAuthorizeTicket::ToPermissions({"something.read"}),
                                                                        {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}}
                                                                    }};
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", entries)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", entries)), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
    }

    Y_UNIT_TEST(AuthenticationUnsupported) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "Login user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.UnavailableTokens.insert(userToken);
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(userToken)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Token is not supported");
    }

    Y_UNIT_TEST(AuthenticationUnknown) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "bebebe user1";

        // Access Server Mock
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.UnavailableTokens.insert(userToken);
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(userToken)), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Unknown token");
    }

    Y_UNIT_TEST(Authorization) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceApiKey(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));

        // Authorization ApiKey successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           "ApiKey ApiKey-value-valid",
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));

        // Authorization failure with not enough permissions.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.write"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access Denied");
        UNIT_ASSERT(!result->Error.Retryable);

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));

        // Authorization failure with invalid token.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           "invalid",
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access Denied");

        // Authorization failure with access denied token.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           "invalid-token1",
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access Denied");

        // Authorization failure with wrong folder_id.
        accessServiceMock.AllowedResourceIds.emplace("cccc1234");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "XXXXXXXX"}, {"database_id", "XXXXXXXX"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access Denied");

        // Authorization successful with right folder_id.
        accessServiceMock.AllowedResourceIds.emplace("aaaa1234");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "XXXXXXXX"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-XXXXXXXX@as"));

        // Authorization successful with right database_id.
        accessServiceMock.AllowedResourceIds.clear();
        accessServiceMock.AllowedResourceIds.emplace("bbbb4554");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "XXXXXXXX"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
    }

    Y_UNIT_TEST(AuthorizationWithRequiredPermissions) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           TVector<TEvTicketParser::TEvAuthorizeTicket::TPermission>{TEvTicketParser::TEvAuthorizeTicket::Optional("something.read"), TEvTicketParser::TEvAuthorizeTicket::Optional("something.write")})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));

        // Authorization failure with not enough permissions.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           TVector<TEvTicketParser::TEvAuthorizeTicket::TPermission>{TEvTicketParser::TEvAuthorizeTicket::Optional("something.read"), TEvTicketParser::TEvAuthorizeTicket::Required("something.write")})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "something.write for folder_id aaaa1234 - Access Denied");
    }

    Y_UNIT_TEST(AuthorizationWithUserAccount) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        TString accessServiceEndpoint = "localhost:" + ToString(tp.GetPort(4284));
        TString userAccountServiceEndpoint = "localhost:" + ToString(tp.GetPort(4285));
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseStaff(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseUserAccountService(true);
        authConfig.SetUseUserAccountServiceTLS(false);
        authConfig.SetUserAccountServiceEndpoint(userAccountServiceEndpoint);
        // placemark1
        authConfig.SetCacheAccessServiceAuthorization(false);
        //
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder1;
        builder1.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder1.BuildAndStart());

        // User Account Service Mock
        TUserAccountServiceMock userAccountServiceMock;
        auto& user1 = userAccountServiceMock.UserAccountData["user1"];
        user1.mutable_yandex_passport_user_account()->set_login("login1");
        grpc::ServerBuilder builder2;
        builder2.AddListeningPort(userAccountServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&userAccountServiceMock);
        std::unique_ptr<grpc::Server> userAccountServer(builder2.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "login1@passport");

        // Authorization failure with not enough permissions.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.write"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access Denied");

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "login1@passport");

        accessServiceMock.AllowedUserPermissions.insert("user1-something.write");

        // Authorization successful - 2
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           TVector<TString>{"something.read", "something.write"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        // placemark 1
        UNIT_ASSERT(result->Token->IsExist("something.write-bbbb4554@as"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "login1@passport");
    }

    Y_UNIT_TEST(AuthorizationWithUserAccount2) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        TString accessServiceEndpoint = "localhost:" + ToString(tp.GetPort(4284));
        TString userAccountServiceEndpoint = "localhost:" + ToString(tp.GetPort(4285));
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseStaff(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseUserAccountService(true);
        authConfig.SetUseUserAccountServiceTLS(false);
        authConfig.SetUserAccountServiceEndpoint(userAccountServiceEndpoint);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder1;
        builder1.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder1.BuildAndStart());

        // User Account Service Mock
        TUserAccountServiceMock userAccountServiceMock;
        auto& user1 = userAccountServiceMock.UserAccountData["user1"];
        user1.mutable_yandex_passport_user_account()->set_login("login1");
        grpc::ServerBuilder builder2;
        builder2.AddListeningPort(userAccountServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&userAccountServiceMock);
        std::unique_ptr<grpc::Server> userAccountServer(builder2.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.AllowedUserPermissions.insert("user1-something.write");
        accessServiceMock.AllowedUserPermissions.erase("user1-something.list");
        accessServiceMock.AllowedUserPermissions.erase("user1-something.read");

        // Authorization successful - 2
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.list", "something.read", "something.write", "something.eat", "somewhere.sleep"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(!result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.list-bbbb4554@as"));
        UNIT_ASSERT(result->Token->IsExist("something.write-bbbb4554@as"));
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "login1@passport");
    }

    Y_UNIT_TEST(AuthorizationUnavailable) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.UnavailableUserPermissions.insert(userToken + "-something.write");

        // Authorization unsuccessfull.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           TVector<TString>{"something.read", "something.write"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");
    }

    Y_UNIT_TEST(AuthorizationModify) {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TString userToken = "user1";

        // Access Server Mock
        NKikimr::TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));

        accessServiceMock.AllowedUserPermissions.insert(userToken + "-something.write");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvDiscardTicket(userToken)), 0);

        // Authorization successful with new permissions.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}},
                                           TVector<TString>{"something.read", "something.write"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(result->Token->IsExist("something.write-bbbb4554@as"));
    }
}
}
