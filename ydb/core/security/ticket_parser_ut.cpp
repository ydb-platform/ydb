#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/testlib/service_mocks/user_account_service_mock.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/nebius_access_service_mock.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <util/system/tempfile.h>

#include "ldap_auth_provider.h"
#include "ticket_parser.h"

namespace NKikimr {

using TAccessServiceMock = TTicketParserAccessServiceMock;
using TNebiusAccessServiceMock = TTicketParserNebiusAccessServiceMock;

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

void InitLdapSettingsWithEmptyHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
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

void InitLdapSettingsWithCustomGroupAttribute(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, TTempFileHandle& certificateFile) {
    InitLdapSettings(ldapSettings, ldapPort, certificateFile);
    ldapSettings->SetRequestedGroupAttribute("groupDN");
}

class TLdapKikimrServer {
public:
    TLdapKikimrServer(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)> initLdapSettings)
        : CaCertificateFile()
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
    Tests::TServerSettings InitSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)>&& initLdapSettings) {
        using namespace Tests;
        TPortManager tp;
        LdapPort = tp.GetPort(389);
        ui16 kikimrPort = tp.GetPort(2134);
        GrpcPort = tp.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetRefreshTime("5s");

        initLdapSettings(authConfig.MutableLdapAuthentication(), LdapPort, CaCertificateFile);

        Tests::TServerSettings settings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

private:
    TTempFileHandle CaCertificateFile;
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

void CheckRequiredLdapSettings(std::function<void(NKikimrProto::TLdapAuthentication*, ui16, TTempFileHandle&)> initLdapSettings, const TString& expectedErrorMessage) {
    TLdapKikimrServer server(initLdapSettings);

    LdapMock::TLdapMockResponses responses;
    LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

    TString login = "ldapuser";
    TString password = "ldapUserPassword";

    TAutoPtr<IEventHandle> handle = LdapAuthenticate(server, login, password);
    TEvTicketParser::TEvAuthorizeTicketResult* ticketParserResult = handle->Get<TEvTicketParser::TEvAuthorizeTicketResult>();
    UNIT_ASSERT_C(!ticketParserResult->Error.empty(), "Expected return error message");
    UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, expectedErrorMessage);

    ldapServer.Stop();
}

template <class TAccessServiceMock>
void SetUseAccessService(NKikimrProto::TAuthConfig& authConfig);

template <>
void SetUseAccessService<NKikimr::TAccessServiceMock>(NKikimrProto::TAuthConfig& authConfig) {
    authConfig.SetUseAccessService(true);
    authConfig.SetAccessServiceType("Yandex_v2");
}

template <>
void SetUseAccessService<TTicketParserAccessServiceMockV2>(NKikimrProto::TAuthConfig& authConfig) {
    authConfig.SetUseAccessService(true);
    authConfig.SetAccessServiceType("Yandex_v2");
}

template <>
void SetUseAccessService<NKikimr::TNebiusAccessServiceMock>(NKikimrProto::TAuthConfig& authConfig) {
    authConfig.SetUseAccessService(true);
    authConfig.SetAccessServiceType("Nebius_v1");
}

template <class TAccessServiceMock>
constexpr bool IsNebiusAccessService() {
    return false;
}

template <>
constexpr bool IsNebiusAccessService<NKikimr::TNebiusAccessServiceMock>() {
    return true;
}

template <class TAccessServiceMock>
constexpr bool IsApiKeySupported() {
    return !IsNebiusAccessService<TAccessServiceMock>();
}

template <class TAccessServiceMock>
constexpr bool IsSignatureSupported() {
    return !IsNebiusAccessService<TAccessServiceMock>();
}

} // namespace

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

    Y_UNIT_TEST(LdapFetchGroupsWithDefaultGroupAttributeGood) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login));

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

    Y_UNIT_TEST(LdapFetchGroupsWithCustomGroupAttributeGood) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettingsWithCustomGroupAttribute);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), TCorrectLdapResponse::GetResponses(login, "groupDN"));

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

    Y_UNIT_TEST(LdapFetchGroupsWithDontExistGroupAttribute) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettingsWithCustomGroupAttribute);

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

        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), responses);

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

    Y_UNIT_TEST(LdapServerIsUnavailable) {
        CheckRequiredLdapSettings(InitLdapSettingsWithUnavailableHost, "Could not start TLS\nCan't contact LDAP server");
    }

    Y_UNIT_TEST(LdapRequestWithEmptyHost) {
        CheckRequiredLdapSettings(InitLdapSettingsWithEmptyHost, "Ldap server host is empty");
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

    Y_UNIT_TEST(LdapRefreshGroupsInfoGood) {
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

        TLdapKikimrServer server(InitLdapSettings);
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), {responses, updatedResponses});

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

    Y_UNIT_TEST(LdapRefreshRemoveUserBad) {
        TString login = "ldapuser";
        TString password = "ldapUserPassword";

        TLdapKikimrServer server(InitLdapSettings);
        auto responses = TCorrectLdapResponse::GetResponses(login);
        LdapMock::TLdapMockResponses updatedResponses = responses;
        LdapMock::TSearchResponseInfo newFetchGroupsSearchResponseInfo {
            .ResponseEntries = {}, // User has been removed. Return empty entries list
            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
        };

        auto& searchResponse = updatedResponses.SearchResponses.front();
        searchResponse.second = newFetchGroupsSearchResponseInfo;
        LdapMock::TLdapSimpleServer ldapServer(server.GetLdapPort(), {responses, updatedResponses});

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
        UNIT_ASSERT_STRINGS_EQUAL(ticketParserResult->Error.Message, "LDAP user " + login + " does not exist. "
                                                                     "LDAP search for filter uid=" + login + " on server localhost return no entries");
        UNIT_ASSERT_EQUAL(ticketParserResult->Error.Retryable, false);

        ldapServer.Stop();
    }

    template <typename TAccessServiceMock>
    void AccessServiceAuthenticationOk() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 accessServicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
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
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        TString userToken = "user1";

        // Access Server Mock
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket("Bearer " + userToken)), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_VALUES_EQUAL(accessServiceMock.AuthorizeCount.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accessServiceMock.AuthenticateCount.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Ticket, userToken);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), userToken + "@as");
    }

    Y_UNIT_TEST(AccessServiceAuthenticationOk) {
        AccessServiceAuthenticationOk<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(NebiusAccessServiceAuthenticationOk) {
        AccessServiceAuthenticationOk<NKikimr::TNebiusAccessServiceMock>();
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

    template <typename TAccessServiceMock>
    void AuthenticationUnavailable() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
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
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");
        UNIT_ASSERT_VALUES_EQUAL(accessServiceMock.AuthorizeCount.load(), 0);
        UNIT_ASSERT_GE(accessServiceMock.AuthenticateCount.load(), 1);
    }

    Y_UNIT_TEST(AuthenticationUnavailable) {
        AuthenticationUnavailable<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(NebiusAuthenticationUnavailable) {
        AuthenticationUnavailable<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock>
    void AuthenticationRetryError() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
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
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateRetryableError = true;

        // for signature
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", {})), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1")), 0);
        }
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));
        accessServiceMock.ShouldGenerateRetryableError = false;
        Sleep(TDuration::Seconds(10));

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", {})), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1")), 0);
        }
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT_VALUES_EQUAL(accessServiceMock.AuthorizeCount.load(), 0);
        UNIT_ASSERT_GE(accessServiceMock.AuthenticateCount.load(), 2);
    }

    Y_UNIT_TEST(AuthenticationRetryError) {
        AuthenticationRetryError<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(NebiusAuthenticationRetryError) {
        AuthenticationRetryError<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock>
    void AuthenticationRetryErrorImmediately() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
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
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateOneRetryableError = true;

        // for signature
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", {})), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(TString("user1"))), 0);
        }
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", {})), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(TString("user1"))), 0);
        }
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT_VALUES_EQUAL(accessServiceMock.AuthorizeCount.load(), 0);
        UNIT_ASSERT_GE(accessServiceMock.AuthenticateCount.load(), 2);
    }

    Y_UNIT_TEST(AuthenticationRetryErrorImmediately) {
        AuthenticationRetryErrorImmediately<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(NebiusAuthenticationRetryErrorImmediately) {
        AuthenticationRetryErrorImmediately<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationRetryError() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetMinErrorRefreshTime("300ms");
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateRetryableError = true;

        // for signature
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;

        const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                                                                        TEvTicketParser::TEvAuthorizeTicket::ToPermissions({"something.read"}),
                                                                        {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}}
                                                                    }};

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", entries)), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1", entries)), 0);
        }

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));
        accessServiceMock.ShouldGenerateRetryableError = false;
        Sleep(TDuration::Seconds(10));

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", entries)), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1", entries)), 0);
        }

        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
    }

    Y_UNIT_TEST(AuthorizationRetryError) {
        AuthorizationRetryError<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationRetryError) {
        AuthorizationRetryError<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorizationRetryError) {
        AuthorizationRetryError<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationRetryErrorImmediately() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        authConfig.SetRefreshPeriod("5s");
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        accessServiceMock.ShouldGenerateOneRetryableError = true;

        // for signature
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;

        const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                                                                        TEvTicketParser::TEvAuthorizeTicket::ToPermissions({"something.read"}),
                                                                        {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}}
                                                                    }};

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", entries)), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1", entries)), 0);
        }

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Service Unavailable");

        Sleep(TDuration::Seconds(2));

        if (IsSignatureSupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(retrySignature), "", entries)), 0);
        } else {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket("user1", entries)), 0);
        }

        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(result->Token->GetUserSID(), "user1@as");
        UNIT_ASSERT(result->Token->IsExist("something.read-bbbb4554@as"));
        UNIT_ASSERT(!result->Token->IsExist("something.write-bbbb4554@as"));
    }

    Y_UNIT_TEST(AuthorizationRetryErrorImmediately) {
        AuthorizationRetryErrorImmediately<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationRetryErrorImmediately) {
        AuthorizationRetryErrorImmediately<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorizationRetryErrorImmediately) {
        AuthorizationRetryErrorImmediately<NKikimr::TNebiusAccessServiceMock>();
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

    Y_UNIT_TEST(NebiusAccessKeySignatureUnsupported) {
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
        authConfig.SetAccessServiceType("Nebius_v1");
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

        // Access Server Mock
        NKikimr::TNebiusAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature {.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"};
        TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature retrySignature = signature;
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(std::move(signature), "", {})), 0);

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(!result->Error.empty());
        UNIT_ASSERT(!result->Error.Retryable);
        UNIT_ASSERT_VALUES_EQUAL(result->Error.Message, "Access key signature is not supported");
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

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void Authorization() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceApiKey(IsApiKeySupported<TAccessServiceMock>());
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        TVector<std::pair<TString, TString>> attrs = {{"folder_id", "aaaa1234"}, {"database_id", "bbbb4554"}};
        if constexpr (IsNebiusAccessService<TAccessServiceMock>()) {
            accessServiceMock.ContainerId = "aaaa1234";
        }

        // Authorization successful.
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           attrs,
                                           {"something.read"})), 0);
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("something.read-bbbb4554@as"), result->Token->ShortDebugString());
        UNIT_ASSERT_C(!result->Token->IsExist("something.write-bbbb4554@as"), result->Token->ShortDebugString());

        accessServiceMock.AllowedUserPermissions.insert("user1-something.connect");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           attrs,
                                           {"something.read", "something.connect", "something.list", "something.update"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("something.read-bbbb4554@as"), result->Token->ShortDebugString());
        UNIT_ASSERT_C(result->Token->IsExist("something.connect-bbbb4554@as"), result->Token->ShortDebugString());
        UNIT_ASSERT_C(!result->Token->IsExist("something.list-bbbb4554@as"), result->Token->ShortDebugString());
        UNIT_ASSERT_C(!result->Token->IsExist("something.update-bbbb4554@as"), result->Token->ShortDebugString());

        // Authorization ApiKey successful.
        if constexpr (IsApiKeySupported<TAccessServiceMock>()) {
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                            "ApiKey ApiKey-value-valid",
                                            attrs,
                                            {"something.read"})), 0);
            result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
            UNIT_ASSERT_C(result->Error.empty(), result->Error);
            UNIT_ASSERT_C(result->Token->IsExist("something.read-bbbb4554@as"), result->Token->ShortDebugString());
            UNIT_ASSERT_C(!result->Token->IsExist("something.write-bbbb4554@as"), result->Token->ShortDebugString());
        }

        if constexpr (IsNebiusAccessService<TAccessServiceMock>()) {
            // check wrong container
            accessServiceMock.ContainerId = "other_container";
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           attrs,
                                           {"something.read", "read.something", "something.connect", "something.list", "something.update"})), 0);
            TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
            UNIT_ASSERT_C(!result->Error.empty(), result->Token->ShortDebugString());

            // switch off this check
            accessServiceMock.ContainerId = "";
        }

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
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("something.read-bbbb4554@as"), result->Token->ShortDebugString());
        UNIT_ASSERT_C(!result->Token->IsExist("something.write-bbbb4554@as"), result->Token->ShortDebugString());

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

        if constexpr (IsNebiusAccessService<TAccessServiceMock>()) {
            accessServiceMock.ContainerId = "aaaa1234";
        }
        accessServiceMock.AllowedResourceIds.clear();
        accessServiceMock.AllowedResourceIds.emplace("aaaa1234");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "aaaa1234"}, {"database_id", "XXXXXXXX"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("something.read-XXXXXXXX@as"), result->Token->ShortDebugString());

        if constexpr (IsNebiusAccessService<TAccessServiceMock>()) {
            accessServiceMock.ContainerId = "";
        } else {
            accessServiceMock.AllowedResourceIds.clear();
        }

        // Authorization successful with right database_id.
        accessServiceMock.AllowedResourceIds.emplace("bbbb4554");
        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                           userToken,
                                           {{"folder_id", "XXXXXXXX"}, {"database_id", "bbbb4554"}},
                                           {"something.read"})), 0);
        result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("something.read-bbbb4554@as"), result->Token->ShortDebugString());

        if constexpr (!IsNebiusAccessService<TAccessServiceMock>()) {
            // Authorization successful for gizmo resource
            accessServiceMock.AllowedResourceIds.clear();
            accessServiceMock.AllowedResourceIds.emplace("gizmo");
            runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(
                                            userToken,
                                            {{"gizmo_id", "gizmo"}, },
                                            {"monitoring.view"})), 0);
            result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
            UNIT_ASSERT_C(result->Error.empty(), result->Error);
            UNIT_ASSERT_C(result->Token->IsExist("monitoring.view@as"), result->Token->ShortDebugString());
            UNIT_ASSERT_C(result->Token->IsExist("monitoring.view-gizmo@as"), result->Token->ShortDebugString());
        }
    }

    Y_UNIT_TEST(Authorization) {
        Authorization<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorization) {
        Authorization<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorization) {
        Authorization<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationWithRequiredPermissions() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
        if constexpr (IsNebiusAccessService<TAccessServiceMock>()) {
            accessServiceMock.ContainerId = "aaaa1234";
        }
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
        UNIT_ASSERT_STRING_CONTAINS(result->Error.Message, "something.write for ");
        UNIT_ASSERT_STRING_CONTAINS(result->Error.Message, "aaaa1234");
    }

    Y_UNIT_TEST(AuthorizationWithRequiredPermissions) {
        AuthorizationWithRequiredPermissions<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationWithRequiredPermissions) {
        AuthorizationWithRequiredPermissions<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorizationWithRequiredPermissions) {
        AuthorizationWithRequiredPermissions<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationWithUserAccount() {
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
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
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

    Y_UNIT_TEST(AuthorizationWithUserAccount) {
        AuthorizationWithUserAccount<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationWithUserAccount) {
        AuthorizationWithUserAccount<TTicketParserAccessServiceMockV2, true>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationWithUserAccount2() {
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
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
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

    Y_UNIT_TEST(AuthorizationWithUserAccount2) {
        AuthorizationWithUserAccount2<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationWithUserAccount2) {
        AuthorizationWithUserAccount2<TTicketParserAccessServiceMockV2, true>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationUnavailable() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
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

    Y_UNIT_TEST(AuthorizationUnavailable) {
        AuthorizationUnavailable<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationUnavailable) {
        AuthorizationUnavailable<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorizationUnavailable) {
        AuthorizationUnavailable<NKikimr::TNebiusAccessServiceMock>();
    }

    template <typename TAccessServiceMock, bool EnableBulkAuthorization = false>
    void AuthorizationModify() {
        using namespace Tests;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 servicePort = tp.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(servicePort);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        SetUseAccessService<TAccessServiceMock>(authConfig);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseStaff(false);
        auto settings = TServerSettings(port, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(EnableBulkAuthorization);
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
        TAccessServiceMock accessServiceMock;
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

    Y_UNIT_TEST(AuthorizationModify) {
        AuthorizationModify<NKikimr::TAccessServiceMock>();
    }

    Y_UNIT_TEST(BulkAuthorizationModify) {
        AuthorizationModify<TTicketParserAccessServiceMockV2, true>();
    }

    Y_UNIT_TEST(NebiusAuthorizationModify) {
        AuthorizationModify<NKikimr::TNebiusAccessServiceMock>();
    }
}
}
