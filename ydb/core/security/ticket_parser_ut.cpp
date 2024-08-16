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

#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <util/system/tempfile.h>

#include <ydb/core/security/certificate_check/cert_auth_utils.h>
#include "ticket_parser.h"

namespace NKikimr {

using TAccessServiceMock = TTicketParserAccessServiceMock;
using TNebiusAccessServiceMock = TTicketParserNebiusAccessServiceMock;

namespace {

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

    Y_UNIT_TEST(TicketFromCertificateCheckIssuerGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");

        const TCertAndKey ca = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(ca, TProps::AsServer());
        const TCertAndKey clientCert = GenerateSignedCert(ca, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        const TString expectedGroup = settings.AppConfig->GetClientCertificateAuthorization().GetDefaultGroup();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert"), result->Token->ShortDebugString());
        const auto& groups = result->Token->GetGroupSIDs();
        const std::unordered_set<TString> groupsSet(groups.cbegin(), groups.cend());
        UNIT_ASSERT_C(groupsSet.contains(expectedGroup), "Groups should contain " + expectedGroup);
    }

    Y_UNIT_TEST(TicketFromCertificateCheckIssuerBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");

        const TCertAndKey caForServerCert = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(caForServerCert, TProps::AsServer());
        TProps caPropertiesForClientCert = TProps::AsCA();
        caPropertiesForClientCert.Organization = "Other org";
        const TCertAndKey caForClientCert = GenerateCA(caPropertiesForClientCert);
        const TCertAndKey clientCert = GenerateSignedCert(caForClientCert, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(!result->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(result->Error.Message, "Cannot create token from certificate. Client`s certificate and server`s certificate have different issuers");
        UNIT_ASSERT(result->Token == nullptr);
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& firstCertDef = *clientCertDefinitions.Add();
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *firstCertDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});
        firstCertDef.AddMemberGroups("first.Register.Node.Group@cert");
        firstCertDef.AddMemberGroups("second.Register.Node.Group@cert");

        auto& secondCertDef = *clientCertDefinitions.Add();
        *secondCertDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *secondCertDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *secondCertDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        secondCertDef.AddMemberGroups("first.Common.Group@cert");
        secondCertDef.AddMemberGroups("second.Common.Group@cert");

        const TCertAndKey ca = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(ca, TProps::AsServer());
        const TCertAndKey clientCert = GenerateSignedCert(ca, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert"), result->Token->ShortDebugString());
        const auto& groups = result->Token->GetGroupSIDs();
        const std::unordered_set<TString> groupsSet(groups.cbegin(), groups.cend());
        std::vector<TString> expectedGroups(firstCertDef.GetMemberGroups().cbegin(), firstCertDef.GetMemberGroups().cend());
        expectedGroups.insert(expectedGroups.end(), secondCertDef.GetMemberGroups().cbegin(), secondCertDef.GetMemberGroups().cend());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groupsSet.contains(expectedGroup), "Groups should contain: " + expectedGroup);
        }
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationDifferentIssuersGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& certDef = *clientCertDefinitions.Add();
        *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});
        certDef.SetRequireSameIssuer(false);
        certDef.AddMemberGroups("test.Register.Node.Group@cert");

        const TCertAndKey caForServerCert = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(caForServerCert, TProps::AsServer());
        TProps caPropertiesForClientCert = TProps::AsCA();
        caPropertiesForClientCert.Organization = "Other org";
        const TCertAndKey caForClientCert = GenerateCA(caPropertiesForClientCert);
        const TCertAndKey clientCert = GenerateSignedCert(caForClientCert, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert"), result->Token->ShortDebugString());
        const auto& groups = result->Token->GetGroupSIDs();
        const std::unordered_set<TString> groupsSet(groups.cbegin(), groups.cend());
        const std::vector<TString> expectedGroups(certDef.GetMemberGroups().cbegin(), certDef.GetMemberGroups().cend());
        for (const auto& expectedGroup : expectedGroups) {
            UNIT_ASSERT_C(groupsSet.contains(expectedGroup), "Groups should contain: " + expectedGroup);
        }
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationDifferentIssuersBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& certDef = *clientCertDefinitions.Add();
        *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});
        certDef.AddMemberGroups("test.Register.Node.Group@cert");

        const TCertAndKey caForServerCert = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(caForServerCert, TProps::AsServer());
        TProps caPropertiesForClientCert = TProps::AsCA();
        caPropertiesForClientCert.Organization = "Other org";
        const TCertAndKey caForClientCert = GenerateCA(caPropertiesForClientCert);
        const TCertAndKey clientCert = GenerateSignedCert(caForClientCert, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(!result->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(result->Error.Message, "Cannot create token from certificate. Client certificate failed verification");
        UNIT_ASSERT(result->Token == nullptr);
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationDefaultGroupGood) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& certDef = *clientCertDefinitions.Add();
        *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});

        const TCertAndKey ca = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(ca, TProps::AsServer());
        const TCertAndKey clientCert = GenerateSignedCert(ca, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        const TString expectedGroup = settings.AppConfig->GetClientCertificateAuthorization().GetDefaultGroup();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(result->Error.empty(), result->Error);
        UNIT_ASSERT_C(result->Token->IsExist("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert"), result->Token->ShortDebugString());
        const auto& groups = result->Token->GetGroupSIDs();
        const std::unordered_set<TString> groupsSet(groups.cbegin(), groups.cend());
        UNIT_ASSERT_C(groupsSet.contains(expectedGroup), "Groups should contain: " + expectedGroup);
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& certDef = *clientCertDefinitions.Add();
        *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"Other org"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});

        const TCertAndKey ca = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(ca, TProps::AsServer());
        const TCertAndKey clientCert = GenerateSignedCert(ca, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(!result->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(result->Error.Message, "Cannot create token from certificate. Client certificate failed verification");
        UNIT_ASSERT(result->Token == nullptr);
    }

    Y_UNIT_TEST(TicketFromCertificateWithValidationCheckIssuerBad) {
        using namespace Tests;
        TPortManager tp;
        ui16 kikimrPort = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(kikimrPort);
        settings.SetDomainName("Root");
        auto& clientCertDefinitions = *settings.AppConfig->MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
        auto& certDef = *clientCertDefinitions.Add();
        *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"Other org"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".test.ut.ru"});

        const TCertAndKey caForServerCert = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(caForServerCert, TProps::AsServer());
        TProps caPropertiesForClientCert = TProps::AsCA();
        caPropertiesForClientCert.Organization = "Other org";
        const TCertAndKey caForClientCert = GenerateCA(caPropertiesForClientCert);
        const TCertAndKey clientCert = GenerateSignedCert(caForClientCert, TProps::AsClient());

        TTempFileHandle serverCertificateFile;
        serverCertificateFile.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
        settings.ServerCertFilePath = serverCertificateFile.Name();

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();
        TTestActorRuntime* runtime = server.GetRuntime();

        runtime->Send(new IEventHandle(MakeTicketParserID(), runtime->AllocateEdgeActor(), new TEvTicketParser::TEvAuthorizeTicket(TString(clientCert.Certificate))), 0);

        TAutoPtr<IEventHandle> handle;
        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT_C(!result->Error.empty(), "Expected return error message");
        UNIT_ASSERT_STRINGS_EQUAL(result->Error.Message, "Cannot create token from certificate. Client certificate failed verification");
        UNIT_ASSERT(result->Token == nullptr);
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
