#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/testlib/service_mocks/user_account_service_mock.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include "ticket_parser.h"

namespace NKikimr {

using TAccessServiceMock = TTicketParserAccessServiceMock;

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

        accessServiceMock.ShouldGenerateRetryableError = false;
        Sleep(TDuration::Seconds(10));

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
