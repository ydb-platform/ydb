#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include "ticket_parser.h"

namespace NKikimr {

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
        UNIT_ASSERT_EQUAL(result->Token->GetUserSID(), "user1");
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

        UNIT_ASSERT_EQUAL(loginResponse.Error, "");

        runtime->Send(new IEventHandle(MakeTicketParserID(), sender, new TEvTicketParser::TEvAuthorizeTicket(loginResponse.Token)), 0);

        TAutoPtr<IEventHandle> handle;

        TEvTicketParser::TEvAuthorizeTicketResult* result = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        UNIT_ASSERT(result->Error.empty());
        UNIT_ASSERT(result->Token != nullptr);
        UNIT_ASSERT_EQUAL(result->Token->GetUserSID(), "user1");
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
        UNIT_ASSERT_EQUAL(result->Error.Message, "Token is not in correct format");
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
        UNIT_ASSERT_EQUAL(result->Error.Message, "Ticket is empty");
    }
}
}
