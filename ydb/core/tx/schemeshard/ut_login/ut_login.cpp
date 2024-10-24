#include <util/string/join.h>

#include <ydb/library/login/login.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/auditlog_helpers.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/login_page.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace NSchemeShardUT_Private {

// convert into generic test helper?
void TestCreateAlterLoginCreateUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& password, const TVector<TExpectedResult>& expectedResults) {
    std::unique_ptr<NEvSchemeShard::TEvModifySchemeTransaction> modifyTx(CreateAlterLoginCreateUser(txId, user, password));
    //TODO: move setting of TModifyScheme.WorkingDir into CreateAlterLoginCreateUser()
    //NOTE: TModifyScheme.Name isn't set, intentionally
    modifyTx->Record.MutableTransaction(0)->SetWorkingDir(database);
    AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
    // AlterLoginCreateUser is synchronous in nature, result is returned immediately
    TestModificationResults(runtime, txId, expectedResults);
}

}  // namespace NSchemeShardUT_Private

Y_UNIT_TEST_SUITE(TSchemeShardLoginTest) {
    Y_UNIT_TEST(BasicLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusSuccess}});
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);

        // check token
        NLogin::TLoginProvider login;
        login.UpdateSecurityState(describe.GetPathDescription().GetDomainDescription().GetSecurityState());
        auto resultValidate = login.ValidateToken({.Token = resultLogin.token()});
        UNIT_ASSERT_VALUES_EQUAL(resultValidate.User, "user1");
    }

    Y_UNIT_TEST(DisableBuiltinAuthMechanism) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().AuthConfig.SetEnableLoginAuthentication(false);
        ui64 txId = 100;
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusPreconditionFailed}});
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Login authentication is disabled");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);
    }

    Y_UNIT_TEST(AuditLogLoginSuccess) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusSuccess}});
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        {
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_C(resultLogin.error().empty(), resultLogin);
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);   // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=schemeshard");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=");  // can't check the value
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_auth_domain={none}");
    }

    Y_UNIT_TEST(AuditLogLoginFailure) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusSuccess}});
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        {
            auto resultLogin = Login(runtime, "user1", "bad_password");
            UNIT_ASSERT_C(!resultLogin.error().empty(), resultLogin);
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);   // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=schemeshard");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=");  // can't check the value
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Invalid password");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_auth_domain={none}");
    }
}

namespace NSchemeShardUT_Private {

void EatWholeString(NHttp::THttpIncomingRequestPtr request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

NHttp::THttpIncomingRequestPtr MakeLoginRequest(const TString& user, const TString& password) {
    TString payload = [](const auto& user, const auto& password) {
        NJson::TJsonValue value;
        value["user"] = user;
        value["password"] = password;
        return NJson::WriteJson(value, false);
    }(user, password);
    TStringBuilder text;
    text << "POST /login HTTP/1.1\r\n"
        << "Host: test.ydb\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << payload.size() << "\r\n"
        << "\r\n"
        << payload;
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, text);
    // WebLoginService will crash without address
    request->Address = std::make_shared<TSockAddrInet>("127.0.0.1", 0);
    // Cerr << "TEST: http login request: " << text << Endl;
    return request;
}

NHttp::THttpIncomingRequestPtr MakeLogoutRequest(const TString& cookieName, const TString& cookieValue) {
    TStringBuilder text;
    text << "POST /logout HTTP/1.1\r\n"
        << "Host: test.ydb\r\n"
        << "Content-Type: text/plain\r\n"
        << "Cookie: " << cookieName << "=" << cookieValue << "\r\n"
        << "\r\n";
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, text);
    // WebLoginService will crash without address
    request->Address = std::make_shared<TSockAddrInet>("127.0.0.1", 0);
    // Cerr << "TEST: http logout request: " << text << Endl;
    return request;
}

}

Y_UNIT_TEST_SUITE(TWebLoginService) {

    Y_UNIT_TEST(Logout) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        // Add ticket parser to the mix
        {
            NKikimrProto::TAuthConfig authConfig;
            authConfig.SetUseBlackBox(false);
            authConfig.SetUseLoginProvider(true);

            IActor* ticketParser = NKikimr::CreateTicketParser({.AuthConfig = authConfig});
            TActorId ticketParserId = runtime.Register(ticketParser);
            runtime.RegisterService(NKikimr::MakeTicketParserID(), ticketParserId);
        }

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);  // alter root subdomain

        ui64 txId = 100;

        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusSuccess}});
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        TString ydbSessionId;
        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            ydbSessionId = cookies["ydb_session_id"];
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        // New security keys are created in the subdomain as a consequence of a login.
        // In real system they are transferred to the ticket parser by the grpc-proxy
        // on receiving subdomain update notification.
        // Here there are no grpc-proxy, so we should transfer keys to the ticket parser manually.
        {
            const auto describe = DescribePath(runtime, "/MyRoot");
            const auto& securityState = describe.GetPathDescription().GetDomainDescription().GetSecurityState();
            TActorId edge = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakeTicketParserID(), edge, new TEvTicketParser::TEvUpdateLoginSecurityState(securityState)), 0);
        }

        // Then we are ready to test some authentication on /logout
        {  // no cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("not-an-ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "401");

            // no audit record for actions without auth
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
        }
        {  // bad cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", "jklhagsfjhg")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");

            // no audit record for actions without auth
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
        }
        {  // good cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");

            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 4);  // +user web logout

            auto last = FindAuditLine(lines, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "component=web-login");
            UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(last, "subject=user1");
            UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        }
    }
}
