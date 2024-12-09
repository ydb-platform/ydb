#include <util/string/join.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/login/login.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/auditlog_helpers.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/login_page.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace NSchemeShardUT_Private {

void SetPasswordCheckerParameters(TTestActorRuntime &runtime, ui64 schemeShard, const NLogin::TPasswordComplexity::TInitializer& initializer) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    ::NKikimrProto::TPasswordComplexity passwordComplexity;
    passwordComplexity.SetMinLength(initializer.MinLength);
    passwordComplexity.SetMinLowerCaseCount(initializer.MinLowerCaseCount);
    passwordComplexity.SetMinUpperCaseCount(initializer.MinUpperCaseCount);
    passwordComplexity.SetMinNumbersCount(initializer.MinNumbersCount);
    passwordComplexity.SetMinSpecialCharsCount(initializer.MinSpecialCharsCount);
    passwordComplexity.SetSpecialChars(initializer.SpecialChars);
    passwordComplexity.SetCanContainUsername(initializer.CanContainUsername);
    *request->Record.MutableConfig()->MutableAuthConfig()->MutablePasswordComplexity() = passwordComplexity;
    SetConfig(runtime, schemeShard, std::move(request));
}

}  // namespace NSchemeShardUT_Private

struct TLogStopwatch {
    TLogStopwatch(TString message)
        : Message(std::move(message))
        , Started(TAppData::TimeProvider->Now())
    {}
    
    ~TLogStopwatch() {
        Cerr << "[STOPWATCH] " << Message << " in " << (TAppData::TimeProvider->Now() - Started).MilliSeconds() << "ms" << Endl;
    }

private:
    TString Message;
    TInstant Started;
};

Y_UNIT_TEST_SUITE(TSchemeShardLoginTest) {

    Y_UNIT_TEST(BasicLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            UNIT_ASSERT(describe.HasPathDescription());
            UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
            UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().SidsSize(), 0);
        }

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            UNIT_ASSERT(describe.HasPathDescription());
            UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
            UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().SidsSize(), 1);
        }

        // public keys are filled after the first login
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            UNIT_ASSERT(describe.HasPathDescription());
            UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
            UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetDomainDescription().GetSecurityState().SidsSize(), 1);
        }

        // check token
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            NLogin::TLoginProvider login;
            login.UpdateSecurityState(describe.GetPathDescription().GetDomainDescription().GetSecurityState());
            auto resultValidate = login.ValidateToken({.Token = resultLogin.token()});
            UNIT_ASSERT_VALUES_EQUAL(resultValidate.User, "user1");
        }
    }

    Y_UNIT_TEST(RemoveLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        AsyncMkDir(runtime, ++txId, "/MyRoot/Dir1", "DirSub1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        AsyncMkDir(runtime, ++txId, "/MyRoot/Dir1", "DirSub2");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        AsyncModifyACL(runtime, ++txId, "/MyRoot/Dir1", "DirSub2", diffACL.SerializeAsString(), "");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
            {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir2"),
            {NLs::HasNoRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
            {NLs::HasNoRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub2"),
            {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});

        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1/DirSub2").DebugString() << Endl;
        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1").DebugString() << Endl;

        for (auto path : {"/MyRoot", "/MyRoot/Dir1", "/MyRoot/Dir2", "/MyRoot/Dir1/DirSub1", "/MyRoot/Dir1/DirSub2"}) {
            TestDescribeResult(DescribePath(runtime, path),
                {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1")});
        }

        // check login
        {
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Invalid user");
        }
    }

    Y_UNIT_TEST(RemoveLogin_Many) {
        const size_t pathsToCreate = 1000;
        const size_t usersToAdd = 10;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        runtime.SetDispatchedEventsLimit(100'000'000'000);

        for (auto userId : xrange(usersToAdd)) {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user" + std::to_string(userId), "password" + std::to_string(userId));
        }
        auto resultLogin = Login(runtime, "user0", "password0");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        
        {
            TLogStopwatch stopwatch(TStringBuilder() << "Created " << pathsToCreate << " paths");

            NACLib::TDiffACL diffACL;
            for (auto userId : xrange(usersToAdd)) {
                diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user" + std::to_string(userId));
            }

            THashSet<TString> paths;
            paths.emplace("MyRoot");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

            auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(++txId, TTestTxConfig::SchemeShard);

            // creating a random directories tree:
            while (paths.size() < pathsToCreate) {
                TString path = "/MyRoot";
                ui32 index = RandomNumber<ui32>();
                for (ui32 depth : xrange(15)) {
                    Y_UNUSED(depth);
                    TString dir = "Dir" + std::to_string(index % 3);
                    index /= 3;
                    if (paths.size() < pathsToCreate && paths.emplace(path + "/" + dir).second) {
                        auto transaction = evTx->Record.AddTransaction();
                        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
                        transaction->SetWorkingDir(path);
                        transaction->MutableMkDir()->SetName(dir);
                        transaction->MutableModifyACL()->SetDiffACL(diffACL.SerializeAsString());
                    }
                    path += "/" + dir;
                }
            }

            AsyncSend(runtime, TTestTxConfig::SchemeShard, evTx);
            TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        }

        Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot").DebugString() << Endl;

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Added single root acl");
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "userX");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Removed single root acl");
            NACLib::TDiffACL diffACL;
            diffACL.RemoveAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "userX");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }
    
        for (auto userId : xrange(usersToAdd))
        {
            TLogStopwatch stopwatch(TStringBuilder() << "Removed user" + std::to_string(userId));
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user" + std::to_string(userId));
        }
    }

    Y_UNIT_TEST(DisableBuiltinAuthMechanism) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().AuthConfig.SetEnableLoginAuthentication(false);
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusPreconditionFailed}});
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Login authentication is disabled");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);
    }

    Y_UNIT_TEST(ChangeAcceptablePasswordParameters) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        // required: cannot contain username
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);

        // Accept password without lower case symbols
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "PASSWORDU2");
         resultLogin = Login(runtime, "user2", "PASSWORDU2");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 0
        //  optional: upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case = 3, cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLowerCaseCount = 3});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASSWORDU3", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 lower case character"}});
        // Add lower case symbols to password
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASswORDu3");
        resultLogin = Login(runtime, "user3", "PASswORDu3");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without upper case symbols
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user4", "passwordu4");
        resultLogin = Login(runtime, "user4", "passwordu4");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 0
        //  optional: lower case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: upper case = 3, cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLowerCaseCount = 0, .MinUpperCaseCount = 3});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "passwordu5", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 upper case character"}});
        // Add 3 upper case symbols to password
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "PASswORDu5");
        resultLogin = Login(runtime, "user5", "PASswORDu5");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept short password
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinUpperCaseCount = 0});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user6", "passwu6");
        resultLogin = Login(runtime, "user6", "passwu6");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 8
        //  optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLength = 8});
        // Too short password
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user7", "passwu7", {{NKikimrScheme::StatusPreconditionFailed, "Password is too short"}});
        // Password has correct length
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user7", "passwordu7");
        resultLogin = Login(runtime, "user7", "passwordu7");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without numbers
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLength = 0});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user8", "passWorDueitgh");
        resultLogin = Login(runtime, "user8", "passWorDueitgh");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case,special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: numbers = 3, cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinNumbersCount = 3});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user9", "passwordunine", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 number"}});
        // Password with numbers
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user9", "pas1swo5rdu9");
        resultLogin = Login(runtime, "user9", "pas1swo5rdu9");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without special symbols
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinNumbersCount = 0});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user10", "passWorDu10");
        resultLogin = Login(runtime, "user10", "passWorDu10");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers
        //  required: special symbols from list !@#$%^&*()_+{}|<>?= , cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinSpecialCharsCount = 3});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user11", "passwordu11", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 special character"}});
        // Password with special symbols
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user11", "passwordu11*&%#");
        resultLogin = Login(runtime, "user11", "passwordu11*&%#");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers
        //  required: special symbols from list *# , cannot contain username
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.SpecialChars = "*#"}); // Only 2 special symbols are valid
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "passwordu12*&%#", {{NKikimrScheme::StatusPreconditionFailed, "Password contains unacceptable characters"}});
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "passwordu12*#");
        resultLogin = Login(runtime, "user12", "passwordu12*#");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
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

    Y_UNIT_TEST(AuditLogLoginSuccess) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
    }

    Y_UNIT_TEST(AuditLogLoginBadPassword) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        TString ydbSessionId;
        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");
            NJson::TJsonValue body(responseEv->Response->Body);
            UNIT_ASSERT_STRINGS_EQUAL(body.GetStringRobust(), "{\"error\":\"Invalid password\"}");
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Invalid password");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginSuccess) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "200", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("detailed_status"));
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadPassword) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "bad_password"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP login failed for user uid=user1,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadUser) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("bad_user", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("bad_user@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP user bad_user does not exist. LDAP search for filter uid=bad_user on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=bad_user@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    // LDAP responses to bad BindDn or bad BindPassword are the same, so this test covers the both cases.
    Y_UNIT_TEST(AuditLogLdapLoginBadBind) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLogout) {
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
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
            {
                NHttp::THeaders headers(responseEv->Response->Headers);
                UNIT_ASSERT_STRINGS_EQUAL(headers["Set-Cookie"], "ydb_session_id=; Max-Age=0");
            }

            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 4);  // +user web logout

            auto last = FindAuditLine(lines, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "component=web-login");
            UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(last, "subject=user1");
            UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
            UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
        }
    }
}
