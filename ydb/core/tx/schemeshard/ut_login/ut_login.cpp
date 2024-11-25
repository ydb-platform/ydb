#include <util/string/join.h>

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

void SetPasswordCheckerParameters(TTestActorRuntime &runtime, ui64 schemeShard, const NLogin::TPasswordCheckParameters::TInitializer& parameters) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    ::NKikimrProto::TPasswordCheckerParameters passwordCheckParameters;
    passwordCheckParameters.SetMinimumLength(parameters.MinPasswordLength);
    passwordCheckParameters.SetMaximumLength(parameters.MaxPasswordLength);
    passwordCheckParameters.SetRestrictLower(parameters.NeedLowerCase);
    passwordCheckParameters.SetRestrictUpper(parameters.NeedUpperCase);
    passwordCheckParameters.SetRestrictNumbers(parameters.NeedNumbers);
    passwordCheckParameters.SetRestrictSpecial(parameters.NeedSpecialSymbols);
    passwordCheckParameters.SetSpecialChars(parameters.SpecialSymbols);
    *request->Record.MutableConfig()->MutableAuthConfig()->MutablePasswordCheckerParameters() = passwordCheckParameters;
    SetConfig(runtime, schemeShard, std::move(request));
}

const TString VALID_SPECIAL_SYMBOLS = "!@#$%^&*()_+{}|<>?=";

}  // namespace NSchemeShardUT_Private

Y_UNIT_TEST_SUITE(TSchemeShardLoginTest) {

    Y_UNIT_TEST(BasicLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
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
        //  length 0 - 4294967295
        //  optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusSuccess}});
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);

        // Accept password without lower case symbols
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "PASSWORDU2", {{NKikimrScheme::StatusSuccess}});
         resultLogin = Login(runtime, "user2", "PASSWORDU2");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 0 - 4294967295
        //  optional: upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.NeedLowerCase = true, .SpecialSymbols = VALID_SPECIAL_SYMBOLS});
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASSWORDU3", {{NKikimrScheme::StatusPreconditionFailed}});
        // Add lower case symbols to password
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASswORDu3", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user3", "PASswORDu3");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without upper case symbols
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user4", "passwordu4", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user4", "passwordu4");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 0 - 4294967295
        //  optional: numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case, upper case
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.NeedLowerCase = true, .NeedUpperCase = true, .SpecialSymbols = VALID_SPECIAL_SYMBOLS});
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "passwordu5", {{NKikimrScheme::StatusPreconditionFailed}});
        // Add upper case symbols to password
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "PASswORDu5", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user5", "PASswORDu5");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept short and long passwords
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user6", "pasSWu6", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user6", "pasSWu6");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user7", "pasSW12345Word!*&u7", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user7", "pasSW12345Word!*&u7");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 8 - 15
        //  optional: numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case, upper case
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinPasswordLength = 8, .MaxPasswordLength = 15, .NeedLowerCase = true, .NeedUpperCase = true, .SpecialSymbols = VALID_SPECIAL_SYMBOLS});
        // Too short password
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user8", "pasSWu8", {{NKikimrScheme::StatusPreconditionFailed}});
        // Too long password
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user8", "pasSW12345Word!*&u8", {{NKikimrScheme::StatusPreconditionFailed}});
        // Password has correct length
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user8", "PASswORDu8", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user8", "PASswORDu8");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without numbers
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user9", "passWorDunine", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user9", "passWorDunine");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 8 - 15
        //  optional: special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case, upper case, numbers
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinPasswordLength = 8,
                                                                                         .MaxPasswordLength = 15,
                                                                                         .NeedLowerCase = true,
                                                                                         .NeedUpperCase = true,
                                                                                         .NeedNumbers = true,
                                                                                         .SpecialSymbols = VALID_SPECIAL_SYMBOLS});
         TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user10", "passWorDuten", {{NKikimrScheme::StatusPreconditionFailed}});
        // Password with numbers
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user10", "PASswORDu10", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user10", "PASswORDu10");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        // Accept password without special symbols
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user11", "passWorDu11", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user11", "passWorDu11");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 8 - 15
        //  required: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinPasswordLength = 8,
                                                                                         .MaxPasswordLength = 15,
                                                                                         .NeedLowerCase = true,
                                                                                         .NeedUpperCase = true,
                                                                                         .NeedNumbers = true,
                                                                                         .NeedSpecialSymbols = true,
                                                                                         .SpecialSymbols = VALID_SPECIAL_SYMBOLS});
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "passWorDu12", {{NKikimrScheme::StatusPreconditionFailed}});
        // Password with special symbols
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "PASswORDu12*&%#", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user12", "PASswORDu12*&%#");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        // Password parameters:
        //  length 8 - 15
        //  required: lower case, upper case, numbers, special symbols from list *#
        SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinPasswordLength = 8,
                                                                                         .MaxPasswordLength = 15,
                                                                                         .NeedLowerCase = true,
                                                                                         .NeedUpperCase = true,
                                                                                         .NeedNumbers = true,
                                                                                         .NeedSpecialSymbols = true,
                                                                                         .SpecialSymbols = "*#"}); // Only 2 special symbols are valid
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user13", "PASswORDu13*&%#", {{NKikimrScheme::StatusPreconditionFailed}});
        TestCreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user13", "PASswORDu12*#", {{NKikimrScheme::StatusSuccess}});
        resultLogin = Login(runtime, "user13", "PASswORDu12*#");
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
