#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/ut_common.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NKikimr::Tests;

namespace NKikimr {

namespace {

void WaitForAuditLogLines(const std::vector<std::string>& lines,
    size_t expectedLines, TDuration timeout = TDuration::MilliSeconds(500))
{
    auto deadline = TInstant::Now() + timeout;
    while (lines.size() < expectedLines && TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_C(lines.size() == expectedLines,
        "Audit log line did not appear within timeout. Expected: " << expectedLines << ", got: " << lines.size());
}

void EatWholeString(NHttp::THttpIncomingRequestPtr request, const TString& data) {
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
    return request;
}

void CreateUser(TTestEnv& env, const TString& user, const TString& password) {
    NQuery::TQueryClient queryClient(env.GetDriver());
    auto result = queryClient.ExecuteQuery(
        TStringBuilder() << "CREATE USER " << user << " PASSWORD '" << password << "'",
        NQuery::TTxControl::NoTx()
    ).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void ChangeUserPassword(TTestEnv& env, const TString& user, const TString& password) {
    NQuery::TQueryClient queryClient(env.GetDriver());
    auto result = queryClient.ExecuteQuery(
        TStringBuilder() << "ALTER USER " << user << " PASSWORD '" << password << "'",
        NQuery::TTxControl::NoTx()
    ).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void ChangeUserPasswordHash(TTestEnv& env, const TString& user, const TString& passwordHash) {
    NQuery::TQueryClient queryClient(env.GetDriver());
    auto result = queryClient.ExecuteQuery(
        TStringBuilder() << "ALTER USER " << user << " HASH '" << passwordHash << "'",
        NQuery::TTxControl::NoTx()
    ).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void ChangeUserIsEnabled(TTestEnv& env, const TString& user, bool isEnabled) {
    NQuery::TQueryClient queryClient(env.GetDriver());
    TStringBuilder query;
    query << "ALTER USER " << user;
    if (isEnabled) {
        query << " LOGIN";
    } else {
        query << " NOLOGIN";
    }

    auto result = queryClient.ExecuteQuery(
        query,
        NQuery::TTxControl::NoTx()
    ).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(WebLoginServiceAudit) {

    void AuditLogLoginTest(TTestEnv& env, bool isUserAdmin) {
        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        CreateUser(env, "user1", "password1");

        // User creation adds 1 audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }

        // After login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);

        if (isUserAdmin) {
            UNIT_ASSERT_STRING_CONTAINS(last, "login_user_level=admin");
        } else {
            UNIT_ASSERT(!last.contains("login_user_level=admin"));
        }
    }

    Y_UNIT_TEST(AuditLogEmptySIDsLoginSuccess) {
        TTestEnv env;
        AuditLogLoginTest(env, true);
    }

    Y_UNIT_TEST(AuditLogAdminLoginSuccess) {
        TTestEnv env;
        env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("user1");
        AuditLogLoginTest(env, true);
    }

    Y_UNIT_TEST(AuditLogLoginSuccess) {
        TTestEnv env;
        env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("user2");
        AuditLogLoginTest(env, false);
    }

    Y_UNIT_TEST(AuditLogLoginBadPassword) {
        TTestEnv env;

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        CreateUser(env, "user1", "password1");

        // User creation adds 1 audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");
            NJson::TJsonValue body(responseEv->Response->Body);
            UNIT_ASSERT_STRINGS_EQUAL(body.GetStringRobust(), "{\"error\":\"Invalid password\"}");
        }

        // After failed login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Invalid password");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginSuccess) {
        // mock ldap server predefined responses
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

        TTestEnvSettings settings;
        settings.EnableLDAP = true;
        settings.LDAPResponses = ldapResponses("user1", "password1");
        TTestEnv env(settings);

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "200", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }

        // After LDAP login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("detailed_status"));
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadPassword) {
        // mock ldap server predefined responses
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

        TTestEnvSettings settings;
        settings.EnableLDAP = true;
        settings.LDAPResponses = ldapResponses("user1", "bad_password");
        TTestEnv env(settings);

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }

        // After failed LDAP login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP login failed for user uid=user1,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadUser) {
        // mock ldap server predefined responses
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

        TTestEnvSettings settings;
        settings.EnableLDAP = true;
        settings.LDAPResponses = ldapResponses("bad_user", "password1");
        TTestEnv env(settings);

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("bad_user@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }

        // After failed LDAP login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP user bad_user does not exist. LDAP search for filter uid=bad_user on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=bad_user@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    // LDAP responses to bad BindDn or bad BindPassword are the same, so this test covers the both cases.
    Y_UNIT_TEST(AuditLogLdapLoginBadBind) {
       // mock ldap server predefined responses
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

        TTestEnvSettings settings;
        settings.EnableLDAP = true;
        settings.LDAPResponses = ldapResponses("user1", "password1");
        TTestEnv env(settings);

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }

        // After failed LDAP login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/Root");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLogout) {
        TTestEnv env;

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        CreateUser(env, "user1", "password1");

        // User creation adds 1 audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        // test body
        const auto target = env.GetWebLoginService();
        const auto edge = env.GetServer().GetRuntime()->AllocateEdgeActor();

        TString ydbSessionId;
        {
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            ydbSessionId = cookies["ydb_session_id"];
        }

        // After login, we should have 1 additional audit log entry
        expectedLines += 1;
        WaitForAuditLogLines(lines, expectedLines);

        // Then we are ready to test some authentication on /logout
        {  // no cookie
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("not-an-ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "401");

            // no audit record for actions without auth
            WaitForAuditLogLines(lines, expectedLines);
        }
        {  // bad cookie
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", "jklhagsfjhg")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");

            // no audit record for actions without auth
            WaitForAuditLogLines(lines, expectedLines);
        }
        {  // good cookie
            env.GetServer().GetRuntime()->Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = env.GetServer().GetRuntime()->GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            {
                NHttp::THeaders headers(responseEv->Response->Headers);
                UNIT_ASSERT_STRINGS_EQUAL(headers["Set-Cookie"], "ydb_session_id=; Max-Age=0");
            }

            // After logout, we should have 1 additional audit log entry
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);

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

    Y_UNIT_TEST(AuditLogCreateModifyUser) {
        TTestEnv env;

        const auto& lines = env.GetAuditLogLines();
        size_t expectedLines = lines.size();

        TString database = "/Root";
        TString user = "user1";
        TString password = "password1";
        TString newPassword = "password2";
        TString hash = R"(
        {
            "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
            "salt": "U+tzBtgo06EBQCjlARA6Jg==",
            "type": "argon2id"
        }
        )";

        auto check = [&](const TString& operation, const TVector<TString>& details = {}) {
            auto render = [](const TVector<TString>& list) {
                auto result = TStringBuilder();
                result << "[" << JoinStrings(list.begin(), list.end(), ", ") << "]";
                return result;
            };

            auto last = FindAuditLine(lines, Sprintf("operation=%s", operation.c_str()));

            if (!details.empty()) {
                UNIT_ASSERT_STRING_CONTAINS(last, Sprintf("login_user_change=%s", render(details).c_str()));
            }

            UNIT_ASSERT_STRING_CONTAINS(last, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(last, Sprintf("database=%s", database.c_str()));
            UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=StatusSuccess");
            UNIT_ASSERT_STRING_CONTAINS(last, "login_user_level=admin");
            UNIT_ASSERT_STRING_CONTAINS(last, Sprintf("login_user=%s", user.c_str()));
        };

        {
            CreateUser(env, user, password);
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("CREATE USER");
        }

        {
            ChangeUserPassword(env, user, newPassword);
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("MODIFY USER", {"password"});
        }

        {
            ChangeUserIsEnabled(env, user, false);
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("MODIFY USER", {"blocking"});
        }

        {
            ChangeUserIsEnabled(env, user, true);
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("MODIFY USER", {"unblocking"});
        }

        {
            ChangeUserPasswordHash(env, user, hash);
            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("MODIFY USER", {"password"});
        }

        {
            NQuery::TQueryClient queryClient(env.GetDriver());
            auto result = queryClient.ExecuteQuery(
                TStringBuilder() << "ALTER USER " << user << " PASSWORD '" << password << "'" << " NOLOGIN",
                NQuery::TTxControl::NoTx()
            ).GetValueSync();

            expectedLines += 1;
            WaitForAuditLogLines(lines, expectedLines);
            check("MODIFY USER", {"password", "blocking"});
        }
    }
}

} // namespace NKikimr
