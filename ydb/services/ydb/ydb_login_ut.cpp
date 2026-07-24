#include <format>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>

#include <ydb/core/testlib/test_client.h>

#include <util/system/tempfile.h>
#include "ydb_common_ut.h"

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

namespace {

class TLoginClientConnection {
public:
    TLoginClientConnection(bool isLoginAuthenticationEnabled = true, bool hideAuthenticationFailureReasons = false)
        : Server(InitAuthSettings(isLoginAuthenticationEnabled, hideAuthenticationFailureReasons))
        , Connection(GetDriverConfig(Server.GetPort()))
        , Client(Connection)
    {}

    void Stop() {
        Connection.Stop(true);
    }

    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility() {
        return Client.GetCoreFacility();
    }

    NYdb::NQuery::TExecuteQueryResult ExecuteSql(const TString& token, const TString& sql) {
        NYdb::NQuery::TClientSettings settings;
        settings.Database("/Root");
        settings.AuthToken(token);

        NYdb::NQuery::TQueryClient client = NYdb::NQuery::TQueryClient(Connection, settings);
        return client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    }

    NYdb::NQuery::TExecuteQueryResult CreateUser(const TString& user, const TString& password, const TString& token = "") {
        auto query = std::format("CREATE USER {0:} PASSWORD '{1:}'", std::string(user), std::string(password));
        return ExecuteSql(token.empty() ? RootToken : token, TString(query));
    }

    NYdb::NQuery::TExecuteQueryResult CreateUserHash(const TString& user, const TString& hash, const TString& token = "") {
        auto query = std::format("CREATE USER {0:} HASH '{1:}'", std::string(user), std::string(hash));
        return ExecuteSql(token.empty() ? RootToken : token, TString(query));
    }

    NYdb::NQuery::TExecuteQueryResult AlterUserHash(const TString& user, const TString& hash, const TString& token = "") {
        auto query = std::format("ALTER USER {0:} HASH '{1:}'", std::string(user), std::string(hash));
        return ExecuteSql(token.empty() ? RootToken : token, TString(query));
    }

    void SetPasswordComplexity(const NKikimrProto::TPasswordComplexity& passwordComplexity) {
        auto* runtime = Server.GetRuntime();
        for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
            *runtime->GetAppData(i).AuthConfig.MutablePasswordComplexity() = passwordComplexity;
        }
    }

    void ModifyACL(bool add, TString user, ui32 access) {
        NACLib::TDiffACL acl;
        if (add)
            acl.AddAccess(NACLib::EAccessType::Allow, access, user);
        else
            acl.RemoveAccess(NACLib::EAccessType::Allow, access, user);
        TClient client(*(Server.ServerSettings));
        client.SetSecurityToken(RootToken);
        client.ModifyACL("", "Root", acl.SerializeAsString());
    }

    void TestConnectRight(TString token, TString expectedErrorReason) {
        const TString sql = "SELECT 1;";
        const auto result = ExecuteSql(token, sql);

        if (expectedErrorReason.empty()) {
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_C(result.GetStatus() != NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedErrorReason, result.GetIssues().ToString());
        }
    }

    void TestDescribeRight(TString token, TString expectedErrorReason) {
        TClient client(*(Server.ServerSettings));
        client.SetSecurityToken(token);
        auto response = client.Ls("Root");
        UNIT_ASSERT(response);

        if (expectedErrorReason.empty()) {
            UNIT_ASSERT_C(response->Record.GetStatus() == NMsgBusProxy::MSTATUS_OK, response->Record.ShortDebugString());
            UNIT_ASSERT_C(response->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS, response->Record.ShortDebugString());
            UNIT_ASSERT_STRINGS_EQUAL_C(response->Record.GetPathDescription().GetSelf().GetName(), "Root", response->Record.ShortDebugString());
        } else {
            UNIT_ASSERT_C(response->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK, response->Record.ShortDebugString());
            UNIT_ASSERT_C(response->Record.GetStatusCode() != NKikimrIssues::TStatusIds::SUCCESS, response->Record.ShortDebugString());
            UNIT_ASSERT_STRINGS_EQUAL_C(response->Record.GetErrorReason(), expectedErrorReason, response->Record.ShortDebugString());
        }
    }

    TString GetToken(const TString& user, const TString& password) {
        auto factory = CreateLoginCredentialsProviderFactory({.User = user, .Password = password});
        auto loginProvider = factory->CreateProvider(this->GetCoreFacility());
        TString token;
        UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
        UNIT_ASSERT(!token.empty());
        return token;
    }

private:
    NKikimrConfig::TAppConfig InitAuthSettings(bool isLoginAuthenticationEnabled = true,
        bool hideAuthenticationFailureReasons = false)
    {
        NKikimrConfig::TAppConfig appConfig;

        auto& authConfig = *appConfig.MutableAuthConfig();
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(true);
        authConfig.SetEnableLoginAuthentication(isLoginAuthenticationEnabled);

        auto& securityConfig = *appConfig.MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenRequirement(true);
        securityConfig.AddAdministrationAllowedSIDs(RootToken);
        securityConfig.SetHideAuthenticationFailureReasons(hideAuthenticationFailureReasons);

        appConfig.MutableFeatureFlags()->SetCheckDatabaseAccessPermission(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);

        return appConfig;
    }

    TDriverConfig GetDriverConfig(ui16 grpcPort) {
        TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(grpcPort));
        return config;
    }

private:
    TString RootToken = "root@builtin";
    TKikimrWithGrpcAndRootSchemaWithAuth Server;
    NYdb::TDriver Connection;
    NConsoleClient::TDummyClient Client;
};

} // namespace


Y_UNIT_TEST_SUITE(TGRpcAuthentication) {
    const TString User = "user";
    const TString Password = "UserPassword";

    Y_UNIT_TEST(ValidCredentials) {
        TLoginClientConnection loginConnection;
        loginConnection.CreateUser(User, Password);
        loginConnection.ModifyACL(true, User, NACLib::EAccessRights::ConnectDatabase | NACLib::EAccessRights::DescribeSchema);

        auto token = loginConnection.GetToken(User, Password);

        loginConnection.TestConnectRight(token, "");
        loginConnection.TestDescribeRight(token, "");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(ValidHashCredentials) {
        TLoginClientConnection loginConnection;

        std::string hash = R"(
            {
                "version": 1,
                "argon2id": "HTkpQjtVJgBoA0CZu+i3zg==$ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
            }
        )";
        auto result = loginConnection.CreateUserHash(User, Base64Encode(hash));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = "password1"});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_NO_EXCEPTION(loginProvider->GetAuthInfo());

        hash = R"(
            {
                "version": 1,
                "argon2id": "HTkpQjtVJgBoA0CZu+i3zg==$ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw="
            }
        )";
        result = loginConnection.AlterUserHash(User, Base64Encode(hash));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = "password1"});
        loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_NO_EXCEPTION(loginProvider->GetAuthInfo());

        loginConnection.Stop();
    }

    Y_UNIT_TEST_TWIN(InvalidPassword, HideAuthenticationFailureReasons) {
        TLoginClientConnection loginConnection(true, HideAuthenticationFailureReasons);
        loginConnection.CreateUser(User, Password);

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = "WrongPassword"});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());

        static constexpr char error[] = "Invalid password";
        const auto exceptionDoesntContain = [](const auto& e) {
            return e.AsStrBuf().find(error) == std::string::npos;
        };

        if (HideAuthenticationFailureReasons) {
            UNIT_ASSERT_EXCEPTION_SATISFIES(loginProvider->GetAuthInfo(), yexception, exceptionDoesntContain);
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, error);
        }

        std::string hash = R"(
            {
                "version": 1,
                "argon2id": "HTkpQjtVJgBoA0CZu+i3zg==$ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
            }
        )";
        auto result = loginConnection.AlterUserHash(User, Base64Encode(hash));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());

        if (HideAuthenticationFailureReasons) {
            UNIT_ASSERT_EXCEPTION_SATISFIES(loginProvider->GetAuthInfo(), yexception, exceptionDoesntContain);
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, error);
        }

        loginConnection.Stop();
    }

    Y_UNIT_TEST_TWIN(UnknownUser, HideAuthenticationFailureReasons) {
        TLoginClientConnection loginConnection(true, HideAuthenticationFailureReasons);

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());

        static constexpr char error[] = "Cannot find user 'user'";
        const auto exceptionDoesntContain = [](const auto& e) {
            return e.AsStrBuf().find(error) == std::string::npos;
        };

        if (HideAuthenticationFailureReasons) {
            UNIT_ASSERT_EXCEPTION_SATISFIES(loginProvider->GetAuthInfo(), yexception, exceptionDoesntContain);
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, error);
        }

        loginConnection.CreateUser(User, Password);

        factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_NO_EXCEPTION(loginProvider->GetAuthInfo());

        auto dropUserQuery = std::format("DROP USER {0:}", std::string(User));
        auto result = loginConnection.ExecuteSql("root@builtin", TString(dropUserQuery));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());

        if (HideAuthenticationFailureReasons) {
            UNIT_ASSERT_EXCEPTION_SATISFIES(loginProvider->GetAuthInfo(), yexception, exceptionDoesntContain);
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, error);
        }

        loginConnection.Stop();
    }

    Y_UNIT_TEST(DisableLoginAuthentication) {
        TLoginClientConnection loginConnection(false);

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TStringBuilder expectedErrorMessage;
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Login authentication is disabled");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(NoConnectRights) {
        TLoginClientConnection loginConnection;
        loginConnection.CreateUser(User, Password);

        auto token = loginConnection.GetToken(User, Password);

        loginConnection.TestConnectRight(token, "No permission to connect to the database");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(NoDescribeRights) {
        TLoginClientConnection loginConnection;
        loginConnection.CreateUser(User, Password);
        loginConnection.ModifyACL(true, User, NACLib::EAccessRights::ConnectDatabase);

        auto token = loginConnection.GetToken(User, Password);

        loginConnection.TestConnectRight(token, "");
        loginConnection.TestDescribeRight(token, "Access denied");

        loginConnection.Stop();
    }
}

Y_UNIT_TEST_SUITE(TAuthenticationWithSqlExecution) {
    Y_UNIT_TEST(CreateAlterUserWithHash) {
        std::string user = "user1";
        std::string password = "password1";
        std::string hash = R"(
            {
                "hash":"ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                "salt":"HTkpQjtVJgBoA0CZu+i3zg==",
                "type":"argon2id"
            }
        )";

        TLoginClientConnection loginConnection;
        TString adminName = "admin";
        TString adminPassword = "superPassword";
        loginConnection.CreateUser(adminName, adminPassword);
        loginConnection.ModifyACL(true, adminName, NACLib::EAccessRights::GenericFull);

        auto adminToken = loginConnection.GetToken(adminName, adminPassword);

        auto createResult = loginConnection.CreateUser(TString(user), TString(password), adminToken);
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        auto result = loginConnection.AlterUserHash(TString(user), TString(hash), adminToken);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        loginConnection.ModifyACL(true, TString(user), NACLib::EAccessRights::ConnectDatabase);
        auto userToken = loginConnection.GetToken(TString(user), TString(password));
        loginConnection.TestConnectRight(userToken, "");

        loginConnection.Stop();
    }
}

Y_UNIT_TEST_SUITE(TModifyUser) {
    Y_UNIT_TEST(ModifyUserPassword) {
        TLoginClientConnection loginConnection;

        loginConnection.CreateUser("user1", "pass1");
        loginConnection.CreateUser("user2", "pass2");
        loginConnection.ModifyACL(true, "user1", NACLib::EAccessRights::ConnectDatabase);
        loginConnection.ModifyACL(true, "user2", NACLib::EAccessRights::ConnectDatabase);

        auto user1Token = loginConnection.GetToken("user1", "pass1");
        auto user2Token = loginConnection.GetToken("user2", "pass2");

        // user2 cannot change password for user1: user2 has no ydb.granular.alter_schema permission
        {
            auto result = loginConnection.ExecuteSql(user2Token, "ALTER USER user1 PASSWORD 'password1'");
            UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // user2 can change password for self
        {
            auto result = loginConnection.ExecuteSql(user2Token, "ALTER USER user2 PASSWORD 'password2'");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // user2 cannot login with the old password
        {
            auto factory = CreateLoginCredentialsProviderFactory({.User = "user2", .Password = "pass2"});
            auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
            UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Invalid password");
        }

        // user2 can login with the new password
        loginConnection.GetToken("user2", "password2");

        // grant ydb.granular.alter_schema to user1
        loginConnection.ModifyACL(true, "user1", NACLib::EAccessRights::AlterSchema);

        // user1 can change password for user2 now: user1 has ydb.granular.alter_schema permission
        {
            auto result = loginConnection.ExecuteSql(user1Token, "ALTER USER user2 PASSWORD 'pas2user'");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // user2 can login with the password set by user1
        loginConnection.GetToken("user2", "pas2user");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(ModifyUserIsEnabled) {
        TLoginClientConnection loginConnection;

        loginConnection.CreateUser("user1", "pass1");
        loginConnection.CreateUser("user2", "pass2");
        loginConnection.ModifyACL(true, "user1", NACLib::EAccessRights::ConnectDatabase);
        loginConnection.ModifyACL(true, "user2", NACLib::EAccessRights::ConnectDatabase);

        auto user2Token = loginConnection.GetToken("user2", "pass2");

        // user2 cannot change isEnabled for user1: user2 has no ydb.granular.alter_schema permission
        {
            auto result = loginConnection.ExecuteSql(user2Token, "ALTER USER user1 NOLOGIN");
            UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // user2 cannot change isEnabled for self
        {
            auto result = loginConnection.ExecuteSql(user2Token, "ALTER USER user2 NOLOGIN");
            UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // user2 cannot change isEnabled for self even together with a password change
        {
            auto result = loginConnection.ExecuteSql(user2Token, "ALTER USER user2 PASSWORD 'password' NOLOGIN");
            UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, result.GetIssues().ToString());
        }

        loginConnection.Stop();
    }
}

Y_UNIT_TEST_SUITE(TLoginPasswordComplexity) {

    void CheckPasswordAccepted(TLoginClientConnection& conn, const TString& user, const TString& password) {
        auto result = conn.CreateUser(user, password);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        conn.GetToken(user, password);
    }

    void CheckPasswordRejected(TLoginClientConnection& conn, const TString& user, const TString& password,
        const TString& expectedError)
    {
        auto result = conn.CreateUser(user, password);
        UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedError, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ChangeAcceptablePasswordParameters) {
        TLoginClientConnection conn;

        // Default complexity: min length 0
        // optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        // required: cannot contain username
        {
            CheckPasswordAccepted(conn, "user1", "Pass_word1");
            CheckPasswordAccepted(conn, "user2", "PASSWORDU2");
            CheckPasswordRejected(conn, "user3", "user3passwd", "Password must not contain user name");
        }

        // Require at least 3 lower case characters.
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetMinLowerCaseCount(3);
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user4", "PASSWORDU4", "should contain at least 3 lower case character");
            CheckPasswordAccepted(conn, "user4", "PASswORDu4");
        }

        // Require at least 3 upper case characters.
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetMinUpperCaseCount(3);
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user5", "passwordu5", "should contain at least 3 upper case character");
            CheckPasswordAccepted(conn, "user5", "PASswORDu5");
        }

        // Require a minimum length of 8.
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetMinLength(8);
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user6", "passwu6", "Password is too short");
            CheckPasswordAccepted(conn, "user6", "passwordu6");
        }

        // Require at least 3 numbers.
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetMinNumbersCount(3);
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user7", "passworduseven", "should contain at least 3 number");
            CheckPasswordAccepted(conn, "user7", "pas1swo5rdu7");
        }

        // Require at least 3 special characters (default set of special characters).
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetMinSpecialCharsCount(3);
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user8", "passwordu8", "should contain at least 3 special character");
            CheckPasswordAccepted(conn, "user8", "passwordu8*&%#");
        }

        // Restrict the set of acceptable special characters to "*#".
        {
            NKikimrProto::TPasswordComplexity complexity;
            complexity.SetSpecialChars("*#");
            conn.SetPasswordComplexity(complexity);
            CheckPasswordRejected(conn, "user9", "passwordu9*&%#", "Password contains unacceptable characters");
            CheckPasswordAccepted(conn, "user9", "passwordu9*#");
        }

        conn.Stop();
    }
}

} //namespace NKikimr
