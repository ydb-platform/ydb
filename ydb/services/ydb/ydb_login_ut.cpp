#include <format>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>

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
    TLoginClientConnection(bool isLoginAuthenticationEnabled = true)
        : Server(InitAuthSettings(isLoginAuthenticationEnabled))
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

    void CreateUser(TString user, TString password) {
        TClient client(*(Server.ServerSettings));
        client.SetSecurityToken(RootToken);
        auto status = client.CreateUser("/Root", user, password);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
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
    NKikimrConfig::TAppConfig InitAuthSettings(bool isLoginAuthenticationEnabled = true) {
        NKikimrConfig::TAppConfig appConfig;
        auto authConfig = appConfig.MutableAuthConfig();

        authConfig->SetUseBlackBox(false);
        authConfig->SetUseLoginProvider(true);
        authConfig->SetEnableLoginAuthentication(isLoginAuthenticationEnabled);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->AddAdministrationAllowedSIDs(RootToken);
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

    Y_UNIT_TEST(InvalidPassword) {
        TLoginClientConnection loginConnection;
        loginConnection.CreateUser(User, Password);

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = "WrongPassword"});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        UNIT_ASSERT_EXCEPTION_CONTAINS(loginProvider->GetAuthInfo(), yexception, "Invalid password");

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

        auto query = std::format("CREATE USER {0:}; ALTER USER {0:} HASH '{1:}';", user, hash);
        auto result = loginConnection.ExecuteSql(adminToken, TString(query));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        loginConnection.ModifyACL(true, TString(user), NACLib::EAccessRights::ConnectDatabase);
        auto userToken = loginConnection.GetToken(TString(user), TString(password));
        loginConnection.TestConnectRight(userToken, "");

        loginConnection.Stop();
    }
}

} //namespace NKikimr
