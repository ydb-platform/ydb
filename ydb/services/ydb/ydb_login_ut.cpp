#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
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

    void CreateUser(TString user, TString password) {
        TClient client(*(Server.ServerSettings));
        client.SetSecurityToken("root@builtin");
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
        client.SetSecurityToken("root@builtin");
        client.ModifyACL("", "Root", acl.SerializeAsString());
    }
    
    void Ls(TString token, TString expectedErrorReason) {
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

private:
    NKikimrConfig::TAppConfig InitAuthSettings(bool isLoginAuthenticationEnabled = true) {
        NKikimrConfig::TAppConfig appConfig;
        auto authConfig = appConfig.MutableAuthConfig();

        authConfig->SetUseBlackBox(false);
        authConfig->SetUseLoginProvider(true);
        authConfig->SetEnableLoginAuthentication(isLoginAuthenticationEnabled);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);
        appConfig.MutableFeatureFlags()->SetCheckDatabaseAccessPermission(true);

        return appConfig;
    }

    TDriverConfig GetDriverConfig(ui16 grpcPort) {
        TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(grpcPort));
        return config;
    }

private:
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
        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString token;
        UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
        UNIT_ASSERT(!token.empty());

        loginConnection.Ls(token, "");

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

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString token;
        UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
        UNIT_ASSERT(!token.empty());
        
        loginConnection.Ls(token, "Access denied");

        loginConnection.Stop();
    }

    Y_UNIT_TEST(NoDescribeRights) {
        TLoginClientConnection loginConnection;
        loginConnection.CreateUser(User, Password);
        loginConnection.ModifyACL(true, User, NACLib::EAccessRights::ConnectDatabase);

        auto factory = CreateLoginCredentialsProviderFactory({.User = User, .Password = Password});
        auto loginProvider = factory->CreateProvider(loginConnection.GetCoreFacility());
        TString token;
        UNIT_ASSERT_NO_EXCEPTION(token = loginProvider->GetAuthInfo());
        UNIT_ASSERT(!token.empty());
        
        loginConnection.Ls(token, "Access denied");

        loginConnection.Stop();
    }
}
} //namespace NKikimr
