#include <util/string/join.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/library/login/login.h>
#include <ydb/core/protos/auth.pb.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace NSchemeShardUT_Private {

// convert into generic test helper?
void TestCreateAlterLoginCreateUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& password, const TVector<TExpectedResult>& expectedResults) {
    std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> modifyTx(CreateAlterLoginCreateUser(txId, user, password));
    //TODO: move setting of TModifyScheme.WorkingDir into CreateAlterLoginCreateUser()
    //NOTE: TModifyScheme.Name isn't set, intentionally
    modifyTx->Record.MutableTransaction(0)->SetWorkingDir(database);
    AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
    // AlterLoginCreateUser is synchronous in nature, result is returned immediately
    TestModificationResults(runtime, txId, expectedResults);
}

}  // namespace NSchemeShardUT_Private

namespace {

class TMemoryLogBackend: public TLogBackend {
public:
    std::vector<std::string>& Buffer;

    TMemoryLogBackend(std::vector<std::string>& buffer)
        : Buffer(buffer)
    {}

    virtual void WriteData(const TLogRecord& rec) override {
        Buffer.emplace_back(rec.Data, rec.Len);
    }

    virtual void ReopenLog() override {
    }
};

}  // anonymous namespace

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

    NAudit::TAuditLogBackends CreateTestAuditLogBackends(std::vector<std::string>& buffer) {
        NAudit::TAuditLogBackends logBackends;
        logBackends[NKikimrConfig::TAuditConfig::TXT].emplace_back(new TMemoryLogBackend(buffer));
        return logBackends;
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

        Cerr << "auditlog lines:\n" << JoinSeq('\n', lines) << Endl;
        auto last = lines[lines.size() - 1];
        Cerr << "auditlog last line:\n" << last << Endl;

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

        Cerr << "auditlog lines:\n" << JoinSeq('\n', lines) << Endl;
        auto last = lines[lines.size() - 1];
        Cerr << "auditlog last line:\n" << last << Endl;

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
