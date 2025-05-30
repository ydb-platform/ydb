#include <chrono>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet_flat/util_fmt_basic.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

struct TLogStopwatch {
    TLogStopwatch(TString message)
        : Message(std::move(message))
        , Started(std::chrono::steady_clock::now())
    {}
    
    ~TLogStopwatch() {
        std::chrono::steady_clock::time_point ended = std::chrono::steady_clock::now();
        Cerr << "[STOPWATCH] " << Message << " in " << NFmt::TDelay(TDuration::MicroSeconds(std::chrono::duration_cast<std::chrono::microseconds>(ended - Started).count())) << Endl;
    }

private:
    TString Message;
    std::chrono::steady_clock::time_point Started;
};

Y_UNIT_TEST_SUITE(TSchemeShardLoginLargeTest) {

    Y_UNIT_TEST(RemoveLogin_Many) {
        const size_t pathsToCreate = 10'000;
        const size_t usersWithAccess = 200; // 2M ACL rules in total
        const size_t usersTotal = 300;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        runtime.SetDispatchedEventsLimit(100'000'000'000);

        for (auto userId : xrange(usersTotal)) {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user" + std::to_string(userId), "password" + std::to_string(userId));
        }
        auto resultLogin = Login(runtime, "user0", "password0");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        
        {
            TLogStopwatch stopwatch(TStringBuilder() << "Created " << pathsToCreate << " paths with " << usersWithAccess * pathsToCreate << " acls");

            NACLib::TDiffACL diffACL;
            for (auto userId : xrange(usersWithAccess)) {
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
            TLogStopwatch stopwatch(TStringBuilder() << "Created single user userx");
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "userx", "passwordX");
        }

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Added single root acl userx");
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "userx");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Removed single root acl userx");
            NACLib::TDiffACL diffACL;
            diffACL.RemoveAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "userx");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }

        {
            TLogStopwatch stopwatch(TStringBuilder() << "Removed single user userx");
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "userx");
        }
    
        // removing users without access:
        for (auto userId : xrange(usersWithAccess, Min<size_t>(usersTotal, usersWithAccess + 3)))
        {
            TLogStopwatch stopwatch(TStringBuilder() << "Removed user" + std::to_string(userId));
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user" + std::to_string(userId));
        }

        // removing users with access (failing with a error):
        for (auto userId : xrange(Min<size_t>(usersWithAccess, 3)))
        {
            TLogStopwatch stopwatch(TStringBuilder() << "Don't removed user" + std::to_string(userId));
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user" + std::to_string(userId),
                {{NKikimrScheme::StatusPreconditionFailed}});
        }
    }

}
