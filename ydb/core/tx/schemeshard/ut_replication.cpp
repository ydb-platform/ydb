#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TReplicationTests) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
        )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateSequential) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        for (int i = 0; i < 2; ++i) {
            const auto name = Sprintf("Replication%d", i);

            TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "%s"
            )", name.c_str()));
            env.TestWaitNotification(runtime, txId);
            TestLs(runtime, "/MyRoot/" + name, false, NLs::PathExist);
        }
    }

    Y_UNIT_TEST(CreateInParallel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        for (int i = 0; i < 2; ++i) {
            TVector<TString> names;
            TVector<ui64> txIds;

            for (int j = 0; j < 2; ++j) {
                auto name = Sprintf("Replication%d-%d", i, j);

                TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "%s"
                )", name.c_str()));

                names.push_back(std::move(name));
                txIds.push_back(txId);
            }

            env.TestWaitNotification(runtime, txIds);
            for (const auto& name : names) {
                TestLs(runtime, "/MyRoot/" + name, false, NLs::PathExist);
            }
        }
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
        )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/Replication", false, NLs::PathNotExist);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
        )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);
    }

} // TReplicationTests
