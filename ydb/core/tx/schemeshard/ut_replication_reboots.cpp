#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TReplicationWithRebootsTests) {
    static TString DefaultScheme(const TString& name) {
        return Sprintf(R"(
            Name: "%s"
            Config {
              StaticCredentials {
                User: "user"
                Password: "pwd"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", name.c_str());
    }

    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
    }

    Y_UNIT_TEST(Create) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().InitYdbDriver(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                SetupLogging(runtime);
            }

            TestCreateReplication(runtime, ++t.TxId, "/MyRoot", DefaultScheme("Replication"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);
        });
    }

    void CreateMultipleReplications(bool withInitialController) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().InitYdbDriver(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                SetupLogging(runtime);

                if (withInitialController) {
                    TestCreateReplication(runtime, ++t.TxId, "/MyRoot", DefaultScheme("Replication0"));

                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                    TestLs(runtime, "/MyRoot/Replication0", false, NLs::PathExist);
                }
            }

            TVector<TString> names;
            TVector<ui64> txIds;

            for (int i = 1; i <= 3; ++i) {
                auto name = Sprintf("Replication%d", i);
                auto request = CreateReplicationRequest(++t.TxId, "/MyRoot", DefaultScheme(name));

                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusAlreadyExists,
                    NKikimrScheme::StatusMultipleModifications,
                });

                names.push_back(std::move(name));
                txIds.push_back(t.TxId);
            }
            t.TestEnv->TestWaitNotification(runtime, txIds);

            {
                TInactiveZone inactive(activeZone);

                for (const auto& name : names) {
                    TestLs(runtime, "/MyRoot/" + name, false, NLs::PathExist);
                }
            }
        });
    }

    Y_UNIT_TEST(CreateInParallelWithoutInitialController) {
        CreateMultipleReplications(false);
    }

    Y_UNIT_TEST(CreateInParallelWithInitialController) {
        CreateMultipleReplications(true);
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().InitYdbDriver(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                SetupLogging(runtime);
            }

            {
                auto request = CreateReplicationRequest(++t.TxId, "/MyRoot", DefaultScheme("Replication"));
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusAlreadyExists,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);

            {
                auto request = DropReplicationRequest(++t.TxId, "/MyRoot", "Replication");
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestLs(runtime, "/MyRoot/Replication", false, NLs::PathNotExist);

            {
                auto request = CreateReplicationRequest(++t.TxId, "/MyRoot", DefaultScheme("Replication"));
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusAlreadyExists,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestLs(runtime, "/MyRoot/Replication", false, NLs::PathExist);
        });
    }

} // TReplicationWithRebootsTests
