#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TTransferTests) {
    static TString DefaultScheme(const TString& name) {
        return Sprintf(R"(
            Name: "%s"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  Password: "pwd"
                }
              }
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )", name.c_str());
    }

    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
    }

    ui64 ExtractControllerId(const NKikimrSchemeOp::TPathDescription& desc) {
        UNIT_ASSERT(desc.HasReplicationDescription());
        const auto& r = desc.GetReplicationDescription();
        UNIT_ASSERT(r.HasControllerId());
        return r.GetControllerId();
    }

    void CreateTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
      TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table"
        ColumnShardCount: 1
        Schema {
            Columns { Name: "key" Type: "Uint32" NotNull: true }
            Columns { Name: "data" Type: "Utf8" }
            KeyColumnNames: [ "key" ]
        }
      )");
      env.TestWaitNotification(runtime, txId);
    }


    Y_UNIT_TEST(RepeatedStandbyIsForwarded) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        CreateTable(runtime, env, txId);
        TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
        env.TestWaitNotification(runtime, txId);
        THashMap<ui64, NKikimrReplication::TEvAlterReplication> forwarded;
        auto observer = runtime.AddObserver<NReplication::TEvController::TEvAlterReplication>(
            [&](auto& ev) { forwarded[ev->Get()->Record.GetOperationId().GetTxId()] = ev->Get()->Record; });
        for (ui32 i = 0; i < 2; ++i) {
            TestAlterTransfer(runtime, ++txId, "/MyRoot", R"(Name: "Transfer" State { StandBy {} })");
            env.TestWaitNotification(runtime, txId);
            UNIT_ASSERT(forwarded.contains(txId));
            UNIT_ASSERT_C(forwarded.at(txId).GetSwitchState().HasStandBy(), forwarded.at(txId).DebugString());
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
                NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy)});
        }
        UNIT_ASSERT_VALUES_EQUAL(forwarded.size(), 2);
    }

    Y_UNIT_TEST(AlterSettingsInStandbyCarriesStateAndConfig) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        CreateTable(runtime, env, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "transfer-dir");
        env.TestWaitNotification(runtime, txId);
        TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
        env.TestWaitNotification(runtime, txId);
        THashMap<ui64, NKikimrReplication::TEvAlterReplication> forwarded;
        auto observer = runtime.AddObserver<NReplication::TEvController::TEvAlterReplication>(
            [&](auto& ev) { forwarded[ev->Get()->Record.GetOperationId().GetTxId()] = ev->Get()->Record; });
        enum class ESetting { Lambda, Directory, Batch, Flush, Unchanged };
        for (const auto setting : {ESetting::Lambda, ESetting::Directory, ESetting::Batch, ESetting::Flush, ESetting::Unchanged}) {
            auto expected = DescribePath(runtime, "/MyRoot/Transfer").GetPathDescription().GetReplicationDescription().GetConfig();
            TString field;
            auto* specific = expected.MutableTransferSpecific();
            switch (setting) {
                case ESetting::Lambda:
                    field = R"(TransformLambda: "($row) -> { RETURN AsList($row); }")";
                    specific->MutableTarget()->SetTransformLambda("($row) -> { RETURN AsList($row); }");
                    break;
                case ESetting::Directory:
                    field = R"(DirectoryPath: "/MyRoot/transfer-dir")";
                    specific->MutableTarget()->SetDirectoryPath("/MyRoot/transfer-dir");
                    break;
                case ESetting::Batch:
                    field = "BatchSizeBytes: 4096";
                    specific->MutableBatching()->SetBatchSizeBytes(4096);
                    break;
                case ESetting::Flush:
                case ESetting::Unchanged:
                    field = "FlushIntervalMilliSeconds: 2000";
                    specific->MutableBatching()->SetFlushIntervalMilliSeconds(2000);
                    break;
            }
            TestAlterTransfer(runtime, ++txId, "/MyRoot", TStringBuilder() << "Name: \"Transfer\" AlterTransfer { " << field << " }");
            env.TestWaitNotification(runtime, txId);
            UNIT_ASSERT(forwarded.contains(txId));
            const auto& event = forwarded.at(txId);
            UNIT_ASSERT_C(event.GetSwitchState().HasStandBy(), event.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(event.GetConfig().GetTransferSpecific().SerializeAsString(), expected.GetTransferSpecific().SerializeAsString());
            const auto stored = DescribePath(runtime, "/MyRoot/Transfer").GetPathDescription().GetReplicationDescription();
            UNIT_ASSERT(stored.GetState().HasStandBy());
            UNIT_ASSERT_VALUES_EQUAL(stored.GetConfig().GetTransferSpecific().SerializeAsString(), expected.GetTransferSpecific().SerializeAsString());
        }
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        CreateTable(runtime, env, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
    }

    Y_UNIT_TEST(CreateSequential) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        THashSet<ui64> controllerIds;

        CreateTable(runtime, env, txId);

        for (int i = 0; i < 2; ++i) {
            const auto name = Sprintf("Transfer%d", i);

            TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme(name));
            env.TestWaitNotification(runtime, txId);

            const auto desc = DescribePath(runtime, "/MyRoot/" + name);
            TestDescribeResult(desc, {
                NLs::PathExist,
                NLs::Finished,
            });

            controllerIds.insert(ExtractControllerId(desc.GetPathDescription()));
        }

        UNIT_ASSERT_VALUES_EQUAL(controllerIds.size(), 2);
    }

    Y_UNIT_TEST(CreateInParallel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        THashSet<ui64> controllerIds;

        CreateTable(runtime, env, txId);

        for (int i = 0; i < 2; ++i) {
            TVector<TString> names;
            TVector<ui64> txIds;

            for (int j = 0; j < 2; ++j) {
                auto name = Sprintf("Transfer%d-%d", i, j);

                TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme(name));

                names.push_back(std::move(name));
                txIds.push_back(txId);
            }

            env.TestWaitNotification(runtime, txIds);
            for (const auto& name : names) {
                const auto desc = DescribePath(runtime, "/MyRoot/" + name);
                TestDescribeResult(desc, {
                    NLs::PathExist,
                    NLs::Finished,
                });

                controllerIds.insert(ExtractControllerId(desc.GetPathDescription()));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(controllerIds.size(), 4);
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        ui64 controllerId = 0;

        CreateTable(runtime, env, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Transfer");
            TestDescribeResult(desc, {NLs::PathExist});
            controllerId = ExtractControllerId(desc.GetPathDescription());
        }

        TestDropTransfer(runtime, ++txId, "/MyRoot", "Transfer");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {NLs::PathNotExist});

        TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Transfer");
            TestDescribeResult(desc, {NLs::PathExist});
            UNIT_ASSERT_VALUES_UNEQUAL(controllerId, ExtractControllerId(desc.GetPathDescription()));
        }
    }

    Y_UNIT_TEST(CreateWithoutCredentials) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        CreateTable(runtime, env, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/Transfer");
        const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
        UNIT_ASSERT_VALUES_UNEQUAL("root@builtin", params.GetOAuthToken().GetToken());
    }

    Y_UNIT_TEST(CreateWrongConfig) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        CreateTable(runtime, env, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )", {{NKikimrScheme::StatusInvalidParameter}});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(CreateWrongBatchSize) {
      TTestBasicRuntime runtime;
      TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
      ui64 txId = 100;

      SetupLogging(runtime);

      CreateTable(runtime, env, txId);

      TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
          Name: "Transfer"
          Config {
            TransferSpecific {
              Target {
                SrcPath: "/MyRoot1/Table"
                DstPath: "/MyRoot/Table"
              }
              Batching {
                BatchSizeBytes: 1073741825
              }
            }
          }
      )", {{NKikimrScheme::StatusInvalidParameter}});
      env.TestWaitNotification(runtime, txId);

      TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
          NLs::PathNotExist
      });
    }

    Y_UNIT_TEST(CreateWrongFlushIntervalIsSmall) {
      TTestBasicRuntime runtime;
      TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
      ui64 txId = 100;

      SetupLogging(runtime);

      CreateTable(runtime, env, txId);

      TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
          Name: "Transfer"
          Config {
            TransferSpecific {
              Target {
                SrcPath: "/MyRoot1/Table"
                DstPath: "/MyRoot/Table"
              }
              Batching {
                FlushIntervalMilliSeconds: 1
              }
            }
          }
      )", {{NKikimrScheme::StatusInvalidParameter}});
      env.TestWaitNotification(runtime, txId);

      TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
          NLs::PathNotExist
      });
    }

    Y_UNIT_TEST(CreateWrongFlushIntervalIsBig) {
      TTestBasicRuntime runtime;
      TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
      ui64 txId = 100;

      SetupLogging(runtime);

      CreateTable(runtime, env, txId);

      TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
          Name: "Transfer"
          Config {
            TransferSpecific {
              Target {
                SrcPath: "/MyRoot1/Table"
                DstPath: "/MyRoot/Table"
              }
              Batching {
                FlushIntervalMilliSeconds: 86400001
              }
            }
          }
      )", {{NKikimrScheme::StatusInvalidParameter}});
      env.TestWaitNotification(runtime, txId);

      TestDescribeResult(DescribePath(runtime, "/MyRoot/Transfer"), {
          NLs::PathNotExist
      });
    }

    Y_UNIT_TEST(ConsistencyLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        CreateTable(runtime, env, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Transfer1");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer2"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
              ConsistencySettings {
                Row {}
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Transfer2");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer3"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
              ConsistencySettings {
                Global {
                  CommitIntervalMilliSeconds: 10000
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Transfer3");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 10000);
        }
    }

    Y_UNIT_TEST(Alter) {
      TTestBasicRuntime runtime;
      TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
      ui64 txId = 100;

      SetupLogging(runtime);

      CreateTable(runtime, env, txId);

      TestCreateTransfer(runtime, ++txId, "/MyRoot", DefaultScheme("Transfer"));
      env.TestWaitNotification(runtime, txId);
      {
          const auto desc = DescribePath(runtime, "/MyRoot/Transfer");
          TestDescribeResult(desc, {NLs::PathExist});
      }

      TestAlterTransfer(runtime, ++txId, "/MyRoot", R"(
          Name: "Transfer"
          State {
            Paused {
            }
          }
      )");

      TestAlterTransfer(runtime, ++txId, "/MyRoot", R"(
          Name: "Transfer"
          State {
            StandBy {
            }
          }
      )");
  }

} // TTransferTests
