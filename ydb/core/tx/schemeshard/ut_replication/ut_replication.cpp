#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <yql/essentials/public/udf/udf_data_type.h>

using namespace NSchemeShardUT_Private;

namespace {

struct TReplicationRebuildTest {
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
    const TString Table = "/MyRoot/Table";
    const TString IndexPath = Table + "/Index";
    TVector<TString> TableRows;
    TVector<TString> IndexRows;

    TReplicationRebuildTest()
        : Env(Runtime)
    {
        TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            Columns { Name: "payload" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env.TestWaitNotification(Runtime, TxId);
        for (ui32 key = 1; key <= 3; ++key) {
            const TString value = TStringBuilder() << "value-" << key;
            const TString payload = TStringBuilder() << "payload-" << key;
            UploadRow(Runtime, Table, 0, {1}, {2, 3}, {TCell::Make(key)},
                {TCell(value.data(), value.size()), TCell(payload.data(), payload.size())});
        }
        const ui64 build = Start(false);
        Wait(build);
        TestForgetBuildIndex(Runtime, ++TxId, TTestTxConfig::SchemeShard, "/MyRoot", build);
        TableRows = ReadShards(Runtime, TTestTxConfig::SchemeShard, Table);
        IndexRows = ReadShards(Runtime, TTestTxConfig::SchemeShard, IndexPath + "/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(Runtime, Table), 3);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(Runtime, IndexPath + "/indexImplTable"), 3);
    }

    ui64 Start(bool rebuild = true, Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS) {
        Ydb::Table::TableIndex index;
        index.set_name("Index");
        index.mutable_global_index();
        if (!rebuild) {
            index.add_index_columns("value");
            index.add_data_columns("payload");
        }
        const ui64 id = ++TxId;
        auto* request = CreateBuildIndexRequest(id, "/MyRoot", Table, index);
        request->Record.MutableSettings()->set_is_rebuild(rebuild);
        const auto sender = Runtime.AllocateEdgeActor();
        Runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request);
        const auto response = Runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), expected, response->Get()->Record.DebugString());
        if (expected == Ydb::StatusIds::BAD_REQUEST) {
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.DebugString(), "path is an async replica table");
        }
        return id;
    }

    void Wait(ui64 id) {
        Env.TestWaitNotification(Runtime, id);
        const auto result = TestGetBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot", id);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            result.DebugString());
    }

    void SetMode(const TString& directory, const TString& table, bool replica,
            bool global = false, NKikimrScheme::EStatus expected = NKikimrScheme::StatusAccepted)
    {
        const TString config = replica
            ? TStringBuilder() << "Mode: REPLICATION_MODE_READ_ONLY ConsistencyLevel: "
                << (global ? "CONSISTENCY_LEVEL_GLOBAL" : "CONSISTENCY_LEVEL_ROW")
            : TString("Mode: REPLICATION_MODE_NONE");
        AsyncSend(Runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++TxId, directory,
            Sprintf(R"(Name: "%s" ReplicationConfig { %s })", table.c_str(), config.c_str()))));
        TestModificationResults(Runtime, TxId, {expected});
        if (expected == NKikimrScheme::StatusAccepted) {
            Env.TestWaitNotification(Runtime, TxId);
        }
    }

    ui64 CheckIndex() {
        const auto description = DescribePrivatePath(Runtime, IndexPath);
        TestDescribeResult(description, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal)});
        const auto& index = description.GetPathDescription().GetTableIndex();
        UNIT_ASSERT_VALUES_EQUAL(index.KeyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetKeyColumnNames(0), "value");
        UNIT_ASSERT_VALUES_EQUAL(index.DataColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumnNames(0), "payload");
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(Runtime, TTestTxConfig::SchemeShard, Table), TableRows);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(Runtime, TTestTxConfig::SchemeShard, IndexPath + "/indexImplTable"), IndexRows);
        return description.GetPathDescription().GetSelf().GetPathId();
    }

    void CheckNoTemporaryIndex(ui64 id) {
        TestDescribeResult(DescribePrivatePath(Runtime, TStringBuilder() << Table << "/__ydb_rebuild_" << id),
            {NLs::PathNotExist});
        TestDescribeResult(DescribePath(Runtime, Table), {NLs::IndexesCount(1)});
    }

    void CheckReplica(bool replica) {
        const auto mode = replica ? NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY
            : NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE;
        const TUserAttrs attrs = replica ? TUserAttrs{{"__async_replica", "true"}} : TUserAttrs{};
        for (const auto& path : {Table, IndexPath + "/indexImplTable"}) {
            const auto description = DescribePrivatePath(Runtime, path);
            TestDescribeResult(description, {NLs::UserAttrsEqual(attrs)});
            UNIT_ASSERT_VALUES_EQUAL_C(description.GetPathDescription().GetTable().GetReplicationConfig().GetMode(), mode, path);
        }
    }

    void Forget(ui64 id) {
        CheckNoTemporaryIndex(id);
        TestForgetBuildIndex(Runtime, ++TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
        UNIT_ASSERT_VALUES_EQUAL(TestListBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot").EntriesSize(), 0);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TReplicationTests) {
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

    ui64 ExtractControllerId(const NKikimrSchemeOp::TPathDescription& desc) {
        UNIT_ASSERT(desc.HasReplicationDescription());
        const auto& r = desc.GetReplicationDescription();
        UNIT_ASSERT(r.HasControllerId());
        return r.GetControllerId();
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
    }

    TString AvailabilityScheme(const TString& name, const TString& period) {
        auto scheme = DefaultScheme(name);
        scheme.insert(scheme.find("Config {") + TStringBuf("Config {").size(),
            TStringBuilder() << " AvailabilityPeriod { " << period << " } ");
        return scheme;
    }

    Y_UNIT_TEST(AvailabilityPeriodCreateAndReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        const TVector<TString> periods = {"", "Unlimited {}", "Duration { seconds: 10 nanos: 1 }", "Duration { nanos: 1 }"};
        for (ui32 i = 0; i < periods.size(); ++i) {
            const auto name = TStringBuilder() << "Replication" << i;
            TestCreateReplication(runtime, ++txId, "/MyRoot",
                i == 0 ? DefaultScheme(name) : AvailabilityScheme(name, periods[i]));
            env.TestWaitNotification(runtime, txId);
        }

        const auto check = [&] {
            for (ui32 i = 0; i < periods.size(); ++i) {
                const auto desc = DescribePath(runtime, TStringBuilder() << "/MyRoot/Replication" << i);
                const auto& path = desc.GetPathDescription();
                const auto& period = path.GetReplicationDescription().GetConfig().GetAvailabilityPeriod();
                UNIT_ASSERT_VALUES_EQUAL(period.HasUnlimited(), i < 2);
                if (i >= 2) {
                    UNIT_ASSERT(period.HasDuration());
                    UNIT_ASSERT_VALUES_EQUAL(period.GetDuration().seconds(), i == 2 ? 10 : 0);
                    UNIT_ASSERT_VALUES_EQUAL(period.GetDuration().nanos(), 1000000);
                }
                using namespace NKikimr::NReplication;
                const auto edge = runtime.AllocateEdgeActor();
                auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                request->Record.MutablePathId()->SetOwnerId(path.GetSelf().GetSchemeshardId());
                request->Record.MutablePathId()->SetLocalId(path.GetSelf().GetPathId());
                runtime.SendToPipe(ExtractControllerId(path), edge, request.Release());
                const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(edge);
                UNIT_ASSERT(result->Get()->Record.HasAvailabilityPeriod());
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetAvailabilityPeriod().SerializeAsString(), period.SerializeAsString());
            }
        };
        check();
        for (ui32 i = 0; i < periods.size(); ++i) {
            const TString name = TStringBuilder() << "Replication" << i;
            TestAlterReplication(runtime, ++txId, "/MyRoot",
                Sprintf(R"(Name: "%s" State { Paused {} })", name.c_str()));
            env.TestWaitNotification(runtime, txId);
            TestAlterReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "%s"
                Config { SrcConnectionParams { StaticCredentials { Password: "new-password" } } }
            )", name.c_str()));
            env.TestWaitNotification(runtime, txId);
        }
        check();
        // Both tablets must reload the persisted, normalized policy.
        TVector<ui64> controllers;
        for (ui32 i = 0; i < periods.size(); ++i) {
            controllers.push_back(ExtractControllerId(DescribePath(runtime,
                TStringBuilder() << "/MyRoot/Replication" << i).GetPathDescription()));
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        for (const auto id : controllers) {
            RebootTablet(runtime, id, runtime.AllocateEdgeActor());
        }
        check();
    }

    Y_UNIT_TEST_FLAG(AvailabilityPeriodLegacyControllerConfig, CustomDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        if (CustomDefault) {
            runtime.GetAppData().ReplicationConfig.SetConsumerAvailabilityPeriodSeconds(7201);
        }
        const auto seconds = runtime.GetAppData().ReplicationConfig.GetConsumerAvailabilityPeriodSeconds();
        using namespace NKikimr::NReplication;
        auto observer = runtime.AddObserver<TEvController::TEvCreateReplication>([](const auto& ev) {
            ev->Get()->Record.MutableConfig()->ClearAvailabilityPeriod();
        });
        TestCreateReplication(runtime, 101, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, 101);
        observer.Remove();
        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& path = desc.GetPathDescription();
        const auto controller = ExtractControllerId(path);
        const auto check = [&] {
            const auto edge = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            request->Record.MutablePathId()->SetOwnerId(path.GetSelf().GetSchemeshardId());
            request->Record.MutablePathId()->SetLocalId(path.GetSelf().GetPathId());
            runtime.SendToPipe(controller, edge, request.Release());
            const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(edge);
            UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
            const auto& record = result->Get()->Record;
            UNIT_ASSERT(record.HasAvailabilityPeriod());
            UNIT_ASSERT(record.GetAvailabilityPeriod().HasDuration());
            UNIT_ASSERT_VALUES_EQUAL(record.GetAvailabilityPeriod().GetDuration().seconds(), seconds);

            Ydb::Replication::DescribeReplicationResult proto;
            FillReplicationDescription(proto, record);
            const NYdb::NReplication::TReplicationDescription description(proto);
            UNIT_ASSERT(description.GetAvailabilityPeriodKind() ==
                NYdb::NReplication::TReplicationDescription::EAvailabilityPeriodKind::Finite);
            UNIT_ASSERT_VALUES_EQUAL(*description.GetAvailabilityPeriod(), TDuration::Seconds(seconds));
            const auto query = NYdb::NDump::BuildCreateReplicationQuery("/MyRoot", "/MyRoot", "Replication", description);
            UNIT_ASSERT_STRING_CONTAINS(query, TStringBuilder() << "AVAILABILITY_PERIOD = Interval('PT" << seconds << "S')");

            const auto stored = ReadSystemTable(runtime, controller, "Replications", {"Id"}, {"Config"});
            const auto& rows = stored.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            UNIT_ASSERT_VALUES_EQUAL(rows.ListSize(), 1);
            NKikimrReplication::TReplicationConfig config;
            UNIT_ASSERT(config.ParseFromString(rows.GetList(0).GetStruct(0).GetOptional().GetBytes()));
            UNIT_ASSERT(!config.HasAvailabilityPeriod());
        };
        check();
        RebootTablet(runtime, controller, runtime.AllocateEdgeActor());
        check();
    }

    Y_UNIT_TEST(AvailabilityPeriodInvalidCreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        for (const TString period : {"", "Duration {}", "Duration { seconds: -1 }", "Duration { nanos: -1 }",
                "Duration { seconds: 1 nanos: -1 }", "Duration { nanos: 1000000000 }",
                "Duration { seconds: 315576000000 nanos: 999999999 }", "Duration { seconds: 315576000001 }", "Duration { seconds: 9223372036854775807 }"})
        {
            TestCreateReplication(runtime, ++txId, "/MyRoot", AvailabilityScheme("Replication", period),
                {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
        }
        // Failed validation must not reserve the name or leave a partial object.
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AvailabilityPeriodSqlIntervalBoundary) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        const auto lastValid = TDuration::MilliSeconds((NYql::NUdf::MAX_TIMESTAMP - 1) / 1000);
        const auto scheme = [&](const TString& name, TDuration value) {
            return AvailabilityScheme(name, TStringBuilder() << "Duration { seconds: " << value.Seconds()
                << " nanos: " << value.NanoSecondsOfSecond() << " }");
        };

        for (const auto value : {lastValid - TDuration::MicroSeconds(1), lastValid}) {
            const TString name = TStringBuilder() << "Valid" << txId;
            TestCreateReplication(runtime, ++txId, "/MyRoot", scheme(name, value));
            env.TestWaitNotification(runtime, txId);
            const auto desc = DescribePath(runtime, "/MyRoot/" + name);
            const auto& duration = desc.GetPathDescription().GetReplicationDescription()
                .GetConfig().GetAvailabilityPeriod().GetDuration();
            UNIT_ASSERT_VALUES_EQUAL(duration.seconds(), lastValid.Seconds());
            UNIT_ASSERT_VALUES_EQUAL(duration.nanos(), lastValid.NanoSecondsOfSecond());
        }

        for (const auto value : {lastValid + TDuration::MicroSeconds(1),
                TDuration::MicroSeconds(NYql::NUdf::MAX_TIMESTAMP), lastValid + TDuration::Days(1)})
        {
            TestCreateReplication(runtime, ++txId, "/MyRoot", scheme("Replication", value),
                {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AvailabilityPeriodCreateOnly) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State { Paused {} }
        )");
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config { AvailabilityPeriod { Duration { seconds: 10 } } }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);
        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        UNIT_ASSERT(desc.GetPathDescription().GetReplicationDescription().GetConfig().GetAvailabilityPeriod().HasUnlimited());
    }

    Y_UNIT_TEST(CreateWithoutInitialScan) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kPaused),
        });

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State { StandBy {} }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(Disabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableReplication(false));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"),
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateSequential) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        THashSet<ui64> controllerIds;

        for (int i = 0; i < 2; ++i) {
            const auto name = Sprintf("Replication%d", i);

            TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme(name));
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

        for (int i = 0; i < 2; ++i) {
            TVector<TString> names;
            TVector<ui64> txIds;

            for (int j = 0; j < 2; ++j) {
                auto name = Sprintf("Replication%d-%d", i, j);

                TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme(name));

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

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
            controllerId = ExtractControllerId(desc.GetPathDescription());
        }

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
            UNIT_ASSERT_VALUES_UNEQUAL(controllerId, ExtractControllerId(desc.GetPathDescription()));
        }
    }

    Y_UNIT_TEST(CreateWithoutCredentials) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
        UNIT_ASSERT_VALUES_UNEQUAL("root@builtin", params.GetOAuthToken().GetToken());
    }

    Y_UNIT_TEST(ConsistencyLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
              ConsistencySettings {
                Row {}
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication3"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
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
            const auto desc = DescribePath(runtime, "/MyRoot/Replication3");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 10000);
        }
    }

    Y_UNIT_TEST(CommitInterval) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
              ConsistencySettings {
                Global {}
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 10000);
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
              ConsistencySettings {
                Global {
                  CommitIntervalMilliSeconds: 15000
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 15000);
        }
    }

    Y_UNIT_TEST(SecureMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                EnableSsl: false
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              SrcConnectionParams {
                EnableSsl: true
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
            UNIT_ASSERT(params.GetEnableSsl());
            UNIT_ASSERT_VALUES_EQUAL(params.GetCaCert(), "-----BEGIN CERTIFICATE-----");
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              SrcConnectionParams {
                EnableSsl: true
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
            UNIT_ASSERT(params.GetEnableSsl());
            UNIT_ASSERT(!params.HasCaCert());
        }
    }

    Y_UNIT_TEST(Alter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
        }

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State {
              Paused {
              }
            }
        )");

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
          Name: "Replication"
          State {
            StandBy {
            }
          }
        )");

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State {
              Done {
                FailoverMode: FAILOVER_MODE_FORCE
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kDone),
        });
    }

    Y_UNIT_TEST(Describe) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication")); // default with user & password
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
        UNIT_ASSERT(!params.GetStaticCredentials().HasPassword());
    }

    void CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode mode, const TUserAttrs& attrs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: %s
            }
        )", NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode_Name(mode).c_str()));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(mode),
            NLs::UserAttrsEqual(attrs),
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(mode),
            NLs::UserAttrsEqual(attrs),
        });
    }

    Y_UNIT_TEST(CreateReplicatedTable) {
        CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE, {});
        CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY, {{"__async_replica", "true"}});
    }

    Y_UNIT_TEST(CannotAddReplicationConfig) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(CannotSetAsyncReplicaAttribute) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusInvalidParameter}, AlterUserAttrs({{"__async_replica", "true"}}));

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::UserAttrsHas({{"__async_replica", "true"}}),
        });

        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__async_replica", "true"}}, {}));

        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({}, {"__async_replica"}));
    }

    Y_UNIT_TEST(AlterReplicatedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusSchemeError});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusSchemeError});

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "by_value", {"value"},
            Ydb::StatusIds::BAD_REQUEST);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
              ConsistencyLevel: CONSISTENCY_LEVEL_ROW
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusInvalidParameter});

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE),
            NLs::UserAttrsEqual({}),
        });
    }

    Y_UNIT_TEST_FLAG(DropReplicatedTableWithCdc, GlobalConsistency) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // A destination may already have a CDC stream before attachment.
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto consistency = GlobalConsistency
            ? NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
            : NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW;
        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", Sprintf(R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
              ConsistencyLevel: %s
            }
        )", NKikimrSchemeOp::TTableReplicationConfig::EConsistencyLevel_Name(consistency).c_str()))));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY),
        });
        for (const auto* path : {"/MyRoot/Table/Stream", "/MyRoot/Table/Stream/streamImpl"}) {
            TestDescribeResult(DescribePrivatePath(runtime, path), {NLs::PathExist});
        }

        // Standalone CDC removal on an active replica must remain forbidden.
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )", {{NKikimrScheme::StatusSchemeError, "path is an async replica table"}});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathExist});

        // This is the internal table drop issued by replication CASCADE.
        AsyncSend(runtime, TTestTxConfig::SchemeShard,
            InternalTransaction(DropTableRequest(++txId, "/MyRoot", "Table")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        for (const auto* path : {"/MyRoot/Table", "/MyRoot/Table/Stream", "/MyRoot/Table/Stream/streamImpl"}) {
            TestDescribeResult(DescribePrivatePath(runtime, path), {NLs::PathNotExist});
        }
    }

    Y_UNIT_TEST(AlterReplicatedIndexTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(CreateIndexedTableRequest(++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
              ReplicationConfig {
                Mode: REPLICATION_MODE_READ_ONLY
              }
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
              IndexImplTableDescriptions: [ {
                ReplicationConfig {
                  Mode: REPLICATION_MODE_READ_ONLY
                }
              } ]
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY),
        });

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot/Table/Index", R"(
            Name: "indexImplTable"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE),
        });
    }

    Y_UNIT_TEST_FLAG(RebuildIndexOnReplicaPreservesLiveIndex, GlobalConsistency) {
        TReplicationRebuildTest test;
        auto& runtime = test.Runtime;
        const ui64 liveId = test.CheckIndex();
        test.SetMode("/MyRoot", "Table", true, GlobalConsistency);
        test.SetMode(test.IndexPath, "indexImplTable", true, GlobalConsistency);
        test.CheckReplica(true);
        const auto consistency = GlobalConsistency
            ? NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
            : NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW;
        for (const auto& path : {test.Table, test.IndexPath + "/indexImplTable"}) {
            const auto description = DescribePrivatePath(runtime, path);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetTable().GetReplicationConfig().GetConsistencyLevel(),
                consistency);
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        test.CheckReplica(true);

        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            const ui64 rejected = test.Start(true, Ydb::StatusIds::BAD_REQUEST);
            test.CheckNoTemporaryIndex(rejected);
            UNIT_ASSERT_VALUES_EQUAL(test.CheckIndex(), liveId);
            UNIT_ASSERT_VALUES_EQUAL(TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot").EntriesSize(), 0);
        }
        WriteRow(runtime, ++test.TxId, test.Table, 0, 1, "forbidden", false);
        TestAlterTable(runtime, ++test.TxId, "/MyRoot", R"(
            Name: "Table" DropColumns { Name: "payload" }
        )", {NKikimrScheme::StatusSchemeError});
        TestCreateCdcStream(runtime, ++test.TxId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription { Name: "Stream" Mode: ECdcStreamModeKeysOnly Format: ECdcStreamFormatProto }
        )", {NKikimrScheme::StatusSchemeError});
        TestDescribeResult(DescribePrivatePath(runtime, test.Table + "/Stream"), {NLs::PathNotExist});
        UNIT_ASSERT_VALUES_EQUAL(test.CheckIndex(), liveId);

        // Replication detaches each implementation table as well as the base table.
        test.SetMode(test.IndexPath, "indexImplTable", false);
        test.SetMode("/MyRoot", "Table", false);
        test.CheckReplica(false);
        ui64 previousId = liveId;
        for (ui32 iteration = 0; iteration < 2; ++iteration) {
            const ui64 build = test.Start();
            test.Wait(build);
            const ui64 rebuiltId = test.CheckIndex();
            UNIT_ASSERT(rebuiltId != previousId);
            previousId = rebuiltId;
            test.CheckReplica(false);
            test.Forget(build);
        }
    }

    Y_UNIT_TEST(RebuildIndexProtectsReplicationAttachment) {
        TReplicationRebuildTest test;
        auto& runtime = test.Runtime;
        TestCreateCdcStream(runtime, ++test.TxId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
                Name: "ReplicationStream"
                Mode: ECdcStreamModeUpdate
                Format: ECdcStreamFormatJson
                UserAttributes {
                    Key: "__async_replication"
                    Value: "{\"id\":\"replication-id\",\"path\":\"/MyRoot/Replication\"}"
                }
            }
        )");
        test.Env.TestWaitNotification(runtime, test.TxId);
        const auto stream = DescribePrivatePath(runtime, test.Table + "/ReplicationStream");
        const ui64 liveId = test.CheckIndex();
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> filling(runtime);
        Y_DEFER { filling.Stop().Unblock(); };
        const ui64 build = test.Start();
        runtime.WaitFor("index rebuild scan", [&] { return !filling.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", build)
            .GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);

        test.SetMode("/MyRoot", "Table", true, false, NKikimrScheme::StatusMultipleModifications);
        test.SetMode(test.IndexPath, "indexImplTable", true, false, NKikimrScheme::StatusMultipleModifications);
        TestAlterTable(runtime, ++test.TxId, "/MyRoot", R"(
            Name: "Table" Columns { Name: "extra" Type: "Uint64" }
        )", {NKikimrScheme::StatusMultipleModifications});
        test.CheckReplica(false);
        UNIT_ASSERT_VALUES_EQUAL(test.CheckIndex(), liveId);

        // An unrelated replica can be created while this table is rebuilding.
        TestCreateTable(runtime, ++test.TxId, "/MyRoot", R"(
            Name: "OtherReplica"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
            ReplicationConfig { Mode: REPLICATION_MODE_READ_ONLY }
        )");
        test.Env.TestWaitNotification(runtime, test.TxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/OtherReplica"),
            {NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY)});

        filling.Stop().Unblock();
        test.Wait(build);
        UNIT_ASSERT(test.CheckIndex() != liveId);
        test.CheckReplica(false);
        test.Forget(build);
        const auto currentStream = DescribePrivatePath(runtime, test.Table + "/ReplicationStream");
        TestDescribeResult(currentStream, {NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::UserAttrsHas({{"__async_replication", "{\"id\":\"replication-id\",\"path\":\"/MyRoot/Replication\"}"}})});
        UNIT_ASSERT_VALUES_EQUAL(currentStream.GetPathDescription().GetSelf().GetPathId(),
            stream.GetPathDescription().GetSelf().GetPathId());

        // The legacy replication stream still forbids column changes after the rebuild lock is gone.
        TestAlterTable(runtime, ++test.TxId, "/MyRoot", R"(
            Name: "Table" Columns { Name: "extra" Type: "Uint64" }
        )", {NKikimrScheme::StatusPreconditionFailed});
        const ui64 rebuiltId = test.CheckIndex();
        test.SetMode("/MyRoot", "Table", true);
        test.SetMode(test.IndexPath, "indexImplTable", true);
        test.CheckReplica(true);
        const ui64 rejected = test.Start(true, Ydb::StatusIds::BAD_REQUEST);
        test.CheckNoTemporaryIndex(rejected);
        UNIT_ASSERT_VALUES_EQUAL(test.CheckIndex(), rebuiltId);
        test.SetMode(test.IndexPath, "indexImplTable", false);
        test.SetMode("/MyRoot", "Table", false);
        const ui64 retry = test.Start();
        test.Wait(retry);
        UNIT_ASSERT(test.CheckIndex() != rebuiltId);
        test.CheckReplica(false);
        test.Forget(retry);
    }

    Y_UNIT_TEST(CopyReplicatedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/CopyTable");
        const auto& table = desc.GetPathDescription().GetTable();
        UNIT_ASSERT(!table.HasReplicationConfig());
    }

    Y_UNIT_TEST(DropReplicationWithInvalidCredentials) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathExist});

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropReplicationWithUnknownSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  PasswordSecretName: "unknown_secret"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathExist});

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DescribeDuringDropAfterControllerShardDeletion) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
        // The controller shard is the only shard in the domain: the root
        // schemeshard is fresh in this test and assigns local shard ids
        // from 1, so the controller shard has local id 1. (In a tenant
        // schemeshard this would never hold.)
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {NLs::ShardsInsideDomain(1)});

        // Hold the drop's proposal to the coordinator so the drop cannot
        // commit. The controller tablet is deleted before the commit: the
        // drop queues the Hive deletion fire-and-forget, and schemeshard
        // erases the controller shard from ShardInfos upon processing
        // Hive's confirmation while the replication is still describable.
        // This is the race the original crash happened on.
        TBlockEvents<TEvTxProxy::TEvProposeTransaction> dropPropose(runtime);

        const auto dropTxId = ++txId;
        TestDropReplication(runtime, dropTxId, "/MyRoot", "Replication");
        runtime.WaitFor("drop propose", [&] { return !dropPropose.empty(); });

        // TEvNotifyShardDeleted is sent by the transaction that erases the
        // shard from ShardInfos.
        env.TestWaitShardDeletion(runtime, {1});

        // The drop's proposal is withheld, so the drop is not committed
        // yet: this describe crashed the whole schemeshard before the
        // tolerant ShardInfos lookup was introduced.
        const auto described = DescribePath(runtime, "/MyRoot/Replication");
        TestDescribeResult(described, {
            NLs::PathExist,
            NLs::CheckPathState(NKikimrSchemeOp::EPathStateDrop),
        });
        UNIT_ASSERT(!described.GetPathDescription().GetReplicationDescription().HasControllerId());

        dropPropose.Stop().Unblock();
        env.TestWaitNotification(runtime, dropTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

} // TReplicationTests
