#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SetNotNullTest) {
    Y_UNIT_TEST(BasicRequest) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 0u);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 0u);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell(TStringBuf("test_value"))
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }
    }

    Y_UNIT_TEST(BasicRequestThreeDataShards) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 100 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 200 } } } }
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 0u);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 0u);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell(TStringBuf("test_value_1")),
                TCell::Make((ui32)100), TCell(TStringBuf("test_value_100")),
                TCell::Make((ui32)1000), TCell(TStringBuf("test_value_1000"))
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 3, 2), true);
        }

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)2), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }
    }

    Y_UNIT_TEST(ThreeDataShardsSplitBoundariesNullInOnePartitionValidationFails) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 100 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 200 } } } }
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell(TStringBuf("test_value_1")),
                TCell::Make((ui32)101), TCell(),
                TCell::Make((ui32)201), TCell(TStringBuf("test_value_201"))
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 3, 2), true);
        }

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 3u);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)300), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }
    }

    Y_UNIT_TEST(InvalidDatabase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/NonExistentDB",
            "/NonExistentDB/Table",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            response.GetIssues(0).message(),
            "path hasn't been resolved",
            response.ShortDebugString());
    }

    Y_UNIT_TEST(InvalidTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "/MyRoot/NonExistentTable",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            response.GetIssues(0).message(),
            "path hasn't been resolved",
            response.ShortDebugString());
    }

    Y_UNIT_TEST(MissingSettings) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        auto response = TestSetColumnConstraintWithoutSettings(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "skip",
            {"skip"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues(0).message(),
            "Failed item check: Missing settings",
            response.ShortDebugString());
    }

    Y_UNIT_TEST(InvalidColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "/MyRoot/Table",
            {"non_existent_column"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues(0).message(),
            "Failed item check: Column 'non_existent_column' does not exist",
            response.ShortDebugString());

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});
    }

    Y_UNIT_TEST(EmptyColumns) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            tablePath,
            {});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues(0).message(),
            "Failed item check: There are no columns that need to be updated",
            response.ShortDebugString());

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});
    }

    Y_UNIT_TEST(DuplicateColumns) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            tablePath,
            {"value", "value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues(0).message(),
            "Duplicate column name `value` in not null columns.",
            response.ShortDebugString());

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});
    }

    Y_UNIT_TEST(IndexPathRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";
        TString indexPath = tablePath + "/ByValue";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(
            runtime,
            ++txId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            "ByValue",
            {"value"});
        env.TestWaitNotification(runtime, txId);

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            root,
            indexPath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::BAD_REQUEST,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetIssues().size(),
            1,
            response.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            response.GetIssues(0).message(),
            "path is not a table",
            response.ShortDebugString());

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});
    }

    Y_UNIT_TEST(NullDataValidationFails) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)2), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }
    }

    Y_UNIT_TEST(NullWriteIsBlockedDuringValidation) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayedValidateRequest;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!delayedValidateRequest && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                delayedValidateRequest.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedValidateRequest](IEventHandle&) -> bool {
                return bool(delayedValidateRequest);
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_C(delayedValidateRequest, "Failed to intercept first TEvValidateRowConditionRequest");

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayedValidateRequest.Release(), 0, true);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(DatashardRebootLostMessage) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 datashardTabletId = 0;
        bool firstRequestDropped = false;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!firstRequestDropped &&
                ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType)
            {
                datashardTabletId = ev->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record.GetTabletId();
                firstRequestDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        runtime.WaitFor("first validate request", [&] {
            return firstRequestDropped;
        });

        UNIT_ASSERT_C(datashardTabletId != 0, "Failed to capture DataShard tabletId");

        RebootTablet(runtime, datashardTabletId, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }
    }

    Y_UNIT_TEST(AlreadyNotNull) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   NotNull: true }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell(TStringBuf("test_value"))
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)2), TCell(TStringBuf("test_value"))
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }

        {
            TVector<TCell> cells = {
                TCell::Make((ui32)2), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        }
    }

    Y_UNIT_TEST(DuplicateTxId) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto firstResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "FIRST SET COLUMN CONSTRAINT RESPONSE: " << firstResponse.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            firstResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            firstResponse.ShortDebugString());

        auto secondResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SECOND SET COLUMN CONSTRAINT RESPONSE: " << secondResponse.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            secondResponse.GetStatus(),
            Ydb::StatusIds::ALREADY_EXISTS,
            secondResponse.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            secondResponse.GetIssues().size(),
            1,
            secondResponse.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            secondResponse.GetIssues(0).message(),
            TStringBuilder() << "SetColumnConstraint operation with id '" << setConstraintTxId << "' already exists",
            secondResponse.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(NullDataValidationFailsPreservesOtherColumns) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"          Type: "Uint32" }
              Columns { Name: "notnull_col"  Type: "Utf8"   NotNull: true }
              Columns { Name: "nullable_col" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCheckColumnsNotNull(runtime, tablePath, {
            {"notnull_col",  true},
            {"nullable_col", false},
        });

        {
            TString notnullVal = "hello";
            TVector<TCell> cells = {
                TCell::Make((ui32)1),
                TCell(notnullVal.data(), notnullVal.size()),
                TCell() // NULL
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2, 3}, TSerializedCellMatrix(cells, 1, 3), true);
        }

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"notnull_col", "nullable_col"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {
            {"notnull_col",  true},
            {"nullable_col", false},
        });
    }

    Y_UNIT_TEST_TWIN(BulkUpsert, ExplicitNullValue) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayedValidateRequest;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!delayedValidateRequest && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                delayedValidateRequest.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedValidateRequest](IEventHandle&) -> bool {
                return bool(delayedValidateRequest);
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_C(delayedValidateRequest, "Failed to intercept first TEvValidateRowConditionRequest");

        auto tryToUpsert = [&](){
            using TEvBulkUpsertRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
                Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;

            Ydb::Table::BulkUpsertRequest request;
            request.set_table(tablePath);
            auto* r = request.mutable_rows();

            auto* reqRowType = r->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
            auto* reqKeyType = reqRowType->add_members();
            reqKeyType->set_name("key");
            reqKeyType->mutable_type()->set_type_id(Ydb::Type::UINT32);

            if (ExplicitNullValue) {
                auto* reqValueType = reqRowType->add_members();
                reqValueType->set_name("value");
                reqValueType->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
            }

            auto* reqRows = r->mutable_value();
            auto* row1 = reqRows->add_items();
            row1->add_items()->set_uint32_value(1);

            if (ExplicitNullValue) {
                row1->add_items()->set_null_flag_value(google::protobuf::NULL_VALUE);
            }

            auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
                std::move(request), root, /* token */ "", runtime.GetActorSystem(0));

            while (!future.HasValue() && !future.HasException()) {
                runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            }

            auto expectedStatus = Ydb::StatusIds::BAD_REQUEST;
            if (!ExplicitNullValue) {
                // A scheme_error is correct in this context, as the failure relates to an invalid schema
                // provided during bulk_upsert initialization,
                // rather than a direct attempt to insert NULL into a NOT NULL column.
                expectedStatus = Ydb::StatusIds::SCHEME_ERROR;
            }

            auto bulkUpsertResponse = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                bulkUpsertResponse.operation().status(),
                expectedStatus,
                "BulkUpsert should be rejected, got: "
                    << Ydb::StatusIds::StatusCode_Name(bulkUpsertResponse.operation().status()));
        };

        tryToUpsert();

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayedValidateRequest.Release(), 0, true);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
        tryToUpsert();
    }

    Y_UNIT_TEST(DirectModifySchemeSetNotNullRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "value" NotNull: true }
        )", {{NKikimrScheme::StatusInvalidParameter,
              "Cannot set NotNull to true on column 'value' in a ModifyScheme request — Internal flag is not set. "
              "To override, set Internal = true. This is dangerous: only do this if you know what you are doing"}});
    }

    Y_UNIT_TEST(DirectModifySchemeSetNotNullInProgressRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "value" SetNotNullInProgress: true }
        )", {{NKikimrScheme::StatusInvalidParameter,
              "Cannot set NotNullInProgress on column 'value' in a ModifyScheme request — Internal flag is not set. "
              "To override, set Internal = true. This is dangerous: only do this if you know what you are doing"}});
    }

    Y_UNIT_TEST(DirectModifySchemeSetNotNullOnSerialColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
              TableDescription {
                  Name: "Table"
                  Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
              }
              SequenceDescription {
                  Name: "myseq"
              }
        )");
        env.TestWaitNotification(runtime, txId);

        AsyncSend(runtime, TTestTxConfig::SchemeShard,
            InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
                  Name: "Table"
                  Columns { Name: "key" NotNull: true }
            )")));
        TestModificationResults(runtime, txId,
            {{NKikimrScheme::StatusInvalidParameter, "Cannot alter serial column 'key'"}});
    }

    Y_UNIT_TEST(DirectModifySchemeSetNotNullInProgressOnSerialColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
              TableDescription {
                  Name: "Table"
                  Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
              }
              SequenceDescription {
                  Name: "myseq"
              }
        )");
        env.TestWaitNotification(runtime, txId);

        AsyncSend(runtime, TTestTxConfig::SchemeShard,
            InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
                  Name: "Table"
                  Columns { Name: "key" SetNotNullInProgress: true }
            )")));
        TestModificationResults(runtime, txId,
            {{NKikimrScheme::StatusInvalidParameter, "Cannot alter serial column 'key'"}});
    }

    NKikimrSetColumnConstraint::TSetColumnConstraint DoGetRequest(
        ui64 setConstraintTxId,
        TTestActorRuntime& runtime,
        const TString& root,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS)
    {
        auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
            new TEvSetColumnConstraint::TEvGetRequest(root, setConstraintTxId));

        TAutoPtr<IEventHandle> handle;
        auto* event = runtime.GrabEdgeEvent<TEvSetColumnConstraint::TEvGetResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL_C(
            event->Record.GetStatus(),
            status,
            event->Record.ShortDebugString());

        return event->Record.GetSetColumnConstraint();
    }

    Y_UNIT_TEST_TWIN(GetRequestSimple, isShouldBeFailed) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        if (isShouldBeFailed) {
            TVector<TCell> cells = {
                TCell::Make((ui32)1), TCell()
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
        }

        ui64 setConstraintTxId = ++txId;

        using TConstraintState = Ydb::Table::SetNotNullState_State;
        std::vector<TConstraintState> answers;
        std::vector<TConstraintState> expectedAnswers = {
            Ydb::Table::SetNotNullState::STATE_PREPARING,
            Ydb::Table::SetNotNullState::STATE_PREPARING,
            Ydb::Table::SetNotNullState::STATE_VALIDATING,
            (isShouldBeFailed ? Ydb::Table::SetNotNullState::STATE_REJECTED : Ydb::Table::SetNotNullState::STATE_APPLYING),
            (isShouldBeFailed ? Ydb::Table::SetNotNullState::STATE_REJECTED : Ydb::Table::SetNotNullState::STATE_APPLYING),
            (isShouldBeFailed ? Ydb::Table::SetNotNullState::STATE_REJECTED : Ydb::Table::SetNotNullState::STATE_DONE)
        };

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            // 1 [EOperationState::Locking]           => TEvModifySchemeTransaction, STATE_PREPARING
            // 2 [EOperationState::LockingNullWrites] => TEvModifySchemeTransaction, STATE_PREPARING
            // 3 [EOperationState::Validating]        => TEvValidateRowConditionRequest, STATE_VALIDATING
            // 4 [EOperationState::Finishing]         => TEvModifySchemeTransaction, STATE_APPLYING
            // 5 [EOperationState::Unlocking]         => TEvModifySchemeTransaction, STATE_APPLYING

            if (ev->GetTypeRewrite() == TEvSchemeShard::TEvModifySchemeTransaction::EventType ||
                ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                answers.push_back(DoGetRequest(setConstraintTxId, runtime, root).GetState());
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        // STATE_DONE/STATE_REJECTED
        answers.push_back(DoGetRequest(setConstraintTxId, runtime, root).GetState());

        UNIT_ASSERT_VALUES_EQUAL_C(
            answers.size(),
            expectedAnswers.size(),
            TStringBuilder() << "Wrong number of observed states: got " << answers.size()
                << ", expected " << expectedAnswers.size());

        for (size_t i = 0; i < expectedAnswers.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                static_cast<int>(answers[i]),
                static_cast<int>(expectedAnswers[i]),
                TStringBuilder() << "State mismatch at index " << i
                    << ": got " << static_cast<int>(answers[i])
                    << " (" << Ydb::Table::SetNotNullState_State_Name(answers[i]) << ")"
                    << ", expected " << static_cast<int>(expectedAnswers[i])
                    << " (" << Ydb::Table::SetNotNullState_State_Name(expectedAnswers[i]) << ")"
            );
        }
    }

    Y_UNIT_TEST(GetRequestCheckProgress) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        const ui32 shardsCount = 7;
        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 100 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 200 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 400 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 500 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 600 } } } }
              SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 700 } } } }
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;

        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), Ydb::StatusIds::SUCCESS);

        // Block all TEvValidateRowConditionResponse events so we can release them one by one
        TBlockEvents<TEvDataShard::TEvValidateRowConditionResponse> responseBlocker(runtime);

        // Wait until all shards have responded (all responses are blocked)
        runtime.WaitFor("all validation responses", [&] {
            return responseBlocker.size() >= shardsCount;
        });

        std::vector<float> answers;
        // Release responses one by one and check progress after each
        for (ui32 i = 0; i < shardsCount; ++i) {
            responseBlocker.Unblock(1);
            answers.push_back(DoGetRequest(setConstraintTxId, runtime, root).GetProgress());
        }

        responseBlocker.Stop();
        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        UNIT_ASSERT_VALUES_EQUAL_C(
            answers.size(),
            shardsCount,
            TStringBuilder() << "Wrong number of progress readings: got " << answers.size()
                << ", expected " << shardsCount);

        for (ui32 i = 0; i < shardsCount; ++i) {
            float expected = static_cast<float>(i + 1) / static_cast<float>(shardsCount) * 100.0f;
            UNIT_ASSERT_DOUBLES_EQUAL_C(
                answers[i],
                expected,
                0.01,
                TStringBuilder() << "Progress mismatch at index " << i
                    << ": got " << answers[i]
                    << ", expected " << expected
            );
        }
    }

    Y_UNIT_TEST(GetRequestNotFound) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TString root = "/MyRoot";

        TTestEnv env(runtime);

        ui64 txId = 100;
        ui64 setConstraintTxId = ++txId;

        DoGetRequest(setConstraintTxId, runtime, root, Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(GetRequestBadRequest) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TString wrongRoot = "/MyWrongRoot";

        TTestEnv env(runtime);

        ui64 txId = 100;
        ui64 setConstraintTxId = ++txId;

        DoGetRequest(setConstraintTxId, runtime, wrongRoot, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(GetRequestUserSidBeginEndTime) {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>&) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;

        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"},
            "someuser@some_suffix");
        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);


        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        auto getResponse = DoGetRequest(setConstraintTxId, runtime, root);

        UNIT_ASSERT(getResponse.HasUserSID() && getResponse.GetUserSID() == "someuser@some_suffix");
        UNIT_ASSERT(getResponse.HasStartTime());
        UNIT_ASSERT(getResponse.HasEndTime());
        UNIT_ASSERT(getResponse.GetStartTime().seconds() > 0);
        UNIT_ASSERT(getResponse.GetEndTime().seconds() > getResponse.GetStartTime().seconds());
    }

    Y_UNIT_TEST(ListOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath1 = root + "/Table1";
        TString tablePath2 = root + "/Table2";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table1"
              Columns { Name: "key"    Type: "Uint32" }
              Columns { Name: "value"  Type: "Utf8"   }
              Columns { Name: "value2" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TString notnullVal = "hello";
            TVector<TCell> cells = {
                TCell::Make((ui32)1),
                TCell(notnullVal.data(), notnullVal.size()),
                TCell() // NULL
            };

            WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath1,
                0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {1, 2, 3}, TSerializedCellMatrix(cells, 1, 3), true);
        }

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId1 = ++txId;
        auto response1 = TestSetColumnConstraint(
            runtime, setConstraintTxId1,
            TTestTxConfig::SchemeShard,
            root,
            tablePath1,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(response1.GetStatus(), Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, setConstraintTxId1, TTestTxConfig::SchemeShard);

        ui64 setConstraintTxId2 = ++txId;
        auto response2 = TestSetColumnConstraint(
            runtime, setConstraintTxId2,
            TTestTxConfig::SchemeShard,
            root,
            tablePath1,
            {"value2"});

        UNIT_ASSERT_VALUES_EQUAL(response2.GetStatus(), Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, setConstraintTxId2, TTestTxConfig::SchemeShard);

        ui64 setConstraintTxId3 = ++txId;
        auto response3 = TestSetColumnConstraint(
            runtime, setConstraintTxId3,
            TTestTxConfig::SchemeShard,
            root,
            tablePath2,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(response3.GetStatus(), Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, setConstraintTxId3, TTestTxConfig::SchemeShard);

        // =========================================

        auto listResponse = TestListSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, root);

        UNIT_ASSERT_VALUES_EQUAL(listResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GE(listResponse.GetEntries().size(), 3);

        bool foundOp1 = false, foundOp2 = false, foundOp3 = false;
        for (const auto& entry : listResponse.GetEntries()) {
            const auto& columns = entry.GetSettings().GetNotNullColumns();
            if (entry.GetId() == setConstraintTxId1) {
                foundOp1 = true;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(entry.GetState()), static_cast<int>(Ydb::Table::SetNotNullState::STATE_DONE));
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0), "value");
            }
            if (entry.GetId() == setConstraintTxId2) {
                foundOp2 = true;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(entry.GetState()), static_cast<int>(Ydb::Table::SetNotNullState::STATE_REJECTED));
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0), "value2");
            }
            if (entry.GetId() == setConstraintTxId3) {
                foundOp3 = true;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(entry.GetState()), static_cast<int>(Ydb::Table::SetNotNullState::STATE_DONE));
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0), "value");
            }
        }

        UNIT_ASSERT(foundOp1);
        UNIT_ASSERT(foundOp2);
        UNIT_ASSERT(foundOp3);
    }

    Y_UNIT_TEST(ListEmptyOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TString root = "/MyRoot";

        auto listResponse = TestListSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, root);

        UNIT_ASSERT_VALUES_EQUAL(listResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(listResponse.GetEntries().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(listResponse.GetNextPageToken(), "0");
    }

    Y_UNIT_TEST(ListOperationsWithPagination) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";

        // =============================

        TVector<TString> tablePaths;
        TVector<ui64> operationIds;

        for (ui32 i = 1; i <= 5; ++i) {
            TString tablePath = root + "/Table" + ToString(i);
            tablePaths.push_back(tablePath);

            TestCreateTable(runtime, ++txId, root, Sprintf(R"(
                  Name: "Table%d"
                  Columns { Name: "key"   Type: "Uint32" }
                  Columns { Name: "value" Type: "Utf8"   }
                  KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);

            ui64 setConstraintTxId = ++txId;
            auto response = TestSetColumnConstraint(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value"});

            UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), Ydb::StatusIds::SUCCESS);
            operationIds.push_back(setConstraintTxId);
            env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);
        }

        // =============================

        TVector<ui64> foundOperationIds;
        TString pageToken = "";
        ui32 pageCount = 0;
        const ui64 pageSize = 1;

        do {
            auto listResponse = TestListSetColumnConstraint(
                runtime,
                TTestTxConfig::SchemeShard,
                root,
                pageSize,
                pageToken);

            UNIT_ASSERT_VALUES_EQUAL(listResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
            
            UNIT_ASSERT_LE(static_cast<ui64>(listResponse.GetEntries().size()), pageSize);

            for (const auto& entry : listResponse.GetEntries()) {
                foundOperationIds.push_back(entry.GetId());
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().Get(0), "value");
            }

            pageToken = listResponse.GetNextPageToken();
            ++pageCount;
        } while (pageToken != "0" && !pageToken.empty());

        UNIT_ASSERT_GE(foundOperationIds.size(), 5);

        // =============================

        for (ui64 expectedId : operationIds) {
            bool found = false;
            for (ui64 foundId : foundOperationIds) {
                if (foundId == expectedId) {
                    found = true;
                    break;
                }
            }
            UNIT_ASSERT_C(found, "Operation ID " << expectedId << " not found in paginated results");
        }

        Cerr << "Pagination test completed successfully with " << pageCount << " pages" << Endl;
    }

    Y_UNIT_TEST(ListAfterReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        // =================================

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // =================================

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        // =================================

        auto listResponseBeforeReboot = TestListSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, root);

        UNIT_ASSERT_VALUES_EQUAL(listResponseBeforeReboot.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GE(listResponseBeforeReboot.GetEntries().size(), 1);

        bool foundBeforeReboot = false;
        for (const auto& entry : listResponseBeforeReboot.GetEntries()) {
            if (entry.GetId() == setConstraintTxId) {
                foundBeforeReboot = true;
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(entry.GetState()),
                    static_cast<int>(Ydb::Table::SetNotNullState::STATE_DONE));
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().Get(0), "value");
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetTablePath(), tablePath);
                break;
            }
        }
        UNIT_ASSERT_C(foundBeforeReboot, "Operation not found before reboot");

        Cerr << "List operation verified successfully before reboot" << Endl;

        // =================================

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        Cerr << "SchemeShard rebooted successfully" << Endl;

        // =================================
        auto listResponseAfterReboot = TestListSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, root);

        UNIT_ASSERT_VALUES_EQUAL(listResponseAfterReboot.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GE(listResponseAfterReboot.GetEntries().size(), 1);

        bool foundAfterReboot = false;
        for (const auto& entry : listResponseAfterReboot.GetEntries()) {
            if (entry.GetId() == setConstraintTxId) {
                foundAfterReboot = true;
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(entry.GetState()),
                    static_cast<int>(Ydb::Table::SetNotNullState::STATE_DONE));
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetNotNullColumns().Get(0), "value");
                UNIT_ASSERT_VALUES_EQUAL(entry.GetSettings().GetTablePath(), tablePath);
                break;
            }
        }
        UNIT_ASSERT_C(foundAfterReboot, "Operation not found after reboot - persistence failed!");

        Cerr << "List operation verified successfully after reboot - persistence works!" << Endl;

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(ForgetNonExistentOperation) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, root, 999999, Ydb::StatusIds::NOT_FOUND);

        Cerr << "FORGET RESPONSE: " << forgetResponse.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse.GetStatus(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT(forgetResponse.IssuesSize() > 0);
    }

    Y_UNIT_TEST(ForgetOperationInProgress) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayedValidateRequest;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!delayedValidateRequest && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                delayedValidateRequest.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        ui64 operationId = setConstraintTxId;

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedValidateRequest](IEventHandle&) -> bool {
                return bool(delayedValidateRequest);
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_C(delayedValidateRequest, "Failed to intercept TEvValidateRowConditionRequest");

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, root, operationId, Ydb::StatusIds::PRECONDITION_FAILED);

        Cerr << "FORGET IN PROGRESS RESPONSE: " << forgetResponse.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT(forgetResponse.IssuesSize() > 0);

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayedValidateRequest.Release(), 0, true);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);
    }

    Y_UNIT_TEST(ForgetCompletedOperation) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        ui64 operationId = setConstraintTxId;

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        // check that operation exists
        DoGetRequest(operationId, runtime, root, Ydb::StatusIds::SUCCESS);

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, root, operationId, Ydb::StatusIds::SUCCESS);

        Cerr << "FORGET COMPLETED RESPONSE: " << forgetResponse.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse.GetStatus(), Ydb::StatusIds::SUCCESS);

        DoGetRequest(operationId, runtime, root, Ydb::StatusIds::NOT_FOUND);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(ForgetOperationPersistsAfterReboot) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        ui64 operationId = setConstraintTxId;

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, root, operationId, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse.GetStatus(), Ydb::StatusIds::SUCCESS);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        DoGetRequest(operationId, runtime, root, Ydb::StatusIds::NOT_FOUND);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(ForgetOperationTwice) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        ui64 operationId = setConstraintTxId;

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        auto forgetResponse1 = TestForgetSetColumnConstraint(
            runtime, ++txId, root, operationId, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse1.GetStatus(), Ydb::StatusIds::SUCCESS);

        auto forgetResponse2 = TestForgetSetColumnConstraint(
            runtime, ++txId, root, operationId, Ydb::StatusIds::NOT_FOUND);

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse2.GetStatus(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT(forgetResponse2.IssuesSize() > 0);
    }

    Y_UNIT_TEST(ForgetOperationWrongDatabase) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 setConstraintTxId = ++txId;
        auto createResponse = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL(createResponse.GetStatus(), Ydb::StatusIds::SUCCESS);
        ui64 operationId = setConstraintTxId;

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, "/WrongRoot", operationId, Ydb::StatusIds::NOT_FOUND);

        UNIT_ASSERT_VALUES_EQUAL(forgetResponse.GetStatus(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT(forgetResponse.IssuesSize() > 0);
    }

    Y_UNIT_TEST(CancelGetListForgetIntegration) {
        // ========================================
        // Preparing

        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // ========================================
        // Create operation and cancel it

        TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest> blocker(runtime);

        ui64 setConstraintTxId = ++txId;
        AsyncSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        runtime.WaitFor("block validation", [&]{ return blocker.size() > 0; });

        blocker.Stop();

        auto cancelResponse = TestCancelSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            cancelResponse.ShortDebugString());

        blocker.Unblock();

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        // ========================================
        // Check schema

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        // ========================================
        // List

        auto listResponse = TestListSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, root);

        UNIT_ASSERT_VALUES_EQUAL_C(
            listResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            listResponse.ShortDebugString());
        UNIT_ASSERT_C(
            listResponse.GetEntries().size() >= 1,
            "Expected at least one operation in list");

        bool foundOperation = false;
        for (const auto& entry : listResponse.GetEntries()) {
            if (entry.GetId() == setConstraintTxId) {
                foundOperation = true;

                UNIT_ASSERT_VALUES_EQUAL_C(
                    entry.GetId(),
                    setConstraintTxId,
                    "TxId mismatch in list response");

                UNIT_ASSERT_VALUES_EQUAL_C(
                    static_cast<int>(entry.GetState()),
                    static_cast<int>(Ydb::Table::SetNotNullState::STATE_CANCELLED),
                    TStringBuilder() << "Expected STATE_CANCELLED, got: " << Ydb::Table::SetNotNullState_State_Name(entry.GetState()));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    entry.GetSettings().GetNotNullColumns().size(),
                    1,
                    "Expected one column in settings");
                UNIT_ASSERT_VALUES_EQUAL_C(
                    entry.GetSettings().GetNotNullColumns().Get(0),
                    "value",
                    "Column name mismatch");

                break;
            }
        }

        UNIT_ASSERT_C(foundOperation, "Operation not found in list response");

        // ========================================
        // Get

        // Get full response to check issues
        auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
            new TEvSetColumnConstraint::TEvGetRequest(root, setConstraintTxId));

        TAutoPtr<IEventHandle> handle;
        auto* getEvent = runtime.GrabEdgeEvent<TEvSetColumnConstraint::TEvGetResponse>(handle);
        UNIT_ASSERT(getEvent);
        
        UNIT_ASSERT_VALUES_EQUAL_C(
            getEvent->Record.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            getEvent->Record.ShortDebugString());

        const auto& getResponse = getEvent->Record.GetSetColumnConstraint();

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(getResponse.GetState()),
            static_cast<int>(Ydb::Table::SetNotNullState::STATE_CANCELLED),
            TStringBuilder() << "Expected STATE_CANCELLED in Get, got: " << Ydb::Table::SetNotNullState_State_Name(getResponse.GetState()));

        UNIT_ASSERT_C(
            getEvent->Record.IssuesSize() > 0,
            "Expected issues with error message in cancelled operation");
        UNIT_ASSERT_STRING_CONTAINS(
            getEvent->Record.GetIssues(0).message(),
            "Cancelled by user request");
        
        UNIT_ASSERT_STRING_CONTAINS_C(
            getEvent->Record.GetIssues(0).message(),
            "Cancelled",
            "Error message should mention cancellation");

        UNIT_ASSERT_C(
            getResponse.HasSettings(),
            "Expected settings in Get response");
        UNIT_ASSERT_VALUES_EQUAL_C(
            getResponse.GetSettings().GetNotNullColumns().size(),
            1,
            "Expected one column in Get settings");
        UNIT_ASSERT_VALUES_EQUAL_C(
            getResponse.GetSettings().GetNotNullColumns().Get(0),
            "value",
            "Column name mismatch in Get");

        // ========================================
        // Forget

        auto forgetResponse = TestForgetSetColumnConstraint(
            runtime, ++txId, root, setConstraintTxId, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL_C(
            forgetResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            forgetResponse.ShortDebugString());

        DoGetRequest(setConstraintTxId, runtime, root, Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(CancelNonExistentOperation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TString root = "/MyRoot";

        auto cancelResponse = TestCancelSetColumnConstraint(
            runtime, TTestTxConfig::SchemeShard, 0, root, 999999);

        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse.GetStatus(),
            Ydb::StatusIds::NOT_FOUND,
            cancelResponse.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse.GetIssues().size(),
            1,
            cancelResponse.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            cancelResponse.GetIssues(0).message(),
            "not found",
            cancelResponse.ShortDebugString());
    }

    Y_UNIT_TEST(CancelOperationInNonExistentDatabase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TString wrongRoot = "/NonExistentDB";

        auto cancelResponse = TestCancelSetColumnConstraint(
            runtime, TTestTxConfig::SchemeShard, 0, wrongRoot, 100);

        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse.GetStatus(),
            Ydb::StatusIds::NOT_FOUND,
            cancelResponse.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse.GetIssues().size(),
            1,
            cancelResponse.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            cancelResponse.GetIssues(0).message(),
            "Database",
            cancelResponse.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            cancelResponse.GetIssues(0).message(),
            "not found",
            cancelResponse.ShortDebugString());
    }

    Y_UNIT_TEST(CancelAtFinishingUnlockingOrDoneStages) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";

        struct TStageTest {
            TString StageName;
            std::function<THolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(TTestActorRuntime&)> CreateBlocker;
        };

        TVector<TStageTest> stages = {
            {
                "Finishing",
                [](TTestActorRuntime& runtime) {
                    return MakeHolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(runtime, [](const auto& ev) {
                        if (ev->Get()->Record.TransactionSize() > 0) {
                            const auto& tx = ev->Get()->Record.GetTransaction(0);
                            if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable &&
                                tx.HasAlterTable() && tx.GetAlterTable().ColumnsSize() > 0) {
                                for (const auto& col : tx.GetAlterTable().GetColumns()) {
                                    if (col.HasSetNotNullInProgress() && !col.GetSetNotNullInProgress()) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    });
                }
            },
            {
                "Unlocking",
                [](TTestActorRuntime& runtime) {
                    return MakeHolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(runtime, [](const auto& ev) {
                        if (ev->Get()->Record.TransactionSize() > 0) {
                            const auto& tx = ev->Get()->Record.GetTransaction(0);
                            return tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock;
                        }
                        return false;
                    });
                }
            },
            {
                "Done",
                nullptr  // No blocker needed, operation will complete fully
            }
        };

        for (size_t i = 0; i < stages.size(); ++i) {
            const auto& stage = stages[i];
            Cerr << "=== Testing cancel rejection at stage: " << stage.StageName << " ===" << Endl;

            // Create a new table for this stage
            TString tableName = TStringBuilder() << "Table_" << stage.StageName;
            TString tablePath = root + "/" + tableName;

            TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
                  Name: ")" << tableName << R"("
                  Columns { Name: "key"   Type: "Uint32" }
                  Columns { Name: "value" Type: "Utf8"   }
                  KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            THolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>> blocker;
            if (stage.CreateBlocker) {
                blocker = stage.CreateBlocker(runtime);
            }

            ui64 setConstraintTxId = ++txId;
            AsyncSetColumnConstraint(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value"});

            if (blocker) {
                runtime.WaitFor("block event at " + stage.StageName, [&]{ return blocker->size() > 0; });

                // Try to cancel while at this stage - should be rejected
                auto cancelResponse = TestCancelSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    cancelResponse.GetStatus(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cancel should be rejected at stage: " + stage.StageName);
                UNIT_ASSERT_C(
                    cancelResponse.GetIssues().size() > 0,
                    "Expected error message for cancel at " + stage.StageName);
                UNIT_ASSERT_STRING_CONTAINS_C(
                    cancelResponse.GetIssues(0).message(),
                    "finished",
                    cancelResponse.ShortDebugString());

                blocker->Stop().Unblock();
            } else {
                env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

                auto cancelResponse = TestCancelSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    cancelResponse.GetStatus(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cancel should be rejected at stage: " + stage.StageName);
                UNIT_ASSERT_C(
                    cancelResponse.GetIssues().size() > 0,
                    "Expected error message for cancel at " + stage.StageName);
                UNIT_ASSERT_STRING_CONTAINS_C(
                    cancelResponse.GetIssues(0).message(),
                    "finished",
                    cancelResponse.ShortDebugString());
            }

            if (blocker) {
                env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);
            }

            TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

            Cerr << "=== Cancel correctly rejected at stage: " << stage.StageName << " ===" << Endl;
        }
    }

    Y_UNIT_TEST(CancelAlreadyCanceledOperation) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;
        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayedValidateRequest;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!delayedValidateRequest && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                delayedValidateRequest.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedValidateRequest](IEventHandle&) -> bool {
                return bool(delayedValidateRequest);
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_C(delayedValidateRequest, "Failed to intercept TEvValidateRowConditionRequest");

        auto cancelResponse1 = TestCancelSetColumnConstraint(
            runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);

        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse1.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            cancelResponse1.ShortDebugString());

        auto cancelResponse2 = TestCancelSetColumnConstraint(
            runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);

        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse2.GetStatus(),
            Ydb::StatusIds::PRECONDITION_FAILED,
            cancelResponse2.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            cancelResponse2.GetIssues().size(),
            1,
            cancelResponse2.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            cancelResponse2.GetIssues(0).message(),
            "already cancelled",
            cancelResponse2.ShortDebugString());

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayedValidateRequest.Release(), 0, true);

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});
    }

    Y_UNIT_TEST(CancelAtLockingStage) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        struct TStageTest {
            TString StageName;
            std::function<THolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(TTestActorRuntime&)> CreateModifySchemeBlocker;
            std::function<THolder<TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest>>(TTestActorRuntime&)> CreateDataShardBlocker;
        };

        TVector<TStageTest> stages = {
            {
                "Locking",
                [](TTestActorRuntime& runtime) {
                    return MakeHolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(runtime, [](const auto& ev) {
                        if (ev->Get()->Record.TransactionSize() > 0) {
                            const auto& tx = ev->Get()->Record.GetTransaction(0);
                            return tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock;
                        }
                        return false;
                    });
                },
                nullptr
            },
            {
                "LockingNullWrites",
                [](TTestActorRuntime& runtime) {
                    return MakeHolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>>(runtime, [](const auto& ev) {
                        if (ev->Get()->Record.TransactionSize() > 0) {
                            const auto& tx = ev->Get()->Record.GetTransaction(0);
                            if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable &&
                                tx.HasAlterTable() && tx.GetAlterTable().ColumnsSize() > 0) {
                                for (const auto& col : tx.GetAlterTable().GetColumns()) {
                                    if (col.HasSetNotNullInProgress() && col.GetSetNotNullInProgress()) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    });
                },
                nullptr
            },
            {
                "Validating",
                nullptr,
                [](TTestActorRuntime& runtime) {
                    return MakeHolder<TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest>>(runtime);
                }
            }
        };

        for (const auto& stage : stages) {
            Cerr << "=== Testing cancel at stage: " << stage.StageName << " ===" << Endl;

            THolder<TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>> modifySchemeBlocker;
            THolder<TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest>> dataShardBlocker;

            if (stage.CreateModifySchemeBlocker) {
                modifySchemeBlocker = stage.CreateModifySchemeBlocker(runtime);
            }
            if (stage.CreateDataShardBlocker) {
                dataShardBlocker = stage.CreateDataShardBlocker(runtime);
            }

            ui64 setConstraintTxId = ++txId;
            AsyncSetColumnConstraint(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value"});

            if (modifySchemeBlocker) {
                runtime.WaitFor("block event at " + stage.StageName, [&]{ return modifySchemeBlocker->size() > 0; });
                modifySchemeBlocker->Stop();
            }
            if (dataShardBlocker) {
                runtime.WaitFor("block event at " + stage.StageName, [&]{ return dataShardBlocker->size() > 0; });
                dataShardBlocker->Stop();
            }

            auto cancelResponse = TestCancelSetColumnConstraint(runtime, TTestTxConfig::SchemeShard, 0, root, setConstraintTxId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                cancelResponse.GetStatus(),
                Ydb::StatusIds::SUCCESS,
                "Failed to cancel at stage: " + stage.StageName);

            if (modifySchemeBlocker) {
                modifySchemeBlocker->Unblock();
            }
            if (dataShardBlocker) {
                dataShardBlocker->Unblock();
            }

            env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

            TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

            Cerr << "=== Successfully cancelled at stage: " << stage.StageName << " ===" << Endl;
        }
    }

    Y_UNIT_TEST(MoveTableFailsWhileValidating) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;
        TString movedTablePath = root + "/TableMoved";

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Block validation requests to datashards so that the operation is stuck
        // in the Validating stage (the table lock is already held at this point).
        TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest> validateBlocker(runtime);

        ui64 setConstraintTxId = ++txId;
        AsyncSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        runtime.WaitFor("block validate request", [&]{ return validateBlocker.size() > 0; });

        // While SetNotNull is stuck validating shards (table is locked), attempt to move
        // the table. MoveTable must be rejected immediately at Propose (no TxState is even
        // created for it), because CheckLocks() sees the path is locked by the SetNotNull
        // operation and MoveTable's transaction carries no matching LockGuard.
        ui64 moveTxId = ++txId;
        TestMoveTable(runtime, moveTxId, tablePath, movedTablePath,
            {NKikimrScheme::StatusMultipleModifications});

        // The table must still be exactly where it was - move never actually started.
        TestDescribeResult(DescribePath(runtime, tablePath), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, movedTablePath), {NLs::PathNotExist});

        // Let SetNotNull finish normally.
        validateBlocker.Stop();
        validateBlocker.Unblock();

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        // Now that the lock is released, the very same move must succeed.
        TestMoveTable(runtime, ++txId, tablePath, movedTablePath);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, tablePath), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, movedTablePath), {NLs::PathExist});
    }

    Y_UNIT_TEST(SetNotNullFailsWhileMoveTableInProgress) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;
        TString movedTablePath = root + "/TableMoved";

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Keep MoveTable stuck in ConfigureParts by blocking the flat scheme tx
        // it sends to the source table's datashards. By the time this event is
        // observed, MoveTable's local Propose has already committed - the source
        // path is already marked EPathStateMoving.
        TBlockEvents<TEvDataShard::TEvProposeTransaction> moveProposeBlocker(runtime);

        ui64 moveTxId = ++txId;
        AsyncMoveTable(runtime, moveTxId, tablePath, movedTablePath);

        runtime.WaitFor("block move propose to datashards", [&]{ return moveProposeBlocker.size() > 0; });

        // The source path is now "under operation" (moving). SetNotNull's internal
        // CreateLock sub-operation must be rejected right away with
        // StatusMultipleModifications (translated to Ydb::StatusIds::OVERLOADED),
        // so the whole SetColumnConstraint request fails and never gets to lock the
        // table or validate anything.
        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE (while moving): " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_UNEQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::OVERLOADED,
            response.ShortDebugString());

        // Column must remain nullable - SetNotNull never actually ran.
        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        // Let MoveTable finish normally.
        moveProposeBlocker.Stop();
        moveProposeBlocker.Unblock();
        env.TestWaitNotification(runtime, moveTxId);

        TestDescribeResult(DescribePath(runtime, tablePath), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, movedTablePath), {NLs::PathExist});

        // Now that the move is done, SetNotNull on the (now moved) table must succeed.
        auto secondResponse = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            root,
            movedTablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL_C(
            secondResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            secondResponse.ShortDebugString());

        env.TestWaitNotification(runtime, txId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, movedTablePath, {{"value", true}});
    }

    Y_UNIT_TEST(CopyTableNoBackupWhileValidating) {
        /*
            The test is based on the following idea.

            There are essentially two different ways CopyTable can be invoked:

            1. As a standalone operation initiated by the user.
            2. Internally, as part of a Backup meta-operation.

            This test covers the first scenario.        

            For a standalone CopyTable, it is fine to fail the request and tell the user that the table is currently locked
            by another long-running operation. In that scenario, CopyTable is a single explicit operation that the user can simply retry.
            This is also the case where CopyTable is expected to check whether the table is locked by other schema operations.
        */

        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;
        TString copyTablePath = root + "/TableBackup";

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest> validateBlocker(runtime);

        ui64 setConstraintTxId = ++txId;
        AsyncSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        runtime.WaitFor("block validate request", [&]{ return validateBlocker.size() > 0; });

        ui64 copyTx1 = ++txId;
        TestConsistentCopyTables(runtime, copyTx1, root, R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/TableBackup"
                IsBackup: false
            }
        )", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, copyTx1);

        TestDescribeResult(DescribePath(runtime, copyTablePath), {NLs::PathNotExist});

        validateBlocker.Stop();
        validateBlocker.Unblock();

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});

        ui64 copyTx2 = ++txId;
        TestConsistentCopyTables(runtime, copyTx2, root, R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/TableBackup"
                IsBackup: false
            }
        )");
        env.TestWaitNotification(runtime, copyTx2);

        TestDescribeResult(DescribePath(runtime, copyTablePath), {NLs::PathExist});
        TestCheckColumnsNotNull(runtime, copyTablePath, {{"value", true}});
    }

    Y_UNIT_TEST(CopyTableWithBackupWhileValidating) {
        /*
            The test is based on the following idea.

            There are essentially two different ways CopyTable can be invoked:

            1. As a standalone operation initiated by the user.
            2. Internally, as part of a Backup meta-operation.

            This test covers the second scenario.

            When CopyTable is invoked as part of Backup, it should not fail just because the table is locked by another
            long-running operation. If ConsistentCopyTable gets blocked this way during a backup, it may lead to a large
            number of retries. Each retry effectively means trying to copy all tables in the database again, which is a very
            expensive and long-running operation.

            Therefore, the practical way to avoid such retries is to make the new long-running operation pause and wait
            until the backup's CopyTable finishes.
        */
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;
        TString copyTablePath = root + "/TableBackup";

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvValidateRowConditionRequest> validateBlocker(runtime);

        ui64 setConstraintTxId = ++txId;
        AsyncSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        runtime.WaitFor("block validate request", [&]{ return validateBlocker.size() > 0; });

        ui64 copyTx = ++txId;
        TestConsistentCopyTables(runtime, copyTx, root, R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/TableBackup"
                IsBackup: true
            }
        )");
        env.TestWaitNotification(runtime, copyTx);

        TestDescribeResult(DescribePath(runtime, copyTablePath), {NLs::PathExist});

        validateBlocker.Stop();
        validateBlocker.Unblock();

        env.TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
        TestCheckColumnsNotNull(runtime, copyTablePath, {{"value", false}});

        {
            const auto describeResult = DescribePath(runtime, tablePath);
            const auto& columns = describeResult.GetPathDescription().GetTable().GetColumns();
            for (const auto& column : columns) {
                if (column.GetName() == "value") {
                    UNIT_ASSERT_VALUES_EQUAL(column.GetSetNotNullInProgress(), false);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(SetNotNullFailsWhileCopyTableInProgress, IsBackup) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);
        TString root = "/MyRoot";

        ui64 txId = 100;
        TString tableName = "Table";
        TString tablePath = root + "/" + tableName;
        TString copyTablePath = root + "/TableCopy";

        TestCreateTable(runtime, ++txId, root, TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Keep CopyTable stuck in ConfigureParts by blocking the flat scheme tx
        // it sends to the source table's datashards. By the time this event is
        // observed, CopyTable's local Propose has already committed - the source
        // path is already marked EPathStateCopying.
        TBlockEvents<TEvDataShard::TEvProposeTransaction> copyProposeBlocker(runtime);

        ui64 copyTxId = ++txId;
        if (IsBackup) {
            AsyncConsistentCopyTables(runtime, copyTxId, root, R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Table"
                    DstPath: "/MyRoot/TableCopy"
                    IsBackup: true
                }
            )");
        } else {
            AsyncConsistentCopyTables(runtime, copyTxId, root, R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Table"
                    DstPath: "/MyRoot/TableCopy"
                    IsBackup: false
                }
            )");
        }

        runtime.WaitFor("block copy propose to datashards", [&]{ return copyProposeBlocker.size() > 0; });

        // The source path is now "under operation" (copying). SetNotNull's internal
        // CreateLock sub-operation must be rejected right away with
        // StatusMultipleModifications (translated to Ydb::StatusIds::OVERLOADED),
        // so the whole SetColumnConstraint request fails and never gets to lock the
        // table or validate anything.
        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE (while copying): " << response.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::OVERLOADED,
            response.ShortDebugString());

        // Column must remain nullable - SetNotNull never actually ran.
        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        // Let CopyTable finish normally.
        copyProposeBlocker.Stop();
        copyProposeBlocker.Unblock();
        env.TestWaitNotification(runtime, copyTxId);

        TestDescribeResult(DescribePath(runtime, tablePath), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, copyTablePath), {NLs::PathExist});

        // Now that the copy is done, SetNotNull on the source table must succeed.
        auto secondResponse = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL_C(
            secondResponse.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            secondResponse.ShortDebugString());

        env.TestWaitNotification(runtime, txId, TTestTxConfig::SchemeShard);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }

    Y_UNIT_TEST(SetNotNullWithUsualSchemeOperation) {
        // As an example of a 'usual' scheme operation, this test uses TRUNCATE TABLE.

        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TTestEnv env(runtime);

        ui64 txId = 100;

        TString root = "/MyRoot";
        TString tablePath = root + "/Table";

        TestCreateTable(runtime, ++txId, root, R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvProposeTransaction> truncateTableBlocker(runtime);
        const ui64 truncateTxId = ++txId;
        AsyncTruncateTable(runtime, truncateTxId, root, "Table", TTestTxConfig::SchemeShard);
        runtime.WaitFor("block truncate propose to datashards", [&]{ return truncateTableBlocker.size() > 0; });

        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            root,
            tablePath,
            {"value"});

        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::OVERLOADED,
            response.ShortDebugString());

        // Column must remain nullable - SetNotNull never actually ran.
        TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

        truncateTableBlocker.Stop();
        truncateTableBlocker.Unblock();
        env.TestWaitNotification(runtime, truncateTxId);
    }
} // Y_UNIT_TEST_SUITE(SetNotNullTest)

