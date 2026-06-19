#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

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

        auto response = TestSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "skip",
            {"skip"},
            true);

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

        using TConstraintState = Ydb::Table::SetColumnConstraintState_State;
        std::vector<TConstraintState> answers;
        std::vector<TConstraintState> expectedAnswers = {
            Ydb::Table::SetColumnConstraintState::STATE_PREPARING,
            Ydb::Table::SetColumnConstraintState::STATE_PREPARING,
            Ydb::Table::SetColumnConstraintState::STATE_VALIDATING,
            Ydb::Table::SetColumnConstraintState::STATE_APPLYING,
            Ydb::Table::SetColumnConstraintState::STATE_APPLYING,
            (isShouldBeFailed ? Ydb::Table::SetColumnConstraintState::STATE_CANCELLED : Ydb::Table::SetColumnConstraintState::STATE_DONE)
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

        // STATE_DONE/STATE_CANCELLED
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
                    << " (" << Ydb::Table::SetColumnConstraintState_State_Name(answers[i]) << ")"
                    << ", expected " << static_cast<int>(expectedAnswers[i])
                    << " (" << Ydb::Table::SetColumnConstraintState_State_Name(expectedAnswers[i]) << ")"
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
} // Y_UNIT_TEST_SUITE(SetNotNullTest)
