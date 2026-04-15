#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SetColumnConstraintTest) {
    NKikimrSetColumnConstraint::TEvCreateResponse TestSetColumnConstraint(
        TTestActorRuntime& runtime,
        ui64 txId,
        ui64 schemeShard,
        const TString& dbName,
        const TString& tablePath,
        const TVector<TString>& notNullColumns)
    {
        // We can't do `GetRequest`, because it is not implemented at the time of writing the test
        NKikimrSetColumnConstraint::TSetColumnConstraintSettings settings;
        settings.SetTablePath(tablePath);
        for (const auto& col : notNullColumns) {
            settings.AddNotNullColumns(col);
        }

        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvSetColumnConstraint::TEvCreateRequest>(txId, dbName, std::move(settings));
        ForwardToTablet(runtime, schemeShard, sender, request.Release());

        TAutoPtr<IEventHandle> handle;
        auto* event = runtime.GrabEdgeEvent<TEvSetColumnConstraint::TEvCreateResponse>(handle);
        UNIT_ASSERT(event);
        return event->Record;
    }

    NKikimrSetColumnConstraint::TEvCreateResponse TestSetColumnConstraintWithoutSettings(
        TTestActorRuntime& runtime,
        ui64 txId,
        ui64 schemeShard,
        const TString& dbName)
    {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvSetColumnConstraint::TEvCreateRequest>();
        request->Record.SetTxId(txId);
        request->Record.SetDatabaseName(dbName);
        ForwardToTablet(runtime, schemeShard, sender, request.Release());

        TAutoPtr<IEventHandle> handle;
        auto* event = runtime.GrabEdgeEvent<TEvSetColumnConstraint::TEvCreateResponse>(handle);
        UNIT_ASSERT(event);
        return event->Record;
    }

    void CheckColumnsNotNull(
        TTestActorRuntime& runtime,
        const TString& tablePath,
        const std::map<TString, bool>& expectedColumnNotNullStates)
    {
        const auto describeResult = DescribePath(runtime, tablePath);
        const auto& columns = describeResult.GetPathDescription().GetTable().GetColumns();

        std::map<TString, bool> currentNotNull;

        for (const auto& column : columns) {
            currentNotNull[column.GetName()] = column.GetNotNull();
        }

        for (const auto& [columnName, expectedNotNullValue] : expectedColumnNotNullStates) {
            auto it = currentNotNull.find(columnName);
            UNIT_ASSERT_C(
                it != currentNotNull.end(),
                TStringBuilder()
                    << "[CheckColumnsNotNull] Column `" << columnName << "` not found. "
                    << describeResult.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(
                it->second,
                expectedNotNullValue,
                TStringBuilder()
                    << "[CheckColumnsNotNull] Column `" << columnName << "` not null state mismatch. "
                    << describeResult.ShortDebugString());
        }
    }

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

        CheckColumnsNotNull(runtime, tablePath, {{"value", true}});

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

        CheckColumnsNotNull(runtime, tablePath, {{"value", true}});

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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});

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
            "/MyRoot");

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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", false}});

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

        if (!delayedValidateRequest) {
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", true}});
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

        CheckColumnsNotNull(runtime, tablePath, {{"value", true}});

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

        CheckColumnsNotNull(runtime, tablePath, {{"value", true}});
    }
} // Y_UNIT_TEST_SUITE(SetColumnConstraintTest)
