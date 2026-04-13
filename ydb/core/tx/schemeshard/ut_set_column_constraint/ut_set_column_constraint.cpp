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

        // todo(flown4qqqq)
        Sleep(TDuration::Seconds(4));

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
    }
} // Y_UNIT_TEST_SUITE(SetColumnConstraintTest)
