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

    Y_UNIT_TEST(BasicRequest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        // Create table with nullable column "value"
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Set NOT NULL constraint on "value" column
        ui64 setConstraintTxId = ++txId;
        auto response = TestSetColumnConstraint(
            runtime, setConstraintTxId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "/MyRoot/Table",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        // Should get SUCCESS response (operation accepted)
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());

        // Wait for the operation to complete
        env.TestWaitNotification(runtime, setConstraintTxId);

        // Test 1: Insert row with non-NULL value - should succeed
        NKikimrMiniKQL::TResult result;
        TString error;
        bool success = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, R"(
            (
                (let key '( '('key (Uint32 '1) ) ) )
                (let row '( '('value (Utf8 '"test_value") ) ) )
                (return (AsList (UpdateRow '__user__Table key row) ))
            )
        )", result, error);

        UNIT_ASSERT_C(success, "Insert with non-NULL value should succeed: " << error);

        // Test 2: Insert row with NULL value - should fail
        success = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, R"(
            (
                (let key '( '('key (Uint32 '2) ) ) )
                (let row '( '('value (Nothing (OptionalType (DataType 'Utf8)))) ) )
                (return (AsList (UpdateRow '__user__Table key row) ))
            )
        )", result, error);

        UNIT_ASSERT_C(!success, "Insert with NULL value should fail but succeeded");
        UNIT_ASSERT_STRING_CONTAINS(error, "NOT NULL");
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
