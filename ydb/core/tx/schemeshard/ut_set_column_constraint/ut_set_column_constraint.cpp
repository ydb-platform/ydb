#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SetColumnConstraintTest) {

    // Helper: send TEvSetColumnConstraint::TEvCreateRequest and grab the response.
    NKikimrSetColumnConstraint::TEvCreateResponse AsyncSetColumnConstraint(
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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Send a set column constraint request — currently returns StatusPreconditionFailed
        // because the implementation is not yet complete.
        auto response = AsyncSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "/MyRoot/Table",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        // The current stub implementation always returns StatusPreconditionFailed.
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::PRECONDITION_FAILED,
            response.ShortDebugString());
    }

    Y_UNIT_TEST(InvalidTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        // Send request for a non-existent table
        auto response = AsyncSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/MyRoot",
            "/MyRoot/NonExistentTable",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        // Table does not exist — expect an error status
        UNIT_ASSERT_VALUES_UNEQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());
    }

    Y_UNIT_TEST(InvalidDatabase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        // Send request with a non-existent database
        auto response = AsyncSetColumnConstraint(
            runtime, ++txId,
            TTestTxConfig::SchemeShard,
            "/NonExistentDB",
            "/NonExistentDB/Table",
            {"value"});

        Cerr << "SET COLUMN CONSTRAINT RESPONSE: " << response.ShortDebugString() << Endl;

        // Database does not exist — expect an error status
        UNIT_ASSERT_VALUES_UNEQUAL_C(
            response.GetStatus(),
            Ydb::StatusIds::SUCCESS,
            response.ShortDebugString());
    }

} // Y_UNIT_TEST_SUITE(SetColumnConstraintTest)
