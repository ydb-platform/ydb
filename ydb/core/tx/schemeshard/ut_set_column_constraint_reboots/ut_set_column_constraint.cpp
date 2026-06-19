#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SetNotNullReboots) {

    /*
    todo:
    IF (BUILD_TYPE == "DEBUG" OR SANITIZER_TYPE)
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()
    */

    Y_UNIT_TEST_WITH_REBOOTS(SetNotNullOnSingleColumn) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
                runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8"   }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            const ui64 setConstraintTxId = ++t.TxId;
            {
                NKikimrSetColumnConstraint::TSetColumnConstraintSettings settings;
                settings.SetTablePath("/MyRoot/Table");
                settings.AddNotNullColumns("value");

                auto sender = runtime.AllocateEdgeActor();
                auto request = MakeHolder<TEvSetColumnConstraint::TEvCreateRequest>(
                    setConstraintTxId, "/MyRoot", std::move(settings));
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request.Release());
            }

            t.TestEnv->TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

            {
                TInactiveZone inactive(activeZone);

                const auto describeResult = DescribePath(runtime, "/MyRoot/Table");
                const auto& columns = describeResult.GetPathDescription().GetTable().GetColumns();

                bool found = false;
                for (const auto& column : columns) {
                    if (column.GetName() == "value") {
                        found = true;
                        UNIT_ASSERT_C(
                            column.GetNotNull(),
                            "Column 'value' must be NOT NULL after SetColumnConstraint completes");
                    }
                }
                UNIT_ASSERT_C(found, "Column 'value' not found in table description");
            }
        });
    }

} // Y_UNIT_TEST_SUITE(SetNotNullReboots)
