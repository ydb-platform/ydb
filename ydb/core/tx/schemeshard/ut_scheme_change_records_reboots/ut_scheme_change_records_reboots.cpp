#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_tx_infly.h>
#include <ydb/core/tx/schemeshard/ut_scheme_change_records/ut_scheme_change_records_helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;

namespace {

void RegisterSubscriber(TTestActorRuntime& runtime, const TString& subscriberId) {
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvRegisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    TAutoPtr<IEventHandle> handle;
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvRegisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsReboots) {

    Y_UNIT_TEST_WITH_REBOOTS(CreateTableWithReboots) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");
            }

            t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                "Name: \"Table1\""
                "Columns { Name: \"key\"   Type: \"Uint64\" }"
                "Columns { Name: \"value\" Type: \"Utf8\" }"
                "KeyColumnNames: [\"key\"]"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                 NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                    {NLs::Finished, NLs::IsTable});

                auto entries = ReadSchemeChangeRecords(runtime);
                bool found = false;
                for (const auto& e : entries) {
                    if (e.Path == "Table1") {
                        found = true;
                        UNIT_ASSERT(e.Order > 0);
                        UNIT_ASSERT_VALUES_EQUAL(e.ObjectType, (ui32)NKikimrSchemeOp::EPathTypeTable);
                        break;
                    }
                }
                UNIT_ASSERT_C(found, "Scheme change record for Table1 not found");
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MultipleCreateTablesWithReboots) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");
                t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                    "Name: \"T1\""
                    "Columns { Name: \"key\" Type: \"Uint64\" }"
                    "KeyColumnNames: [\"key\"]"),
                    {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                     NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                "Name: \"T2\""
                "Columns { Name: \"key\" Type: \"Uint64\" }"
                "KeyColumnNames: [\"key\"]"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                 NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T1"),
                    {NLs::Finished, NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T2"),
                    {NLs::Finished, NLs::IsTable});

                auto entries = ReadSchemeChangeRecords(runtime);

                // T1 was created in inactive zone -- its entry must exist
                bool foundT1 = false;
                for (const auto& e : entries) {
                    if (e.Path == "T1") {
                        foundT1 = true;
                        UNIT_ASSERT(e.Order > 0);
                    }
                }
                UNIT_ASSERT_C(foundT1, "Scheme change record for T1 not found");

                // Verify monotonic sequence IDs across all entries
                for (size_t i = 1; i < entries.size(); ++i) {
                    UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                        "Orders must be strictly monotonic");
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(AlterTableWithReboots) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");
                t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                    "Name: \"Table1\""
                    "Columns { Name: \"key\"   Type: \"Uint64\" }"
                    "Columns { Name: \"value\" Type: \"Utf8\" }"
                    "KeyColumnNames: [\"key\"]"),
                    {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                     NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "extra" Type: "Uint32" }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                    {NLs::Finished, NLs::IsTable,
                     NLs::CheckColumns("Table1", {"key", "value", "extra"}, {}, {"key"})});

                auto entries = ReadSchemeChangeRecords(runtime);

                // CREATE was done in inactive zone -- its entry must exist
                bool foundCreate = false;
                for (const auto& e : entries) {
                    if (e.Path == "Table1" && e.OperationType == (ui32)TTxState::TxCreateTable) {
                        foundCreate = true;
                        UNIT_ASSERT(e.Order > 0);
                    }
                }
                UNIT_ASSERT_C(foundCreate, "CREATE TABLE entry not found in notification log");

                // Verify monotonic sequence IDs across all entries
                for (size_t i = 1; i < entries.size(); ++i) {
                    UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                        "Orders must be strictly monotonic");
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(OrderCounterSurvivesReboot) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");
                t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                    "Name: \"T1\""
                    "Columns { Name: \"key\" Type: \"Uint64\" }"
                    "KeyColumnNames: [\"key\"]"),
                    {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                     NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Create T2 in active zone -- reboots injected here.
            t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                "Name: \"T2\""
                "Columns { Name: \"key\" Type: \"Uint64\" }"
                "KeyColumnNames: [\"key\"]"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                 NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T1"),
                    {NLs::Finished, NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T2"),
                    {NLs::Finished, NLs::IsTable});

                auto entries = ReadSchemeChangeRecords(runtime);

                // T1 was created in inactive zone -- its entry must exist
                ui64 t1Order = 0;
                for (const auto& e : entries) {
                    if (e.Path == "T1") t1Order = e.Order;
                }
                UNIT_ASSERT_C(t1Order > 0, "T1 entry not found in notification log");

                // If T2's entry exists, verify counter continuity
                for (const auto& e : entries) {
                    if (e.Path == "T2") {
                        UNIT_ASSERT_C(e.Order > t1Order,
                            "T2 Order (" << e.Order
                                << ") must be greater than T1 Order (" << t1Order << ")");
                    }
                }

                // Verify monotonic sequence IDs across all entries
                for (size_t i = 1; i < entries.size(); ++i) {
                    UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                        "Orders must be strictly monotonic");
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(SchemeChangeRecordsSurviveReboot) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");
            }

            t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId, "/MyRoot",
                "Name: \"T1\""
                "Columns { Name: \"key\" Type: \"Uint64\" }"
                "KeyColumnNames: [\"key\"]"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                 NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                auto entries = ReadSchemeChangeRecords(runtime);
                bool found = false;
                for (const auto& e : entries) {
                    if (e.Path == "T1") {
                        found = true;
                        break;
                    }
                }
                UNIT_ASSERT_C(found, "Scheme change record for T1 not found after reboot");
            }
        });
    }
}
