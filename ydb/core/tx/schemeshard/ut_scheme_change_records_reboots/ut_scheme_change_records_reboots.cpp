#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_tx_infly.h>
#include <ydb/core/tx/schemeshard/ut_scheme_change_records/ut_scheme_change_records_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

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
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
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
                    if (e.Body.GetCreateTable().GetName() == "Table1") {
                        found = true;
                        UNIT_ASSERT(e.Order > 0);
                        break;
                    }
                }
                UNIT_ASSERT_C(found, "Scheme change record for Table1 not found");
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MultipleCreateTablesWithReboots) {
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
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
                    if (e.Body.GetCreateTable().GetName() == "T1") {
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
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
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
                    if (e.Body.HasCreateTable()
                        && e.Body.GetCreateTable().GetName() == "Table1") {
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
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
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
                    if (e.Body.GetCreateTable().GetName() == "T1") t1Order = e.Order;
                }
                UNIT_ASSERT_C(t1Order > 0, "T1 entry not found in notification log");

                // If T2's entry exists, verify counter continuity
                for (const auto& e : entries) {
                    if (e.Body.GetCreateTable().GetName() == "T2") {
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

    // A multi-target op's N (Path, SourcePaths) targets are carried in
    // TSchemeChangeSlot in memory, backed by table 144 for recovery -- reboot
    // mid-operation and confirm all N targets AND their sources survive, not
    // just the destination paths.
    Y_UNIT_TEST_WITH_REBOOTS(MultiTargetPathsSurviveReboot) {
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                RegisterSubscriber(runtime, "test:sub");

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Src1"
                    Columns { Name: "key" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Src2"
                    Columns { Name: "key" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestConsistentCopyTables(runtime, ++t.TxId, "/MyRoot", R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Src1"
                    DstPath: "/MyRoot/Dst1"
                }
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Src2"
                    DstPath: "/MyRoot/Dst2"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dst1"), {NLs::Finished, NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dst2"), {NLs::Finished, NLs::IsTable});

                auto entries = ReadSchemeChangeRecords(runtime);
                const NSchemeChangeRecordTestHelpers::TSchemeChangeRecordEntry* found = nullptr;
                for (const auto& e : entries) {
                    if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                        found = &e;
                        break;
                    }
                }
                UNIT_ASSERT_C(found, "CreateConsistentCopyTables entry not found after reboot");
                UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 2u,
                    "both copy targets must survive a reboot, got " << found->Targets.size());
                THashMap<TString, TString> dstToSrc;
                for (const auto& target : found->Targets) {
                    UNIT_ASSERT_VALUES_EQUAL_C(target.SourcePaths.size(), 1u,
                        "each target's source must survive a reboot, got "
                            << target.SourcePaths.size() << " for " << target.Path);
                    dstToSrc[target.Path] = target.SourcePaths[0];
                }
                UNIT_ASSERT_C(dstToSrc.contains("Dst1"), "Dst1 missing after reboot");
                UNIT_ASSERT_C(dstToSrc.contains("Dst2"), "Dst2 missing after reboot");
                UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst1"], "Src1", "Dst1's source must survive a reboot");
                UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst2"], "Src2", "Dst2's source must survive a reboot");
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(SchemeChangeRecordsSurviveReboot) {
        t.GetTestEnvOptions().EnableSchemeChangeRecords(true);
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
                    if (e.Body.GetCreateTable().GetName() == "T1") {
                        found = true;
                        break;
                    }
                }
                UNIT_ASSERT_C(found, "Scheme change record for T1 not found after reboot");
            }
        });
    }
}
