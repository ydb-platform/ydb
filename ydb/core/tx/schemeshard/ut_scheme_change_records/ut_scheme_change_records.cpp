#include "ut_scheme_change_records_helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/schemeshard_counters.h>

#include <util/string/printf.h>
#include <util/string/join.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecordsFull;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSchemaTests) {
    Y_UNIT_TEST(SchemeChangeRecordsTableExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        auto entries = ReadSchemeChangeRecords(runtime);
        Y_UNUSED(entries);
    }

    Y_UNIT_TEST(NoRecordsCreatedWithoutSubscribers) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.empty(),
            "No records should be created without subscribers, got " << entries.size());
    }

    Y_UNIT_TEST(RecordsCreatedAfterSubscriberRegistered) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        // Create T1 without subscriber -- no record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Register subscriber
        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Create T2 with subscriber -- record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool foundT1 = false;
        bool foundT2 = false;
        for (const auto& e : entries) {
            if (e.Body.HasCreateTable()) {
                const auto& name = e.Body.GetCreateTable().GetName();
                if (name == "T1") foundT1 = true;
                if (name == "T2") foundT2 = true;
            }
        }
        UNIT_ASSERT_C(!foundT1, "T1 record should not exist (created before subscriber)");
        UNIT_ASSERT_C(foundT2, "T2 record should exist (created after subscriber)");
    }

    Y_UNIT_TEST(CreateTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.TxId, (ui64)txId);
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(e.Targets[0].Path, "Table1");
                UNIT_ASSERT(e.Order > 0);
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(AlterTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 alterCount = 0;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                ++alterCount;
            }
        }
        UNIT_ASSERT_C(alterCount >= 1, "ALTER TABLE entry not found in notification log");
    }

    // UserSID must record who issued the DDL, not who owns the target object.
    // A ModifyACL changes the owner without the owner ever issuing anything;
    // a later ALTER by a different (here: anonymous-token) issuer must not be
    // attributed to that owner.
    Y_UNIT_TEST(UserSIDRecordsIssuerNotOwner) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Positive companion: the owner is genuinely changed to someone who
        // never issues the later ALTER, so a later match can't be coincidence.
        TestModifyACL(runtime, ++txId, "/MyRoot", "Table1", "", "bob@builtin");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* alterEntry = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                alterEntry = &e;
            }
        }
        UNIT_ASSERT_C(alterEntry, "ALTER TABLE entry not found in notification log");
        // The test issues the ALTER with no user token, so the true issuer
        // SID is empty -- never "bob@builtin", the object's owner.
        UNIT_ASSERT_VALUES_EQUAL_C(alterEntry->UserSID, "",
            "UserSID must reflect the DDL issuer (empty, no token supplied here), "
            "not the target's owner (\"bob@builtin\"); got \"" << alterEntry->UserSID << "\"");
    }

    // TCreateCdcStream has no top-level "Name" field (the target is nested
    // under TableName/StreamDescription.Name and is not a direct child of
    // WorkingDir), so path resolution fails -- the propose must be refused
    // rather than produce a record stamped with the parent directory's
    // identity or an empty path. See PathBearingOpNeverStoresAnApproximatePath
    // for the counter and record-absence assertions.
    Y_UNIT_TEST(CdcStreamRecordDoesNotImpersonateParentDirectory) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        auto entries = ReadSchemeChangeRecords(runtime);
        for (const auto& e : entries) {
            UNIT_ASSERT_C(e.Body.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateCdcStream,
                "the record must not be stamped with the parent directory's identity "
                "or an empty path -- the propose itself must be refused instead");
        }
    }

    Y_UNIT_TEST(DropTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTable
                && e.Body.GetDrop().GetName() == "Table1") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DROP TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(OrdersAreMonotonicAcrossOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        for (size_t i = 1; i < entries.size(); ++i) {
            UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                "Orders must be strictly monotonic");
        }
    }

    Y_UNIT_TEST(OverflowRejectsNewOperations) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 2;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.size() >= baseline.size() + 2, "Expected at least baseline+2 entries");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(AckFreesOverflowCapacityImmediately) {
        // Overflow check uses (NextSchemeChangeOrder - MinSubscriberOrder), so
        // an ack restores capacity immediately without waiting for cleanup.
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 2;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // At capacity: next op rejected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3a"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // Ack everything (without manually firing background cleanup)
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!entries.empty());
        ui64 lastOrder = entries.back().Order;
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "test:sub", lastOrder, ackHandle);

        // Capacity must be free immediately after ack: overflow check is based
        // on unacked range, not row count.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3b"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RaisingLimitViaConfigUnblocksOperations) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(baseline.size() + 2);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(baseline.size() + 10);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(LoweringLimitViaConfigBlocksOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(1);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(PlanStepIsRecordedForCoordinatedOps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!entries.empty());

        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = true;
                UNIT_ASSERT_C(e.PlanStep > 0,
                    "CreateTable should have a valid PlanStep, got: " << e.PlanStep);
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(PlanStepIsRecordedForAlterTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawAlter = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                sawAlter = true;
                UNIT_ASSERT_C(e.PlanStep > 0,
                    "AlterTable Table1 should have a valid PlanStep, got: " << e.PlanStep);
            }
        }
        UNIT_ASSERT(sawAlter);
    }

    Y_UNIT_TEST(PlanStepMonotonicAcrossOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        ui64 prevPlanStep = 0;
        for (const auto& e : entries) {
            UNIT_ASSERT_C(e.PlanStep >= prevPlanStep,
                "PlanStep should be monotonically non-decreasing: prev=" << prevPlanStep
                    << " current=" << e.PlanStep << " path=" << AllTargetPaths(e));
            prevPlanStep = e.PlanStep;
        }

        for (size_t i = 1; i < entries.size(); ++i) {
            const auto& prev = entries[i-1];
            const auto& curr = entries[i];
            if (curr.PlanStep != prev.PlanStep || curr.TxId != prev.TxId) {
                bool planStepTxIdOrdering = std::tie(curr.PlanStep, curr.TxId) > std::tie(prev.PlanStep, prev.TxId);
                UNIT_ASSERT_C(planStepTxIdOrdering,
                    "(PlanStep, TxId) ordering must match Order ordering:"
                        << " prev=(" << prev.PlanStep << "," << prev.TxId << ") order=" << prev.Order
                        << " curr=(" << curr.PlanStep << "," << curr.TxId << ") order=" << curr.Order);
            }
        }
    }

    Y_UNIT_TEST(MkDirWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMkDir
                && e.Body.GetMkDir().GetName() == "DirA") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "MkDir should produce a scheme change record");
    }

    Y_UNIT_TEST(WatermarkDoesNotRegressAfterAllOpsComplete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto result = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT(!result.Entries.empty());
        // ClosedThroughPlanStep must be monotonic even when TxInFlight empties.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep > 0,
            "ClosedThroughPlanStep must not regress to 0 after an op completes, got: "
                << result.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(WatermarkSurvivesTabletReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "reboot:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto before = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(before.ClosedThroughPlanStep > 0,
            "Pre-reboot watermark should be > 0, got: " << before.ClosedThroughPlanStep);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto after = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(after.ClosedThroughPlanStep >= before.ClosedThroughPlanStep,
            "Post-reboot watermark must not regress: before="
                << before.ClosedThroughPlanStep << " after=" << after.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(WatermarkReflectsInFlightPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TVector<THolder<IEventHandle>> heldEvents;
        ui64 firstTxId = txId + 1;
        bool captured = false;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvSchemaChanged) {
                auto* msg = ev->Get<TEvDataShard::TEvSchemaChanged>();
                if (msg->Record.GetTxId() == firstTxId) {
                    captured = true;
                    heldEvents.push_back(THolder<IEventHandle>(ev.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");

        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&]() { return captured; };
            runtime.DispatchEvents(opts);
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Table2 should be present, Table1 still in-flight.
        auto result = ReadSchemeChangeRecordsFull(runtime);

        UNIT_ASSERT_C(result.ClosedThroughPlanStep > 0,
            "ClosedThroughPlanStep should be > 0 while Table1 is still in-flight, got: "
                << result.ClosedThroughPlanStep);

        for (auto& ev : heldEvents) {
            runtime.Send(ev.Release());
        }
        heldEvents.clear();
        env.TestWaitNotification(runtime, firstTxId);

        auto result2 = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(result2.ClosedThroughPlanStep >= result.ClosedThroughPlanStep,
            "ClosedThroughPlanStep must not regress after all ops complete: had "
                << result.ClosedThroughPlanStep << ", now " << result2.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(CreateTableWithIndexProducesSingleParentRecord) {
        // A multi-part DDL emits exactly one record carrying the user-level
        // body; the target cluster re-runs decomposition on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 parentCount = 0;
        bool haveMain = false;
        bool haveIndex = false;
        for (const auto& e : entries) {
            if (e.TxId != (ui64)txId) continue;
            UNIT_ASSERT_VALUES_EQUAL(
                (ui32)e.Body.GetOperationType(),
                (ui32)NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
            ++parentCount;
            const auto& ct = e.Body.GetCreateIndexedTable();
            if (ct.GetTableDescription().GetName() == "Main") haveMain = true;
            for (const auto& idx : ct.GetIndexDescription()) {
                if (idx.GetName() == "IdxByValue") haveIndex = true;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(parentCount, 1u,
            "Expected exactly 1 parent-level record, got " << parentCount);
        UNIT_ASSERT_C(haveMain, "Parent record must carry the main table description");
        UNIT_ASSERT_C(haveIndex, "Parent record must carry the index description");
    }

    Y_UNIT_TEST(AutoMkDirBodyPreservedOnParent) {
        // Only the user's original CreateTable body is persisted; the target
        // cluster regenerates the auto-mkdir chain on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "A/B/C/Leaf"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool foundLeaf = false;
        for (const auto& e : entries) {
            if (e.TxId != (ui64)txId) continue;
            UNIT_ASSERT_VALUES_EQUAL(
                (ui32)e.Body.GetOperationType(),
                (ui32)NKikimrSchemeOp::ESchemeOpCreateTable);
            if (e.Body.GetCreateTable().GetName() == "A/B/C/Leaf") {
                foundLeaf = true;
            }
        }
        UNIT_ASSERT_C(foundLeaf, "User-level CreateTable body must preserve the original path");
    }

    Y_UNIT_TEST(ParentBodyCarriesFullSubDescriptions) {
        // Every sub-description the decomposer needs must live inside the one
        // parent body, or replay cannot reproduce the DDL on the target.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "body:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const NKikimrSchemeOp::TIndexedTableCreationConfig* ct = nullptr;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId && e.Body.HasCreateIndexedTable()) {
                ct = &e.Body.GetCreateIndexedTable();
                break;
            }
        }
        UNIT_ASSERT(ct);
        UNIT_ASSERT_VALUES_EQUAL(ct->GetTableDescription().GetName(), "Main");
        UNIT_ASSERT_VALUES_EQUAL(ct->IndexDescriptionSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ct->GetIndexDescription(0).GetName(), "IdxByValue");
    }

    Y_UNIT_TEST(FetchBodiesReturnsOnlyRequestedSparseOrders) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bodies:sub", regHandle);

        for (int i = 1; i <= 10; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "bodies:sub", 0, 1000, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 10u);

        TVector<ui64> allOrders;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            allOrders.push_back(fetch->Record.GetEntries(i).GetOrder());
        }

        TVector<ui64> requested = {allOrders[1], allOrders[4], allOrders[7]};

        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, "bodies:sub", requested, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.EntriesSize(), requested.size());

        THashSet<ui64> requestedSet(requested.begin(), requested.end());
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            const auto& e = bodies->Record.GetEntries(i);
            UNIT_ASSERT_C(requestedSet.contains(e.GetOrder()),
                "FetchBodies returned unrequested order " << e.GetOrder());
            UNIT_ASSERT_C(!e.GetBody().empty(),
                "FetchBodies returned empty body for order " << e.GetOrder());
            NKikimrSchemeOp::TModifyScheme body;
            UNIT_ASSERT(body.ParseFromString(e.GetBody()));
            UNIT_ASSERT_VALUES_EQUAL((ui32)body.GetOperationType(), (ui32)NKikimrSchemeOp::ESchemeOpCreateTable);
        }
    }

    Y_UNIT_TEST(AckWithLargeBacklogDrainsAcrossMultipleTxs) {
        // A single Ack tx deletes at most SchemeChangeCleanupBatchSize rows;
        // the rest drains via follow-up cleanup txs. Ack reply must return
        // with the correct LastAckedOrder before the drain completes.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backlog:sub", regHandle);

        // Small cap so we can trigger continuation without creating 1000+ records.
        schemeshard->SchemeChangeCleanupBatchSize = 3;

        constexpr int kRecords = 10;
        for (int i = 0; i < kRecords; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "B%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "backlog:sub", 0, 1000, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= kRecords);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        // Measure only the chain triggered by the single Ack below.
        schemeshard->SchemeChangeCleanupTxCount = 0;

        TAutoPtr<IEventHandle> ackHandle;
        auto* ackRes = AckSchemeChangeRecords(runtime, "backlog:sub", latest, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)ackRes->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ackRes->Record.GetLastAckedOrder(), latest);

        for (int i = 0; i < 50; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT_C(schemeshard->SchemeChangeCleanupTxCount >= 3,
            "Expected >=3 continuation cleanup txs for " << kRecords
            << " records at batch size 3, got " << schemeshard->SchemeChangeCleanupTxCount);
    }

    // Bounding the fetch scan by NextSchemeChangeOrder must not change which
    // records are returned. This does NOT prove the precharge cost bound --
    // charge counters stay at 0 in this in-memory test regardless of range
    // width, so the cost effect is unobservable here. It only proves the
    // upper bound is not off-by-one at the boundary or under a maxCount cap.
    Y_UNIT_TEST(FetchUpperBoundDoesNotChangeResults) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bound:sub", regHandle);

        constexpr int kRecords = 10;
        for (int i = 0; i < kRecords; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Bound%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        // Ground truth: a fetch wide enough to cover everything at once. The
        // boundary/capped cases below are checked against this, not against
        // an independent oracle -- ReadSchemeChangeRecords is itself built on
        // this same fetch path, so it cannot serve as an independent check.
        TAutoPtr<IEventHandle> fullHandle;
        auto* full = FetchSchemeChangeRecords(runtime, "bound:sub", 0, 1000, fullHandle);
        UNIT_ASSERT_C(full->Record.EntriesSize() >= kRecords,
            "precondition: the unbounded fetch must see all " << kRecords << " records");
        TVector<ui64> allOrders;
        ui64 maxOrder = 0;
        for (size_t i = 0; i < static_cast<size_t>(full->Record.EntriesSize()); ++i) {
            const ui64 order = full->Record.GetEntries(i).GetOrder();
            allOrders.push_back(order);
            maxOrder = Max(maxOrder, order);
        }

        // Boundary case: AfterOrder pinned one below the true max, so the
        // query's GreaterOrEqual/LessOrEqual window collapses to exactly the
        // last row. An off-by-one bound would return zero entries here.
        TAutoPtr<IEventHandle> boundaryHandle;
        auto* boundary = FetchSchemeChangeRecords(runtime, "bound:sub", maxOrder - 1, 1000, boundaryHandle);
        UNIT_ASSERT_VALUES_EQUAL_C(boundary->Record.EntriesSize(), 1,
            "the last record must still be reachable right at the upper bound");
        UNIT_ASSERT_VALUES_EQUAL(boundary->Record.GetEntries(0).GetOrder(), maxOrder);

        // Count-capped case: maxCount stops the scan before the bound would.
        // Same leading records as the unbounded fetch, in the same order.
        TAutoPtr<IEventHandle> cappedHandle;
        auto* capped = FetchSchemeChangeRecords(runtime, "bound:sub", 0, 3, cappedHandle);
        UNIT_ASSERT_VALUES_EQUAL(capped->Record.EntriesSize(), 3);
        UNIT_ASSERT_C(capped->Record.GetHasMore(), "capped fetch must report more entries remain");
        for (int i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(capped->Record.GetEntries(i).GetOrder(), allOrders[i]);
        }
    }

    Y_UNIT_TEST(PersistsNextSchemeChangeOrderOncePerBatch) {
        // A multi-part DDL emits one record, so NextSchemeChangeOrder is
        // persisted once per batch by construction.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "batchpersist:sub", regHandle);

        schemeshard->NextSchemeChangeOrderPersistCount = 0;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        size_t thisTxRecords = 0;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId) ++thisTxRecords;
        }
        UNIT_ASSERT_VALUES_EQUAL(thisTxRecords, 1u);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrderPersistCount, 1u,
            "Expected one NextSchemeChangeOrder persist for a batch, got "
            << schemeshard->NextSchemeChangeOrderPersistCount);
    }

    Y_UNIT_TEST(FetchBodiesUnorderedAndDuplicateRequestedOrdersHandled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bodies:sub2", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "bodies:sub2", 0, 1000, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 5u);

        TVector<ui64> all;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            all.push_back(fetch->Record.GetEntries(i).GetOrder());
        }

        TVector<ui64> requested = {all[3], all[0], all[3], all[2]};

        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, "bodies:sub2", requested, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        THashSet<ui64> returned;
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            returned.insert(bodies->Record.GetEntries(i).GetOrder());
        }
        // Dedup expected: each requested order returned at most once
        UNIT_ASSERT_C(returned.contains(all[0]), "order " << all[0] << " missing");
        UNIT_ASSERT_C(returned.contains(all[2]), "order " << all[2] << " missing");
        UNIT_ASSERT_C(returned.contains(all[3]), "order " << all[3] << " missing");
        UNIT_ASSERT_VALUES_EQUAL(returned.size(), 3u);
    }

    Y_UNIT_TEST(ZeroMaxSchemeChangeRecordsIsIgnored) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "zero:sub", regHandle);

        const ui64 before = schemeshard->MaxSchemeChangeRecords;
        UNIT_ASSERT_C(before > 0, "precondition: the default cap must be non-zero");

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(0);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->MaxSchemeChangeRecords, before,
            "a zero cap rejects every DDL including after a force-advance, so it "
            "must be ignored rather than applied");

        // The consequence that matters: DDL is still accepted.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DisabledByDefaultEmitsNoRecords) {
        // ReadSchemeChangeRecords itself registers a temp subscriber, which is
        // refused while disabled; use the order counter as a disabled-safe oracle.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        // Default options: the flag is off unless a test opts in.
        TTestEnv env(runtime, TTestEnvOptions(), ssFactory);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrder, 0u,
            "no outbox rows should be reserved while the feature is disabled");
    }

    // Positive companion to DisabledByDefaultEmitsNoRecords: the identical DDL
    // with the flag on does produce a row, so the disabled case can't pass
    // vacuously because DDL itself failed.
    Y_UNIT_TEST(EnabledByOptInEmitsRecord) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "expected at least one outbox row with the feature enabled");
    }

    Y_UNIT_TEST(DisabledRefusesRegistration) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> regHandle;
        auto* result = RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, regHandle);
        UNIT_ASSERT_C(!result->Record.GetReason().empty(),
            "a disabled-feature refusal must carry a clear reason, not a silent failure");
    }

    Y_UNIT_TEST(DisabledNeverBlocksDdl) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true), ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 1;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Seed the outbox to the cap: this DDL must be refused while enabled.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // The kill switch: disabling must rescue a cluster wedged by a full outbox.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(false);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(FlagFlipTakesEffectWithoutReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Disabled: registration is refused.
        TAutoPtr<IEventHandle> regHandle1;
        RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, regHandle1);

        // Flip live, on the same running tablet -- no reboot.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);

        TAutoPtr<IEventHandle> regHandle2;
        RegisterSubscriber(runtime, "test:sub", regHandle2);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(),
            "record emission must follow the flag flip without a tablet restart");
    }

    Y_UNIT_TEST(ReEnablingResumesWithoutReportingLoss) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Mid-stream: fetch but don't ack past the tail, so the subscriber's
        // cursor sits in the middle of the retained log.
        TAutoPtr<IEventHandle> fetchHandle;
        FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);

        // Disable, then re-enable: the cursor and its record window must survive.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(false);
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);

        TAutoPtr<IEventHandle> regHandle2;
        auto* result = RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, regHandle2);
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that was mid-stream across a disable/enable cycle "
            "must not be reported as STATE_LOST");
    }

    // Cleanup is only ever enqueued from the Complete() hooks of outbox
    // transactions, and those are gated on the flag. Suppress the scheduled
    // continuation so the self-chain cannot drain on its own; only the
    // disable path (ApplyConsoleConfigs) may rescue it.
    Y_UNIT_TEST(DisablingDrainsAckedRecords) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Small cap so the Ack's own tx cannot finish the drain in one shot.
        schemeshard->SchemeChangeCleanupBatchSize = 1;

        constexpr int kRecords = 3;
        for (int i = 0; i < kRecords; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        auto before = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!before.empty(), "precondition: the record must exist before acking");

        // Suppress the scheduled continuation before acking, so the batch-size
        // limited remainder cannot drain via the ordinary self-chain.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvPrivate::TEvSchemeChangeRecordsCleanup::EventType);

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "test:sub", latest, ackHandle);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        auto stillPresent = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!stillPresent.empty(),
            "precondition: the continuation must be suppressed, so acked "
            "records are not yet swept");

        // Stop suppressing, but deliberately drop the already-suppressed
        // continuation instead of replaying it: the drain below must come
        // from a freshly enqueued cleanup, not from that stale one.
        runtime.SetObserverFunc(prevObserver);
        suppressed.clear();

        // Disabling must kick the drain despite the dropped continuation.
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableSchemeChangeRecords(false);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);
        auto after = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(after.empty(),
            "disabling the flag must self-start a drain of already-acked records");
    }

    // Scope limit: disabling must not bypass GetMinSubscriberOrder. An
    // unacked record stays retained so re-enabling resumes losslessly.
    Y_UNIT_TEST(DisablingRetainsUnackedRecords) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        auto before = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!before.empty(), "precondition: the record must exist before disabling");

        // No ack: the subscriber's cursor still sits below this record.
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableSchemeChangeRecords(false);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);
        auto after = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!after.empty(),
            "an unacked record must survive disabling: the retention floor "
            "must not be bypassed");
    }

    // (a) FinalizeSchemeChangeRecord resolves the target via TPath::Resolve but
    // only writes PathOwnerId/PathLocalId back -- Path itself keeps the
    // propose-time synthesis. A WorkingDir with a trailing slash still
    // canonicalizes on resolve, so the two differ and the record must carry
    // the canonical one.
    Y_UNIT_TEST(RecordCarriesCanonicalResolvedPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Trailing slash: propose-time string concatenation would leave a
        // double slash, while TPath::Resolve canonicalizes it away.
        TestCreateTable(runtime, ++txId, "/MyRoot/", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_C(!found->Targets[0].Path.Contains("//"),
            "Path must be the canonical resolved path, not a raw concatenation; got \"" << found->Targets[0].Path << "\"");
    }

    // (b) + (c) A stored path must be relative to the database root, and the
    // relative remainder must be the full path to the object -- a path
    // truncated to empty must not pass.
    Y_UNIT_TEST(RecordPathIsRelativeToDatabaseRoot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
            Name: "Nested"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Nested") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_C(!found->Targets[0].Path.StartsWith("/MyRoot"),
            "Path must not carry the source database prefix; got \"" << found->Targets[0].Path << "\"");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "DirA/Nested",
            "Path must be the full remainder relative to the database root, "
            "not truncated to empty or to just the leaf name");
    }

    // (c) No fallback, ever: CreateCdcStream's target is not a direct child of
    // WorkingDir (it is WorkingDir/TableName/StreamName), so today's target-name
    // extraction cannot resolve it. The record must not carry an empty or
    // approximate path -- the propose itself must be refused, loudly, with a
    // counter bump naming the gap in the pathless allowlist.
    Y_UNIT_TEST(PathBearingOpNeverStoresAnApproximatePath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_C(rejectedAfter > rejectedBefore,
            "an unresolvable path-bearing op must bump the path-missing counter");

        auto entries = ReadSchemeChangeRecords(runtime);
        for (const auto& e : entries) {
            UNIT_ASSERT_C(e.Body.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateCdcStream,
                "a refused propose must not leave a scheme change record behind");
        }
    }

    // Positive companion to PathBearingOpNeverStoresAnApproximatePath: the
    // pathless allowlist is empty today, so an ordinary op must still produce
    // its record -- the invariant above must not be satisfiable by refusing
    // everything.
    Y_UNIT_TEST(OrdinaryOpsStillProduceRecordsWhenAllowlistIsEmpty) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "PlainTable"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "PlainTable") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "an ordinary path-bearing op must still produce a record");
    }

    // RFC 0129: CreateConsistentCopyTables carries N targets in one repeated
    // TCopyTableConfig field, each pairing a destination with its own source.
    // The record carries N (Path, SourcePaths) targets per record (one record
    // per user-level tx, since Body is the atomic replay unit), so this
    // propose must succeed and the record must list every copy as a target
    // with both its destination Path and its source populated.
    Y_UNIT_TEST(ConsistentCopyTablesRecordsAllTargetsWithSources) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src1"
                DstPath: "/MyRoot/Dst1"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src2"
                DstPath: "/MyRoot/Dst2"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "a resolvable multi-target copy must not bump the path-missing counter");

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CreateConsistentCopyTables entry not found -- the propose must not be refused");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 2u,
            "record must list all N copy targets, got " << found->Targets.size());

        THashMap<TString, TString> dstToSrc;
        for (const auto& target : found->Targets) {
            UNIT_ASSERT_VALUES_EQUAL_C(target.SourcePaths.size(), 1u,
                "each copy target must have exactly one source, got "
                    << target.SourcePaths.size() << " for " << target.Path);
            dstToSrc[target.Path] = target.SourcePaths[0];
        }
        UNIT_ASSERT_C(dstToSrc.contains("Dst1"), "missing Dst1 in " << AllTargetPaths(*found));
        UNIT_ASSERT_C(dstToSrc.contains("Dst2"), "missing Dst2 in " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst1"], "Src1",
            "Dst1's target must record Src1 as its source");
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst2"], "Src2",
            "Dst2's target must record Src2 as its source");
    }

    // Guards against the repeated field silently turning every record into a
    // list of one-or-more by accident: a plain single-target op must still
    // yield exactly one target, with an empty SourcePaths (a create has no
    // source, it is a repeated field so "empty" is the correct assertion,
    // not "unset").
    Y_UNIT_TEST(SingleTargetOpRecordsExactlyOneTarget) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u,
            "a single-target op must yield exactly one target entry, got " << found->Targets.size());
        UNIT_ASSERT_VALUES_EQUAL(found->Targets[0].Path, "Table1");
        UNIT_ASSERT_C(found->Targets[0].SourcePaths.empty(),
            "a plain create has no source, SourcePaths must be empty");
    }

    // TMove carries SrcPath/DstPath, no `Name` field at all, so a plain
    // reflection-based lookup for "Name" finds nothing and the propose was
    // being refused outright (see git history / diagnosis notes). A rename
    // must be accepted and must record the object's new location (DstPath,
    // since that is where a consumer would look the object up afterwards)
    // as Path, and its old location as the sole entry in SourcePaths.
    Y_UNIT_TEST(MoveRecordsDestinationAndSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "MoveSrc"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/MoveSrc", "/MyRoot/MoveDst");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "MOVE TABLE entry not found -- the propose must not be refused");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "MoveDst",
            "Path must be the object's new (destination) location, database-relative");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].SourcePaths.size(), 1u,
            "a rename has exactly one source, got " << found->Targets[0].SourcePaths.size());
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].SourcePaths[0], "MoveSrc",
            "SourcePaths must record the object's old (source) location, database-relative");
    }

    // Altering the database root itself: WorkingDir="/", Name="MyRoot"
    // targets /MyRoot, which TTestEnv sets up as the domain root -- so
    // RelativeToDomain collapses the resolved path down to the empty string,
    // per its documented contract (a domain root's PathString() equals the
    // domain prefix). This must succeed and must not carry an approximate
    // or non-empty path.
    Y_UNIT_TEST(AlterDatabaseRootItselfStoresEmptyPath) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        runtime.GetAppData().FeatureFlags.SetEnableAlterDatabase(true);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterSubDomain(runtime, ++txId, "/", R"(
            Name: "MyRoot"
            SchemeLimits {
                MaxPaths: 1000
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterSubDomain) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ALTER SUBDOMAIN(root) entry not found -- altering the root must not be refused");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "",
            "the database root's own path, relative to itself, must be the empty string");
    }

    // CreateIndexedTable already has a special case in
    // ExtractSchemeChangeTargetName (returns TableDescription.Name directly),
    // bypassing the reflection loop entirely. Confirm it actually produces
    // exactly one record and that the stored path is the table, not an
    // index impl table synthesized during the same user-level tx.
    Y_UNIT_TEST(CreateIndexedTableWritesExactlyOneRecordForTheTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "IndexedTable1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "byValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 indexedTableRecords = 0;
        TVector<TString> storedPaths;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
                ++indexedTableRecords;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                storedPaths.push_back(e.Targets[0].Path);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(indexedTableRecords, 1u,
            "CreateIndexedTable must write exactly one record for the whole user-level tx");
        UNIT_ASSERT_VALUES_EQUAL_C(storedPaths[0], "IndexedTable1",
            "the stored path must be the table itself, not an index impl table");
    }
}
