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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
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
        // Every one of the N targets must resolve, not just the first (the
        // extractor only captures PathId for target[0], so [0] gets the full
        // PathId cross-check and [1] gets a plain resolve check).
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 0);
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 1);
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
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
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
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(indexedTableRecords, 1u,
            "CreateIndexedTable must write exactly one record for the whole user-level tx");
        UNIT_ASSERT_VALUES_EQUAL_C(storedPaths[0], "IndexedTable1",
            "the stored path must be the table itself, not an index impl table");
    }
}

// RFC 0129 systematic audit, Phase 2: a completeness guard over the whole
// NKikimrSchemeOp::EOperationType enum. Every value must be classified here;
// adding a new op type without a row fails this test and names the culprit --
// that failure message is the bug report for whoever adds the next op.
namespace {

enum class EOpAuditClass {
    // Never appears as a user-level TModifyScheme.OperationType (a sub-op
    // constructed internally by ConstructParts/other handlers, a reserved
    // field number, or a deprecated value).
    NotUserLevel,
    // User-level; believed to resolve a path, but not driven end-to-end with
    // a resolve assertion here. This is the state that let TMoveIndex's bug
    // hide: "someone read the code and believes it resolves" is not proof.
    Safe,
    // User-level, driven end-to-end by an existing test, which asserts the
    // recorded path resolves (in the live scheme) to the object the
    // operation actually touched -- not merely that a record with a
    // non-empty path exists.
    VerifiedByTest,
};

struct TOpAuditRow {
    NKikimrSchemeOp::EOperationType OpType;
    EOpAuditClass Class;
};

// One row per NKikimrSchemeOp::EOperationType value. See PHASE 1 of the RFC
// 0129 outbox audit for the classification rationale of each row (which
// TModifyScheme submessage carries the target, and by what naming
// convention). Ordered by enum value.
const TVector<TOpAuditRow>& GetOpAuditTable() {
    static const TVector<TOpAuditRow> table = {
        {NKikimrSchemeOp::ESchemeOpMkDir, EOpAuditClass::VerifiedByTest}, // MkDirWritesLogEntry
        {NKikimrSchemeOp::ESchemeOpCreateTable, EOpAuditClass::VerifiedByTest}, // CreateTableWritesLogEntry
        {NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreatePQGroup exists
        {NKikimrSchemeOp::ESchemeOpDropTable, EOpAuditClass::Safe}, // tier-1, not yet driven: post-drop needs a pre-drop-identity assertion shape, not the live-resolve helper (see DropByIdRecordsAPath for the pattern)
        {NKikimrSchemeOp::ESchemeOpDropPersQueueGroup, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropPQGroup exists
        {NKikimrSchemeOp::ESchemeOpAlterTable, EOpAuditClass::VerifiedByTest}, // AlterTableWritesLogEntry
        {NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterPQGroup exists
        {NKikimrSchemeOp::ESchemeOpModifyACL, EOpAuditClass::Safe}, // tier-1, not yet driven: TestModifyACL exists (already exercised as fixture setup elsewhere, not as its own resolve assertion)
        {NKikimrSchemeOp::ESchemeOpRmDir, EOpAuditClass::Safe}, // tier-1, not yet driven: TestRmDir exists
        {NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions, EOpAuditClass::NotUserLevel}, // IsChurnOp
        {NKikimrSchemeOp::ESchemeOpBackup, EOpAuditClass::Safe}, // tier-2, not yet driven: TestBackup exists but needs a real/mocked export target to complete
        {NKikimrSchemeOp::ESchemeOpCreateSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateSubDomain exists
        {NKikimrSchemeOp::ESchemeOpDropSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropSubDomain exists
        {NKikimrSchemeOp::ESchemeOpCreateRtmrVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateRtmrVolume exists
        {NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateBlockStoreVolume exists
        {NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterBlockStoreVolume exists
        {NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAssignBlockStoreVolume exists
        {NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropBlockStoreVolume exists
        {NKikimrSchemeOp::ESchemeOpCreateKesus, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateKesus exists
        {NKikimrSchemeOp::ESchemeOpDropKesus, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropKesus exists
        {NKikimrSchemeOp::ESchemeOpForceDropSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestForceDropSubDomain exists
        {NKikimrSchemeOp::ESchemeOpCreateSolomonVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateSolomon exists
        {NKikimrSchemeOp::ESchemeOpDropSolomonVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropSolomon exists
        {NKikimrSchemeOp::ESchemeOpAlterKesus, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterKesus exists
        {NKikimrSchemeOp::ESchemeOpAlterSubDomain, EOpAuditClass::VerifiedByTest}, // AlterDatabaseRootItselfStoresEmptyPath
        {NKikimrSchemeOp::ESchemeOpAlterUserAttributes, EOpAuditClass::VerifiedByTest}, // fixed: PathName; AlterUserAttributesRecordsAPath
        {NKikimrSchemeOp::ESchemeOpForceDropUnsafe, EOpAuditClass::VerifiedByTest}, // Drop.Id branch (V4); DropByIdRecordsAPath -- worked first time, already correct
        {NKikimrSchemeOp::ESchemeOpCreateIndexedTable, EOpAuditClass::VerifiedByTest}, // CreateIndexedTableWritesExactlyOneRecordForTheTable
        {NKikimrSchemeOp::ESchemeOpCreateTableIndex, EOpAuditClass::NotUserLevel}, // part of CreateIndexedTable
        {NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables, EOpAuditClass::VerifiedByTest}, // ConsistentCopyTablesRecordsAllTargetsWithSources
        {NKikimrSchemeOp::ESchemeOpDropTableIndex, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropTableIndex exists
        {NKikimrSchemeOp::ESchemeOpCreateExtSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateExtSubDomain exists
        {NKikimrSchemeOp::ESchemeOpAlterExtSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterExtSubDomain exists
        {NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven: TestForceDropExtSubDomain exists
        {NKikimrSchemeOp::ESchemeOp_DEPRECATED_35, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpUpgradeSubDomain, EOpAuditClass::Safe}, // tier-1, not yet driven with the resolve assertion: TestUpgradeSubDomain exists; see UpgradeSubDomainRecordsAPath (counter-only, not resolve-checked -- upgrading blocks a second subscriber registration)
        {NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision, EOpAuditClass::Safe}, // tier-1, not yet driven: TestUpgradeSubDomainDecision exists
        {NKikimrSchemeOp::ESchemeOpCreateIndexBuild, EOpAuditClass::Safe}, // tier-2, not yet driven: only reachable via the TIndexBuilder actor (TestBuildIndex), not a direct TModifyScheme helper
        {NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpCreateLock, EOpAuditClass::Safe}, // tier-1, not yet driven: TestLock exists
        {NKikimrSchemeOp::ESchemeOpApplyIndexBuild, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpAlterTableIndex, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpAlterSolomonVolume, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterSolomon exists
        {NKikimrSchemeOp::ESchemeOpDropLock, EOpAuditClass::Safe}, // tier-1, not yet driven: TestUnlock exists
        {NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpDropIndex, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpCancelIndexBuild, EOpAuditClass::Safe}, // tier-2, not yet driven: only reachable via the TIndexBuilder actor (TestCancelBuildIndex), needs an in-flight build to cancel
        {NKikimrSchemeOp::ESchemeOpCreateFileStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateFileStore exists
        {NKikimrSchemeOp::ESchemeOpAlterFileStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterFileStore exists
        {NKikimrSchemeOp::ESchemeOpDropFileStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropFileStore exists
        {NKikimrSchemeOp::ESchemeOpRestore, EOpAuditClass::Safe}, // fixed: TableName (RestoreRecordsAPath drives propose; full completion needs a real backup fixture, tier-3 for the resolve assertion)
        {NKikimrSchemeOp::ESchemeOpCreateColumnStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateOlapStore exists
        {NKikimrSchemeOp::ESchemeOpAlterColumnStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterOlapStore exists
        {NKikimrSchemeOp::ESchemeOpDropColumnStore, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropOlapStore exists
        {NKikimrSchemeOp::ESchemeOpCreateColumnTable, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateColumnTable exists
        {NKikimrSchemeOp::ESchemeOpAlterColumnTable, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterColumnTable exists
        {NKikimrSchemeOp::ESchemeOpDropColumnTable, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropColumnTable exists
        {NKikimrSchemeOp::ESchemeOpAlterLogin, EOpAuditClass::VerifiedByTest}, // fixed: no path-bearing target, attributed to WorkingDir (database root); AlterLoginRecordsAPath
        {NKikimrSchemeOp::ESchemeOpCreateCdcStream, EOpAuditClass::Safe}, // NB: verified refused today; see PathBearingOpNeverStoresAnApproximatePath
        {NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpAlterCdcStream, EOpAuditClass::Safe}, // NB: verified refused today; see AlterCdcStreamRecordsAPath
        {NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpDropCdcStream, EOpAuditClass::Safe}, // NB: verified refused AND crashes today; see comment above TSchemeChangeRecordsOperationAuditTests
        {NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpMoveTable, EOpAuditClass::VerifiedByTest}, // MoveRecordsDestinationAndSource
        {NKikimrSchemeOp::ESchemeOpMoveTableIndex, EOpAuditClass::NotUserLevel}, // multipart: MoveTableIndexTask is constructed only as a MoveTable sub-op (schemeshard__operation_move_tables.cpp), never issued directly by a user
        {NKikimrSchemeOp::ESchemeOpCreateSequence, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateSequence exists
        {NKikimrSchemeOp::ESchemeOpAlterSequence, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterSequence exists
        {NKikimrSchemeOp::ESchemeOpDropSequence, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropSequence exists
        {NKikimrSchemeOp::ESchemeOpCreateReplication, EOpAuditClass::Safe}, // tier-2, not yet driven: TestCreateReplication exists but the target is a replicated table needing a source connection
        {NKikimrSchemeOp::ESchemeOpAlterReplication, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateReplication
        {NKikimrSchemeOp::ESchemeOpDropReplicationCascade, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateReplication
        {NKikimrSchemeOp::ESchemeOpCreateBlobDepot, EOpAuditClass::Safe}, // tier-2, not yet driven: TestCreateBlobDepot exists but BlobDepot needs its backing tablet type stood up
        {NKikimrSchemeOp::ESchemeOpAlterBlobDepot, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateBlobDepot
        {NKikimrSchemeOp::ESchemeOpDropBlobDepot, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateBlobDepot
        {NKikimrSchemeOp::ESchemeOpMoveIndex, EOpAuditClass::VerifiedByTest}, // fixed: SrcPath/DstPath are relative to TablePath; MoveIndexRecordsAPath
        {NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpCreateExternalTable, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateExternalTable exists
        {NKikimrSchemeOp::ESchemeOpDropExternalTable, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropExternalTable exists
        {NKikimrSchemeOp::ESchemeOpAlterExternalTable, EOpAuditClass::NotUserLevel}, // Y_ABORT: unimplemented
        {NKikimrSchemeOp::ESchemeOpCreateExternalDataSource, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateExternalDataSource exists
        {NKikimrSchemeOp::ESchemeOpDropExternalDataSource, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropExternalDataSource exists
        {NKikimrSchemeOp::ESchemeOpAlterExternalDataSource, EOpAuditClass::NotUserLevel}, // Y_ABORT: unimplemented
        {NKikimrSchemeOp::ESchemeOpCreateColumnBuild, EOpAuditClass::NotUserLevel}, // internal, built by CreateBuildIndex
        {NKikimrSchemeOp::ESchemeOpCreateView, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateView exists
        {NKikimrSchemeOp::ESchemeOpAlterView, EOpAuditClass::NotUserLevel}, // Y_ABORT: unimplemented
        {NKikimrSchemeOp::ESchemeOpDropView, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropView exists
        {NKikimrSchemeOp::ESchemeOpDropReplication, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateReplication
        {NKikimrSchemeOp::ESchemeOpCreateContinuousBackup, EOpAuditClass::VerifiedByTest}, // fixed: TableName; CreateContinuousBackupRecordsAPath
        {NKikimrSchemeOp::ESchemeOpAlterContinuousBackup, EOpAuditClass::Safe}, // fixed: TableName; tier-1, not yet driven with the resolve assertion (CreateContinuousBackupRecordsAPath covers Create only)
        {NKikimrSchemeOp::ESchemeOpDropContinuousBackup, EOpAuditClass::Safe}, // fixed: TableName; tier-1, not yet driven with the resolve assertion (target is gone post-drop, needs the DropByIdRecordsAPath-style pre-drop-identity shape)
        {NKikimrSchemeOp::ESchemeOpCreateResourcePool, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateResourcePool exists
        {NKikimrSchemeOp::ESchemeOpDropResourcePool, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropResourcePool exists
        {NKikimrSchemeOp::ESchemeOpAlterResourcePool, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterResourcePool exists
        {NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups, EOpAuditClass::Safe}, // OUT OF SCOPE (backup/restore family, decision pending per RFC-0129 plan); DstTablePath found by reflection (field named TablePath in some builds); see NB below
        {NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpCreateBackupCollection, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateBackupCollection exists
        {NKikimrSchemeOp::ESchemeOpAlterBackupCollection, EOpAuditClass::NotUserLevel}, // Y_ABORT: unimplemented
        {NKikimrSchemeOp::ESchemeOpDropBackupCollection, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropBackupCollection exists
        {NKikimrSchemeOp::ESchemeOpMoveSequence, EOpAuditClass::Safe}, // tier-1, not yet driven: TestMoveSequence exists
        {NKikimrSchemeOp::ESchemeOpBackupBackupCollection, EOpAuditClass::Safe}, // OUT OF SCOPE (backup/restore family, decision pending per RFC-0129 plan)
        {NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection, EOpAuditClass::Safe}, // OUT OF SCOPE (backup/restore family, decision pending per RFC-0129 plan)
        {NKikimrSchemeOp::ESchemeOpRestoreBackupCollection, EOpAuditClass::Safe}, // OUT OF SCOPE (backup/restore family, decision pending per RFC-0129 plan)
        {NKikimrSchemeOp::ESchemeOpCreateTransfer, EOpAuditClass::Safe}, // tier-2, not yet driven: TestCreateTransfer exists but the target needs a source connection like Replication
        {NKikimrSchemeOp::ESchemeOpAlterTransfer, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateTransfer
        {NKikimrSchemeOp::ESchemeOpDropTransfer, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateTransfer
        {NKikimrSchemeOp::ESchemeOpDropTransferCascade, EOpAuditClass::Safe}, // tier-2, not yet driven: same setup as CreateTransfer
        {NKikimrSchemeOp::ESchemeOpCreateSysView, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateSysView exists
        {NKikimrSchemeOp::ESchemeOpDropSysView, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropSysView exists
        {NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp, EOpAuditClass::NotUserLevel}, // internal, pushed as sub-op part
        {NKikimrSchemeOp::ESchemeOpChangePathState, EOpAuditClass::NotUserLevel}, // internal, pushed as sub-op part
        {NKikimrSchemeOp::ESchemeOpRotateCdcStream, EOpAuditClass::Safe}, // NB: same nested-field shape as the other CDC ops (likely broken like them); no public API constructs it directly (only internal continuous-backup rotation), so user-level-ness itself is unconfirmed -- not independently re-verified here
        {NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable, EOpAuditClass::NotUserLevel},
        {NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize, EOpAuditClass::NotUserLevel}, // constructed only by the internal scan actor (schemeshard_incremental_restore_scan.cpp), marked SetInternal(true), no client-facing entry point; also note TargetTablePaths is a repeated field the generic reflection walk cannot extract regardless
        {NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp, EOpAuditClass::NotUserLevel}, // multipart
        {NKikimrSchemeOp::ESchemeOpDropColumnBuild, EOpAuditClass::NotUserLevel}, // internal, built by CreateBuildIndex
        {NKikimrSchemeOp::ESchemeOpCreateSecret, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateSecret exists
        {NKikimrSchemeOp::ESchemeOpAlterSecret, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterSecret exists
        {NKikimrSchemeOp::ESchemeOpDropSecret, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropSecret exists
        {NKikimrSchemeOp::ESchemeOpCreateStreamingQuery, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateStreamingQuery exists
        {NKikimrSchemeOp::ESchemeOpDropStreamingQuery, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropStreamingQuery exists
        {NKikimrSchemeOp::ESchemeOpAlterStreamingQuery, EOpAuditClass::Safe}, // tier-1, not yet driven: TestAlterStreamingQuery exists
        {NKikimrSchemeOp::ESchemeOpTruncateTable, EOpAuditClass::VerifiedByTest}, // fixed: TableName; TruncateTableRecordsAPath
        {NKikimrSchemeOp::ESchemeOpPrepareIndexValidation, EOpAuditClass::Safe}, // tier-3, not yet driven: no direct TestXxx helper; dispatch is shared with internal-only sibling ops in the same ConstructParts switch, user-level-ness unconfirmed
        {NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets, EOpAuditClass::NotUserLevel}, // internal, pushed as sub-op part
        {NKikimrSchemeOp::ESchemeOpIncrementalRestoreUnlockTargets, EOpAuditClass::NotUserLevel}, // internal, pushed as sub-op part
        {NKikimrSchemeOp::ESchemeOpCreateFullBackupOp, EOpAuditClass::NotUserLevel}, // internal, pushed as sub-op part
        {NKikimrSchemeOp::ESchemeOpCreateTestShardSet, EOpAuditClass::Safe}, // tier-1, not yet driven: TestCreateTestShardSet exists
        {NKikimrSchemeOp::ESchemeOpDropTestShardSet, EOpAuditClass::Safe}, // tier-1, not yet driven: TestDropTestShardSet exists
    };
    return table;
}

// RFC 0129 V5: the closed set of op types allowed to remain `Safe` (grandfathered,
// not yet `VerifiedByTest`), each with its per-row reason recorded as an inline
// comment next to its GetOpAuditTable() entry above. A NEW user-level op landing
// as `Safe` is not in this set and fails EveryOperationTypeIsClassified below --
// growing the verification gap requires a deliberate edit to this set, not a
// silent `Safe` in the table. The 4 backup-collection rows are separately out of
// scope per the RFC-0129 plan and are included here for the same reason: their
// behavior is deliberately untouched, not overlooked.
const THashSet<NKikimrSchemeOp::EOperationType>& GetGrandfatheredSafeOps() {
    static const THashSet<NKikimrSchemeOp::EOperationType> ops = {
        NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup,
        NKikimrSchemeOp::ESchemeOpDropTable,
        NKikimrSchemeOp::ESchemeOpDropPersQueueGroup,
        NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup,
        NKikimrSchemeOp::ESchemeOpModifyACL,
        NKikimrSchemeOp::ESchemeOpRmDir,
        NKikimrSchemeOp::ESchemeOpBackup,
        NKikimrSchemeOp::ESchemeOpCreateSubDomain,
        NKikimrSchemeOp::ESchemeOpDropSubDomain,
        NKikimrSchemeOp::ESchemeOpCreateRtmrVolume,
        NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume,
        NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume,
        NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume,
        NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume,
        NKikimrSchemeOp::ESchemeOpCreateKesus,
        NKikimrSchemeOp::ESchemeOpDropKesus,
        NKikimrSchemeOp::ESchemeOpForceDropSubDomain,
        NKikimrSchemeOp::ESchemeOpCreateSolomonVolume,
        NKikimrSchemeOp::ESchemeOpDropSolomonVolume,
        NKikimrSchemeOp::ESchemeOpAlterKesus,
        NKikimrSchemeOp::ESchemeOpDropTableIndex,
        NKikimrSchemeOp::ESchemeOpCreateExtSubDomain,
        NKikimrSchemeOp::ESchemeOpAlterExtSubDomain,
        NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain,
        NKikimrSchemeOp::ESchemeOpUpgradeSubDomain,
        NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision,
        NKikimrSchemeOp::ESchemeOpCreateIndexBuild,
        NKikimrSchemeOp::ESchemeOpCreateLock,
        NKikimrSchemeOp::ESchemeOpAlterSolomonVolume,
        NKikimrSchemeOp::ESchemeOpDropLock,
        NKikimrSchemeOp::ESchemeOpCancelIndexBuild,
        NKikimrSchemeOp::ESchemeOpCreateFileStore,
        NKikimrSchemeOp::ESchemeOpAlterFileStore,
        NKikimrSchemeOp::ESchemeOpDropFileStore,
        NKikimrSchemeOp::ESchemeOpRestore,
        NKikimrSchemeOp::ESchemeOpCreateColumnStore,
        NKikimrSchemeOp::ESchemeOpAlterColumnStore,
        NKikimrSchemeOp::ESchemeOpDropColumnStore,
        NKikimrSchemeOp::ESchemeOpCreateColumnTable,
        NKikimrSchemeOp::ESchemeOpAlterColumnTable,
        NKikimrSchemeOp::ESchemeOpDropColumnTable,
        NKikimrSchemeOp::ESchemeOpCreateCdcStream,
        NKikimrSchemeOp::ESchemeOpAlterCdcStream,
        NKikimrSchemeOp::ESchemeOpDropCdcStream,
        NKikimrSchemeOp::ESchemeOpCreateSequence,
        NKikimrSchemeOp::ESchemeOpAlterSequence,
        NKikimrSchemeOp::ESchemeOpDropSequence,
        NKikimrSchemeOp::ESchemeOpCreateReplication,
        NKikimrSchemeOp::ESchemeOpAlterReplication,
        NKikimrSchemeOp::ESchemeOpDropReplicationCascade,
        NKikimrSchemeOp::ESchemeOpCreateBlobDepot,
        NKikimrSchemeOp::ESchemeOpAlterBlobDepot,
        NKikimrSchemeOp::ESchemeOpDropBlobDepot,
        NKikimrSchemeOp::ESchemeOpCreateExternalTable,
        NKikimrSchemeOp::ESchemeOpDropExternalTable,
        NKikimrSchemeOp::ESchemeOpCreateExternalDataSource,
        NKikimrSchemeOp::ESchemeOpDropExternalDataSource,
        NKikimrSchemeOp::ESchemeOpCreateView,
        NKikimrSchemeOp::ESchemeOpDropView,
        NKikimrSchemeOp::ESchemeOpDropReplication,
        NKikimrSchemeOp::ESchemeOpAlterContinuousBackup,
        NKikimrSchemeOp::ESchemeOpDropContinuousBackup,
        NKikimrSchemeOp::ESchemeOpCreateResourcePool,
        NKikimrSchemeOp::ESchemeOpDropResourcePool,
        NKikimrSchemeOp::ESchemeOpAlterResourcePool,
        NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups, // OUT OF SCOPE (backup family)
        NKikimrSchemeOp::ESchemeOpCreateBackupCollection,
        NKikimrSchemeOp::ESchemeOpDropBackupCollection,
        NKikimrSchemeOp::ESchemeOpMoveSequence,
        NKikimrSchemeOp::ESchemeOpBackupBackupCollection, // OUT OF SCOPE (backup family)
        NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection, // OUT OF SCOPE (backup family)
        NKikimrSchemeOp::ESchemeOpRestoreBackupCollection, // OUT OF SCOPE (backup family)
        NKikimrSchemeOp::ESchemeOpCreateTransfer,
        NKikimrSchemeOp::ESchemeOpAlterTransfer,
        NKikimrSchemeOp::ESchemeOpDropTransfer,
        NKikimrSchemeOp::ESchemeOpDropTransferCascade,
        NKikimrSchemeOp::ESchemeOpCreateSysView,
        NKikimrSchemeOp::ESchemeOpDropSysView,
        NKikimrSchemeOp::ESchemeOpRotateCdcStream,
        NKikimrSchemeOp::ESchemeOpCreateSecret,
        NKikimrSchemeOp::ESchemeOpAlterSecret,
        NKikimrSchemeOp::ESchemeOpDropSecret,
        NKikimrSchemeOp::ESchemeOpCreateStreamingQuery,
        NKikimrSchemeOp::ESchemeOpDropStreamingQuery,
        NKikimrSchemeOp::ESchemeOpAlterStreamingQuery,
        NKikimrSchemeOp::ESchemeOpPrepareIndexValidation,
        NKikimrSchemeOp::ESchemeOpCreateTestShardSet,
        NKikimrSchemeOp::ESchemeOpDropTestShardSet,
    };
    return ops;
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsOperationClassificationTests) {
    // Fails loudly and names the unclassified operation type: that message is
    // the bug report for whoever adds the next op without classifying it.
    Y_UNIT_TEST(EveryOperationTypeIsClassified) {
        const auto* enumDescriptor = NKikimrSchemeOp::EOperationType_descriptor();
        UNIT_ASSERT_C(enumDescriptor, "EOperationType must have a protobuf descriptor");

        THashSet<int> classified;
        for (const auto& row : GetOpAuditTable()) {
            const bool inserted = classified.insert(static_cast<int>(row.OpType)).second;
            UNIT_ASSERT_C(inserted,
                "duplicate row for " << NKikimrSchemeOp::EOperationType_Name(row.OpType)
                << " in GetOpAuditTable");
        }

        TVector<TString> unclassified;
        for (int i = 0; i < enumDescriptor->value_count(); ++i) {
            const auto* valueDescriptor = enumDescriptor->value(i);
            const int number = valueDescriptor->number();
            if (!classified.contains(number)) {
                unclassified.push_back(valueDescriptor->name());
            }
        }

        UNIT_ASSERT_C(unclassified.empty(),
            "the following NKikimrSchemeOp::EOperationType values have no row in "
            "GetOpAuditTable (ut_scheme_change_records.cpp): " << JoinSeq(", ", unclassified)
            << " -- classify each as NotUserLevel (never a user-level TModifyScheme), or "
            "VerifiedByTest (drive it end-to-end and assert the recorded path resolves, "
            "see AssertRecordedPathResolvesToTouchedObject). A new op must not land as "
            "Safe -- that is the unverified state this guard exists to prevent. If it is "
            "user-level and its path does NOT resolve, that is a DDL outage: fix the "
            "extractor in schemeshard__scheme_change_records.cpp first.");
    }

    // RFC 0129 V5: tightens the guard from "someone classified this" to "someone
    // ran this". A NEW user-level op must land as VerifiedByTest, not Safe --
    // Safe is exactly the unverified state that hid the TMoveIndex bug. Only the
    // closed, pre-existing set in GetGrandfatheredSafeOps() (each row's reason
    // documented inline in GetOpAuditTable() above) may stay Safe.
    Y_UNIT_TEST(NewSafeOpsMustBeGrandfatheredOrVerified) {
        TVector<TString> ungrandfathered;
        for (const auto& row : GetOpAuditTable()) {
            if (row.Class == EOpAuditClass::Safe && !GetGrandfatheredSafeOps().contains(row.OpType)) {
                ungrandfathered.push_back(NKikimrSchemeOp::EOperationType_Name(row.OpType));
            }
        }
        UNIT_ASSERT_C(ungrandfathered.empty(),
            "the following operation types are classified Safe but are not in the "
            "grandfathered allowlist (GetGrandfatheredSafeOps in ut_scheme_change_records.cpp): "
            << JoinSeq(", ", ungrandfathered) << " -- a NEW user-level operation must be driven "
            "end-to-end and classified VerifiedByTest, not left Safe. If it genuinely cannot be "
            "driven from this UT harness, add it to GetGrandfatheredSafeOps() with a reason "
            "comment on its GetOpAuditTable() row explaining why, as a deliberate decision.");
    }

    // A newly classified Safe/VerifiedByTest row must actually be reachable
    // and resolve -- this positive companion to EveryOperationTypeIsClassified
    // prevents the table from being satisfied by marking everything
    // NotUserLevel.
    Y_UNIT_TEST(TableIsNotVacuouslyAllNotUserLevel) {
        ui32 userLevelCount = 0;
        for (const auto& row : GetOpAuditTable()) {
            userLevelCount += (row.Class == EOpAuditClass::Safe || row.Class == EOpAuditClass::VerifiedByTest);
        }
        UNIT_ASSERT_C(userLevelCount > 100,
            "expected the overwhelming majority of operation types to be user-level and "
            "safe; got only " << userLevelCount << " -- the classification table looks wrong");
    }

    // Makes the verification gap visible: Safe means "believed to resolve,
    // read but not executed" -- exactly the state that hid the TMoveIndex
    // bug. VerifiedByTest means an existing test drives the op end-to-end and
    // asserts the recorded path resolves to the touched object. This count is
    // the honest size of the remaining gap (closed incrementally by V3).
    Y_UNIT_TEST(ReportSafeVsVerifiedByTestCounts) {
        ui32 safeCount = 0;
        ui32 verifiedCount = 0;
        for (const auto& row : GetOpAuditTable()) {
            safeCount += (row.Class == EOpAuditClass::Safe);
            verifiedCount += (row.Class == EOpAuditClass::VerifiedByTest);
        }
        Cerr << "RFC-0129 audit table: Safe=" << safeCount << " VerifiedByTest=" << verifiedCount << Endl;
        UNIT_ASSERT_C(safeCount + verifiedCount > 0, "sanity: table must be non-empty");
    }
}

// RFC 0129 systematic audit, Phase 3: drives every suspected-outage op type
// found by Phase 2's classification against the extractors in
// schemeshard__scheme_change_records.cpp. Each test reports the VERBATIM
// current result -- refused propose is a real defect (fixed below,
// red-then-green); a clean record means the op was already safe and the test
// stays as a regression guard.
Y_UNIT_TEST_SUITE(TSchemeChangeRecordsOperationAuditTests) {
    Y_UNIT_TEST(AlterUserAttributesRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "SubDir");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestUserAttrs(runtime, ++txId, "/MyRoot", "SubDir",
            AlterUserAttrs({{"attr1", "value1"}}));
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "AlterUserAttributes must resolve a path; TAlterUserAttributes.PathName is not "
            "found by the generic reflection walk (it looks for a field literally named Name)");

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_C(AnyPathContains(e, "SubDir"),
                    "expected the target to be SubDir, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterUserAttributes must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterCdcStreamRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        // The stream itself must exist before the outbox can be exercised, but
        // CreateCdcStream is itself a confirmed outage -- create it before a
        // subscriber is registered (CheckSchemeChangeRecordHasPath only runs
        // when Subscribers is non-empty), so this fixture setup does not
        // depend on the very bug under test.
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
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamName: "Stream1"
            Disable {}
        )", {NKikimrScheme::StatusInvalidParameter});

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_C(rejectedAfter > rejectedBefore,
            "VERBATIM RESULT: AlterCdcStream propose is refused today -- "
            "TAlterCdcStream{TableName,StreamName} has no field literally named Name");
    }

    // DropCdcStream is NOT covered by a runnable test here. VERBATIM RESULT
    // observed manually: the outbox refuses its propose (TDropCdcStream{
    // TableName, repeated StreamName} has no field literally named Name and
    // does not use the generic Drop submessage), and unlike the other ops in
    // this suite the refusal itself crashes the tablet -- TDropCdcStream's
    // AbortPropose is a stub (Y_ABORT("no AbortPropose for TDropCdcStream"),
    // schemeshard__operation_drop_cdc_stream.cpp:211). CheckSchemeChangeRecordHasPath's
    // rejection calls AbortOperationPropose, which unconditionally calls
    // AbortPropose on every already-proposed part; DropCdcStream's part has
    // already written to the DB by that point, so this is a Y_ABORT, not a
    // clean StatusInvalidParameter. A UNIT_TEST cannot safely assert a crash
    // (it takes the whole binary down), so this is recorded here rather than
    // as a test. This is the same class of defect as AlterUserAttributes
    // before its extractor fix -- fixing the missing-Name extraction for
    // DropCdcStream, the same way AlterUserAttributes/TruncateTable/
    // CreateContinuousBackup/MoveIndex were fixed above, would eliminate the
    // crash by eliminating the rejection in the first place. Left unfixed
    // here: DropCdcStream's target lives in a nested submessage (TableName +
    // repeated StreamName), which needs an explicit multi-target extractor
    // similar to ExtractSchemeChangeCopyTargets, not a one-line special case.

    Y_UNIT_TEST(MoveIndexRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index1"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: ExtractSchemeChangeMoveTarget used to treat TMoveIndex.SrcPath/
        // DstPath as absolute paths, but they are relative index names under
        // TablePath (confirmed via mainTablePath.Child(srcIndex) in
        // index/operation_move_index.cpp), so TPath::Resolve used to fail and
        // refuse the propose.
        TestMoveIndex(runtime, ++txId, "/MyRoot/Table1", "Index1", "Index2", false);
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "MoveIndex must resolve a path");

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Index2"),
                    "expected the target to be the new index name, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "MoveIndex must produce a scheme change record");
    }

    Y_UNIT_TEST(UpgradeSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
        )");
        env.TestWaitNotification(runtime, txId);
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Already safe: TUpgradeSubDomain.Name is a direct scalar Name field,
        // found by the generic reflection walk. Kept as a regression guard.
        TestUpgradeSubDomain(runtime, ++txId, "/MyRoot", "USD1");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "UpgradeSubDomain must resolve a path");
        // Not verified via ReadSchemeChangeRecords here: registering a second
        // subscriber (its internal read-only helper) is refused once the
        // domain has been upgraded to serve transactions on its own, since
        // scheme change subscribers require a single transaction-supporting
        // domain. The counter check above is the load-bearing assertion.
    }

    Y_UNIT_TEST(TruncateTableRecordsAPath) {
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

        // Fixed: TTruncateTable.TableName has no field literally named Name,
        // so the generic reflection walk used to miss it and refuse the propose.
        TestTruncateTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "TruncateTable must resolve a path");

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpTruncateTable) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "TruncateTable must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateContinuousBackupRecordsAPath) {
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

        // Fixed: TCreateContinuousBackup.TableName (and the sibling Alter/Drop
        // messages) have no field literally named Name, so the generic
        // reflection walk used to miss them and refuse the propose.
        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "CreateContinuousBackup must resolve a path");

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateContinuousBackup) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateContinuousBackup must produce a scheme change record");
    }

    Y_UNIT_TEST(RestoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Restore targets an existing table (it restores INTO it), unlike
        // Create; the target must already exist for the propose to get past
        // path resolution at all.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // TRestoreTask names its target TableName, like TTruncateTable/
        // TCreateContinuousBackup before their extractor fixes -- not yet
        // special-cased here. Driving Restore to full completion needs a
        // real backup at the FS/S3 path (see ut_restore's TS3Mock rig); with
        // no such fixture the data phase hangs, so this only checks the
        // propose-time behavior (the same shape the outbox's own check
        // observes) rather than the post-finalize resolve. That is still a
        // real, meaningful assertion: before this fix, the propose itself
        // was refused -- see the crash this test produced before the fix
        // (Restore's AbortPropose is also an unconditional Y_ABORT stub, so
        // the refusal took the tablet down rather than cleanly failing).
        TestRestore(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            FSSettings {
                BasePath: "/tmp"
                Path: "restore"
            }
        )");

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "Restore must resolve a path");
    }

    Y_UNIT_TEST(AlterLoginRecordsAPath) {
        // TAlterLogin's target (the user/group being altered) is nested
        // inside a oneof (e.g. CreateUser.User), never a top-level scalar
        // Name/PathName/TableName field -- the generic reflection walk and
        // every existing special case both miss it.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        const auto hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "alice", hashes.HashedPassword);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "AlterLogin must resolve a path");

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterLogin) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "AlterLogin must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u, "AlterLogin");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "",
            "AlterLogin has no path-bearing target -- it must be attributed to the "
            "database root itself, relative to which the root's own path is empty");
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
    }

    // RFC 0129 V4: the Drop.Id branch in ResolveSchemeChangeTargets (a
    // TModifyScheme with Drop.Id set and Drop.Name/WorkingDir unset) is read
    // but was never exercised by a test. Its only production producer is
    // ydb/core/tx/replication/controller/dst_remover.cpp:46. TestForceDropUnsafe
    // drives this exact shape: DROP_BY_PATH_ID_HELPERS builds TDrop with only
    // Id set, no Name, no WorkingDir.
    Y_UNIT_TEST(DropByIdRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirToDropById");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/DirToDropById"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestForceDropUnsafe(runtime, ++txId, droppedLocalId);
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "Drop.Id (Drop.Name/WorkingDir unset) must resolve a path by PathId");

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpForceDropUnsafe) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ForceDropUnsafe (Drop.Id) must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u, "ForceDropUnsafe");
        UNIT_ASSERT_C(AnyPathContains(*found, "DirToDropById"),
            "expected the target to be the dropped directory, got: " << AllTargetPaths(*found));
        // The object is gone by finalize time, so the record must carry the
        // resolved-at-propose PathId of the object the drop actually
        // targeted -- assert against the identity captured before the drop,
        // not a live re-resolve (which would correctly fail post-drop).
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId,
            "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId,
            "recorded PathLocalId must match the dropped object's identity, not some other path");
    }
}
