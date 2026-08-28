#include "ut_scheme_change_records_helpers.h"

#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/schemeshard_counters.h>

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/printf.h>
#include <util/string/join.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSubscriberTests) {
    Y_UNIT_TEST(MockBackupSubscriberEndToEnd) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backup:collection:1", regHandle);

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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "backup:collection:1", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        // Should have at least 4 entries (CREATE, ALTER, CREATE, DROP)
        UNIT_ASSERT_C(fetch->Record.EntriesSize() >= 4,
            "Expected >= 4 entries, got " << fetch->Record.EntriesSize());

        // Verify order is monotonic
        for (int i = 1; i < (int)fetch->Record.EntriesSize(); ++i) {
            UNIT_ASSERT(fetch->Record.GetEntries(i).GetOrder() >
                        fetch->Record.GetEntries(i-1).GetOrder());
        }

        // ACK all
        ui64 lastOrder = fetch->Record.EntriesSize() > 0
            ? fetch->Record.GetEntries(fetch->Record.EntriesSize() - 1).GetOrder()
            : 0;
        TAutoPtr<IEventHandle> ackHandle;
        auto ack = AckSchemeChangeRecords(runtime, "backup:collection:1", lastOrder, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)ack->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        // Fetch again - should be empty
        TAutoPtr<IEventHandle> fetch2Handle;
        auto fetch2 = FetchSchemeChangeRecords(runtime, "backup:collection:1", lastOrder, 100, fetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL(fetch2->Record.EntriesSize(), 0);
        UNIT_ASSERT(!fetch2->Record.GetHasMore());
    }

    Y_UNIT_TEST(MockBackupSubscriberPaginatedFetch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backup:sub", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        ui64 cursor = 0;
        ui32 totalFetched = 0;
        while (true) {
            TAutoPtr<IEventHandle> fetchHandle;
            auto fetch = FetchSchemeChangeRecords(runtime, "backup:sub", cursor, 2, fetchHandle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
            if (fetch->Record.EntriesSize() == 0) {
                break;
            }
            totalFetched += fetch->Record.EntriesSize();
            cursor = fetch->Record.EntriesSize() > 0
                ? fetch->Record.GetEntries(fetch->Record.EntriesSize() - 1).GetOrder()
                : cursor;
            TAutoPtr<IEventHandle> ackHandle;
            AckSchemeChangeRecords(runtime, "backup:sub", cursor, ackHandle);
            if (!fetch->Record.GetHasMore()) {
                break;
            }
        }

        UNIT_ASSERT(totalFetched >= 5);
    }

    Y_UNIT_TEST(TwoSubscribersIndependentConsumption) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> reg1Handle, reg2Handle;
        RegisterSubscriber(runtime, "backup:collection:1", reg1Handle);
        RegisterSubscriber(runtime, "audit:system", reg2Handle);

        for (int i = 1; i <= 3; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        // backup fetches and acks all
        TAutoPtr<IEventHandle> backupFetchHandle;
        auto backupFetch = FetchSchemeChangeRecords(runtime, "backup:collection:1", 0, 100, backupFetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)backupFetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(backupFetch->Record.EntriesSize() >= 3);
        ui64 backupLastOrder = backupFetch->Record.EntriesSize() > 0
            ? backupFetch->Record.GetEntries(backupFetch->Record.EntriesSize() - 1).GetOrder()
            : 0;
        TAutoPtr<IEventHandle> backupAckHandle;
        AckSchemeChangeRecords(runtime, "backup:collection:1", backupLastOrder, backupAckHandle);

        // audit fetches but only acks first entry
        TAutoPtr<IEventHandle> auditFetchHandle;
        auto auditFetch = FetchSchemeChangeRecords(runtime, "audit:system", 0, 100, auditFetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)auditFetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(auditFetch->Record.EntriesSize() >= 3);
        ui64 auditFirstOrder = auditFetch->Record.GetEntries(0).GetOrder();
        TAutoPtr<IEventHandle> auditAckHandle;
        AckSchemeChangeRecords(runtime, "audit:system", auditFirstOrder, auditAckHandle);

        // backup should have nothing new
        TAutoPtr<IEventHandle> backupFetch2Handle;
        auto backupFetch2 = FetchSchemeChangeRecords(runtime, "backup:collection:1", backupLastOrder, 100, backupFetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL(backupFetch2->Record.EntriesSize(), 0);

        // audit should still have remaining entries
        TAutoPtr<IEventHandle> auditFetch2Handle;
        auto auditFetch2 = FetchSchemeChangeRecords(runtime, "audit:system", auditFirstOrder, 100, auditFetch2Handle);
        UNIT_ASSERT(auditFetch2->Record.EntriesSize() > 0);
    }

    Y_UNIT_TEST(ForceAdvanceSubscriber) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> advHandle;
        auto result = ForceAdvanceSubscriber(runtime, "stuck:sub", advHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(result->Record.GetLastAckedOrder() > 0);

        // Fetch should return empty (cursor is at tail)
        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "stuck:sub", result->Record.GetLastAckedOrder(), 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 0);
    }

    Y_UNIT_TEST(ForceAdvanceUnknownSubscriberReturnsError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        TAutoPtr<IEventHandle> advHandle;
        ForceAdvanceSubscriberExpect(runtime, "nonexistent:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, advHandle);
    }

    Y_UNIT_TEST(FetchReturnsMetadataWithoutBody) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "meta:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "meta:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 1);
        for (int i = 0; i < (int)fetch->Record.EntriesSize(); ++i) {
            const auto& entry = fetch->Record.GetEntries(i);
            UNIT_ASSERT_C(entry.GetBodySizeBytes() > 0,
                "Metadata should include non-zero BodySizeBytes for entry " << entry.GetOrder());
        }
    }

    Y_UNIT_TEST(FetchBodiesReturnsRequestedBodies) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "body:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "body:sub", 0, 100, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 1);

        TVector<ui64> orders;
        for (int i = 0; i < (int)fetch->Record.EntriesSize(); ++i) {
            orders.push_back(fetch->Record.GetEntries(i).GetOrder());
        }

        TAutoPtr<IEventHandle> bodiesHandle;
        auto bodies = FetchSchemeChangeRecordBodies(runtime, "body:sub", orders, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.EntriesSize(), orders.size());
        for (int i = 0; i < (int)bodies->Record.EntriesSize(); ++i) {
            UNIT_ASSERT(!bodies->Record.GetEntries(i).GetBody().empty());
        }
    }

    Y_UNIT_TEST(FetchBodiesUnregisteredSubscriberRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        TVector<ui64> orders = {1, 2, 3};
        TAutoPtr<IEventHandle> bodiesHandle;
        FetchSchemeChangeRecordBodiesExpect(runtime, "ghost:sub", orders,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, bodiesHandle);
    }

    Y_UNIT_TEST(FetchUnknownSubscriberReturnsNotRegistered) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        TAutoPtr<IEventHandle> fetchHandle;
        FetchSchemeChangeRecordsExpect(runtime, "ghost:sub", 0, 100,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, fetchHandle);
    }

    Y_UNIT_TEST(AckDeletesAckedRecordsInline) {
        // Ack tx deletes records in the same transaction; no background sweep needed.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "cleanup:sub", regHandle);

        for (int i = 1; i <= 3; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "cleanup:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 3);
        ui64 lastOrder = fetch->Record.EntriesSize() > 0
            ? fetch->Record.GetEntries(fetch->Record.EntriesSize() - 1).GetOrder()
            : 0;

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "cleanup:sub", lastOrder, ackHandle);

        // Records must be gone as a side effect of the ack tx — no wakeup needed.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.empty(),
            "Records should be deleted inline by ack, got " << entries.size());
    }

    Y_UNIT_TEST(FetchResultHasNoTailOrderField) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "tail:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "tail:sub", 0, 100, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() > 0);

        // Tail is computed from entries, not a separate field
        ui64 tailOrder = fetch->Record.GetEntries(fetch->Record.EntriesSize() - 1).GetOrder();
        UNIT_ASSERT(tailOrder > 0);

        // Ack using entries-derived tail works end-to-end
        TAutoPtr<IEventHandle> ackHandle;
        auto ack = AckSchemeChangeRecords(runtime, "tail:sub", tailOrder, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)ack->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
    }

    // Pull tail order across Fetch's 1000-record server-side cap.
    static ui64 FetchTailOrder(TTestBasicRuntime& runtime, const TString& subscriberId) {
        ui64 tailOrder = 0;
        ui64 cursor = 0;
        while (true) {
            TAutoPtr<IEventHandle> h;
            auto fetch = FetchSchemeChangeRecords(runtime, subscriberId, cursor, 1000, h);
            if (fetch->Record.EntriesSize() == 0) break;
            tailOrder = fetch->Record.GetEntries(fetch->Record.EntriesSize() - 1).GetOrder();
            cursor = tailOrder;
            if (!fetch->Record.GetHasMore()) break;
        }
        return tailOrder;
    }

    Y_UNIT_TEST(AckDeletesAllAckedRecordsRegardlessOfCount) {
        // A backlog larger than SchemeChangeCleanupBatchSize drains via a
        // scheduled continuation chain; poll until empty.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bulk:sub", regHandle);

        const int kCount = 1200;
        for (int i = 1; i <= kCount; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("D%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        ui64 tailOrder = FetchTailOrder(runtime, "bulk:sub");
        UNIT_ASSERT_C(tailOrder >= (ui64)kCount,
            "Expected tail >= " << kCount << ", got " << tailOrder);

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "bulk:sub", tailOrder, ackHandle);

        TVector<TSchemeChangeRecordEntry> entries;
        for (int i = 0; i < 200; ++i) {
            entries = ReadSchemeChangeRecords(runtime);
            if (entries.empty()) break;
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(entries.empty(),
            "All " << kCount << " records must eventually drain; "
                << entries.size() << " still remaining");
    }

    Y_UNIT_TEST(ForceAdvanceDeletesStaleRecordsInline) {
        // ForceAdvance jumps the cursor to tail and must delete newly-stale
        // records inline, same as Ack/Unregister.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("D%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        auto before = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!before.empty());

        TAutoPtr<IEventHandle> advHandle;
        auto result = ForceAdvanceSubscriber(runtime, "stuck:sub", advHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        // IMMEDIATE read — no wakeup, no time advance.
        auto after = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(after.empty(),
            "ForceAdvance must sweep records inline; got " << after.size() << " remaining");
    }

    Y_UNIT_TEST(UnregisterSweepsStaleRecordsRegardlessOfCount) {
        // Slow subscriber holds min cursor at 0 while fast acks everything.
        // Unregistering the slow one must sweep all records immediately.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> reg1Handle, reg2Handle;
        RegisterSubscriber(runtime, "slow:sub", reg1Handle);
        RegisterSubscriber(runtime, "fast:sub", reg2Handle);

        const int kCount = 1100;
        for (int i = 1; i <= kCount; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("D%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        ui64 tailOrder = FetchTailOrder(runtime, "fast:sub");
        UNIT_ASSERT_C(tailOrder >= (ui64)kCount,
            "Expected tail >= " << kCount << ", got " << tailOrder);
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "fast:sub", tailOrder, ackHandle);

        // slow:sub still at 0 -> records held by slow
        auto stillThere = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!stillThere.empty());

        // Unregister slow: min jumps to fast's tail, draining via continuation chain.
        TAutoPtr<IEventHandle> unregHandle;
        UnregisterSubscriber(runtime, "slow:sub", unregHandle);

        TVector<TSchemeChangeRecordEntry> entries;
        for (int i = 0; i < 200; ++i) {
            entries = ReadSchemeChangeRecords(runtime);
            if (entries.empty()) break;
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(entries.empty(),
            "Unregister of the slow subscriber must eventually sweep all "
                << kCount << " stale records; " << entries.size() << " still remaining");
    }

    // NextSchemeChangeOrder is the last assigned order, not the next one, so
    // "register at tail" means LastAckedOrder == NextSchemeChangeOrder exactly.

    Y_UNIT_TEST(RegisterSubscriberDefaultsToTailNotZero) {
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

        // A first subscriber makes records start accumulating.
        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "sub-A", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail > 0, "precondition: some records must exist");

        // A brand-new subscriber must start at the tail, not at 0: starting at
        // 0 replays unwanted history and pins the retention floor low (see the
        // next test).
        TAutoPtr<IEventHandle> regB;
        auto* resB = RegisterSubscriber(runtime, "sub-B", regB);
        UNIT_ASSERT_VALUES_EQUAL((ui32)resB->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL_C(resB->Record.GetCurrentOrder(), tail,
            "a new subscriber must register at the tail (NextSchemeChangeOrder), not at 0");
    }

    Y_UNIT_TEST(RegisterSubscriberOnMatureClusterDoesNotBlockDdl) {
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

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "sub-A", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3");
        env.TestWaitNotification(runtime, txId);

        // sub-A drains everything, so the retention floor sits at the tail and
        // acked records are swept.
        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail > 0, "precondition: some records must exist");
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "sub-A", tail, ackHandle);

        // "Mature cluster": rather than driving 100k MkDirs to reach the
        // default cap, lower the cap to the current tail instead.
        schemeshard->MaxSchemeChangeRecords = tail;

        // Registering a second subscriber must not pin the floor back to 0.
        TAutoPtr<IEventHandle> regB;
        auto* resB = RegisterSubscriber(runtime, "sub-B", regB);
        UNIT_ASSERT_VALUES_EQUAL((ui32)resB->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        // The anti-wedge invariant: registering a subscriber must leave the
        // unacked window unchanged.
        UNIT_ASSERT_VALUES_EQUAL_C(
            schemeshard->NextSchemeChangeOrder - schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime()), 0u,
            "registering a subscriber must not widen the unacked window");

        // And DDL must still be accepted.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir4", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(ExplicitStartOrderBelowRetentionClampsAndReportsLost) {
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

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "sub-A", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        // Drain with sub-A so the retention floor advances off 0.
        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "sub-A", tail, ackHandle);

        const ui64 floor = schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime());
        UNIT_ASSERT_C(floor > 0, "precondition: the retention floor must have advanced");
        const ui64 unackedBefore =
            schemeshard->NextSchemeChangeOrder - schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime());

        // Ask for history that has already been swept.
        TAutoPtr<IEventHandle> regB;
        auto* resB = RegisterSubscriberAt(runtime, "sub-B", 0, regB);

        UNIT_ASSERT_VALUES_EQUAL((ui32)resB->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL_C(resB->Record.GetCurrentOrder(), floor,
            "a StartOrder below the retention floor must be clamped up to the floor");
        UNIT_ASSERT_VALUES_EQUAL_C(resB->Record.GetSkippedEntries(), floor,
            "SkippedEntries must report the size of the hole");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)resB->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST,
            "a clamped subscriber must be told its stream has a hole");

        // Clamping must preserve the anti-wedge invariant: registering must
        // never widen the unacked window.
        UNIT_ASSERT_VALUES_EQUAL_C(
            schemeshard->NextSchemeChangeOrder - schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime()),
            unackedBefore,
            "registering below the floor must not widen the unacked window");
    }

    Y_UNIT_TEST(ExplicitStartOrderOnEmptyLogAccepted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        // Zero subscribers, zero records: the floor is the tail is 0.
        TAutoPtr<IEventHandle> reg;
        auto* res = RegisterSubscriberAt(runtime, "sub-A", 0, reg);

        UNIT_ASSERT_VALUES_EQUAL((ui32)res->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(res->Record.GetCurrentOrder(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(res->Record.GetSkippedEntries(), 0u);
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)res->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "an empty log has no hole to report");
    }

    Y_UNIT_TEST(ExplicitStartOrderAtFloorAccepted) {
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

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "sub-A", regA);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "sub-A", tail, ackHandle);
        const ui64 floor = schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime());

        // Exactly at the floor is accepted verbatim: an off-by-one here would
        // reject every legitimate unswept read.
        TAutoPtr<IEventHandle> regB;
        auto* resB = RegisterSubscriberAt(runtime, "sub-B", floor, regB);

        UNIT_ASSERT_VALUES_EQUAL((ui32)resB->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(resB->Record.GetCurrentOrder(), floor);
        UNIT_ASSERT_VALUES_EQUAL_C(resB->Record.GetSkippedEntries(), 0u,
            "registering exactly at the floor skips nothing");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)resB->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "registering exactly at the floor is not Lost");
    }

    Y_UNIT_TEST(ExplicitStartOrderBeyondTailRejected) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);

        TAutoPtr<IEventHandle> reg;
        // Beyond the tail is not a real position.
        RegisterSubscriberAtExpect(
            runtime, "sub-A", schemeshard->NextSchemeChangeOrder + 100,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, reg);
    }

    Y_UNIT_TEST(SubscriberCarriesStateAndStartOrder) {
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

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "sub-A", regA);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        TAutoPtr<IEventHandle> regB;
        RegisterSubscriber(runtime, "sub-B", regB);

        {
            auto it = schemeshard->Subscribers.find("sub-B");
            UNIT_ASSERT(it != schemeshard->Subscribers.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.StartOrder, tail);
            UNIT_ASSERT_VALUES_EQUAL((ui32)it->second.State,
                (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY);
        }

        // Both columns must survive a reboot.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            auto it = schemeshard->Subscribers.find("sub-B");
            UNIT_ASSERT_C(it != schemeshard->Subscribers.end(),
                "subscriber must survive reboot");
            UNIT_ASSERT_VALUES_EQUAL_C(it->second.StartOrder, tail,
                "StartOrder must round-trip through reboot");
            UNIT_ASSERT_VALUES_EQUAL_C((ui32)it->second.State,
                (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
                "State must round-trip through reboot");
        }
    }

    Y_UNIT_TEST(ReadHelperSeesAllRecordsRegardlessOfTail) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "keeper:sub", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3");
        env.TestWaitNotification(runtime, txId);

        // The read helper mints a temp subscriber fresh each call. Without an
        // explicit StartOrder it would scan from tail+1 and return empty on
        // every call made after any DDL.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.size() >= 3,
            "read helper must see the whole retained log, not start at the tail; got "
                << entries.size());

        // Must still work on a second call, i.e. not sensitive to its own
        // previous registration.
        auto again = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(again.size(), entries.size(),
            "read helper must be idempotent across calls");
    }

    Y_UNIT_TEST(ReadHelperReturnsEmptyOnlyWhenLogIsEmpty) {
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

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "keeper:sub", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= 2, "precondition: records must exist");

        // Cursor-independent oracle: the rows are physically there.
        auto present = ProbeRecordOrdersPresent(runtime, "keeper:sub", {1, tail});
        UNIT_ASSERT_C(!present.empty(),
            "physical probe must see rows before the sweep");

        // Force-advance the only subscriber, which sweeps everything.
        TAutoPtr<IEventHandle> faHandle;
        ForceAdvanceSubscriber(runtime, "keeper:sub", faHandle);

        // The helper must return empty because the log is empty, not because
        // its registration was rejected or its cursor clamped past live rows.
        // The physical probe below independently confirms the rows are gone.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(entries.size(), 0u,
            "after a full sweep the helper must read empty");

        auto afterSweep = ProbeRecordOrdersPresent(runtime, "keeper:sub", {1, tail});
        UNIT_ASSERT_VALUES_EQUAL_C(afterSweep.size(), 0u,
            "emptiness must be real: no record rows may remain on disk");
    }

    // The measurement is taken mid-DDL, after a reboot: TTxInit rebuilds
    // Operations[txId]->SchemeChangeSlots only from persisted rows, so
    // "nothing was reserved at propose" is directly visible.

    Y_UNIT_TEST(NoSubscribersMeansNoReservedOutboxRows) {
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

        // Deliberately NO subscriber.
        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto it = schemeshard->Operations.find(TTxId(opTxId));
        if (it != schemeshard->Operations.end()) {
            UNIT_ASSERT_C(it->second->SchemeChangeSlots.empty(),
                "with no subscribers no outbox row may be reserved; found "
                    << it->second->SchemeChangeSlots.size() << " restored slots");
        }

        env.TestWaitNotification(runtime, opTxId);

        // Absence above must mean "never written", not "written and swept".
        UNIT_ASSERT_VALUES_EQUAL_C(ReadSchemeChangeRecords(runtime).size(), 0u,
            "an unsubscribed cluster must leave the outbox completely empty");
    }

    Y_UNIT_TEST(SubscriberPresentMeansRowsArePersisted) {
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
        RegisterSubscriber(runtime, "keeper:sub", regHandle);

        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Mirror of the test above: the gate must not kill durability across
        // a propose->done reboot.
        auto it = schemeshard->Operations.find(TTxId(opTxId));
        if (it != schemeshard->Operations.end()) {
            UNIT_ASSERT_C(!it->second->SchemeChangeSlots.empty(),
                "with a subscriber registered the reserved outbox rows must survive "
                "a propose->done reboot");
        }

        env.TestWaitNotification(runtime, opTxId);

        // The reservation must actually turn into a delivered record.
        UNIT_ASSERT_VALUES_EQUAL_C(ReadSchemeChangeRecords(runtime).size(), 1u,
            "the surviving reservation must be finalised into a fetchable record");
    }

    // A stale subscriber still holds the retention floor and DDL wedges until
    // an admin acts. Letting it stop holding the floor would silently drop
    // the forgotten consumer's records. Staleness detection must still work
    // even though nothing gets swept automatically.

    Y_UNIT_TEST(StaleSubscriberBlocksDdlUntilAdminOverride) {
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

        const ui64 ttlSeconds = 100;
        ApplySchemeShardConfig(runtime, {
            .MaxSchemeChangeRecords = 2,
            .SchemeChangeSubscriberStaleTtlSeconds = ttlSeconds,
        });

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "dead:sub", regHandle);

        // Drive DDL past the cap without acking; cap=2 means exactly 2 records
        // fit, so the next DDL is at the cap.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        // The subscriber goes dark for well past the TTL.
        runtime.SimulateSleep(TDuration::Seconds(2 * ttlSeconds));

        // Going dark buys it nothing: it still holds the floor, so DDL stays
        // wedged.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3", {NKikimrScheme::StatusResourceExhausted});

        // Staleness must still be detected, or an operator cannot tell which
        // consumer to blame.
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribersStale"), 1u,
            "the idle subscriber must be visible as stale even though nothing was swept");
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribersLost"), 0u,
            "stale is not lost: it has been told nothing, but it has lost nothing either");

        // The admin override is the only way out.
        TAutoPtr<IEventHandle> advHandle;
        ForceAdvanceSubscriber(runtime, "dead:sub", advHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(FreshSubscriberStillBlocksDdl) {
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

        const ui64 ttlSeconds = 100;
        ApplySchemeShardConfig(runtime, {
            .MaxSchemeChangeRecords = 2,
            .SchemeChangeSubscriberStaleTtlSeconds = ttlSeconds,
        });

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "slow:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        // Alive but slow: it keeps fetching inside the TTL without acking.
        runtime.SimulateSleep(TDuration::Seconds(ttlSeconds / 2));
        TAutoPtr<IEventHandle> fetchHandle;
        FetchSchemeChangeRecords(runtime, "slow:sub", 0, 10, fetchHandle);
        runtime.SimulateSleep(TDuration::Seconds(ttlSeconds / 2));

        // Backpressure must still apply, not just when a subscriber is stale.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3", {NKikimrScheme::StatusResourceExhausted});
    }

    // A live subscriber's ack must not sweep a stale one's unread records.
    Y_UNIT_TEST(StaleSubscriberRecordsSurviveAnotherSubscribersAck) {
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

        // Keep the TTL small: SimulateSleep cost scales with simulated time.
        const ui64 ttlSeconds = 5;
        ApplySchemeShardConfig(runtime, {
            .SchemeChangeSubscriberStaleTtlSeconds = ttlSeconds,
        });

        // Both register on an empty log so both cursors sit at 0 and the ack
        // path's delete range (oldMin, newMin] covers what dead:sub never read.
        // No reboot here: it would restore the 30-day default TTL.
        TAutoPtr<IEventHandle> regDead, regLive;
        RegisterSubscriber(runtime, "dead:sub", regDead);
        RegisterSubscriber(runtime, "live:sub", regLive);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= 2, "precondition: records must exist");

        // Both go quiet past the TTL...
        runtime.SimulateSleep(TDuration::Seconds(2 * ttlSeconds));

        // ...but live:sub comes back and drains the whole log. dead:sub still
        // holds the retention floor, so live:sub's ack must not sweep records
        // dead:sub never consumed.
        TAutoPtr<IEventHandle> liveFetch;
        FetchSchemeChangeRecords(runtime, "live:sub", 0, 100, liveFetch);
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "live:sub", tail, ackHandle);

        // Physical survival, read by explicit order so no cursor can mask it.
        auto present = ProbeRecordOrdersPresent(runtime, "live:sub", {1, 2});
        UNIT_ASSERT_VALUES_EQUAL_C(present.size(), 2u,
            "a stale subscriber still pins retention; another subscriber's ack "
            "must not delete records it never read");

        // And when it comes back it gets its records, not a loss report.
        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "dead:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL_C(fetch->Record.GetSkippedEntries(), 0u,
            "nothing was swept, so nothing may be reported as lost");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)fetch->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that lost nothing must not be marked Lost");
        UNIT_ASSERT_VALUES_EQUAL_C(fetch->Record.EntriesSize(), 2u,
            "it must actually receive the records it was owed");
    }

    // Internal ops (split/merge, temp-dir GC, export/import) are never
    // rejected by the cap; rejecting them would be a cluster outage, not
    // backpressure. Churn ops are excluded separately since a user-initiated
    // split is not Internal=true but still emits no record. The cap is
    // enforced at the propose gate by refusing user DDL; only an admin
    // force-advance may discard records to relieve it.

    Y_UNIT_TEST(UserDdlStillRejectedAtCap) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        ApplySchemeShardConfig(runtime, {.MaxSchemeChangeRecords = 2});

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        // The bypass must not leak to ordinary user DDL: plain, non-internal,
        // non-churn operations are still subject to backpressure.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(UserInitiatedSplitAtCapNotRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "Key"   Type: "Utf8" }
            Columns { Name: "Value" Type: "Utf8" }
            KeyColumnNames: ["Key", "Value"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Park the subscriber at the cap without acking; the CreateTable above
        // already put it at or above the cap, so no extra DDL is needed.
        ApplySchemeShardConfig(runtime, {.MaxSchemeChangeRecords = 1});

        // A user-initiated split is not Internal=true, so this pins the churn
        // clause specifically, not just the Internal bypass.
        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
            SourceTabletId: 72075186233409546
            SplitBoundary {
                KeyPrefix {
                    Tuple { Optional { Text: "A" } }
                }
            })", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(ChurnOpsAtCapDoNotMarkSubscriberLost) {
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
        RegisterSubscriber(runtime, "parked:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "Key"   Type: "Utf8" }
            Columns { Name: "Value" Type: "Utf8" }
            KeyColumnNames: ["Key", "Value"]
        )");
        env.TestWaitNotification(runtime, txId);

        ApplySchemeShardConfig(runtime, {.MaxSchemeChangeRecords = 1});
        const auto before = ReadSchemeChangeRecords(runtime);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
            SourceTabletId: 72075186233409546
            SplitBoundary {
                KeyPrefix {
                    Tuple { Optional { Text: "A" } }
                }
            })", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        // Churn allocates no order, so it must not consume the outbox budget
        // or ever mark a subscriber Lost.
        const auto after = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(after.size(), before.size(),
            "a split must not append scheme change records");

        auto it = schemeshard->Subscribers.find("parked:sub");
        UNIT_ASSERT(it != schemeshard->Subscribers.end());
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)it->second.State,
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "churn at the cap must not mark the subscriber Lost");
    }

    Y_UNIT_TEST(EmptySubscriberIdRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        // Two consumers that both leave SubscriberId unset would silently
        // share one cursor.
        TAutoPtr<IEventHandle> handle;
        RegisterSubscriberExpect(runtime, "",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, handle);
    }

    Y_UNIT_TEST(OverlongSubscriberIdRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        TAutoPtr<IEventHandle> handle;
        RegisterSubscriberExpect(runtime, TString(300, 'x'),
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, handle);
    }

    Y_UNIT_TEST(SubscriberCountCapped) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);

        schemeshard->MaxSchemeChangeSubscribers = 2;

        TAutoPtr<IEventHandle> h1, h2, h3;
        RegisterSubscriber(runtime, "sub-1", h1);
        RegisterSubscriber(runtime, "sub-2", h2);

        // Each subscriber pins retention and costs a pass on the DDL admission
        // path, so the count must be bounded.
        RegisterSubscriberExpect(runtime, "sub-3",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, h3);

        // Re-registering an existing id must still succeed: registration is
        // idempotent.
        TAutoPtr<IEventHandle> h4;
        RegisterSubscriber(runtime, "sub-1", h4);
    }

    Y_UNIT_TEST(ForceAdvanceReachableFromMonitoring) {
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
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= 2, "precondition: the subscriber must be behind");
        UNIT_ASSERT_VALUES_EQUAL(schemeshard->Subscribers.at("stuck:sub").LastAckedOrder, 0u);

        // Drive it the way an operator would.
        auto resp = SendSchemeShardMonRequest(runtime, TTestTxConfig::SchemeShard,
            "/app?Action=ForceAdvanceSchemeChangeSubscriber&SubscriberId=stuck:sub",
            HTTP_METHOD_POST);
        UNIT_ASSERT_C(resp.Body.Contains("stuck:sub"),
            "the action must answer the HTTP request, got: " << resp.Body);

        runtime.SimulateSleep(TDuration::Seconds(1));

        const auto& info = schemeshard->Subscribers.at("stuck:sub");
        UNIT_ASSERT_VALUES_EQUAL_C(info.LastAckedOrder, tail,
            "the monitoring action must advance the stuck cursor to the tail");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)info.State,
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST,
            "and must mark the subscriber Lost -- its unread records are gone");
    }

    Y_UNIT_TEST(ForceAdvanceFromMonitoringRejectsUnknownSubscriber) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));

        auto resp = SendSchemeShardMonRequest(runtime, TTestTxConfig::SchemeShard,
            "/app?Action=ForceAdvanceSchemeChangeSubscriber&SubscriberId=ghost:sub",
            HTTP_METHOD_POST);
        UNIT_ASSERT_C(resp.Body.Contains("ghost:sub") || resp.Body.Contains("No scheme change subscriber"),
            "an unknown subscriber must produce a clear operator-facing error, got: " << resp.Body);
    }

    Y_UNIT_TEST(RecordCarriesResolvedIdentity) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "id:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "a CREATE must produce a record");
        const auto& rec = entries.back();

        // A consumer must be able to act on identity without parsing the
        // request body.
        UNIT_ASSERT_VALUES_EQUAL_C(rec.OperationType,
            (ui32)NKikimrSchemeOp::ESchemeOpCreateTable,
            "OperationType must be the requested EOperationType, not a TxType placeholder");
        UNIT_ASSERT_C(rec.PathLocalId != 0,
            "PathId must be resolved, not left empty");
        UNIT_ASSERT_VALUES_EQUAL_C(rec.ObjectType,
            (ui32)NKikimrSchemeOp::EPathTypeTable,
            "ObjectType must be resolved");
        UNIT_ASSERT_C(AnyPathContains(rec, "T1"),
            "Path must name the target, got: " << AllTargetPaths(rec));
    }

    Y_UNIT_TEST(CreateRecordCarriesResolvedDescription) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "desc:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "extra" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "a CREATE must produce a record");
        const auto& rec = entries.back();

        UNIT_ASSERT_C(!rec.Description.empty(),
            "the record must carry a resolved description");

        // The description makes the object reconstructible from the record
        // alone, with no call back into SchemeShard.
        NKikimrScheme::TEvDescribeSchemeResult desc;
        UNIT_ASSERT_C(desc.ParseFromString(rec.Description),
            "the description must be a parseable TEvDescribeSchemeResult");
        const auto& tableDesc = desc.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL_C(tableDesc.GetName(), "T1",
            "the description must describe the created object");
        UNIT_ASSERT_C(tableDesc.ColumnsSize() >= 2,
            "the description must carry the resolved column set, got "
                << tableDesc.ColumnsSize());
    }

    Y_UNIT_TEST(SplitMergeProducesNoRecords) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "churn:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "Key"   Type: "Utf8" }
            Columns { Name: "Value" Type: "Utf8" }
            KeyColumnNames: ["Key", "Value"]
        )");
        env.TestWaitNotification(runtime, txId);

        const auto before = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!before.empty(), "precondition: the CREATE must have been recorded");

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
            SourceTabletId: 72075186233409546
            SplitBoundary {
                KeyPrefix {
                    Tuple { Optional { Text: "A" } }
                }
            })", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        // Partitioning is layout, not data-encoding, so no consumer needs the
        // history of splits; streaming them would flood the outbox budget.
        const auto after = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(after.size(), before.size(),
            "auto-partitioning must never reach the outbox");
    }

    Y_UNIT_TEST(ChurnListIsTheOnlyFilter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "filter:sub", regHandle);

        // Everything not on the churn list is logged, including ops a naive
        // "user-facing only" rule would drop.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "T1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        THashSet<ui32> seen;
        for (const auto& rec : entries) {
            seen.insert(rec.OperationType);
        }
        UNIT_ASSERT_C(seen.contains((ui32)NKikimrSchemeOp::ESchemeOpMkDir),
            "MkDir must be logged");
        UNIT_ASSERT_C(seen.contains((ui32)NKikimrSchemeOp::ESchemeOpCreateTable),
            "CreateTable must be logged");
        UNIT_ASSERT_C(seen.contains((ui32)NKikimrSchemeOp::ESchemeOpDropTable),
            "DropTable must be logged");
        UNIT_ASSERT_C(!seen.contains((ui32)NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions),
            "the churn list must be the only thing excluded");
    }

    // If targetName is empty, Path falls back to WorkingDir, which resolves to
    // the parent directory -- confidently wrong identity, worse than none.

    Y_UNIT_TEST(NonTableObjectsCarryTheirOwnIdentity) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().RunFakeConfigDispatcher(true).EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "types:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
                Name: "ExtSrc"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "records must exist");

        const TSchemeChangeRecordEntry* extSrc = nullptr;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateExternalDataSource) {
                extSrc = &rec;
            }
        }
        UNIT_ASSERT_C(extSrc, "the external data source CREATE must be recorded");

        UNIT_ASSERT_C(AnyPathContains(*extSrc, "ExtSrc"),
            "Path must name the object itself, not just its parent dir; got: " << AllTargetPaths(*extSrc));
        UNIT_ASSERT_VALUES_EQUAL_C(extSrc->ObjectType,
            (ui32)NKikimrSchemeOp::EPathTypeExternalDataSource,
            "ObjectType must be the object's own type, not the parent's EPathTypeDir");
    }

    Y_UNIT_TEST(EveryRecordHasNonZeroPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "ps:sub", regHandle);

        // A coordinated op...
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        // ...and a propose-time one. TModifyACL has no TxState at all, so it
        // never reaches a coordinator and has no step of its own.
        TestModifyACL(runtime, ++txId, "/MyRoot", "Dir1", "", "user1@builtin");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "records must exist");
        for (const auto& rec : entries) {
            // A persisted 0 would sort a record before everything when a
            // consumer buckets by (PlanStep, PositionKind).
            UNIT_ASSERT_C(rec.PlanStep != 0,
                "no record may carry PlanStep 0; order=" << rec.Order
                    << " opType=" << rec.OperationType);
            UNIT_ASSERT_C(rec.PositionKind != 0,
                "every record must declare its position kind; order=" << rec.Order);
        }
    }

    Y_UNIT_TEST(BucketedRecordsCarryLowerBoundPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "kind:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestModifyACL(runtime, ++txId, "/MyRoot", "Dir1", "", "user1@builtin");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* coordinated = nullptr;
        const TSchemeChangeRecordEntry* proposeTime = nullptr;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpMkDir) {
                coordinated = &rec;
            } else if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpModifyACL) {
                proposeTime = &rec;
            }
        }
        UNIT_ASSERT_C(coordinated, "the MkDir must be recorded");
        UNIT_ASSERT_C(proposeTime, "the ACL change must be recorded");

        UNIT_ASSERT_VALUES_EQUAL_C(coordinated->PositionKind,
            (ui32)NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT,
            "a coordinated op has a real PlanStep and must be Exact");
        UNIT_ASSERT_VALUES_EQUAL_C(proposeTime->PositionKind,
            (ui32)NKikimrSchemeShard::TSchemeChangePosition::KIND_BUCKETED,
            "a propose-time op has no step of its own and must be Bucketed");

        // The borrow is a lower bound, so Bucketed sorts after Exact at equal step.
        UNIT_ASSERT_C(proposeTime->PlanStep >= coordinated->PlanStep,
            "a borrowed step must be >= the step it borrowed from; got "
                << proposeTime->PlanStep << " vs " << coordinated->PlanStep);
    }

    Y_UNIT_TEST(IncrementalBackupProducesIdentifiableSyncPoint) {
        TTestBasicRuntime runtime;
        // Backup collections are feature-gated; without this the op is rejected
        // with StatusPreconditionFailed and the test proves nothing.
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "sync:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Collections live under a fixed reserved path.
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "MyCollection"
            ExplicitEntryList {
                Entries { Type: ETypeTable Path: "/MyRoot/T1" }
            }
            Cluster {}
            IncrementalBackupConfig {}
        )");
        env.TestWaitNotification(runtime, txId);

        // The actual sync point is the backup run, not the collection object.
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot", R"(
            Name: ".backups/collections/MyCollection"
        )");
        env.TestWaitNotification(runtime, txId);

        // Backup artifacts are named from the wall clock at second granularity;
        // without this sleep the incremental backup collides with the full
        // backup's stream name.
        runtime.SimulateSleep(TDuration::Seconds(5));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot", R"(
            Name: ".backups/collections/MyCollection"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(), "records must exist");

        bool sawIncremental = false;
        for (const auto& rec : entries) {
            if (rec.OperationType
                    == (ui32)NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection) {
                sawIncremental = true;
                UNIT_ASSERT_C(rec.PlanStep != 0,
                    "the sync point must carry a usable PlanStep");
            }
        }
        UNIT_ASSERT_C(sawIncremental,
            "BACKUP INCREMENTAL -- the actual sync point -- must appear in the stream "
            "with its own EOperationType");

        // The sync point must be identifiable by the body's EOperationType, not
        // ETxType: ConvertToTxType maps it onto TxInvalid along with other ops.
        bool sawCollection = false;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateBackupCollection) {
                sawCollection = true;
                UNIT_ASSERT_C(rec.PlanStep != 0,
                    "a sync-point-bearing record must carry a usable PlanStep");
            }
        }
        UNIT_ASSERT_C(sawCollection,
            "backup collection ops must reach the outbox with their own EOperationType, "
            "otherwise a consumer cannot locate window boundaries");
    }

    Y_UNIT_TEST(ClosedThroughPlanStepIsInclusiveAtQuiesce) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "close:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        auto result = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(!result.Entries.empty(), "a record must exist");
        const ui64 lastStep = result.Entries.back().PlanStep;

        // With nothing in flight the bound must reach the newest record's own step.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep >= lastStep,
            "a quiesced cluster must be able to close its newest window: closed="
                << result.ClosedThroughPlanStep << " lastRecordStep=" << lastStep);
    }

    Y_UNIT_TEST(PerObjectOrderingHoldsWithoutGlobalOrdering) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "order:sub", regHandle);

        // Two independent objects; no cross-object ordering is required, only
        // per-object order.
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "DirB");
        env.TestWaitNotification(runtime, txId);
        TestRmDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui64 createA = 0, dropA = 0;
        for (const auto& rec : entries) {
            if (!AnyPathContains(rec, "DirA")) {
                continue;
            }
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpMkDir) {
                createA = rec.Order;
            } else if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpRmDir) {
                dropA = rec.Order;
            }
        }
        UNIT_ASSERT_C(createA && dropA, "both DirA records must exist");
        UNIT_ASSERT_C(createA < dropA,
            "per-object order must hold: a create must precede its drop");
    }

    Y_UNIT_TEST(SingleTransactionSupportingDomainPerSchemeShard) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);

        // A second transaction-supporting domain under one SchemeShard would
        // break the global-max borrow as a sound lower bound.
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->CountTransactionSupportingDomains(), 1u,
            "the default test env must serve exactly one transaction-supporting domain");

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "domain:sub", regHandle);
    }

    Y_UNIT_TEST(DdlBucketsIntoCorrectBackupWindow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "window:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        auto makeSyncPoint = [&](const TString& name) {
            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", Sprintf(R"(
                Name: "%s"
                ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/T1" } }
                Cluster {}
                IncrementalBackupConfig {}
            )", name.c_str()));
            env.TestWaitNotification(runtime, txId);
        };

        //   S1 . CREATE T2 . ALTER T1 . S2 . DROP T2
        // Everything between S1 and S2 must land in window (S1, S2].
        makeSyncPoint("C1");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        makeSyncPoint("C2");

        TestDropTable(runtime, ++txId, "/MyRoot", "T2");
        env.TestWaitNotification(runtime, txId);

        auto result = ReadSchemeChangeRecordsFull(runtime);
        const auto& entries = result.Entries;
        UNIT_ASSERT_C(!entries.empty(), "records must exist");

        // Locate the two sync points by the body's EOperationType: ConvertToTxType
        // maps backup-collection ops onto TxInvalid, shared with other ops.
        TVector<ui64> syncSteps;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateBackupCollection) {
                syncSteps.push_back(rec.PlanStep);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(syncSteps.size(), 2u,
            "both sync points must be identifiable in the stream");
        Sort(syncSteps);
        const ui64 s1 = syncSteps[0], s2 = syncSteps[1];

        // Bucket by rank (PlanStep, PositionKind) against the sync points.
        auto windowOf = [&](const TSchemeChangeRecordEntry& r) -> int {
            if (r.PlanStep <= s1) return 0;      // at or before S1
            if (r.PlanStep <= s2) return 1;      // (S1, S2]
            return 2;                            // after S2
        };

        const TSchemeChangeRecordEntry* createT2 = nullptr;
        const TSchemeChangeRecordEntry* dropT2 = nullptr;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateTable
                && AnyPathContains(rec, "T2")) {
                createT2 = &rec;
            } else if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpDropTable
                && AnyPathContains(rec, "T2")) {
                dropT2 = &rec;
            }
        }
        UNIT_ASSERT_C(createT2, "CREATE T2 must be recorded");
        UNIT_ASSERT_C(dropT2, "DROP T2 must be recorded");

        UNIT_ASSERT_VALUES_EQUAL_C(windowOf(*createT2), 1,
            "CREATE T2 happened between the sync points and must bucket into (S1, S2]");
        UNIT_ASSERT_VALUES_EQUAL_C(windowOf(*dropT2), 2,
            "DROP T2 happened after S2 and must NOT be squeezed into the earlier window");

        // Seeing the S2 record is not sufficient: Order is completion order
        // while plan step diverges, so a DDL planned before S2 can be
        // persisted after it. The consumer may only close (S1, S2] once the
        // closure bound has passed S2.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep >= s2,
            "with everything quiesced the consumer must be able to close the window: closed="
                << result.ClosedThroughPlanStep << " s2=" << s2);
    }

    Y_UNIT_TEST(ClosureBoundIsIndexedNotScanned) {
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
        RegisterSubscriber(runtime, "nfr:sub", regHandle);

        for (int i = 0; i < 8; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        // The bound is served from an incrementally maintained index. Once
        // everything completes it must be empty, or the bound would silently
        // stop advancing.
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->InFlightByPlanStep.size(), 0u,
            "every completed op must release its closure-index entry");
        UNIT_ASSERT_C(schemeshard->GetClosedThroughPlanStep() > 0,
            "with nothing in flight the bound must report the ceiling");
    }

    Y_UNIT_TEST(DdlOverheadWithSubscriberIsBounded) {
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

        // With NO subscriber the outbox must cost exactly nothing.
        TestMkDir(runtime, ++txId, "/MyRoot", "Unwatched");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->TestSchemeChangeRedoBytesAccum, 0u,
            "an unsubscribed cluster must pay no outbox redo cost at all");

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "perf:sub", regHandle);

        const ui64 before = schemeshard->TestSchemeChangeRedoBytesAccum;
        TestMkDir(runtime, ++txId, "/MyRoot", "Watched");
        env.TestWaitNotification(runtime, txId);
        const ui64 perDdl = schemeshard->TestSchemeChangeRedoBytesAccum - before;

        UNIT_ASSERT_C(perDdl > 0, "a watched DDL must actually write a record");
        // A MkDir's request body and description are both small.
        UNIT_ASSERT_C(perDdl < 16384,
            "outbox redo per DDL must stay bounded; got " << perDdl << " bytes");
    }

    Y_UNIT_TEST(SyncPointPlanStepIsTheWindowEdge) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "edge:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "MyCollection"
            ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/T1" } }
            Cluster {}
            IncrementalBackupConfig {}
        )");
        env.TestWaitNotification(runtime, txId);
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot", R"(
            Name: ".backups/collections/MyCollection"
        )");
        env.TestWaitNotification(runtime, txId);

        // DDL strictly before the sync point.
        TestMkDir(runtime, ++txId, "/MyRoot", "BeforeSync");
        env.TestWaitNotification(runtime, txId);

        runtime.SimulateSleep(TDuration::Seconds(5));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot", R"(
            Name: ".backups/collections/MyCollection"
        )");
        env.TestWaitNotification(runtime, txId);

        // DDL strictly after it.
        TestMkDir(runtime, ++txId, "/MyRoot", "AfterSync");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui64 syncStep = 0, beforeStep = 0, afterStep = 0;
        for (const auto& rec : entries) {
            if (rec.OperationType
                    == (ui32)NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection) {
                syncStep = rec.PlanStep;
            } else if (AnyPathContains(rec, "BeforeSync")) {
                beforeStep = rec.PlanStep;
            } else if (AnyPathContains(rec, "AfterSync")) {
                afterStep = rec.PlanStep;
            }
        }
        UNIT_ASSERT_C(syncStep && beforeStep && afterStep,
            "sync point and both DDLs must be recorded");

        // The backup op is a coordinated tx, so its PlanStep is the cut: DDL
        // before it falls inside the window, DDL after falls out.
        UNIT_ASSERT_C(beforeStep < syncStep,
            "DDL before the sync point must sit below its PlanStep: "
                << beforeStep << " vs " << syncStep);
        UNIT_ASSERT_C(afterStep > syncStep,
            "DDL after the sync point must sit above its PlanStep: "
                << afterStep << " vs " << syncStep);
    }

    Y_UNIT_TEST(ClosureBoundHeldBackByInFlightOp) {
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
        RegisterSubscriber(runtime, "held:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Settled");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->InFlightByPlanStep.size(), 0u,
            "precondition: nothing in flight");

        // Pin an operation between plan-step assignment and completion. The
        // first state after the step is assigned is ProposedWaitParts, where
        // SchemeShard awaits TEvSchemaChanged; holding that leaves the op with
        // a PlanStep and no emitted record.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 heldTxId = ++txId;
        TestCreateTable(runtime, heldTxId, "/MyRoot", R"(
            Name: "Held"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_C(!schemeshard->InFlightByPlanStep.empty(),
            "the held op must be tracked in the closure index");
        UNIT_ASSERT_C(
            schemeshard->GetClosedThroughPlanStep() < schemeshard->LastAssignedPlanStep,
            "the bound must NOT close over a step whose op is still in flight: closed="
                << schemeshard->GetClosedThroughPlanStep()
                << " ceiling=" << schemeshard->LastAssignedPlanStep);

        // Release it; the bound must then catch up.
        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressed) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressed.clear();
        env.TestWaitNotification(runtime, heldTxId);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->InFlightByPlanStep.size(), 0u,
            "the index must drain once the op completes");
        UNIT_ASSERT_C(
            schemeshard->GetClosedThroughPlanStep() >= schemeshard->LastAssignedPlanStep,
            "once quiesced the bound must reach the ceiling again");
    }

    // A SchemeShard restart between propose and done must still emit the
    // record; the rest of the reboots suite reboots around completion rather
    // than inside this window, so it would not catch a regression here.

    Y_UNIT_TEST(RecordSurvivesProposeToDoneReboot) {
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
        RegisterSubscriber(runtime, "durable:sub", regHandle);

        const auto baseline = ReadSchemeChangeRecords(runtime);

        // Park the operation after the coordinator assigns its plan step but
        // before it completes: TCreateTable reaches ProposedWaitParts and
        // waits on TEvSchemaChanged.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "Durable"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!schemeshard->InFlightByPlanStep.empty(),
            "precondition: the op must be in flight WITH a plan step assigned");

        // Restart inside that window.
        runtime.SetObserverFunc(prevObserver);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Let the restored operation finish.
        env.TestWaitNotification(runtime, opTxId);
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.size() > baseline.size(),
            "a DDL that survived a propose->done reboot must still emit its record; had "
                << baseline.size() << " records, now " << entries.size());

        bool sawDurable = false;
        for (const auto& rec : entries) {
            if (AnyPathContains(rec, "Durable")) {
                sawDurable = true;
                UNIT_ASSERT_VALUES_EQUAL_C(rec.OperationType,
                    (ui32)NKikimrSchemeOp::ESchemeOpCreateTable,
                    "the restored record must carry its real operation type");
                UNIT_ASSERT_C(rec.PlanStep != 0,
                    "the restored record must carry a usable PlanStep");
            }
        }
        UNIT_ASSERT_C(sawDurable,
            "the record for the rebooted operation must be present and identifiable");
    }

    // A multi-target op rebooted between propose and done must restore ALL
    // targets from table 144, not just a subset: finalisation cannot re-read
    // anything else from the DB once InitOperationSchema's snapshot pass has
    // moved on, so a partial restore here would permanently truncate the
    // record.
    Y_UNIT_TEST(MultiTargetPathsSurviveReboot) {
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
        RegisterSubscriber(runtime, "multitarget:sub", regHandle);

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
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Positive companion: the identical operation without a reboot, run
        // first so its target set is the oracle the rebooted run is compared
        // against below.
        const ui64 baselineTxId = ++txId;
        TestConsistentCopyTables(runtime, baselineTxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src1"
                DstPath: "/MyRoot/DstBaseline1"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src2"
                DstPath: "/MyRoot/DstBaseline2"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src3"
                DstPath: "/MyRoot/DstBaseline3"
            }
        )");
        env.TestWaitNotification(runtime, baselineTxId);

        auto baselineEntries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* baselineFound = nullptr;
        for (const auto& e : baselineEntries) {
            if (AnyPathContains(e, "DstBaseline1")) {
                baselineFound = &e;
                break;
            }
        }
        UNIT_ASSERT_C(baselineFound, "baseline (non-rebooted) copy must produce a record");
        UNIT_ASSERT_VALUES_EQUAL_C(baselineFound->Targets.size(), 3u,
            "baseline run must record all 3 targets, got " << baselineFound->Targets.size()
                << ": " << AllTargetPaths(*baselineFound));

        THashMap<TString, TString> baselineDstToSrc;
        for (const auto& target : baselineFound->Targets) {
            UNIT_ASSERT_VALUES_EQUAL_C(target.SourcePaths.size(), 1u,
                "each baseline copy target must have exactly one source, got "
                    << target.SourcePaths.size() << " for " << target.Path);
            baselineDstToSrc[target.Path] = target.SourcePaths[0];
        }

        // Park the rebooted operation after the coordinator assigns its plan
        // step but before it completes, mirroring RecordSurvivesProposeToDoneReboot.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 opTxId = ++txId;
        TestConsistentCopyTables(runtime, opTxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src1"
                DstPath: "/MyRoot/Dst1"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src2"
                DstPath: "/MyRoot/Dst2"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src3"
                DstPath: "/MyRoot/Dst3"
            }
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!schemeshard->InFlightByPlanStep.empty(),
            "precondition: the multi-target op must be in flight WITH a plan step assigned");

        // Restart inside that window: targets must be rebuilt from table 144.
        runtime.SetObserverFunc(prevObserver);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Let the restored operation finish.
        env.TestWaitNotification(runtime, opTxId);
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (AnyPathContains(e, "Dst1") && !AnyPathContains(e, "DstBaseline1")) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found,
            "the rebooted multi-target op must still produce a record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 3u,
            "ALL targets must survive the reboot, not a subset; got "
                << found->Targets.size() << ": " << AllTargetPaths(*found));

        THashMap<TString, TString> dstToSrc;
        for (const auto& target : found->Targets) {
            UNIT_ASSERT_C(!target.Path.empty(), "restored target Path must not be empty");
            UNIT_ASSERT_VALUES_EQUAL_C(target.SourcePaths.size(), 1u,
                "restored target's SourcePaths must survive intact (not empty, not "
                "truncated), got " << target.SourcePaths.size() << " for " << target.Path);
            dstToSrc[target.Path] = target.SourcePaths[0];
        }
        UNIT_ASSERT_C(dstToSrc.contains("Dst1"), "missing Dst1 in " << AllTargetPaths(*found));
        UNIT_ASSERT_C(dstToSrc.contains("Dst2"), "missing Dst2 in " << AllTargetPaths(*found));
        UNIT_ASSERT_C(dstToSrc.contains("Dst3"), "missing Dst3 in " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst1"], "Src1",
            "Dst1's restored target must record Src1 as its source");
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst2"], "Src2",
            "Dst2's restored target must record Src2 as its source");
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst3"], "Src3",
            "Dst3's restored target must record Src3 as its source");

        // The rebooted run's target SET (paths database-relative, dst->src
        // mapping) must match the non-rebooted baseline exactly, proving the
        // reboot degraded nothing relative to normal operation.
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc.size(), baselineDstToSrc.size(),
            "rebooted target count must match the baseline's");
    }

    // A force-drop that aborts an in-flight CreateTable via AbortUnsafe must
    // not leave the outbox reporting StatusSuccess for an object that was
    // never actually created.
    Y_UNIT_TEST(ForceAbortedOperationIsNotRecordedAsSuccess) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "abort:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        const auto dirDesc = DescribePath(runtime, "/MyRoot/DirA");
        const ui64 dirLocalPathId = dirDesc.GetPathId();

        // Park the CreateTable in flight, then force-drop its parent
        // directory concurrently: NForceDrop::AbortRelatedOperations calls
        // AbortUnsafe on the in-flight part, which completes it via
        // DoneOperation without ever creating the table.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 createTxId = ++txId;
        AsyncCreateTable(runtime, createTxId, "/MyRoot/DirA", R"(
            Name: "Aborted"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        const ui64 dropTxId = ++txId;
        AsyncForceDropUnsafe(runtime, dropTxId, dirLocalPathId);

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressed) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressed.clear();
        env.TestWaitNotification(runtime, {createTxId, dropTxId});
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* abortedEntry = nullptr;
        for (const auto& e : entries) {
            if (e.TxId == createTxId) {
                abortedEntry = &e;
            }
        }
        UNIT_ASSERT_C(abortedEntry, "the force-aborted CreateTable must still produce a record "
            "so the stream advances past it");
        // Positive companion: an ordinary completed op in the same run really
        // does get StatusSuccess, so this isn't a status field that is always
        // something else.
        bool sawOrdinarySuccess = false;
        for (const auto& e : entries) {
            if (e.TxId != createTxId && e.Status == (ui32)NKikimrScheme::StatusSuccess) {
                sawOrdinarySuccess = true;
            }
        }
        UNIT_ASSERT_C(sawOrdinarySuccess,
            "precondition: a genuinely completed op (the MkDir) must show StatusSuccess");
        UNIT_ASSERT_C(abortedEntry->Status != (ui32)NKikimrScheme::StatusSuccess,
            "a force-aborted CreateTable must not be recorded as StatusSuccess; "
            "the table was never actually created");
    }

    // A force-drop must not leave the fetch stream stuck behind the in-flight
    // record it aborted: AbortRelatedOperations completes every part via
    // AbortUnsafe, so the operation reaches DoneTransactions and its record
    // gets finalised instead of stranding at CompletedAtUs = 0 forever.
    Y_UNIT_TEST(ForceDropDoesNotStrandInFlightRecord) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "forcedrop:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirB");
        env.TestWaitNotification(runtime, txId);

        const auto dirDesc = DescribePath(runtime, "/MyRoot/DirB");
        const ui64 dirLocalPathId = dirDesc.GetPathId();

        // Park a CreateTable in flight under the subtree, then force-drop the
        // subtree: the in-flight op is aborted via AbortUnsafe.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 createTxId = ++txId;
        AsyncCreateTable(runtime, createTxId, "/MyRoot/DirB", R"(
            Name: "StrandedCandidate"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        const ui64 dropTxId = ++txId;
        AsyncForceDropUnsafe(runtime, dropTxId, dirLocalPathId);

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressed) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressed.clear();
        env.TestWaitNotification(runtime, {createTxId, dropTxId});
        runtime.SimulateSleep(TDuration::Seconds(1));

        // The aborted op's own record must be finalised (not stuck at 0).
        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* abortedEntry = nullptr;
        for (const auto& e : entries) {
            if (e.TxId == createTxId) {
                abortedEntry = &e;
            }
        }
        UNIT_ASSERT_C(abortedEntry, "the force-dropped in-flight op must still produce a record");
        UNIT_ASSERT_C(abortedEntry->CompletedAtUs != 0,
            "a force-dropped in-flight record must not strand at CompletedAtUs = 0; "
            "that would wedge the fetch stream behind it permanently");

        // Positive companion: a subsequent normal DDL's record is delivered
        // afterwards, proving the stream genuinely made progress and was not
        // just empty or already broken before the force-drop.
        TestMkDir(runtime, ++txId, "/MyRoot", "AfterForceDrop");
        env.TestWaitNotification(runtime, txId);
        runtime.SimulateSleep(TDuration::Seconds(1));

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "forcedrop:sub", 0, 1000, fetchHandle);
        bool sawAfter = false;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            const auto& targets = fetch->Record.GetEntries(i).GetTargets();
            if (targets.size() == 1 && targets[0].GetPath() == "AfterForceDrop") {
                sawAfter = true;
            }
        }
        UNIT_ASSERT_C(sawAfter,
            "a normal DDL issued after the force-drop must still be delivered to the "
            "subscriber; the stream must not stop dead at the aborted operation's order");
    }

    // A TolerateOrphanedPaths recovery removes an in-flight tx's rows
    // without finalising its already-reserved outbox row. That row must not
    // stay at CompletedAtUs = 0 forever, or the fetch stream wedges behind
    // it permanently -- with a healthy tablet and no way to recover other
    // than discarding everything above it.
    Y_UNIT_TEST(OrphanedInFlightRecordDoesNotWedgeStreamForever) {
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
        RegisterSubscriber(runtime, "orphan:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Orphaned");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "After");
        env.TestWaitNotification(runtime, txId);

        auto before = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* orphanedEntry = nullptr;
        for (const auto& e : before) {
            if (SinglePathEquals(e, "Orphaned")) {
                orphanedEntry = &e;
            }
        }
        UNIT_ASSERT_C(orphanedEntry, "precondition: the row to be orphaned must exist and be finalised");
        UNIT_ASSERT_C(orphanedEntry->CompletedAtUs != 0,
            "precondition: the row must start out finalised (visible), or reverting it "
            "to CompletedAtUs = 0 proves nothing about recovery");
        const ui64 orphanedOrder = orphanedEntry->Order;

        // Reproduce exactly the state a TolerateOrphanedPaths orphan leaves
        // behind: a table-141 row stuck at CompletedAtUs = 0, plus its
        // table-144 pending-record row, for a txId with no operation left
        // anywhere that could ever finalise it (PersistRemoveTx already
        // dropped the tx). This isolates the damage from finding 6 without
        // needing to fabricate a live in-flight coordinated op or a
        // corrupted PathsById to reach the orphan-skip branch itself.
        const ui64 ghostTxId = 999999;
        NKikimrMiniKQL::TResult result;
        TString err;
        const auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let recKey '('('Order (Uint64 '%lu))))
                (let recUpdate '('('CompletedAtUs (Uint64 '0))))
                (let pendingKey '('('TxId (Uint64 '%lu)) '('RequestIdx (Uint32 '0))))
                (let pendingUpdate '('('Order (Uint64 '%lu)) '('Path (String '"/MyRoot/Orphaned"))))
                (return (AsList
                    (UpdateRow 'SchemeChangeRecords recKey recUpdate)
                    (UpdateRow 'SchemeChangePendingRecords pendingKey pendingUpdate)
                ))
            )
        )", orphanedOrder, ghostTxId, orphanedOrder), result, err);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        // The orphaned row must not stay a permanent barrier: a fetch must
        // still be able to reach records above it, the way it would if the
        // row had been finalised normally.
        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "orphan:sub", 0, 100, fetchHandle);
        bool sawAfter = false;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            const auto& afterTargets = fetch->Record.GetEntries(i).GetTargets();
            if (afterTargets.size() == 1 && afterTargets[0].GetPath() == "After") {
                sawAfter = true;
            }
        }
        UNIT_ASSERT_C(sawAfter,
            "a row orphaned by a TolerateOrphanedPaths recovery must not permanently block "
            "the fetch stream behind it; the record for a later, unrelated DDL "
            "(After) must still be reachable");
    }

    // TTxInit folds every restored tx's PlanStep into the closure index before
    // the TolerateOrphanedPaths check runs. The orphan-skip branch then erases
    // the tx without removing its entry, so the refcount leaks and
    // ClosedThroughPlanStep is pinned below that step forever -- on a healthy
    // tablet, with no way back short of another orphan-free reboot.
    //
    // The existing OrphanedInFlightRecordDoesNotWedgeStreamForever deliberately
    // fabricates the *outbox row* state and never reaches the orphan-skip
    // branch, so it cannot see this.
    Y_UNIT_TEST(OrphanedInFlightTxDoesNotPinClosureBound) {
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
        RegisterSubscriber(runtime, "orphanpin:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        // Fabricate the row a half-finished operation leaves behind: an
        // in-flight tx carrying a real PlanStep whose target path element is
        // gone. Fabricated rather than driven, because orphaning a live
        // operation's target means erasing its Paths row, which trips the
        // non-tolerant Tables verify at schemeshard__init.cpp:1947 long before
        // the TxInFlight loop is reached. Nothing else references this row.
        const ui64 ghostTxId = 999998;
        const ui64 ghostPlanStep = 1000;
        const ui64 ghostLocalPathId = 999999;
        NKikimrMiniKQL::TResult mkqlResult;
        TString mkqlErr;
        const auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('TxId (Uint64 '%lu)) '('TxPartId (Uint32 '0))))
                (let update '(
                    '('TargetOwnerPathId (Uint64 '%lu))
                    '('TargetPathId (Uint64 '%lu))
                    '('MinStep (Uint64 '0))
                    '('PlanStep (Uint64 '%lu))))
                (return (AsList (UpdateRow 'TxInFlightV2 key update)))
            )
        )", ghostTxId, (ui64)TTestTxConfig::SchemeShard, ghostLocalPathId, ghostPlanStep),
            mkqlResult, mkqlErr);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, mkqlErr);

        TControlBoard::SetValue(1, runtime.GetAppData().Icb->SchemeShardControls.TolerateOrphanedPaths);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_C(schemeshard->InFlightByPlanStep.empty(),
            "the skipped orphan's PlanStep leaked into the closure index: "
            << schemeshard->InFlightByPlanStep.size() << " entry(ies) remain, first step "
            << (schemeshard->InFlightByPlanStep.empty() ? 0 : schemeshard->InFlightByPlanStep.begin()->first));

        UNIT_ASSERT_C(
            schemeshard->GetClosedThroughPlanStep() >= schemeshard->LastAssignedPlanStep,
            "ClosedThroughPlanStep is pinned below the ceiling by a tx that no longer exists: "
                << schemeshard->GetClosedThroughPlanStep()
                << " < " << schemeshard->LastAssignedPlanStep);
    }

    // A redelivered plan step (mediator reconnect) must not double-count in
    // the closure index, or ClosedThroughPlanStep freezes below the ceiling
    // forever and no subscriber can ever close that window again.
    Y_UNIT_TEST(RedeliveredPlanStepDoesNotFreezeClosureBound) {
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
        RegisterSubscriber(runtime, "redelivery:sub", regHandle);

        // Park the operation after the coordinator assigns its plan step but
        // before it completes, same as ClosureBoundHeldBackByInFlightOp: it
        // reaches ProposedWaitParts and waits on TEvSchemaChanged. Capture
        // the assigned step and txId while parked.
        TVector<THolder<IEventHandle>> suppressedSchemaChanged;
        auto prevObserver = SetSuppressObserver(runtime, suppressedSchemaChanged,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "Redelivered"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_C(!schemeshard->InFlightByPlanStep.empty(),
            "precondition: the op must be in flight WITH a plan step assigned");
        const ui64 assignedStep = schemeshard->InFlightByPlanStep.begin()->first;

        // Redeliver the plan step in memory, exactly as the mediator would
        // resend it on pipe reconnect: a fresh TEvPlanStep for the same
        // step/txId, sent directly to the schemeshard actor.
        NKikimrTx::TEvMediatorPlanStep record;
        record.SetStep(assignedStep);
        auto* txRecord = record.AddTransactions();
        txRecord->SetTxId(opTxId);
        txRecord->SetCoordinator(ui64(TTestTxConfig::Coordinator));
        ActorIdToProto(runtime.AllocateEdgeActor(), txRecord->MutableAckTo());

        auto duplicate = MakeHolder<TEvTxProcessing::TEvPlanStep>();
        duplicate->Record = record;
        runtime.Send(new IEventHandle(schemeshard->SelfId(), runtime.AllocateEdgeActor(), duplicate.Release()),
            0, false);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Release the held op and let it finish.
        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressedSchemaChanged) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressedSchemaChanged.clear();
        env.TestWaitNotification(runtime, opTxId);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->InFlightByPlanStep.size(), 0u,
            "the op completed, so the index must not still show it in flight");
        UNIT_ASSERT_C(schemeshard->GetClosedThroughPlanStep() >= schemeshard->LastAssignedPlanStep,
            "the redelivered step must not leave a phantom refcount behind: closed="
                << schemeshard->GetClosedThroughPlanStep()
                << " ceiling=" << schemeshard->LastAssignedPlanStep);
    }

    Y_UNIT_TEST(RecordIsWrittenAtProposeTime) {
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
        RegisterSubscriber(runtime, "early:sub", regHandle);

        const auto baseline = ReadSchemeChangeRecords(runtime);

        // Hold the op in ProposedWaitParts: plan step assigned, not yet done.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "Early"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));

        // The record is the durable artifact, so the row must already be on
        // disk while the operation is still in flight.
        auto opIt = schemeshard->Operations.find(TTxId(opTxId));
        UNIT_ASSERT_C(opIt != schemeshard->Operations.end(),
            "precondition: the operation must still be in flight, otherwise this "
            "test measures nothing");
        UNIT_ASSERT_VALUES_EQUAL_C(opIt->second->SchemeChangeSlots.size(), 1u,
            "propose must have reserved exactly one outbox row for this DDL");
        const ui64 reservedOrder = opIt->second->SchemeChangeSlots[0].Order;
        UNIT_ASSERT_C(reservedOrder != 0, "a reserved order must be a real order");

        // Physical presence, read by explicit order so the subscriber cursor
        // cannot mask the answer.
        auto present = ProbeRecordOrdersPresent(runtime, "early:sub", {reservedOrder});
        UNIT_ASSERT_VALUES_EQUAL_C(present.size(), 1u,
            "the record must be persisted at propose, not deferred to completion; "
            "order " << reservedOrder << " is not on disk");

        // Physically present is not the same as deliverable: an un-finalised
        // row must not be fetchable yet, or an ack could advance the cursor
        // past a record that never got its contents.
        auto inFlight = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(inFlight.size(), baseline.size(),
            "a reserved-but-unfinalised row must not be handed to subscribers");

        // Release and let it finish; the same row must then carry the fields
        // that are only knowable at completion.
        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressed) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressed.clear();
        env.TestWaitNotification(runtime, opTxId);

        auto settled = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* done = nullptr;
        for (const auto& rec : settled) {
            if (AnyPathContains(rec, "Early")) {
                done = &rec;
            }
        }
        UNIT_ASSERT_C(done, "the record must become visible once the operation completes");
        UNIT_ASSERT_VALUES_EQUAL_C(done->Order, reservedOrder,
            "completion must finalise the row reserved at propose, not a new one");
        UNIT_ASSERT_VALUES_EQUAL_C(done->OperationType,
            (ui32)NKikimrSchemeOp::ESchemeOpCreateTable,
            "the operation type known at propose must survive finalisation");
        UNIT_ASSERT_C(done->PlanStep != 0,
            "completion must finalise PlanStep on the existing row");
        UNIT_ASSERT_C(done->PathLocalId != 0,
            "completion must finalise the resolved PathId on the existing row");
        UNIT_ASSERT_VALUES_EQUAL_C(settled.size(), baseline.size() + 1,
            "finalising must UPDATE the reserved row, not append a second one");
    }

    // A reserved order is not a consumed one: "the tail" must mean the visible
    // tail, or a cursor silently swallows records still in flight.
    Y_UNIT_TEST(SubscriberRegisteredMidDdlStillReceivesThatRecord) {
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

        // Someone must be subscribed at propose or nothing is reserved at all.
        TAutoPtr<IEventHandle> firstHandle;
        RegisterSubscriber(runtime, "first:sub", firstHandle);

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvDataShard::TEvSchemaChanged::EventType);

        const ui64 opTxId = ++txId;
        TestCreateTable(runtime, opTxId, "/MyRoot", R"(
            Name: "MidDdl"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto opIt = schemeshard->Operations.find(TTxId(opTxId));
        UNIT_ASSERT_C(opIt != schemeshard->Operations.end(),
            "precondition: the operation must still be in flight");
        UNIT_ASSERT_VALUES_EQUAL_C(opIt->second->SchemeChangeSlots.size(), 1u,
            "precondition: propose must have reserved a row to be missed");

        // The late subscriber registers here, with the row reserved but not yet
        // finalised: defaulting to the reserved tail would put its cursor above it.
        TAutoPtr<IEventHandle> lateHandle;
        RegisterSubscriber(runtime, "late:sub", lateHandle);

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : suppressed) {
            runtime.Send(ev.Release(), 0, true);
        }
        suppressed.clear();
        env.TestWaitNotification(runtime, opTxId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetched = FetchSchemeChangeRecords(runtime, "late:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL_C(fetched->Record.EntriesSize(), 1u,
            "a subscriber that registered while the DDL was in flight must still "
            "receive its record once it finalises");
        bool midDdlSeen = false;
        for (const auto& t : fetched->Record.GetEntries(0).GetTargets()) {
            if (t.GetPath().Contains("MidDdl")) {
                midDdlSeen = true;
            }
        }
        UNIT_ASSERT_C(midDdlSeen,
            "and it must be that DDL's record");

        UNIT_ASSERT_VALUES_EQUAL_C(fetched->Record.GetSkippedEntries(), 0u,
            "nothing was swept, so nothing may be reported as skipped");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)fetched->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that lost nothing must not be marked Lost");
    }

    // A consumer that stops acking must wedge DDL, not get its records
    // quietly discarded. The only way past a broken consumer is an explicit
    // admin action.
    Y_UNIT_TEST(BrokenConsumerWedgesDdlUntilAdminOverride) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        // Small cap so the wedge is reachable in a unit test.
        ApplySchemeShardConfig(runtime, {.MaxSchemeChangeRecords = 2});

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "broken:sub", regHandle);

        // The consumer never acks. Fill the outbox to the cap.
        for (int i = 0; i < 2; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "w2t%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        // DDL must now be REFUSED rather than silently making room.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "wedged"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // The backlog must still be on disk: "rejected" is only the right
        // behaviour if the records it was protecting actually survived.
        auto present = ProbeRecordOrdersPresent(runtime, "broken:sub", {1, 2});
        UNIT_ASSERT_VALUES_EQUAL_C(present.size(), 2u,
            "the unacked records must be retained, not discarded to unblock DDL");
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribersLost"), 0u,
            "nothing was discarded, so nobody may be marked Lost");
        UNIT_ASSERT_C(
            GetCumulativeCounter(runtime, "SchemeShard/SchemeChangeDdlRejectedByOutbox") > 0,
            "the wedge firing must be visible to monitoring");

        // The admin override is the one and only release valve.
        TAutoPtr<IEventHandle> advHandle;
        ForceAdvanceSubscriber(runtime, "broken:sub", advHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "released"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribersLost"), 1u,
            "the admin override discarded records, so the subscriber must now be Lost");
    }

    // DDL stops on a broken consumer rather than self-healing, so an operator
    // must be able to see the depth climbing before it reaches the wedge.
    // Asserted through the real exported tablet counters, the same numbers a
    // production dashboard reads.
    // Description is serialized into the local-DB redo log on every DDL, so it
    // must not carry anything that scales with the object's size. Two tables
    // identical except for partition count must produce similarly sized
    // descriptions.
    Y_UNIT_TEST(DescriptionDoesNotGrowWithPartitionCount) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "w1:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "single"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 1
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "spread"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 64
        )");
        env.TestWaitNotification(runtime, txId);

        size_t single = 0;
        size_t spread = 0;
        for (const auto& rec : ReadSchemeChangeRecordsFull(runtime).Entries) {
            if (AnyPathContains(rec, "single")) {
                single = rec.Description.size();
            } else if (AnyPathContains(rec, "spread")) {
                spread = rec.Description.size();
            }
        }

        // "Small" must not be "absent": a fix that stopped capturing
        // Description would otherwise pass this test.
        UNIT_ASSERT_C(single > 0, "the 1-partition table's description must still be captured");
        UNIT_ASSERT_C(spread > 0, "the 64-partition table's description must still be captured");

        UNIT_ASSERT_C(spread < single + 256,
            "Description must not grow with partition count: 1 partition -> " << single
                << " bytes, 64 partitions -> " << spread << " bytes");
    }

    // The order counter must be rewound when a propose is rejected after it
    // has already reserved outbox rows, or the reserved order is never
    // written while the in-memory counter has moved past it, leaving a
    // permanent hole that fetch's gap detection reports as record loss.
    //
    // The source table is created before the subscriber registers so
    // NextSchemeChangeOrder stays at 0 and the aborted propose is the first
    // one that reserves.
    Y_UNIT_TEST(AbortedFirstProposeDoesNotFakeRecordLoss) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        // The redo-size limit is the only check that rejects a propose after
        // IgniteOperation has run, so it's the only way to reach
        // AbortOperationPropose with rows already reserved.
        TControlWrapper maxCommitRedoMB;
        {
            TControlBoard::RegisterSharedControl(
                maxCommitRedoMB, runtime.GetAppData().Icb->TabletControls.MaxCommitRedoMB);
            maxCommitRedoMB.Reset(200, 1, 4096);
        }

        // No subscriber yet, so this reserves nothing and the counter stays 0.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "src"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "w0:sub", regHandle);

        // Reserves an order, then gets rejected: the DB is rolled back but the
        // in-memory counter is not.
        {
            maxCommitRedoMB = 1;
            AsyncCopyTable(runtime, ++txId, "/MyRoot", "src-copy", "/MyRoot/src");
            TestModificationResults(runtime, txId, {{NKikimrScheme::StatusSchemeError,
                "local tx commit redo size generated by IgniteOperation() is more than allowed limit"}});
            env.TestWaitNotification(runtime, txId);
        }

        // ...so this one reserves the NEXT order, and the skipped one exists nowhere.
        {
            maxCommitRedoMB = 200;
            AsyncCopyTable(runtime, ++txId, "/MyRoot", "src-copy", "/MyRoot/src");
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetched = FetchSchemeChangeRecords(runtime, "w0:sub", 0, 100, fetchHandle);

        // Absence of loss must not be absence of data.
        UNIT_ASSERT_VALUES_EQUAL_C(fetched->Record.EntriesSize(), 1u,
            "the successful CopyTable must have produced exactly one record");
        UNIT_ASSERT_VALUES_EQUAL_C(fetched->Record.GetSkippedEntries(), 0u,
            "an aborted propose must not leave a hole in the order sequence; "
            "this subscriber lost nothing but was told it did");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)fetched->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that lost nothing must not be marked Lost");
    }

    // DeleteAckedSchemeChangeRecords selects [oldMinOrder+1, newMinOrder]
    // rather than a one-sided GreaterOrEqual. This test pins the part that can
    // regress: the upper bound is inclusive. A LessOrEqual -> Less slip would
    // strand the boundary record forever, since the next pass starts above it.

    Y_UNIT_TEST(CleanupDeletesThroughTheBoundaryButNotPastIt) {
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
        RegisterSubscriber(runtime, "boundary:sub", regHandle);

        const ui32 total = 6;
        for (ui32 i = 0; i < total; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%u", i));
            env.TestWaitNotification(runtime, txId);
        }

        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= total, "precondition: records must exist");

        const ui64 boundary = 3;

        // The rows are physically present beforehand, so a later "gone" is meaningful.
        auto before = ProbeRecordOrdersPresent(runtime, "boundary:sub", {boundary, boundary + 1});
        UNIT_ASSERT_VALUES_EQUAL_C(before.size(), 2u,
            "both the boundary record and the one above it must exist first");

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "boundary:sub", boundary, ackHandle);
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto after = ProbeRecordOrdersPresent(runtime, "boundary:sub", {boundary, boundary + 1});
        UNIT_ASSERT_VALUES_EQUAL_C(after.size(), 1u,
            "exactly one of the two must survive the sweep");
        UNIT_ASSERT_VALUES_EQUAL_C(after[0], boundary + 1,
            "the acked boundary record must be deleted and the record above it "
            "kept; a stranded boundary row is never revisited");
    }

    // Deleted outbox rows remain as tombstones until compaction, so a cleanup
    // that restarts at order 1 re-seeks the whole dead prefix on every batch.
    // The floor records how far deletion has physically reached and must
    // survive a reboot, or the quadratic drain silently returns.

    Y_UNIT_TEST(CleanupFloorAdvancesAndSurvivesReboot) {
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
        RegisterSubscriber(runtime, "floor:sub", regHandle);

        for (ui32 i = 0; i < 6; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%u", i));
            env.TestWaitNotification(runtime, txId);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->SchemeChangeFloorOrder, 0u,
            "precondition: nothing deleted yet, so the floor must still be 0");

        const ui64 acked = 3;
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "floor:sub", acked, ackHandle);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->SchemeChangeFloorOrder, acked,
            "the floor must advance to the last row actually deleted");

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->SchemeChangeFloorOrder, acked,
            "the floor must be reloaded at TTxInit; resetting to 0 would send "
            "every later cleanup batch back over the tombstoned prefix");
    }

    // Fetching bodies must cost |Orders|, not the span they cover: point
    // lookups instead of walking [min(Orders), max(Orders)].
    Y_UNIT_TEST(FetchBodiesCostTracksRequestedCountNotTheirSpan) {
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
        RegisterSubscriber(runtime, "span:sub", regHandle);

        const ui32 total = 40;
        for (ui32 i = 0; i < total; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%u", i));
            env.TestWaitNotification(runtime, txId);
        }
        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= total, "precondition: records must exist");

        // Two orders at opposite ends of the outbox: minimal request, maximal span.
        const TVector<ui64> orders = {1, tail - 1};

        const ui64 before = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangeOutboxRowsScanned");
        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, "span:sub", orders, bodiesHandle);
        const ui64 scanned = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangeOutboxRowsScanned") - before;

        // The cheap path must still return the data, not just satisfy the
        // scan bound with an empty reply.
        UNIT_ASSERT_VALUES_EQUAL_C(bodies->Record.EntriesSize(), 2u,
            "both requested records must come back; a cheap-but-empty reply "
            "would satisfy the bound below for the wrong reason");

        // 2 metadata lookups + up to 2 detail lookups, generous headroom.
        UNIT_ASSERT_C(scanned <= 8,
            "fetching 2 bodies must not scan the span between them: scanned "
                << scanned << " rows across an outbox of " << tail);
    }

    Y_UNIT_TEST(FetchBodiesRejectsOversizedRequestRatherThanTruncating) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "flood:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        env.TestWaitNotification(runtime, txId);

        TVector<ui64> orders;
        for (ui64 i = 1; i <= 1001; ++i) {
            orders.push_back(i);
        }

        // The plain helper asserts STATUS_SUCCESS, so rejection needs the Expect variant.
        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodiesExpect(runtime, "flood:sub", orders,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, bodiesHandle);

        UNIT_ASSERT_VALUES_EQUAL_C(bodies->Record.EntriesSize(), 0u,
            "a refused request must not carry a partial result");
    }

    Y_UNIT_TEST(RedactedFieldsEmptyWhenNothingSensitive) {
        // Guards against a walker that reports every field it visits, not
        // just the ones actually cleared.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "plain:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawMkDir = false;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpMkDir) {
                sawMkDir = true;
                UNIT_ASSERT_C(rec.RedactedFields.empty(),
                    "a record with nothing sensitive must have an empty RedactedFields");
            }
        }
        UNIT_ASSERT_C(sawMkDir,
            "the MkDir op must itself be among the records checked");
    }

    Y_UNIT_TEST(RedactionDisabledPersistsSensitiveFields) {
        // With the flag off, credentials are persisted in the outbox body and
        // served to every subscriber over a protocol with no authentication
        // (removed deliberately; subscriber-side auth does not exist yet).
        // The flag is only safe once that exists.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        opts.InitYdbDriver(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "unredacted:sub", regHandle);

        ApplySchemeShardConfig(runtime, TSchemeShardConfigOverrides{
            .RedactSensitiveSchemeChangeFields = false,
        });

        const TString password = "s3cr3t-replication-pwd";
        TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  Password: "%s"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", password.c_str()));
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawReplicationOp = false;
        for (const auto& rec : entries) {
            if (rec.OperationType != (ui32)NKikimrSchemeOp::ESchemeOpCreateReplication) {
                continue;
            }
            sawReplicationOp = true;
            TString serializedBody;
            UNIT_ASSERT(rec.Body.SerializeToString(&serializedBody));
            UNIT_ASSERT_C(!serializedBody.empty(),
                "the CreateReplication record must carry a non-empty body for this test to mean anything");
            UNIT_ASSERT_C(serializedBody.Contains(password),
                "with redaction disabled the plaintext password must be present in the body");
            UNIT_ASSERT_C(rec.RedactedFields.empty(),
                "with redaction disabled nothing was cleared, so RedactedFields must be empty");
        }
        UNIT_ASSERT_C(sawReplicationOp,
            "the CreateReplication op must itself be among the records checked");
    }

    // DeleteAckedSchemeChangeRecords used to accept an oldMinOrder watermark
    // from the caller and resume above it. But the subscriber's LastAckedOrder
    // (which feeds oldMinOrder on the *next* call) advances immediately, even
    // when the batch limit stops physical deletion short. A second call with a
    // higher watermark then starts above the undeleted gap and never revisits
    // it: the rows are orphaned forever. SchemeChangeFloorOrder -- what was
    // actually deleted -- is the only sound resume point.
    Y_UNIT_TEST(CleanupNeverOrphansRowsAcrossSuccessiveWatermarkAdvances) {
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
        RegisterSubscriber(runtime, "gap:sub", regHandle);

        const ui32 total = 10;
        for (ui32 i = 0; i < total; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%u", i));
            env.TestWaitNotification(runtime, txId);
        }
        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail >= total, "precondition: records must exist");

        // Small enough that a single Ack's inline delete pass cannot drain a
        // 5-row range in one shot.
        schemeshard->SchemeChangeCleanupBatchSize = 2;

        // Rows [1..5] and [6..7] are both physically present beforehand.
        auto before = ProbeRecordOrdersPresent(runtime, "gap:sub", {1, 2, 3, 4, 5, 6, 7});
        UNIT_ASSERT_VALUES_EQUAL_C(before.size(), 7u,
            "all seven probed rows must exist before any cleanup runs");

        // First Ack: deletes only the first batch (orders 1-2) inline, then
        // stops with hasMore=true. No SimulateSleep is called here, so the
        // scheduled follow-up cleanup tx (10ms later) has not run yet and
        // cannot mask the bug by draining the gap on its own before the
        // second Ack below observes the (already advanced) watermark.
        TAutoPtr<IEventHandle> ack1Handle;
        AckSchemeChangeRecords(runtime, "gap:sub", 5, ack1Handle);

        // Positive companion: orders 3-5 are still physically present right
        // after the first, batch-capped Ack -- the gap this bug orphans.
        auto midGap = ProbeRecordOrdersPresent(runtime, "gap:sub", {3, 4, 5});
        UNIT_ASSERT_VALUES_EQUAL_C(midGap.size(), 3u,
            "precondition: the batch limit must actually leave 3-5 undeleted "
            "after the first Ack, or this test proves nothing");

        // Second Ack: LastAckedOrder is already 5 from the call above, so the
        // buggy code would resume cleanup at order 6, permanently skipping
        // whatever remains undeleted in [3..5].
        TAutoPtr<IEventHandle> ack2Handle;
        AckSchemeChangeRecords(runtime, "gap:sub", 7, ack2Handle);

        // Let any remaining scheduled continuations finish draining. This
        // cannot paper over the bug: once the buggy Ack(7) call above jumps
        // the floor straight to 7, rows 3-5 are below the floor and no later
        // pass -- scheduled or not -- ever selects them again.
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto after = ProbeRecordOrdersPresent(runtime, "gap:sub", {1, 2, 3, 4, 5, 6, 7});
        UNIT_ASSERT_C(after.empty(),
            "every acked row through order 7 must eventually be deleted; "
            << after.size() << " still present");
    }

    Y_UNIT_TEST(OutboxCountersAreExported) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribers"), 0u,
            "no subscriber registered yet");

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "w3:sub", regHandle);

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribers"), 1u,
            "the registered subscriber must be visible to monitoring");
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeOutboxDepth"), 0u,
            "nothing emitted yet, so the backlog must read zero");

        for (int i = 0; i < 3; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "t%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeOutboxDepth"), 3u,
            "three unacked records must show as a backlog of three");
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeSubscribersLost"), 0u,
            "nobody has lost anything");
        UNIT_ASSERT_C(
            GetCumulativeCounter(runtime, "SchemeShard/SchemeChangeDescriptionBytes") > 0,
            "the redo cost of captured descriptions must be attributed somewhere");

        // Draining the backlog must bring the gauge back down.
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "w3:sub", 3, ackHandle);

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetSimpleCounter(runtime, "SchemeShard/SchemeChangeOutboxDepth"), 0u,
            "after acking everything the backlog must return to zero");
    }

    Y_UNIT_TEST(SecretValueNotPersistedInSchemeChangeRecord) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "secret:sub", regHandle);

        const TString secretValue = "s3cr3t-do-not-log-me";
        TestCreateSecret(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "MySecret"
            Value: "%s"
        )", secretValue.c_str()));
        env.TestWaitNotification(runtime, txId);

        // TSecretSchemaOp.Value is marked sensitive in the proto, but that
        // only governs logging; it would still be serialized into the outbox.
        auto entries = ReadSchemeChangeRecords(runtime);
        // An absence-assertion over an empty set proves nothing.
        UNIT_ASSERT_C(!entries.empty(),
            "creating a secret must produce at least one record for this test to mean anything");
        bool sawSecretOp = false;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateSecret) {
                sawSecretOp = true;
            }
            TString serializedBody;
            UNIT_ASSERT(rec.Body.SerializeToString(&serializedBody));
            UNIT_ASSERT_C(!serializedBody.Contains(secretValue),
                "the plaintext secret must not appear in a persisted record body");
            UNIT_ASSERT_C(!rec.Description.Contains(secretValue),
                "the plaintext secret must not appear in a persisted description");
        }
        UNIT_ASSERT_C(sawSecretOp,
            "the secret CREATE must itself be among the records checked");
    }

    Y_UNIT_TEST(ReplicationPasswordNotPersistedInSchemeChangeRecord) {
        // Only CreateSecret/AlterSecret.Value used to be redacted; every other
        // (Ydb.sensitive) field, e.g. TStaticCredentials.Password, was
        // serialized into the outbox body in cleartext.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        opts.InitYdbDriver(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "repl:sub", regHandle);

        const TString password = "s3cr3t-replication-pwd";
        TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  Password: "%s"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", password.c_str()));
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(),
            "creating a replication must produce at least one record for this test to mean anything");
        bool sawReplicationOp = false;
        for (const auto& rec : entries) {
            TString serializedBody;
            UNIT_ASSERT(rec.Body.SerializeToString(&serializedBody));
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateReplication) {
                sawReplicationOp = true;
                UNIT_ASSERT_C(!serializedBody.empty(),
                    "the CreateReplication record must carry a non-empty body for this test to mean anything");
            }
            UNIT_ASSERT_C(!serializedBody.Contains(password),
                "the plaintext replication password must not appear in a persisted record body");
            UNIT_ASSERT_C(!rec.Description.Contains(password),
                "the plaintext replication password must not appear in a persisted description");
        }
        UNIT_ASSERT_C(sawReplicationOp,
            "the CreateReplication op must itself be among the records checked");
    }

    Y_UNIT_TEST(RedactionIsOnByDefault) {
        // With no config applied, the secret must be absent -- the safe
        // default given the protocol has no subscriber-side authentication.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        opts.InitYdbDriver(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "default:sub", regHandle);

        const TString password = "s3cr3t-replication-pwd";
        TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  Password: "%s"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", password.c_str()));
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawReplicationOp = false;
        for (const auto& rec : entries) {
            TString serializedBody;
            UNIT_ASSERT(rec.Body.SerializeToString(&serializedBody));
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateReplication) {
                sawReplicationOp = true;
            }
            UNIT_ASSERT_C(!serializedBody.Contains(password),
                "the plaintext replication password must not appear in a persisted record body by default");
        }
        UNIT_ASSERT_C(sawReplicationOp,
            "the CreateReplication op must itself be among the records checked");
    }

    Y_UNIT_TEST(RedactedFieldsNamesTheStrippedPassword) {
        // A consumer must be able to tell "no password was set" from
        // "a password was stripped" -- RedactedFields is that signal.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        opts.InitYdbDriver(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "redact:sub", regHandle);

        const TString password = "s3cr3t-replication-pwd";
        TestCreateReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  Password: "%s"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", password.c_str()));
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawReplicationOp = false;
        for (const auto& rec : entries) {
            if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpCreateReplication) {
                sawReplicationOp = true;
                UNIT_ASSERT_C(!rec.RedactedFields.empty(),
                    "a record with a stripped password must name what was stripped");
                bool namesPassword = false;
                for (const auto& field : rec.RedactedFields) {
                    if (field.Contains("Password")) {
                        namesPassword = true;
                    }
                }
                UNIT_ASSERT_C(namesPassword,
                    "RedactedFields must name the password field, got: "
                        << JoinSeq(",", rec.RedactedFields));
            }
        }
        UNIT_ASSERT_C(sawReplicationOp,
            "the CreateReplication op must itself be among the records checked");
    }
}
