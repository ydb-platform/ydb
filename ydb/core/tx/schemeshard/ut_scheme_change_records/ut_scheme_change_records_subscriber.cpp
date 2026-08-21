#include "ut_scheme_change_records_helpers.h"

#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSubscriberTests) {
    Y_UNIT_TEST(MockBackupSubscriberEndToEnd) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> advHandle;
        ForceAdvanceSubscriberExpect(runtime, "nonexistent:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, advHandle);
    }

    Y_UNIT_TEST(FetchReturnsMetadataWithoutBody) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
            UNIT_ASSERT_C(entry.GetBodySize() > 0,
                "Metadata should include non-zero BodySize for entry " << entry.GetOrder());
        }
    }

    Y_UNIT_TEST(FetchBodiesReturnsRequestedBodies) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);

        TVector<ui64> orders = {1, 2, 3};
        TAutoPtr<IEventHandle> bodiesHandle;
        FetchSchemeChangeRecordBodiesExpect(runtime, "ghost:sub", orders,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, bodiesHandle);
    }

    Y_UNIT_TEST(FetchUnknownSubscriberReturnsNotRegistered) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> fetchHandle;
        FetchSchemeChangeRecordsExpect(runtime, "ghost:sub", 0, 100,
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED, fetchHandle);
    }

    Y_UNIT_TEST(AckDeletesAckedRecordsInline) {
        // Phase 3 invariant: Ack tx deletes records (up to ReactiveCleanupCap)
        // in the same transaction. No manual wakeup / background sweep needed.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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
        // Ack of a backlog larger than SchemeChangeCleanupBatchSize drains
        // via a scheduled continuation chain; poll until empty.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        // ForceAdvance jumps cursor to tail: the slowest-case stuck subscriber.
        // It must delete newly-stale records inline, same as Ack/Unregister.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        // Slow subscriber holds min cursor at 0. Write N > 1000 records.
        // Fast subscriber acks everything. Unregister the slow one. All
        // records must be gone immediately, no wakeup needed.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        // Unregister slow -> min jumps to fast's tail -> drain via continuation chain.
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

    // --- Phase 1.0: subscriber start-position seam -------------------------
    //
    // NextSchemeChangeOrder is the *last assigned* order, not the next one
    // (schemeshard__scheme_change_records.cpp:6, `ui64 id = ++NextSchemeChangeOrder;`),
    // so "register at tail" means LastAckedOrder == NextSchemeChangeOrder exactly.

    Y_UNIT_TEST(RegisterSubscriberDefaultsToTailNotZero) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
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

        // A brand-new subscriber must start at the tail, not at 0. Starting at
        // 0 both replays history it never asked for and pins the retention
        // floor low, which is what wedges DDL (see the next test).
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
        // the acked records are swept.
        const ui64 tail = schemeshard->NextSchemeChangeOrder;
        UNIT_ASSERT_C(tail > 0, "precondition: some records must exist");
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "sub-A", tail, ackHandle);

        // "Mature cluster": NextSchemeChangeOrder has reached the cap. Rather
        // than driving 100k MkDirs (the default cap, schemeshard_impl.h:287),
        // lower the cap to the current tail -- the gate at
        // schemeshard_impl.cpp:4239 compares
        // NextSchemeChangeOrder - GetMinSubscriberOrder() against it.
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

        // And DDL must still be accepted. Today this returns
        // StatusResourceExhausted with no way out: the records sub-B is
        // "behind" on were already swept, so its Fetch returns nothing and
        // there is nothing it can ack.
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

        // The actual point of this test: clamping preserves the anti-wedge
        // invariant. Registering must never widen the unacked window.
        UNIT_ASSERT_VALUES_EQUAL_C(
            schemeshard->NextSchemeChangeOrder - schemeshard->GetMinSubscriberOrder(runtime.GetCurrentTime()),
            unackedBefore,
            "registering below the floor must not widen the unacked window");
    }

    Y_UNIT_TEST(ExplicitStartOrderOnEmptyLogAccepted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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

        // Exactly at the floor: accepted verbatim. Pins the boundary as `<`,
        // not `<=`. Order is 1-based and StartOrder is an exclusive cursor, so
        // an off-by-one here would reject every legitimate unswept read.
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
        TTestEnv env(runtime, opts, ssFactory);

        TAutoPtr<IEventHandle> reg;
        // Beyond the tail is not a real position; the Expect variant carries
        // the assertion.
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

        // Both columns must survive a reboot -- that is the whole point of
        // adding them to the schema now rather than later.
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

    // --- Phase 0: harness guards --------------------------------------------

    Y_UNIT_TEST(ReadHelperSeesAllRecordsRegardlessOfTail) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regA;
        RegisterSubscriber(runtime, "keeper:sub", regA);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3");
        env.TestWaitNotification(runtime, txId);

        // The read helper mints its temp subscriber fresh on every call. Once
        // registration defaults to the tail, a helper that did not ask for an
        // explicit StartOrder would scan from tail+1 and return empty on every
        // call made after any DDL -- silently turning ~29 existing tests green
        // against a log it never actually read.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.size() >= 3,
            "read helper must see the whole retained log, not start at the tail; got "
                << entries.size());

        // And it must still work on a second call, i.e. it is not sensitive to
        // its own previous registration having advanced anything.
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

        // Second, cursor-independent oracle: the rows are physically there.
        auto present = ProbeRecordOrdersPresent(runtime, "keeper:sub", {1, tail});
        UNIT_ASSERT_C(!present.empty(),
            "physical probe must see rows before the sweep");

        // Force-advance the only subscriber, which sweeps everything.
        TAutoPtr<IEventHandle> faHandle;
        ForceAdvanceSubscriber(runtime, "keeper:sub", faHandle);

        // Now the helper must return empty *because the log is empty*, not
        // because its registration was rejected or its cursor clamped past
        // live rows. The registration inside the helper asserts SUCCESS, and
        // the physical probe independently confirms the rows are gone. This is
        // the guard against the vacuous-green mode.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(entries.size(), 0u,
            "after a full sweep the helper must read empty");

        auto afterSweep = ProbeRecordOrdersPresent(runtime, "keeper:sub", {1, tail});
        UNIT_ASSERT_VALUES_EQUAL_C(afterSweep.size(), 0u,
            "emptiness must be real: no record rows may remain on disk");
    }

    // --- Phase 1.3: no propose-time write when nobody is subscribed ---------
    //
    // The measurement is taken mid-DDL, after a reboot: TTxInit rebuilds
    // Operations[txId]->SchemeChangeSlots *only* from persisted rows, so
    // "nothing was reserved at propose" is directly visible, and so is its
    // mirror. Asserting post-completion instead would not distinguish the two
    // gates -- with no subscriber there is nothing to finalise either.

    Y_UNIT_TEST(NoSubscribersMeansNoReservedOutboxRows) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
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

        // Positive companion: absence above must mean "never written", not
        // "written and swept". Nothing may reach the outbox at all.
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

        // The mirror of the test above: the gate must not be so aggressive that
        // it kills durability across a propose->done reboot, which is the whole
        // reason the reservation is persisted.
        auto it = schemeshard->Operations.find(TTxId(opTxId));
        if (it != schemeshard->Operations.end()) {
            UNIT_ASSERT_C(!it->second->SchemeChangeSlots.empty(),
                "with a subscriber registered the reserved outbox rows must survive "
                "a propose->done reboot");
        }

        env.TestWaitNotification(runtime, opTxId);

        // And the reservation must actually turn into a delivered record.
        UNIT_ASSERT_VALUES_EQUAL_C(ReadSchemeChangeRecords(runtime).size(), 1u,
            "the surviving reservation must be finalised into a fetchable record");
    }

    // --- Phase 1.4: a stale subscriber must not wedge DDL forever ------------
    //
    // All four LastActivityAt writes and the age comparison use ctx.Now().
    // Under TTestActorRuntime the actor-system clock is virtual and starts at
    // 0 while the wall clock does not, and TInstant subtraction saturates at
    // zero -- so a half-conversion would make every age 0 and nothing would
    // ever be stale, silently, in every UT.

    Y_UNIT_TEST(StaleSubscriberDoesNotBlockDdlForever) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        const ui64 ttlSeconds = 100;
        ApplySchemeShardConfig(runtime, {
            .MaxSchemeChangeRecords = 2,
            .SchemeChangeSubscriberStaleTtlSeconds = ttlSeconds,
        });

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "dead:sub", regHandle);

        // Drive DDL past the cap without ever acking, so the gate would reject.
        // cap=2 means exactly 2 records fit; the next DDL is at the cap.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir2");
        env.TestWaitNotification(runtime, txId);

        // The subscriber goes dark for well past the TTL.
        runtime.SimulateSleep(TDuration::Seconds(2 * ttlSeconds));

        // It must stop holding the floor down, so DDL recovers on its own with
        // no operator intervention.
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

        // Backpressure must STILL apply. Without this leg the GREEN could pass
        // by disabling backpressure outright.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir3", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(StaleExclusionMarksSubscriberLost) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        // Keep the TTL small: SimulateSleep cost scales with simulated time.
        const ui64 ttlSeconds = 5;
        ApplySchemeShardConfig(runtime, {
            .SchemeChangeSubscriberStaleTtlSeconds = ttlSeconds,
        });

        // BOTH register on an empty log, so both cursors sit at 0 and the
        // delete range the ack path uses -- (oldMin, newMin] -- will actually
        // cover the records dead:sub never read. Deliberately no reboot here:
        // ApplyConfig settings are in-memory, so a reboot would silently
        // restore the 30-day default TTL and nothing would be stale.
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

        // ...but live:sub comes back, which refreshes only its own
        // LastActivityAt. dead:sub stays stale and is excluded from the floor,
        // so live:sub's ack sweeps records dead:sub never consumed.
        TAutoPtr<IEventHandle> liveFetch;
        FetchSchemeChangeRecords(runtime, "live:sub", 0, 100, liveFetch);
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "live:sub", tail, ackHandle);

        // The stale subscriber comes back and must be TOLD its records are
        // gone. Excluding it is intended; losing its records silently is not.
        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "dead:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_C(fetch->Record.GetSkippedEntries() > 0,
            "a stale subscriber whose records were swept must be told how many it lost");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)fetch->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST,
            "and must be marked Lost, never silently short-changed");
    }

    // --- Phase 1.5: internal ops bypass the gate; the cap binds the subscriber
    //
    // Policy, not merely "bypass":
    //   * Internal ops are NEVER rejected -- rejecting split/merge, temp-dir GC
    //     or export/import is a cluster outage, not backpressure.
    //   * Churn ops are excluded separately, and that is NOT redundant with
    //     Internal: a user-initiated split is not Internal=true, yet emits no
    //     record, so gating it blocks an op on an outbox it never feeds.
    //   * The cap is still enforced -- at the record-allocation site, against
    //     the subscriber, by force-advancing it and marking it Lost.

    Y_UNIT_TEST(UserDdlStillRejectedAtCap) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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

        // Park the subscriber at the cap without acking. The CreateTable above
        // already put the subscriber at or above the cap, so no extra DDL is needed
        // (and any extra DDL would itself be rejected, masking the split).
        ApplySchemeShardConfig(runtime, {.MaxSchemeChangeRecords = 1});

        // A user-initiated split is NOT Internal=true, so the Internal bypass
        // alone would not save it -- this pins the churn clause specifically.
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
        // and must never mark a subscriber Lost. This is the direct
        // counter-test to enforcing the cap at the propose gate.
        const auto after = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(after.size(), before.size(),
            "a split must not append scheme change records");

        auto it = schemeshard->Subscribers.find("parked:sub");
        UNIT_ASSERT(it != schemeshard->Subscribers.end());
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)it->second.State,
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "churn at the cap must not mark the subscriber Lost");
    }

    // --- Phase 1.6: identity, validation, cap -------------------------------
    //
    // These events arrive over a tablet pipe, which carries no caller identity,
    // so the protocol had to grow a UserToken field before any of this could be
    // enforced (see plan finding F4).

    Y_UNIT_TEST(EmptySubscriberIdRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // The dangerous case: two consumers that both leave SubscriberId unset
        // silently share one cursor, so one's ack deletes records the other
        // never saw while SkippedEntries reports 0.
        TAutoPtr<IEventHandle> handle;
        RegisterSubscriberWithTokenExpect(runtime, "", "",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, handle);
    }

    Y_UNIT_TEST(OverlongSubscriberIdRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> handle;
        RegisterSubscriberWithTokenExpect(runtime, TString(300, 'x'), "",
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
        TTestEnv env(runtime, opts, ssFactory);

        schemeshard->MaxSchemeChangeSubscribers = 2;

        TAutoPtr<IEventHandle> h1, h2, h3;
        RegisterSubscriber(runtime, "sub-1", h1);
        RegisterSubscriber(runtime, "sub-2", h2);

        // Each subscriber pins retention and costs an O(|Subscribers|) pass on
        // the DDL admission path, so the count must be bounded.
        RegisterSubscriberWithTokenExpect(runtime, "sub-3", "",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, h3);

        // Re-registering an EXISTING id must still succeed: registration is
        // documented as idempotent and consumers rely on it across restarts.
        TAutoPtr<IEventHandle> h4;
        RegisterSubscriber(runtime, "sub-1", h4);
    }

    Y_UNIT_TEST(UnauthorizedRegisterRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // Without this the test is vacuous: an EMPTY allowlist admits any
        // token, including none (auth.cpp IsTokenAllowedImpl).
        runtime.GetAppData().AdministrationAllowedSIDs.push_back("thou-shalt-not-pass");

        TAutoPtr<IEventHandle> handle;
        RegisterSubscriberWithTokenExpect(runtime, "intruder:sub", "",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_ACCESS_DENIED, handle);
    }

    Y_UNIT_TEST(UnauthorizedForceAdvanceRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "victim:sub", regHandle);

        // Deny only AFTER a legitimate registration, so the rejection below is
        // attributable to the admin gate rather than to setup failing.
        runtime.GetAppData().AdministrationAllowedSIDs.push_back("thou-shalt-not-pass");

        // Force-advance discards unread records on purpose -- operator-only.
        TAutoPtr<IEventHandle> advHandle;
        ForceAdvanceSubscriberExpect(runtime, "victim:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_ACCESS_DENIED, advHandle);
    }

    // --- Phase 1.7: the operator surface actually exists --------------------

    Y_UNIT_TEST(ForceAdvanceReachableFromMonitoring) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
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

        // RFC 0129:359-363 makes force-advance the entire mitigation for a dead
        // subscriber wedging DDL -- but it had no reachable caller at all until
        // this action existed. Drive it the way an operator would.
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
        TTestEnv env(runtime);

        auto resp = SendSchemeShardMonRequest(runtime, TTestTxConfig::SchemeShard,
            "/app?Action=ForceAdvanceSchemeChangeSubscriber&SubscriberId=ghost:sub",
            HTTP_METHOD_POST);
        UNIT_ASSERT_C(resp.Body.Contains("ghost:sub") || resp.Body.Contains("No scheme change subscriber"),
            "an unknown subscriber must produce a clear operator-facing error, got: " << resp.Body);
    }

    // --- Phase 2.2 / 2.3: the record carries resolved identity + description --

    Y_UNIT_TEST(RecordCarriesResolvedIdentity) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        // request body. Every one of these was a hardcoded placeholder before.
        UNIT_ASSERT_VALUES_EQUAL_C(rec.OperationType,
            (ui32)NKikimrSchemeOp::ESchemeOpCreateTable,
            "OperationType must be the user-level EOperationType, not a TxType placeholder");
        UNIT_ASSERT_C(rec.PathLocalId != 0,
            "PathId must be resolved, not left empty");
        UNIT_ASSERT_VALUES_EQUAL_C(rec.ObjectType,
            (ui32)NKikimrSchemeOp::EPathTypeTable,
            "ObjectType must be resolved");
        UNIT_ASSERT_C(rec.Path.Contains("T1"),
            "Path must name the target, got: " << rec.Path);
    }

    Y_UNIT_TEST(CreateRecordCarriesResolvedDescription) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        // The point of the description is self-containment: the object must be
        // reconstructible from the record alone, with no call back into
        // SchemeShard -- which matters because after a DROP it is gone, and a
        // later read would race a newer version.
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

    // --- Phase 2.4: plaintext secrets must never reach the outbox ------------

    Y_UNIT_TEST(SecretValueNotPersistedInSchemeChangeRecord) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "secret:sub", regHandle);

        const TString secretValue = "s3cr3t-do-not-log-me";
        TestCreateSecret(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "MySecret"
            Value: "%s"
        )", secretValue.c_str()));
        env.TestWaitNotification(runtime, txId);

        // TSecretSchemaOp.Value is the "original, unencrypted value". It is
        // marked sensitive in the proto, but that only governs logging -- it
        // would still be serialized verbatim into the outbox and handed to
        // every subscriber.
        auto entries = ReadSchemeChangeRecords(runtime);
        // Guard against a vacuous pass: an absence-assertion over an empty set
        // proves nothing, and would silently "pass" if secrets were gated off
        // or produced no record at all.
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

    // --- Phase 2.1: the churn denylist is the only filter -------------------

    Y_UNIT_TEST(SplitMergeProducesNoRecords) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        // Partitioning is layout, not data-encoding: a row is byte-identical at
        // 10 shards or 40, so no consumer needs the history of splits. Streaming
        // them would also emit thousands of records per repartitioning and, via
        // the outbox budget, stall DDL worst on the largest databases.
        const auto after = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(after.size(), before.size(),
            "auto-partitioning must never reach the outbox");
    }

    Y_UNIT_TEST(ChurnListIsTheOnlyFilter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "filter:sub", regHandle);

        // Everything not on the churn list is logged, including ops a naive
        // "user-facing only" rule would drop. The asymmetry is deliberate: a
        // missing DDL corrupts a restore, an extra record costs bytes.
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

    // --- Phase 2.5: identity must resolve for every object type -------------
    //
    // The name-extraction switch originally covered 6 op types. For anything
    // else targetName stayed empty, so Path fell back to WorkingDir -- which
    // RESOLVES, to the parent directory. That gives the record confidently
    // wrong identity (the parent's PathId, ObjectType=Dir) rather than none,
    // which is strictly worse for a consumer.

    Y_UNIT_TEST(NonTableObjectsCarryTheirOwnIdentity) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().RunFakeConfigDispatcher(true));
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

        UNIT_ASSERT_C(extSrc->Path.Contains("ExtSrc"),
            "Path must name the object itself, not just its parent dir; got: " << extSrc->Path);
        UNIT_ASSERT_VALUES_EQUAL_C(extSrc->ObjectType,
            (ui32)NKikimrSchemeOp::EPathTypeExternalDataSource,
            "ObjectType must be the object's own type, not the parent's EPathTypeDir");
    }

    // --- Phase 3.1: (PlanStep, PositionKind) is always meaningful ------------

    Y_UNIT_TEST(EveryRecordHasNonZeroPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
        TTestEnv env(runtime);
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

        // The borrow is a LOWER bound: SchemeShard had already applied that
        // step, so the ACL change provably happened at or after it. Hence
        // Bucketed sorts after Exact at equal step.
        UNIT_ASSERT_C(proposeTime->PlanStep >= coordinated->PlanStep,
            "a borrowed step must be >= the step it borrowed from; got "
                << proposeTime->PlanStep << " vs " << coordinated->PlanStep);
    }

    // --- Phase 3.3: the sync point is identifiable in the stream ------------

    Y_UNIT_TEST(IncrementalBackupProducesIdentifiableSyncPoint) {
        TTestBasicRuntime runtime;
        // Backup collections are feature-gated; without this the op is rejected
        // with StatusPreconditionFailed and the test proves nothing.
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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

        // The actual sync point is the backup RUN, not the collection object.
        // A full backup must precede an incremental one.
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot", R"(
            Name: ".backups/collections/MyCollection"
        )");
        env.TestWaitNotification(runtime, txId);

        // Backup artifacts are named from the wall clock at second granularity
        // (ToX509String -> "19700101000000Z_continuousBackupImpl"). Under
        // TTestActorRuntime the clock starts at epoch 0 and only moves when the
        // test moves it, so without this the incremental backup collides with
        // the full backup's stream name.
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

        // The sync point must be identifiable by the body's EOperationType.
        // It cannot come from ETxType: ConvertToTxType maps
        // ESchemeOpBackupIncrementalBackupCollection onto TxInvalid, which it
        // shares with 16 other operations including ESchemeOpCreateIndexedTable
        // and ESchemeOpCreateCdcStream.
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

    // --- Phase 4.1 / 3.6: closure bound + pinned assumptions ----------------

    Y_UNIT_TEST(ClosedThroughPlanStepIsInclusiveAtQuiesce) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "close:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        auto result = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(!result.Entries.empty(), "a record must exist");
        const ui64 lastStep = result.Entries.back().PlanStep;

        // With nothing in flight the bound must reach the newest record's own
        // step. The previous implementation reported the min-in-flight, so the
        // last DDL before quiesce was never releasable -- exactly the
        // single-ALTER-then-idle case the signal exists for.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep >= lastStep,
            "a quiesced cluster must be able to close its newest window: closed="
                << result.ClosedThroughPlanStep << " lastRecordStep=" << lastStep);
    }

    Y_UNIT_TEST(PerObjectOrderingHoldsWithoutGlobalOrdering) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "order:sub", regHandle);

        // Two independent objects. D3 says NO cross-object ordering is
        // required; what must hold is per-object order.
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "DirB");
        env.TestWaitNotification(runtime, txId);
        TestRmDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui64 createA = 0, dropA = 0;
        for (const auto& rec : entries) {
            if (!rec.Path.Contains("DirA")) {
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
        TTestEnv env(runtime, opts, ssFactory);

        // D7/Option A rests on this invariant. If a config ever puts a second
        // transaction-supporting domain under one SchemeShard, D6's global-max
        // borrow stops being a sound lower bound -- so fail loudly here rather
        // than emit records with incomparable steps.
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->CountTransactionSupportingDomains(), 1u,
            "the default test env must serve exactly one transaction-supporting domain");

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "domain:sub", regHandle);
    }

    // --- Phase 3.5: window assignment, end to end (acceptance gate) ---------

    Y_UNIT_TEST(DdlBucketsIntoCorrectBackupWindow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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

        // Locate the two sync points by the body's EOperationType. This is the
        // whole reason 2.2 stores the request-level enum: ConvertToTxType maps
        // backup-collection ops onto TxInvalid, shared with 16 others.
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
                && rec.Path.Contains("T2")) {
                createT2 = &rec;
            } else if (rec.OperationType == (ui32)NKikimrSchemeOp::ESchemeOpDropTable
                && rec.Path.Contains("T2")) {
                dropT2 = &rec;
            }
        }
        UNIT_ASSERT_C(createT2, "CREATE T2 must be recorded");
        UNIT_ASSERT_C(dropT2, "DROP T2 must be recorded");

        UNIT_ASSERT_VALUES_EQUAL_C(windowOf(*createT2), 1,
            "CREATE T2 happened between the sync points and must bucket into (S1, S2]");
        UNIT_ASSERT_VALUES_EQUAL_C(windowOf(*dropT2), 2,
            "DROP T2 happened after S2 and must NOT be squeezed into the earlier window");

        // Completeness. Seeing the S2 record is NOT sufficient: Order is
        // completion order, ts is plan order, and they diverge -- a DDL planned
        // before S2 can be persisted after it. The consumer may only close
        // (S1, S2] once the closure bound has passed S2.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep >= s2,
            "with everything quiesced the consumer must be able to close the window: closed="
                << result.ClosedThroughPlanStep << " s2=" << s2);
    }

    // --- Phase 5: NFR gate --------------------------------------------------

    Y_UNIT_TEST(ClosureBoundIsIndexedNotScanned) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "nfr:sub", regHandle);

        for (int i = 0; i < 8; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        // NFR2: the bound is served from an incrementally maintained index, not
        // by scanning TxInFlight on every Fetch. Once everything completes the
        // index must be empty -- entries are released by RemoveTx, so a leak
        // here would mean the bound silently stops advancing.
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
        // NFR2. A MkDir's request body and description are both small; a
        // regression that started stashing whole table descriptions on every
        // op, or emitting per-part instead of per-user-op, would blow this.
        UNIT_ASSERT_C(perDdl < 16384,
            "outbox redo per DDL must stay bounded; got " << perDdl << " bytes");
    }

    // --- Phase 3.4 / 3.5 (contended) ----------------------------------------

    Y_UNIT_TEST(SyncPointPlanStepIsTheWindowEdge) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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
            } else if (rec.Path.Contains("BeforeSync")) {
                beforeStep = rec.PlanStep;
            } else if (rec.Path.Contains("AfterSync")) {
                afterStep = rec.PlanStep;
            }
        }
        UNIT_ASSERT_C(syncStep && beforeStep && afterStep,
            "sync point and both DDLs must be recorded");

        // D8: the backup op is a coordinated tx, so its PlanStep IS the cut --
        // DDL planned before it falls inside the window, DDL after falls out.
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
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "held:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Settled");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->InFlightByPlanStep.size(), 0u,
            "precondition: nothing in flight");

        // Pin an operation between plan-step assignment and completion.
        //
        // Which event to withhold matters, and the obvious choices are wrong.
        // TCreateTable runs CreateParts (Hive) then ConfigureParts (which awaits
        // TEvProposeTransactionResult) and only THEN Propose to the coordinator,
        // so blocking either of those stops the op before it has a PlanStep --
        // the index would be legitimately empty and the test would prove
        // nothing. The first state after the step is assigned is
        // ProposedWaitParts, where SchemeShard awaits TEvSchemaChanged. Holding
        // that leaves the op with a PlanStep and no emitted record, which is
        // exactly the state the closure bound exists to describe.
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

    // --- Option A (4.3): the record must survive a propose->done reboot -----
    //
    // THIS IS THE TEST WHOSE ABSENCE MAKES A NAIVE 4.3 LOOK SAFE.
    //
    // DoPersistSchemeChangeRecords builds every record from the IN-MEMORY
    // operation->UserLevelTransactions. Today table 144 repopulates that vector
    // at TTxInit. Delete the table without replacing that job and a SchemeShard
    // restart between propose and done emits NO record at all -- and the rest of
    // the reboots suite would not notice, because it reboots around completion
    // rather than inside the window.

    Y_UNIT_TEST(RecordSurvivesProposeToDoneReboot) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "durable:sub", regHandle);

        const auto baseline = ReadSchemeChangeRecords(runtime);

        // Park the operation AFTER the coordinator has assigned its plan step
        // but BEFORE it completes: TCreateTable reaches ProposedWaitParts and
        // waits on TEvSchemaChanged. (Blocking Hive or the propose result would
        // hold it before the step exists, which is a different window.)
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
            if (rec.Path.Contains("Durable")) {
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

    Y_UNIT_TEST(RecordIsWrittenAtProposeTime) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
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

        // The record IS the durable artifact, so the row must already be on
        // disk while the operation is still in flight. That is what carries
        // durability across a propose->done reboot.
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

        // ...and the positive companion: physically present is NOT the same as
        // deliverable. An un-finalised row carries no identity and no plan step,
        // so a subscriber must not be able to fetch it yet -- otherwise an ack
        // would advance the cursor past a record that never got its contents.
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
            if (rec.Path.Contains("Early")) {
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

    // A reserved order is not a consumed one. Every path that parks a cursor at
    // "the tail" has to mean the VISIBLE tail, or it silently swallows records
    // belonging to operations that were merely still in flight at the time.
    //
    // Registration is the cheapest of the three such paths to drive (the other
    // two are force-advance and cap relief), and it is the one a real consumer
    // hits first: subscribe while a DDL happens to be running, and that DDL's
    // record must still arrive.
    Y_UNIT_TEST(SubscriberRegisteredMidDdlStillReceivesThatRecord) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
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
        // finalised. Defaulting to the reserved tail would put its cursor above
        // that row.
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
        UNIT_ASSERT_C(fetched->Record.GetEntries(0).GetPath().Contains("MidDdl"),
            "and it must be that DDL's record");

        // Absence of loss must be real, not merely unreported.
        UNIT_ASSERT_VALUES_EQUAL_C(fetched->Record.GetSkippedEntries(), 0u,
            "nothing was swept, so nothing may be reported as skipped");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)fetched->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that lost nothing must not be marked Lost");
    }
}
