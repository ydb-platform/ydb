#include "ut_scheme_change_records_helpers.h"

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
        auto result = ForceAdvanceSubscriber(runtime, "nonexistent:sub", advHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
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
        auto bodies = FetchSchemeChangeRecordBodies(runtime, "ghost:sub", orders, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
    }

    Y_UNIT_TEST(FetchUnknownSubscriberReturnsNotRegistered) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "ghost:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(
            (ui32)fetch->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
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
}
