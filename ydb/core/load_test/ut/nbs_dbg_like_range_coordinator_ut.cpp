#include <ydb/core/load_test/nbs_dbg_like_range_coordinator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <set>

using namespace NKikimr::NNbsDbgLike;

Y_UNIT_TEST_SUITE(NbsDbgLikeRangeCoordinator) {
    Y_UNIT_TEST(AcceptanceOrdersFlushesDespiteReorderedConfirmations) {
        TSlotIoCoordinator slots;
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 0}, 2);
        slots.MakeVisible({0, 0}, 2);
        slots.MakeVisible({0, 0}, 1);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({0, 0}), 2);
        UNIT_ASSERT(!slots.CanFlush({0, 0}, 2));
        UNIT_ASSERT(slots.CanFlush({0, 0}, 1));
        slots.MarkFlushed({0, 0}, 1);
        UNIT_ASSERT(slots.CanFlush({0, 0}, 2));
    }

    Y_UNIT_TEST(PendingOverwritePreservesVisibleVersion) {
        TSlotIoCoordinator slots;
        slots.Accept({0, 0}, 1);
        slots.MakeVisible({0, 0}, 1);
        slots.Accept({0, 0}, 2);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({0, 0}), 1);
        slots.MakeVisible({0, 0}, 2);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({0, 0}), 2);
    }

    Y_UNIT_TEST(PBReadersDelayEraseWhileNextFlushProceeds) {
        TSlotIoCoordinator slots;
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 0}, 2);
        slots.PinPBRead({0, 0}, 1);
        UNIT_ASSERT(slots.CanFlush({0, 0}, 1));
        slots.MarkFlushed({0, 0}, 1);
        UNIT_ASSERT(!slots.CanErase({0, 0}, 1));
        UNIT_ASSERT(slots.CanFlush({0, 0}, 2));
        slots.MarkFlushed({0, 0}, 2);
        UNIT_ASSERT(!slots.CanErase({0, 0}, 2));
        slots.UnpinPBRead({0, 0}, 1);
        UNIT_ASSERT(slots.CanErase({0, 0}, 1));
        slots.Retire({0, 0}, 1);
        UNIT_ASSERT(slots.CanErase({0, 0}, 2));
    }

    Y_UNIT_TEST(DDiskReadersBlockOverlappingFlushOnly) {
        TSlotIoCoordinator slots;
        slots.PinDDiskRead({0, 0});
        slots.PinDDiskRead({0, 0});
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 1}, 2);
        slots.Accept({1, 0}, 3);
        UNIT_ASSERT(!slots.CanFlush({0, 0}, 1));
        UNIT_ASSERT(slots.CanFlush({0, 1}, 2));
        UNIT_ASSERT(slots.CanFlush({1, 0}, 3));
        slots.UnpinDDiskRead({0, 0});
        UNIT_ASSERT(!slots.CanFlush({0, 0}, 1));
        slots.UnpinDDiskRead({0, 0});
        UNIT_ASSERT(slots.CanFlush({0, 0}, 1));
    }

    Y_UNIT_TEST(EraseRetirementCannotExposeAnOlderVersion) {
        TSlotIoCoordinator slots;
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 0}, 2);
        slots.MakeVisible({0, 0}, 2);
        slots.MarkFlushed({0, 0}, 1);
        slots.MarkFlushed({0, 0}, 2);
        slots.Retire({0, 0}, 1);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({0, 0}), 2);
        slots.Retire({0, 0}, 2);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({0, 0}), 0);
    }

    Y_UNIT_TEST(IndependentEraseProceedsPastPinnedOlderRange) {
        TSlotIoCoordinator slots;
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 1}, 2);
        slots.MarkFlushed({0, 0}, 1);
        slots.MarkFlushed({0, 1}, 2);
        slots.PinPBRead({0, 0}, 1);
        UNIT_ASSERT(!slots.CanErase({0, 0}, 1));
        UNIT_ASSERT(slots.CanErase({0, 1}, 2));
    }

    Y_UNIT_TEST(WireVChunksAreDisjointAndUse64Bits) {
        for (ui32 dbg = 0; dbg != 3; ++dbg) {
            for (ui32 v = 0; v != 4; ++v) {
                UNIT_ASSERT_VALUES_EQUAL(WireVChunkIndex(dbg, 4, v), ui64(dbg) * 4 + v);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(WireVChunkIndex(2, 0x80000000u, 7), 0x100000007ull);
    }

    Y_UNIT_TEST(FailedVersionLeavesTheUnflushedListWithoutWalkingPredecessors) {
        TSlotIoCoordinator slots;
        for (ui64 lsn = 1; lsn <= 64; ++lsn) {
            slots.Accept({0, 0}, lsn);
        }
        slots.MarkFlushed({0, 0}, 40);
        UNIT_ASSERT(slots.CanFlush({0, 0}, 1));
        UNIT_ASSERT(!slots.CanFlush({0, 0}, 40));
        UNIT_ASSERT(!slots.CanFlush({0, 0}, 41));
        slots.MarkFlushed({0, 0}, 64);
        UNIT_ASSERT(slots.CanFlush({0, 0}, 1));
        slots.MarkFlushed({0, 0}, 1);
        UNIT_ASSERT(slots.CanErase({0, 0}, 1));
        UNIT_ASSERT(slots.CanFlush({0, 0}, 2));
        slots.Retire({0, 0}, 1);
        UNIT_ASSERT(!slots.CanErase({0, 0}, 2));
        UNIT_ASSERT_VALUES_EQUAL(slots.OldestUnflushed({0, 0}), 2);
    }

    Y_UNIT_TEST(ColdReadsReuseSlotTableCapacity) {
        TSlotIoCoordinator slots;
        auto wave = [&]() {
            for (ui32 index = 0; index < 64; ++index) {
                slots.PinDDiskRead({0, index});
            }
            for (ui32 index = 0; index < 64; ++index) {
                UNIT_ASSERT(slots.UnpinDDiskRead({0, index}));
            }
        };
        wave();
        wave();
        const size_t capacity = slots.SlotCapacity();
        UNIT_ASSERT(capacity >= 64);
        wave();
        UNIT_ASSERT_VALUES_EQUAL(slots.SlotCapacity(), capacity);

        slots.PinDDiskRead({1, 0});
        UNIT_ASSERT(slots.UnpinDDiskRead({1, 0}));
        slots.Accept({1, 0}, 7);
        slots.MakeVisible({1, 0}, 7);
        UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({1, 0}), 7);
        UNIT_ASSERT(slots.CanFlush({1, 0}, 7));
    }

    Y_UNIT_TEST(LastReaderWakesOnlyItsSlot) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        for (ui32 index = 0; index < 20; ++index) {
            slots.Accept({0, index}, index + 1);
            slots.PinDDiskRead({0, index});
            slots.PinDDiskRead({0, index});
            scheduler.NoteFlushReady(index + 1, {0, index});
        }
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 1, true), 20);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 0);
        const ui64 examined = scheduler.Stats().FlushSlotsExamined;

        UNIT_ASSERT(!slots.UnpinDDiskRead({0, 3}));
        scheduler.ConsiderFlush(slots, {0, 3});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 0);
        UNIT_ASSERT(slots.UnpinDDiskRead({0, 3}));
        scheduler.ConsiderFlush(slots, {0, 3});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PeekFlush(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.Stats().FlushSlotsExamined, examined + 2);
    }

    Y_UNIT_TEST(CohortsExcludeLaterAndLowerLsnCompletions) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        for (ui64 lsn = 1; lsn <= 6; ++lsn) {
            slots.Accept({0, static_cast<ui32>(lsn)}, lsn);
        }
        for (ui64 lsn : {ui64(4), ui64(5), ui64(6)}) {
            scheduler.NoteFlushReady(lsn, {0, static_cast<ui32>(lsn)});
            UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false) > 0, lsn == 6);
        }
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 3);
        scheduler.NoteFlushReady(1, {0, 1});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false), 0);
        scheduler.NoteFlushReady(2, {0, 2});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 3);
        scheduler.NoteFlushReady(3, {0, 3});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false), 3);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 6);
        UNIT_ASSERT(scheduler.FlushAdmitted(1));
        UNIT_ASSERT(!scheduler.FlushAdmitted(7));
    }

    Y_UNIT_TEST(AdmissionSurvivesPartialPopAndRetryDedup) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        for (ui64 lsn = 1; lsn <= 3; ++lsn) {
            slots.Accept({0, static_cast<ui32>(lsn)}, lsn);
            scheduler.NoteFlushReady(lsn, {0, static_cast<ui32>(lsn)});
        }
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false), 3);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PopFlush(), 1);
        UNIT_ASSERT(scheduler.FlushAdmitted(1));
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 2);
        scheduler.ConsiderFlush(slots, {0, 1});
        scheduler.ConsiderFlush(slots, {0, 1});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 3);
        scheduler.NoteFlushReady(4, {0, 4});
        slots.Accept({0, 4}, 4);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 3, false), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.UnadmittedFlushCount(), 1);
        UNIT_ASSERT(scheduler.FlushAdmitted(1));
    }

    Y_UNIT_TEST(WakingOneSlotDoesNotRevisitTheBlockedPopulation) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        for (ui32 index = 0; index < 30; ++index) {
            slots.Accept({1, index}, index + 1);
            slots.PinDDiskRead({1, index});
            scheduler.NoteFlushReady(index + 1, {1, index});
        }
        scheduler.AdmitFlush(slots, 1, true);
        const ui64 blockedExamined = scheduler.Stats().FlushSlotsExamined;
        UNIT_ASSERT_VALUES_EQUAL(blockedExamined, 30);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 0);

        slots.Accept({2, 0}, 100);
        scheduler.NoteFlushReady(100, {2, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 1, false), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.Stats().FlushSlotsExamined, blockedExamined + 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushQueueSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PeekFlush(), 100);
    }

    Y_UNIT_TEST(EraseCohortWaitsForSyncCompletionAndKeepsBypass) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        slots.Accept({0, 0}, 1);
        slots.Accept({0, 0}, 2);
        scheduler.BypassEraseGate(slots, 2, {0, 0});
        UNIT_ASSERT(scheduler.EraseAdmitted(2));
        UNIT_ASSERT_VALUES_EQUAL(scheduler.EraseQueueSize(), 0);
        slots.MarkFlushed({0, 0}, 2);
        scheduler.ConsiderErase(slots, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.EraseQueueSize(), 0);

        slots.MarkFlushed({0, 0}, 1);
        scheduler.NoteEraseReady(1, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.UnadmittedEraseCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitErase(slots, 2, false), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitErase(slots, 2, true), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PeekErase(), 1);
        scheduler.PopErase();
        slots.Retire({0, 0}, 1);
        scheduler.Forget(1);
        scheduler.ConsiderErase(slots, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PeekErase(), 2);
    }

    Y_UNIT_TEST(SameIndexVChunksMixProbeBitsAndFingerprints) {
        TSlotIoCoordinator slots;
        std::set<size_t> probes;
        std::set<size_t> fingerprints;
        for (ui32 v = 0; v < 1024; ++v) {
            const size_t hash = TSlotIoCoordinator::TSlotHash{}({v, 7});
            fingerprints.insert(hash & 127);
            probes.insert((hash >> 7) & 1023);
            slots.Accept({v, 7}, v + 1);
            slots.MakeVisible({v, 7}, v + 1);
        }
        UNIT_ASSERT(fingerprints.size() > 64);
        UNIT_ASSERT(probes.size() > 256);
        for (ui32 v = 0; v < 1024; ++v) {
            UNIT_ASSERT_VALUES_EQUAL(slots.VisibleLsn({v, 7}), v + 1);
            slots.MarkFlushed({v, 7}, v + 1);
            slots.Retire({v, 7}, v + 1);
        }
    }

    Y_UNIT_TEST(NormalAndSelectiveAdmissionRetainCapacityAndOrder) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        scheduler.Reserve(64);
        const size_t flushCapacity = scheduler.UnadmittedFlushCapacity();
        const size_t eraseCapacity = scheduler.UnadmittedEraseCapacity();
        for (ui64 wave = 0; wave < 3; ++wave) {
            for (ui32 i = 0; i < 12; ++i) {
                const ui64 lsn = wave * 12 + i + 1;
                slots.Accept({i % 2, i}, lsn);
                scheduler.NoteFlushReady(lsn, {i % 2, i});
            }
            const auto idle = [](ui32 v) {
                return v == 0;
            };
            UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitIdleFlush(slots, idle), 6);
            for (ui32 i = 0; i < 12; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(scheduler.FlushAdmitted(wave * 12 + i + 1), i % 2 == 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitFlush(slots, 6, false), 6);
            for (ui32 parity = 0; parity < 2; ++parity) {
                for (ui32 i = parity; i < 12; i += 2) {
                    const ui64 lsn = wave * 12 + i + 1;
                    UNIT_ASSERT_VALUES_EQUAL(scheduler.PopFlush(), lsn);
                    slots.MarkFlushed({i % 2, i}, lsn);
                }
            }
            for (ui32 i = 0; i < 12; ++i) {
                scheduler.NoteEraseReady(wave * 12 + i + 1, {i % 2, i});
            }
            UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitIdleErase(slots, idle), 6);
            for (ui32 i = 0; i < 12; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(scheduler.EraseAdmitted(wave * 12 + i + 1), i % 2 == 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitErase(slots, 6, false), 6);
            for (ui32 parity = 0; parity < 2; ++parity) {
                for (ui32 i = parity; i < 12; i += 2) {
                    const ui64 lsn = wave * 12 + i + 1;
                    UNIT_ASSERT_VALUES_EQUAL(scheduler.PopErase(), lsn);
                    slots.Retire({i % 2, i}, lsn);
                    scheduler.Forget(lsn);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(scheduler.UnadmittedFlushCapacity(), flushCapacity);
            UNIT_ASSERT_VALUES_EQUAL(scheduler.UnadmittedEraseCapacity(), eraseCapacity);
        }
    }

    Y_UNIT_TEST(SelectiveAdmissionDoesNotRescanPinnedMembers) {
        TSlotIoCoordinator slots;
        TMaintenanceScheduler scheduler;
        slots.Accept({0, 0}, 1);
        slots.PinDDiskRead({0, 0});
        slots.PinDDiskRead({0, 0});
        scheduler.NoteFlushReady(1, {0, 0});
        const auto idle = [](ui32) {
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitIdleFlush(slots, idle), 1);
        const auto examined = scheduler.Stats().FlushSlotsExamined;
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitIdleFlush(slots, idle), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.Stats().FlushSlotsExamined, examined);
        UNIT_ASSERT(!slots.UnpinDDiskRead({0, 0}));
        UNIT_ASSERT(!scheduler.HasFlushWork());
        UNIT_ASSERT(slots.UnpinDDiskRead({0, 0}));
        scheduler.ConsiderFlush(slots, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PopFlush(), 1);
        slots.MarkFlushed({0, 0}, 1);
        slots.PinPBRead({0, 0}, 1);
        scheduler.NoteEraseReady(1, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.AdmitIdleErase(slots, idle), 1);
        UNIT_ASSERT(!scheduler.HasEraseWork());
        UNIT_ASSERT(slots.UnpinPBRead({0, 0}, 1));
        scheduler.ConsiderErase(slots, {0, 0});
        UNIT_ASSERT_VALUES_EQUAL(scheduler.PopErase(), 1);
    }
}
