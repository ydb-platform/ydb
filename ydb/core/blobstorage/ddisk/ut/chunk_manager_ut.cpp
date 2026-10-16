#include <ydb/core/blobstorage/ddisk/chunk_manager.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk {

Y_UNIT_TEST_SUITE(TChunkManagerTest) {
    Y_UNIT_TEST(AllocationsWaitForChunksAndKeepRequestOrder) {
        TChunkManager manager;
        manager.Enqueue(TChunkManager::TChunkForData{11, 21});
        manager.Enqueue(TChunkManager::TChunkForIntegrity{});
        manager.Enqueue(TChunkManager::TChunkForPersistentBuffer{});

        UNIT_ASSERT(!manager.TakeAllocation());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetPendingAllocationCount(), 3);

        manager.ReturnChunk(101);
        auto allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        const auto& data = std::get<TChunkManager::TChunkForData>(allocation->first);
        UNIT_ASSERT_VALUES_EQUAL(data.TabletId, 11);
        UNIT_ASSERT_VALUES_EQUAL(data.VChunkIndex, 21);
        UNIT_ASSERT_VALUES_EQUAL(allocation->second, 101);
        UNIT_ASSERT(!manager.TakeAllocation());

        manager.ReturnChunk(102);
        manager.ReturnChunk(103);
        allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        UNIT_ASSERT(std::holds_alternative<TChunkManager::TChunkForIntegrity>(allocation->first));
        UNIT_ASSERT_VALUES_EQUAL(allocation->second, 102);
        allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        UNIT_ASSERT(std::holds_alternative<TChunkManager::TChunkForPersistentBuffer>(allocation->first));
        UNIT_ASSERT_VALUES_EQUAL(allocation->second, 103);
        UNIT_ASSERT(!manager.HasPendingAllocations());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetReservedChunkCount(), 0);

        manager.ReturnChunk(104);
        UNIT_ASSERT(!manager.TakeAllocation());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetReservedChunkCount(), 1);
        manager.Enqueue(TChunkManager::TChunkForData{12, 22});
        allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        UNIT_ASSERT_VALUES_EQUAL(allocation->second, 104);
    }

    Y_UNIT_TEST(RefillCountsFormattingAndAvoidsConcurrentReservations) {
        TChunkManager manager;
        manager.ReturnChunk(101);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4, 3), 0);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4, 4), 0);

        manager.BeginReservation();
        UNIT_ASSERT(manager.IsReservationInFlight());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4), 0);

        manager.Enqueue(TChunkManager::TChunkForData{11, 21});
        UNIT_ASSERT(manager.TakeAllocation());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4), 0);

        manager.FinishReservation();
        UNIT_ASSERT(!manager.IsReservationInFlight());
        manager.ReturnChunk(102);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetRefillCount(4, 2), 1);
    }

    Y_UNIT_TEST(BrokenDataServiceRetainsPersistentBufferDemandAndReserve) {
        TChunkManager manager;
        manager.Enqueue(TChunkManager::TChunkForData{11, 21});
        manager.Enqueue(TChunkManager::TChunkForPersistentBuffer{});
        manager.Enqueue(TChunkManager::TChunkForIntegrity{});
        manager.Enqueue(TChunkManager::TChunkForPersistentBuffer{});
        manager.Enqueue(TChunkManager::TChunkForData{12, 22});
        manager.ReturnChunk(101);
        manager.ReturnChunk(102);
        manager.BeginReservation();

        UNIT_ASSERT_VALUES_EQUAL(manager.CountPendingPersistentBufferAllocations(), 2);
        manager.RetainPersistentBufferAllocations();
        UNIT_ASSERT_VALUES_EQUAL(manager.GetPendingAllocationCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manager.CountPendingPersistentBufferAllocations(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetReservedChunkCount(), 2);
        UNIT_ASSERT(manager.IsReservationInFlight());

        for (ui32 chunkIdx : {101, 102}) {
            const auto allocation = manager.TakeAllocation();
            UNIT_ASSERT(allocation);
            UNIT_ASSERT(std::holds_alternative<TChunkManager::TChunkForPersistentBuffer>(allocation->first));
            UNIT_ASSERT_VALUES_EQUAL(allocation->second, chunkIdx);
        }
        UNIT_ASSERT(!manager.HasPendingAllocations());
        UNIT_ASSERT_VALUES_EQUAL(manager.CountPendingPersistentBufferAllocations(), 0);
    }

    Y_UNIT_TEST(ShutdownExtractionPreservesOutstandingReservation) {
        TChunkManager manager;
        manager.ReturnChunk(101);
        manager.ReturnChunk(102);
        manager.BeginReservation();
        UNIT_ASSERT(manager.ExtractReservations() == TVector<ui32>({101, 102}));
        UNIT_ASSERT(manager.ExtractReservations().empty());
        UNIT_ASSERT(manager.IsReservationInFlight());
        manager.FinishReservation();
        manager.ReturnChunk(103);
        UNIT_ASSERT(manager.ExtractReservations() == TVector<ui32>({103}));
    }

    Y_UNIT_TEST(ExcessIntegrityAllocationReturnsChunkForNextRequest) {
        TChunkManager manager;
        manager.Enqueue(TChunkManager::TChunkForIntegrity{});
        manager.Enqueue(TChunkManager::TChunkForData{11, 21});
        manager.ReturnChunk(101);

        auto allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        UNIT_ASSERT(std::holds_alternative<TChunkManager::TChunkForIntegrity>(allocation->first));
        manager.ReturnChunk(allocation->second);

        allocation = manager.TakeAllocation();
        UNIT_ASSERT(allocation);
        UNIT_ASSERT(std::holds_alternative<TChunkManager::TChunkForData>(allocation->first));
        UNIT_ASSERT_VALUES_EQUAL(allocation->second, 101);
        UNIT_ASSERT(!manager.HasPendingAllocations());
        UNIT_ASSERT_VALUES_EQUAL(manager.GetReservedChunkCount(), 0);
    }
}

} // namespace NKikimr::NDDisk
