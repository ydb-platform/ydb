#include "deleted_ddisk_storage.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeletedDDiskStorageTest)
{
    Y_UNIT_TEST(ShouldAllocateUniqueIdsAcrossBatchRequests)
    {
        TDeletedDDiskStorage storage;
        ui64 nextRecordId = storage.GetNextRecordId();
        const TVector<NKikimr::NBsController::TDDiskId> firstDDiskIds = {
            {101, 2, 3},
            {102, 4, 5},
        };
        const TVector<NKikimr::NBsController::TDDiskId> secondDDiskIds = {
            {103, 6, 7},
        };

        const auto firstRecords = storage.MakeRecords(
            nextRecordId,
            10,
            17,
            123456,
            firstDDiskIds);
        const auto secondRecords = storage.MakeRecords(
            nextRecordId,
            11,
            17,
            123789,
            secondDDiskIds);

        UNIT_ASSERT_VALUES_EQUAL(2u, firstRecords.size());
        UNIT_ASSERT_VALUES_EQUAL(1u, secondRecords.size());
        UNIT_ASSERT_VALUES_EQUAL(1u, firstRecords[0].GetRecordId());
        UNIT_ASSERT_VALUES_EQUAL(2u, firstRecords[1].GetRecordId());
        UNIT_ASSERT_VALUES_EQUAL(3u, secondRecords[0].GetRecordId());
        UNIT_ASSERT_VALUES_EQUAL(4u, nextRecordId);
        UNIT_ASSERT_VALUES_EQUAL(1u, storage.GetNextRecordId());
        UNIT_ASSERT(storage.GetRecords().empty());

        UNIT_ASSERT_VALUES_EQUAL(10u, firstRecords[0].GetVChunkIndex());
        UNIT_ASSERT_VALUES_EQUAL(17u, firstRecords[0].GetTabletGeneration());
        UNIT_ASSERT_VALUES_EQUAL(123456u, firstRecords[0].GetTimestampUs());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            firstRecords[0].GetStatus() ==
                NYdb::NBS::PartitionDirect::NProto::
                    DELETED_DDISK_STATUS_REGISTERED);
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            firstRecords[0].GetProcessingTabletGeneration());
        UNIT_ASSERT_VALUES_EQUAL(
            101u,
            firstRecords[0].GetDDiskId().GetNodeId());

        storage.AddPersisted(firstRecords);
        storage.AddPersisted(secondRecords);
        UNIT_ASSERT_VALUES_EQUAL(3u, storage.GetRecords().size());
        UNIT_ASSERT_VALUES_EQUAL(4u, storage.GetNextRecordId());
    }

    Y_UNIT_TEST(ShouldRestoreNextIdFromLoadedRecords)
    {
        TDeletedDDiskStorage storage;
        TVector<TDeletedDDiskRecordProto> records(2);
        records[0].SetRecordId(3);
        records[1].SetRecordId(11);

        storage.Load(std::move(records));

        UNIT_ASSERT_VALUES_EQUAL(2u, storage.GetRecords().size());
        UNIT_ASSERT_VALUES_EQUAL(12u, storage.GetNextRecordId());
    }

    Y_UNIT_TEST(ShouldRemovePersistedRecordsWithoutReusingIds)
    {
        TDeletedDDiskStorage storage;
        TVector<TDeletedDDiskRecordProto> records(3);
        records[0].SetRecordId(1);
        records[1].SetRecordId(2);
        records[2].SetRecordId(3);
        storage.Load(std::move(records));

        storage.RemovePersisted({1, 3});

        UNIT_ASSERT_VALUES_EQUAL(1u, storage.GetRecords().size());
        UNIT_ASSERT_VALUES_EQUAL(2u, storage.GetRecords()[0].GetRecordId());
        UNIT_ASSERT_VALUES_EQUAL(4u, storage.GetNextRecordId());
    }

    Y_UNIT_TEST(ShouldBuildCleanupBatchesFromOldExecutedRecords)
    {
        TDeletedDDiskStorage storage;
        TVector<TDeletedDDiskRecordProto> records(1003);
        for (size_t i = 0; i < 1001; ++i) {
            auto& record = records[i];
            record.SetRecordId(i + 1);
            record.SetTimestampUs(10);
            record.SetStatus(
                NYdb::NBS::PartitionDirect::NProto::
                    DELETED_DDISK_STATUS_EXECUTED);
        }
        records[1001].SetRecordId(1002);
        records[1001].SetTimestampUs(10);
        records[1001].SetStatus(
            NYdb::NBS::PartitionDirect::NProto::
                DELETED_DDISK_STATUS_REGISTERED);
        records[1002].SetRecordId(1003);
        records[1002].SetTimestampUs(100);
        records[1002].SetStatus(
            NYdb::NBS::PartitionDirect::NProto::
                DELETED_DDISK_STATUS_EXECUTED);
        storage.Load(std::move(records));

        const auto now =
            TInstant::MicroSeconds(30 * 86400ULL * 1000000ULL + 50);
        auto firstBatch = storage.MakeCleanupBatch(now);
        UNIT_ASSERT_VALUES_EQUAL(1000u, firstBatch.size());
        UNIT_ASSERT_VALUES_EQUAL(1u, firstBatch.front());
        UNIT_ASSERT_VALUES_EQUAL(1000u, firstBatch.back());

        storage.RemovePersisted(firstBatch);
        const auto secondBatch = storage.MakeCleanupBatch(now);
        UNIT_ASSERT_VALUES_EQUAL(1u, secondBatch.size());
        UNIT_ASSERT_VALUES_EQUAL(1001u, secondBatch[0]);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
