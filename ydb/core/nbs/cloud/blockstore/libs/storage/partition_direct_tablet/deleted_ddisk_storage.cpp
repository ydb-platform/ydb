#include "deleted_ddisk_storage.h"

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/system/datetime.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr size_t DeletedDDiskHistoryDays = 30;
constexpr size_t DeletedDDiskCleanupBatchSize = 1000;

}   // namespace

void TDeletedDDiskStorage::Load(
    TVector<TDeletedDDiskRecordProto> records)
{
    Records = std::move(records);
    NextRecordId = 1;
    for (const auto& record: Records) {
        NextRecordId = Max(NextRecordId, record.GetRecordId() + 1);
    }
}

TVector<TDeletedDDiskRecordProto> TDeletedDDiskStorage::MakeRecords(
    ui64& nextRecordId,
    ui32 vChunkIndex,
    ui32 tabletGeneration,
    ui64 timestampUs,
    const TVector<NKikimr::NBsController::TDDiskId>& ddiskIds) const
{
    TVector<TDeletedDDiskRecordProto> records;
    records.reserve(ddiskIds.size());
    for (const auto& ddiskId: ddiskIds) {
        auto& record = records.emplace_back();
        record.SetRecordId(nextRecordId++);
        record.SetVChunkIndex(vChunkIndex);
        record.SetTabletGeneration(tabletGeneration);
        record.SetTimestampUs(timestampUs);
        ddiskId.Serialize(record.MutableDDiskId());
        record.SetStatus(
            NYdb::NBS::PartitionDirect::NProto::
                DELETED_DDISK_STATUS_REGISTERED);
    }
    return records;
}

void TDeletedDDiskStorage::AddPersisted(
    const TVector<TDeletedDDiskRecordProto>& records)
{
    Records.insert(Records.end(), records.begin(), records.end());
    for (const auto& record: records) {
        NextRecordId = Max(NextRecordId, record.GetRecordId() + 1);
    }
}

void TDeletedDDiskStorage::RemovePersisted(const TVector<ui64>& recordIds)
{
    THashSet<ui64> ids;
    ids.insert(recordIds.begin(), recordIds.end());
    EraseIf(Records, [&ids](const auto& record) {
        return ids.contains(record.GetRecordId());
    });
}

TVector<ui64> TDeletedDDiskStorage::MakeCleanupBatch(TInstant now) const
{
    const ui64 cutoffTimestampUs =
        (now - TDuration::Days(DeletedDDiskHistoryDays)).MicroSeconds();

    TVector<ui64> recordIds;
    recordIds.reserve(DeletedDDiskCleanupBatchSize);
    for (const auto& record: Records) {
        if (record.GetStatus() !=
                NYdb::NBS::PartitionDirect::NProto::
                    DELETED_DDISK_STATUS_EXECUTED ||
            record.GetTimestampUs() >= cutoffTimestampUs)
        {
            continue;
        }

        if (recordIds.size() == DeletedDDiskCleanupBatchSize) {
            break;
        }
        recordIds.push_back(record.GetRecordId());
    }
    return recordIds;
}

const TVector<TDeletedDDiskRecordProto>& TDeletedDDiskStorage::GetRecords() const
{
    return Records;
}

ui64 TDeletedDDiskStorage::GetNextRecordId() const
{
    return NextRecordId;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
