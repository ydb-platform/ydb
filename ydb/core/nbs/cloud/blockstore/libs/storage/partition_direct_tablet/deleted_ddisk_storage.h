#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/deleted_ddisk.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Owns the persisted deleted-DDisk history and tracks the next record ID.
class TDeletedDDiskStorage
{
public:
    // Replaces the in-memory history with rows loaded from Local DB.
    void Load(TVector<TDeletedDDiskRecordProto> records);

    // Builds records and increments nextRecordId for each generated record.
    [[nodiscard]] TVector<TDeletedDDiskRecordProto> MakeRecords(
        ui64& nextRecordId,
        ui32 vChunkIndex,
        ui32 tabletGeneration,
        ui64 timestampUs,
        const TVector<NKikimr::NBsController::TDDiskId>& ddiskIds) const;

    // Adds records to the in-memory history after their transaction commits.
    void AddPersisted(const TVector<TDeletedDDiskRecordProto>& records);

    // Removes rows from the in-memory history after their deletion commits.
    void RemovePersisted(const TVector<ui64>& recordIds);

    // Uses the configured retention period and batch size to select old,
    // completed records for deletion.
    [[nodiscard]] TVector<ui64> MakeCleanupBatch(TInstant now) const;

    // Returns all rows for the tablet mon page.
    [[nodiscard]] const TVector<TDeletedDDiskRecordProto>& GetRecords() const;

    // Returns the ID following the last successfully persisted record.
    [[nodiscard]] ui64 GetNextRecordId() const;

private:
    TVector<TDeletedDDiskRecordProto> Records;
    ui64 NextRecordId = 1;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
