#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/deleted_ddisk.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/model/touched_vchunks.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/map.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDatabase: public NKikimr::NIceDb::TNiceDb
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;
    using TAddHostInProgress =
        ::NYdb::NBS::PartitionDirect::NProto::TAddHostInProgress;
    using TRemoveHostInProgress =
        ::NYdb::NBS::PartitionDirect::NProto::TRemoveHostInProgress;

public:
    enum class EBlobIndexScanProgress
    {
        NotReady,
        Completed,
        Partial
    };

public:
    explicit TPartitionDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    bool ReadVolumeConfig(
        TMaybe<NKikimrBlockStore::TVolumeConfig>& volumeConfig);
    void StoreVolumeConfig(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

    bool ReadDirectBlockGroupsConnections(
        TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections);
    void StoreDirectBlockGroupsConnections(
        const TDirectBlockGroupsConnections& directBlockGroupsConnections);

    bool ReadAllVChunkConfigs(TVChunkConfigs& out);
    void StoreVChunkConfig(const TVChunkConfig& cfg);

    bool ReadAllDirtyMapStates(TDirtyMapStateProtos& out);
    void StoreDirtyMapState(ui32 vChunkIndex, const TDirtyMapStateProto& state);

    bool ReadAllTouchedVChunks(TTouchedVChunks& out);
    void StoreTouchedVChunkMask(const TTouchedVChunks::TChunk& chunk);

    bool ReadAllDeletedDDisks(TVector<TDeletedDDiskRecordProto>& out);
    // Appends a deleted-DDisk record using its RecordId as the table key.
    void AddDeletedDDisk(const TDeletedDDiskRecordProto& record);
    // Replaces a deleted-DDisk record using its RecordId as the table key.
    void UpdateDeletedDDisk(const TDeletedDDiskRecordProto& record);
    // Deletes deleted-DDisk records by their table keys.
    void DeleteDeletedDDisk(const TVector<ui64>& recordIds);

    bool ReadAddHostInProgress(TMaybe<TAddHostInProgress>& addHostInProgress);
    void StoreAddHostInProgress(const TAddHostInProgress& addHostInProgress);
    void ClearAddHostInProgress();

    bool ReadRemoveHostInProgress(
        TMaybe<TRemoveHostInProgress>& removeHostInProgress);
    void StoreRemoveHostInProgress(
        const TRemoveHostInProgress& removeHostInProgress);
    void ClearRemoveHostInProgress();

private:
    void WriteDeletedDDisk(const TDeletedDDiskRecordProto& record);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
