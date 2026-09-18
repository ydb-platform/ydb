#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h
=======
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/model/touched_vchunks.h>
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_database.h

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

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
    TPartitionDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    bool ReadVolumeConfig(
        TMaybe<NKikimrBlockStore::TVolumeConfig>& volumeConfig);

    bool ReadDirectBlockGroupsConnections(
        TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections);

    bool ReadAllVChunkConfigs(TVector<TVChunkConfig>& out);

    void StoreVolumeConfig(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

    void StoreDirectBlockGroupsConnections(
        const TDirectBlockGroupsConnections& directBlockGroupsConnections);

    void StoreVChunkConfig(const TVChunkConfig& cfg);

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h
=======
    bool ReadAllDirtyMapStates(TDirtyMapStateProtos& out);
    void StoreDirtyMapState(ui32 vChunkIndex, const TDirtyMapStateProto& state);

    bool ReadAllTouchedVChunks(TTouchedVChunks& out);
    void StoreTouchedVChunkMask(const TTouchedVChunks::TChunk& chunk);

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_database.h
    bool ReadAddHostInProgress(TMaybe<TAddHostInProgress>& addHostInProgress);
    void StoreAddHostInProgress(const TAddHostInProgress& addHostInProgress);
    void ClearAddHostInProgress();

    bool ReadRemoveHostInProgress(
        TMaybe<TRemoveHostInProgress>& removeHostInProgress);
    void StoreRemoveHostInProgress(
        const TRemoveHostInProgress& removeHostInProgress);
    void ClearRemoveHostInProgress();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
