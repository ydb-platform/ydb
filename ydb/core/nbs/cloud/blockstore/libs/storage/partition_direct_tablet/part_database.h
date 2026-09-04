#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

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

    bool ReadAddHostInProgress(TMaybe<TAddHostInProgress>& addHostInProgress);
    void StoreAddHostInProgress(const TAddHostInProgress& addHostInProgress);
    void ClearAddHostInProgress();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
