#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDatabase: public NKikimr::NIceDb::TNiceDb
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

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

    bool ReadAllBarrierLsns(THashMap<ui32, ui64>& out);

    void StoreVolumeConfig(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

    void StoreDirectBlockGroupsConnections(
        const TDirectBlockGroupsConnections& directBlockGroupsConnections);

    void StoreVChunkConfig(const TVChunkConfig& cfg);

    void StoreBarrierLsn(ui32 dbgIndex, ui64 lsn);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
