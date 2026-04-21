#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

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

    void StoreVolumeConfig(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

    void StoreDirectBlockGroupsConnections(
        const TDirectBlockGroupsConnections& directBlockGroupsConnections);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
