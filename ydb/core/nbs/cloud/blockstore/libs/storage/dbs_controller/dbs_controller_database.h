#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller_db.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

class TDbsControllerDatabase: public NKikimr::NIceDb::TNiceDb
{
public:
    TDbsControllerDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    using TDirectKey = std::tuple<ui64, ui64>;
    using TInverseKey = std::tuple<ui32, ui32, ui32>;

    void InitSchema();

    bool GetRecordKeysForTablet(
        ui64 tabletId,
        TVector<TDirectKey>& outDirectKeys,
        TVector<TInverseKey>& outInverseKeys);

    void RemoveRecord(TDirectKey key);

    void RemoveRecord(TInverseKey key);

    bool LoadDirectRecord(
        const TDirectKey& key,
        NProto::TDirectBlockGroupDDisks& outDirectBlockGroupDDisks);

    void StoreDirectRecord(
        const TDirectKey& key,
        const NProto::TDirectBlockGroupDDisks& directBlockGroupDDisks);

    bool LoadInverseRecord(
        const TInverseKey& key,
        NProto::TDDiskDirectBlockGroups& outDDiskDirectBlockGroups);

    void StoreInverseRecord(
        const TInverseKey& key,
        const NProto::TDDiskDirectBlockGroups& ddiskDirectBlockGroups);

    bool GetLogicalNodesCount(
        const TDirectKey& key,
        std::optional<ui64>& outLogicalNodesCount);

    bool GetPartitionsForNode(ui32 nodeId, TVector<ui64>& outPartitions);

    bool GetPartitionsForPDisk(
        ui32 nodeId,
        ui32 pdiskId,
        TVector<ui64>& outPartitions);

    bool GetPartitionsForDDisk(
        ui32 nodeId,
        ui32 pdiskId,
        ui32 slotId,
        TVector<ui64>& outPartitions);

    bool GetAffectedDBGsWithNodeCounts(
        const TVector<ui32>& nodeIds,
        THashMap<TDirectKey, ui64>& outDbgs);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
