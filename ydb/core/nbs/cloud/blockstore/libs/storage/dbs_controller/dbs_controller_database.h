#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

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

    struct TRecordKey
    {
        ui64 TabletId;
        ui64 DirectBlockGroupId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 DDiskSlotId;
    };

    void InitSchema();

    bool GetRecordKeysForTablet(
        ui64 tabletId,
        TVector<TRecordKey>& outRecordKeys);

    void ClearRecords(const TVector<TRecordKey>& recordKeys);

    void FillTabletRecords(
        ui64 tabletId,
        const NProto::TPartitionDDisks& partitionDDisks);

    bool GetTabletsForNode(ui32 nodeId, TVector<ui64>& outTablets);

    bool GetNodesForTablet(ui64 tabletId, TVector<ui32>& outNodes);

private:
    void AddEntry(
        ui64 tabletId,
        ui64 directBlockGroupId,
        const NKikimrBlobStorage::NDDisk::TDDiskId& ddiskId,
        bool isPbuffer);
    void RemoveEntry(const TRecordKey& key);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
