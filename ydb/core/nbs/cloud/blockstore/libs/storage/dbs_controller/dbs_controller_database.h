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

    void InitSchema();

    bool ClearTabletRecords(ui64 tabletId);

    void FillTabletRecords(
        ui64 tabletId,
        const NProto::TPartitionDDisks& partitionDDisks);

    bool GetTabletsForNode(ui32 nodeId, TVector<ui64>& outTablets);

    bool GetNodesForTablet(ui64 tabletId, TVector<ui32>& outNodes);

private:
    void AddEntry(
        ui64 tabletId,
        ui64 directBlockGroupId,
        const NKikimrBlobStorage::NDDisk::TDDiskId& dDiskId,
        bool isPbuffer);
    void RemoveEntry(
        ui64 tabletId,
        ui64 directBlockGroupId,
        ui32 nodeId,
        ui32 pDiskId,
        ui32 slotId);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
