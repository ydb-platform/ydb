#include "dbs_controller_database.h"

#include "dbs_controller_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerDatabase::InitSchema()
{
    Materialize<TDbsControllerSchema>();

    TSchemaInitializer<TDbsControllerSchema::TTables>::InitStorage(
        Database.Alter());
}

bool TDbsControllerDatabase::ClearTabletRecords(const ui64 tabletId)
{
    using TTable = TDbsControllerSchema::DDiskMap;

    auto it = Table<TTable>()
                  .Prefix(tabletId)
                  .Select<
                      TTable::TabletId,
                      TTable::DirectBlockGroupId,
                      TTable::NodeId,
                      TTable::PDiskId,
                      TTable::DDiskSlotId>();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        RemoveEntry(
            it.GetValue<TTable::TabletId>(),
            it.GetValue<TTable::DirectBlockGroupId>(),
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::PDiskId>(),
            it.GetValue<TTable::DDiskSlotId>());
        Table<TTable>().Key(it.GetKey()).Delete();
        it.Next();
    }

    return true;
}

void TDbsControllerDatabase::FillTabletRecords(
    const ui64 tabletId,
    const NProto::TPartitionDDisks& partitionDDisks)
{
    for (size_t dbgId = 0;
         dbgId < partitionDDisks.DirectBlockGroupsDDisksSize();
         dbgId++)
    {
        for (const auto& dDiskIds:
             partitionDDisks.GetDirectBlockGroupsDDisks(dbgId).GetDDiskIds())
        {
            AddEntry(tabletId, dbgId, dDiskIds.GetDDisk(), false);
            AddEntry(tabletId, dbgId, dDiskIds.GetPersistentBuffer(), true);
        }
    }
}

bool TDbsControllerDatabase::GetTabletsForNode(
    const ui32 nodeId,
    TVector<ui64>& outTablets)
{
    using TTable = TDbsControllerSchema::InverseDDiskMap;

    THashSet<ui64> tablets;

    auto it = Table<TTable>().Prefix(nodeId).Select<TTable::TabletId>();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        tablets.insert(it.GetValue<TTable::TabletId>());
        it.Next();
    }

    outTablets.resize(tablets.size());
    outTablets.assign(tablets.begin(), tablets.end());

    return true;
}

bool TDbsControllerDatabase::GetNodesForTablet(
    const ui64 tabletId,
    TVector<ui32>& outNodes)
{
    using TTable = TDbsControllerSchema::DDiskMap;

    THashSet<ui32> nodes;

    auto it = Table<TTable>().Prefix(tabletId).Select<TTable::NodeId>();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        nodes.insert(it.GetValue<TTable::NodeId>());
        it.Next();
    }

    outNodes.resize(nodes.size());
    outNodes.assign(nodes.begin(), nodes.end());

    return true;
}

void TDbsControllerDatabase::AddEntry(
    const ui64 tabletId,
    const ui64 directBlockGroupId,
    const NKikimrBlobStorage::NDDisk::TDDiskId& dDiskId,
    const bool isPbuffer)
{
    Table<TDbsControllerSchema::DDiskMap>()
        .Key(
            tabletId,
            directBlockGroupId,
            dDiskId.GetNodeId(),
            dDiskId.GetPDiskId(),
            dDiskId.GetDDiskSlotId())
        .Update<TDbsControllerSchema::DDiskMap::IsPBuffer>(isPbuffer);

    Table<TDbsControllerSchema::InverseDDiskMap>()
        .Key(
            dDiskId.GetNodeId(),
            dDiskId.GetPDiskId(),
            dDiskId.GetDDiskSlotId(),
            tabletId)
        .Update();
}

void TDbsControllerDatabase::RemoveEntry(
    ui64 tabletId,
    const ui64 directBlockGroupId,
    ui32 nodeId,
    ui32 pDiskId,
    ui32 slotId)
{
    Table<TDbsControllerSchema::DDiskMap>()
        .Key(tabletId, directBlockGroupId, nodeId, pDiskId, slotId)
        .Delete();

    Table<TDbsControllerSchema::InverseDDiskMap>()
        .Key(nodeId, pDiskId, slotId, tabletId)
        .Delete();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
