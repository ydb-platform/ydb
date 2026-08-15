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

bool TDbsControllerDatabase::GetRecordKeysForTablet(
    const ui64 tabletId,
    TVector<TRecordKey>& outRecordKeys)
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

    outRecordKeys.clear();

    while (it.IsValid()) {
        outRecordKeys.emplace_back(
            it.GetValue<TTable::TabletId>(),
            it.GetValue<TTable::DirectBlockGroupId>(),
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::PDiskId>(),
            it.GetValue<TTable::DDiskSlotId>());
        it.Next();
    }

    return true;
}

void TDbsControllerDatabase::ClearRecords(const TVector<TRecordKey>& recordKeys)
{
    for (const auto& recordKey: recordKeys) {
        RemoveEntry(recordKey);
    }
}

void TDbsControllerDatabase::FillTabletRecords(
    const ui64 tabletId,
    const NProto::TPartitionDDisks& partitionDDisks)
{
    for (size_t dbgId = 0;
         dbgId < partitionDDisks.DirectBlockGroupsDDisksSize();
         dbgId++)
    {
        for (const auto& ddiskIds:
             partitionDDisks.GetDirectBlockGroupsDDisks(dbgId).GetDDiskIds())
        {
            AddEntry(tabletId, dbgId, ddiskIds.GetDDisk(), false);
            AddEntry(tabletId, dbgId, ddiskIds.GetPersistentBuffer(), true);
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

    outNodes.assign(nodes.begin(), nodes.end());

    return true;
}

void TDbsControllerDatabase::AddEntry(
    const ui64 tabletId,
    const ui64 directBlockGroupId,
    const NKikimrBlobStorage::NDDisk::TDDiskId& ddiskId,
    const bool isPbuffer)
{
    Table<TDbsControllerSchema::DDiskMap>()
        .Key(
            tabletId,
            directBlockGroupId,
            ddiskId.GetNodeId(),
            ddiskId.GetPDiskId(),
            ddiskId.GetDDiskSlotId())
        .Update<TDbsControllerSchema::DDiskMap::IsPBuffer>(isPbuffer);

    Table<TDbsControllerSchema::InverseDDiskMap>()
        .Key(
            ddiskId.GetNodeId(),
            ddiskId.GetPDiskId(),
            ddiskId.GetDDiskSlotId(),
            tabletId)
        .Update();
}

void TDbsControllerDatabase::RemoveEntry(const TRecordKey& key)
{
    Table<TDbsControllerSchema::DDiskMap>()
        .Key(
            key.TabletId,
            key.DirectBlockGroupId,
            key.NodeId,
            key.PDiskId,
            key.DDiskSlotId)
        .Delete();

    Table<TDbsControllerSchema::InverseDDiskMap>()
        .Key(key.NodeId, key.PDiskId, key.DDiskSlotId, key.TabletId)
        .Delete();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
