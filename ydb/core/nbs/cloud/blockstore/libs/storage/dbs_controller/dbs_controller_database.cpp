#include "dbs_controller_database.h"

#include "dbs_controller_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

namespace {

TVector<ui64> ExtractPartitions(auto& dbIterator)
{
    TSet<ui64> tabletIds;
    NProto::TDDiskDirectBlockGroups record;

    while (dbIterator.IsValid()) {
        const bool success = record.ParseFromString(
            dbIterator.template GetValue<
                TDbsControllerSchema::InverseMap::DirectBlockGroups>());
        Y_ABORT_UNLESS(success);

        for (const auto& entry: record.GetPartitionDirectBlockGroups()) {
            tabletIds.insert(entry.GetPartitionTabletId());
        }

        dbIterator.Next();
    }

    return {tabletIds.begin(), tabletIds.end()};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerDatabase::InitSchema()
{
    Materialize<TDbsControllerSchema>();

    TSchemaInitializer<TDbsControllerSchema::TTables>::InitStorage(
        Database.Alter());
}

bool TDbsControllerDatabase::GetRecordKeysForTablet(
    ui64 tabletId,
    TVector<TDirectKey>& outDirectKeys,
    TVector<TInverseKey>& outInverseKeys)
{
    using TTable = TDbsControllerSchema::DirectMap;

    auto it = Table<TTable>()
                  .Prefix(tabletId)
                  .Select<
                      TTable::TabletId,
                      TTable::DirectBlockGroupIndex,
                      TTable::DDisks>();

    if (!it.IsReady()) {
        return false;
    }

    outDirectKeys.clear();
    outInverseKeys.clear();

    while (it.IsValid()) {
        outDirectKeys.emplace_back(
            tabletId,
            it.GetValue<TTable::DirectBlockGroupIndex>());
        NProto::TDirectBlockGroupDDisks ddisks;
        const bool success =
            ddisks.ParseFromString(it.GetValue<TTable::DDisks>());
        Y_ABORT_UNLESS(success);
        for (const auto& ddiskIds: ddisks.GetDDiskIds()) {
            outInverseKeys.emplace_back(
                ddiskIds.GetDDisk().GetNodeId(),
                ddiskIds.GetDDisk().GetPDiskId(),
                ddiskIds.GetDDisk().GetDDiskSlotId());
            outInverseKeys.emplace_back(
                ddiskIds.GetPersistentBuffer().GetNodeId(),
                ddiskIds.GetPersistentBuffer().GetPDiskId(),
                ddiskIds.GetPersistentBuffer().GetDDiskSlotId());
        }
        it.Next();
    }

    return true;
}

void TDbsControllerDatabase::RemoveRecord(TDirectKey key)
{
    Table<TDbsControllerSchema::DirectMap>().Key(key).Delete();
}

void TDbsControllerDatabase::RemoveRecord(TInverseKey key)
{
    Table<TDbsControllerSchema::InverseMap>().Key(key).Delete();
}

bool TDbsControllerDatabase::LoadDirectRecord(
    const TDirectKey& key,
    NProto::TDirectBlockGroupDDisks& outDirectBlockGroupDDisks)
{
    using TTable = TDbsControllerSchema::DirectMap;

    const auto it = Table<TTable>().Key(key).Select<TTable::DDisks>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        const bool success = outDirectBlockGroupDDisks.ParseFromString(
            it.GetValue<TTable::DDisks>());
        Y_ABORT_UNLESS(success);
    }

    return true;
}

void TDbsControllerDatabase::StoreDirectRecord(
    const TDirectKey& key,
    const NProto::TDirectBlockGroupDDisks& directBlockGroupDDisks)
{
    using TTable = TDbsControllerSchema::DirectMap;

    TString s;
    const bool success = directBlockGroupDDisks.SerializeToString(&s);
    Y_ABORT_UNLESS(success);

    Table<TTable>().Key(key).Update<TTable::DDisks, TTable::LogicalNodesCount>(
        s,
        directBlockGroupDDisks.DDiskIdsSize());
}

bool TDbsControllerDatabase::LoadInverseRecord(
    const TInverseKey& key,
    NProto::TDDiskDirectBlockGroups& outDDiskDirectBlockGroups)
{
    using TTable = TDbsControllerSchema::InverseMap;

    const auto it =
        Table<TTable>().Key(key).Select<TTable::DirectBlockGroups>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        const bool success = outDDiskDirectBlockGroups.ParseFromString(
            it.GetValue<TTable::DirectBlockGroups>());
        Y_ABORT_UNLESS(success);
    }

    return true;
}

void TDbsControllerDatabase::StoreInverseRecord(
    const TInverseKey& key,
    const NProto::TDDiskDirectBlockGroups& ddiskDirectBlockGroups)
{
    using TTable = TDbsControllerSchema::InverseMap;

    TString s;
    const bool success = ddiskDirectBlockGroups.SerializeToString(&s);
    Y_ABORT_UNLESS(success);

    Table<TTable>().Key(key).Update<TTable::DirectBlockGroups>(s);
}

bool TDbsControllerDatabase::GetLogicalNodesCount(
    const TDirectKey& key,
    std::optional<ui64>& outLogicalNodesCount)
{
    using TTable = TDbsControllerSchema::DirectMap;

    const auto it =
        Table<TTable>().Key(key).Select<TTable::LogicalNodesCount>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        outLogicalNodesCount = it.GetValue<TTable::LogicalNodesCount>();
    } else {
        outLogicalNodesCount = std::nullopt;
    }

    return true;
}

bool TDbsControllerDatabase::GetPartitionsForNode(
    const ui32 nodeId,
    TVector<ui64>& outPartitions)
{
    using TTable = TDbsControllerSchema::InverseMap;

    auto it =
        Table<TTable>().Prefix(nodeId).Select<TTable::DirectBlockGroups>();

    if (!it.IsReady()) {
        return false;
    }

    outPartitions = ExtractPartitions(it);

    return true;
}

bool TDbsControllerDatabase::GetPartitionsForPDisk(
    const ui32 nodeId,
    const ui32 pdiskId,
    TVector<ui64>& outPartitions)
{
    using TTable = TDbsControllerSchema::InverseMap;

    auto it = Table<TTable>()
                  .Prefix(nodeId, pdiskId)
                  .Select<TTable::DirectBlockGroups>();

    if (!it.IsReady()) {
        return false;
    }

    outPartitions = ExtractPartitions(it);

    return true;
}

bool TDbsControllerDatabase::GetPartitionsForDDisk(
    const ui32 nodeId,
    const ui32 pdiskId,
    const ui32 slotId,
    TVector<ui64>& outPartitions)
{
    using TTable = TDbsControllerSchema::InverseMap;

    auto it = Table<TTable>()
                  .Key(nodeId, pdiskId, slotId)
                  .Select<TTable::DirectBlockGroups>();

    if (!it.IsReady()) {
        return false;
    }

    outPartitions = ExtractPartitions(it);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
