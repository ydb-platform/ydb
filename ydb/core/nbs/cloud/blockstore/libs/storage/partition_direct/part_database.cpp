#include "part_database.h"

#include "part_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::InitSchema()
{
    Materialize<TPartitionSchema>();

    TSchemaInitializer<TPartitionSchema::TTables>::InitStorage(
        Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionDatabase::ReadVolumeConfig(
    TMaybe<NKikimrBlockStore::TVolumeConfig>& volumeConfig)
{
    using TTable = TPartitionSchema::TabletInfo;

    auto it = Table<TTable>().Key(1).Select<TTable::VolumeConfig>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid() && it.HaveValue<TTable::VolumeConfig>()) {
        volumeConfig = it.GetValue<TTable::VolumeConfig>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionDatabase::ReadDirectBlockGroupsConnections(
    TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections)
{
    using TTable = TPartitionSchema::TabletInfo;

    auto it =
        Table<TTable>().Key(1).Select<TTable::DirectBlockGroupsConnections>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid() && it.HaveValue<TTable::DirectBlockGroupsConnections>()) {
        directBlockGroupsConnections =
            it.GetValue<TTable::DirectBlockGroupsConnections>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::StoreVolumeConfig(
    const NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    using TTable = TPartitionSchema::TabletInfo;

    Table<TTable>().Key(1).Update(
        NKikimr::NIceDb::TUpdate<TTable::VolumeConfig>(volumeConfig));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::StoreDirectBlockGroupsConnections(
    const TDirectBlockGroupsConnections& directBlockGroupsConnections)
{
    using TTable = TPartitionSchema::TabletInfo;

    Table<TTable>().Key(1).Update(
        NKikimr::NIceDb::TUpdate<TTable::DirectBlockGroupsConnections>(
            directBlockGroupsConnections));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
