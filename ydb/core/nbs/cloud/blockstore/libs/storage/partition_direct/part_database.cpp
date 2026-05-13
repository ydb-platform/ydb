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

bool TPartitionDatabase::ReadAllVChunkConfigs(
    TVector<TVChunkConfigProto>& out)
{
    using TTable = TPartitionSchema::VChunkConfigs;

    auto it = Table<TTable>().Range().Select<TTable::Config>();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        if (it.HaveValue<TTable::Config>()) {
            out.push_back(it.GetValue<TTable::Config>());
        }
        it.Next();
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

void TPartitionDatabase::StoreVChunkConfig(const TVChunkConfigProto& cfg)
{
    using TTable = TPartitionSchema::VChunkConfigs;

    Table<TTable>().Key(cfg.GetVChunkIndex()).Update(
        NKikimr::NIceDb::TUpdate<TTable::Config>(cfg));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
