#include "part_database.h"

#include "part_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TVChunkConfigProto = ::NYdb::NBS::PartitionDirect::NProto::TVChunkConfig;

TVChunkConfigProto ToProto(const TVChunkConfig& cfg)
{
    TVChunkConfigProto proto;
    proto.SetVChunkIndex(cfg.VChunkIndex);
    for (THostIndex i = 0; i < cfg.PBufferHosts.HostCount(); ++i) {
        proto.AddPBufferHostRoles(
            static_cast<ui32>(cfg.PBufferHosts.GetRole(i)));
    }
    for (THostIndex i = 0; i < cfg.DDiskHosts.HostCount(); ++i) {
        proto.AddDDiskHostRoles(static_cast<ui32>(cfg.DDiskHosts.GetRole(i)));
    }
    for (const THostIndex host: cfg.EnabledHosts) {
        proto.AddEnabledHosts(host);
    }
    return proto;
}

TVChunkConfig FromProto(const TVChunkConfigProto& proto)
{
    TVChunkConfig cfg;
    cfg.VChunkIndex = proto.GetVChunkIndex();

    cfg.PBufferHosts = THostRoles(proto.PBufferHostRolesSize());
    for (THostIndex i = 0; i < proto.PBufferHostRolesSize(); ++i) {
        cfg.PBufferHosts.SetRole(
            i,
            static_cast<EHostRole>(proto.GetPBufferHostRoles(i)));
    }

    cfg.DDiskHosts = THostRoles(proto.DDiskHostRolesSize());
    for (THostIndex i = 0; i < proto.DDiskHostRolesSize(); ++i) {
        cfg.DDiskHosts.SetRole(
            i,
            static_cast<EHostRole>(proto.GetDDiskHostRoles(i)));
    }

    for (THostIndex i = 0; i < proto.EnabledHostsSize(); ++i) {
        cfg.EnabledHosts.Set(static_cast<THostIndex>(proto.GetEnabledHosts(i)));
    }

    return cfg;
}

}   // namespace

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

bool TPartitionDatabase::ReadAllVChunkConfigs(TVector<TVChunkConfig>& out)
{
    using TTable = TPartitionSchema::VChunkConfigs;

    auto it = Table<TTable>().Range().Select<TTable::Config>();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        if (it.HaveValue<TTable::Config>()) {
            out.push_back(FromProto(it.GetValue<TTable::Config>()));
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

void TPartitionDatabase::StoreVChunkConfig(const TVChunkConfig& cfg)
{
    using TTable = TPartitionSchema::VChunkConfigs;

    Table<TTable>()
        .Key(cfg.VChunkIndex)
        .Update(NKikimr::NIceDb::TUpdate<TTable::Config>(ToProto(cfg)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
