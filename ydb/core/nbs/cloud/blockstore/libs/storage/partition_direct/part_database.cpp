#include "part_database.h"

#include "part_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TVChunkConfigProto = PartitionDirect::NProto::TVChunkConfig;

TVChunkConfigProto ToProto(const TVChunkConfig& cfg)
{
    TVChunkConfigProto proto;
    proto.SetVChunkIndex(cfg.GetVChunkIndex());
    const auto disabled = cfg.GetDisabledHosts();
    for (THostIndex i = 0; i < cfg.GetHostCount(); ++i) {
        proto.AddPBufferHostRoles(static_cast<ui32>(cfg.GetPBufferRole(i)));
        proto.AddDDiskHostRoles(static_cast<ui32>(cfg.GetDDiskRole(i)));
        if (!disabled.Get(i)) {
            proto.AddEnabledHosts(i);
        }

        if (const auto watermark = cfg.GetWatermark(i)) {
            auto* w = proto.AddWatermarks();
            w->SetHostIndex(i);
            w->SetValue(watermark.value());
        }
    }

    return proto;
}

TVChunkConfig FromProto(const TVChunkConfigProto& proto)
{
    THostRoles pbufferHosts(proto.PBufferHostRolesSize());
    THostRoles ddiskHosts(proto.DDiskHostRolesSize());
    THostMask enabledHosts;
    TVector<std::optional<ui64>> watermarks(proto.DDiskHostRolesSize());

    for (THostIndex i = 0; i < proto.PBufferHostRolesSize(); ++i) {
        pbufferHosts.SetRole(
            i,
            static_cast<EHostRole>(proto.GetPBufferHostRoles(i)));
    }

    for (THostIndex i = 0; i < proto.DDiskHostRolesSize(); ++i) {
        ddiskHosts.SetRole(
            i,
            static_cast<EHostRole>(proto.GetDDiskHostRoles(i)));
    }

    for (THostIndex i = 0; i < proto.EnabledHostsSize(); ++i) {
        enabledHosts.Set(static_cast<THostIndex>(proto.GetEnabledHosts(i)));
    }

    for (const auto& w: proto.GetWatermarks()) {
        watermarks[w.GetHostIndex()] = w.GetValue();
    }

    return TVChunkConfig::Make(
        proto.GetVChunkIndex(),
        pbufferHosts,
        ddiskHosts,
        enabledHosts,
        std::move(watermarks));
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
        .Key(cfg.GetVChunkIndex())
        .Update(NKikimr::NIceDb::TUpdate<TTable::Config>(ToProto(cfg)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
