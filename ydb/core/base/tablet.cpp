#include "tablet.h"

namespace NKikimr {

TIntrusivePtr<TTabletStorageInfo> TabletStorageInfoFromProto(const NKikimrTabletBase::TTabletStorageInfo &proto) {
    auto info = MakeIntrusive<TTabletStorageInfo>();

    info->TabletID = proto.GetTabletID();

    if (proto.HasTabletType())
        info->TabletType = proto.GetTabletType();

    if (proto.HasVersion())
        info->Version = proto.GetVersion();

    info->Channels.resize((ui32)proto.ChannelsSize());
    for (ui32 i = 0, e = info->Channels.size(); i != e; ++i) {
        const NKikimrTabletBase::TTabletChannelInfo &channelInfo = proto.GetChannels(i);
        TTabletChannelInfo &x = info->Channels[i];
        x.Channel = channelInfo.GetChannel();

        if (channelInfo.HasChannelType()) {
            auto erasure = (TBlobStorageGroupType::EErasureSpecies)channelInfo.GetChannelType();
            x.Type = TBlobStorageGroupType(erasure);
            Y_ABORT_UNLESS(!channelInfo.HasChannelErasureName());
        } else {
            auto erasure = TBlobStorageGroupType::ErasureSpeciesByName(channelInfo.GetChannelErasureName());
            x.Type = TBlobStorageGroupType(erasure);
        }
        Y_ABORT_UNLESS((ui32)x.Type.GetErasure() < x.Type.ErasureSpeciesCount);

        x.StoragePool = channelInfo.GetStoragePool();

        x.History.resize((ui32)channelInfo.HistorySize());
        for (ui32 j = 0, je = x.History.size(); j != je; ++j) {
            const NKikimrTabletBase::TTabletChannelInfo::THistoryEntry &entry = channelInfo.GetHistory(j);
            TTabletChannelInfo::THistoryEntry &m = x.History[j];

            m.FromGeneration = entry.GetFromGeneration();
            m.GroupID = entry.GetGroupID();
        }
    }

    if ((proto.HasTenantIdOwner() && proto.GetTenantIdOwner() && proto.GetTenantIdOwner() != InvalidOwnerId)
        || (proto.HasTenantIdLocalId() && proto.GetTenantIdLocalId() && proto.GetTenantIdLocalId() != InvalidLocalPathId))
    {
        info->TenantPathId = TPathId(proto.GetTenantIdOwner(), proto.GetTenantIdLocalId());
    }

    return info;
}

void TabletStorageInfoToProto(const TTabletStorageInfo &info, NKikimrTabletBase::TTabletStorageInfo *proto) {
    proto->SetTabletID(info.TabletID);
    proto->SetTabletType(info.TabletType);
    proto->SetVersion(info.Version);
    proto->MutableChannels()->Clear();
    proto->MutableChannels()->Reserve(info.Channels.size());

    for (ui32 i = 0, e = info.Channels.size(); i != e; ++i) {
        NKikimrTabletBase::TTabletChannelInfo *x = proto->MutableChannels()->Add();
        const TTabletChannelInfo &channelInfo = info.Channels[i];
        x->SetChannel(channelInfo.Channel);
        x->SetChannelType(channelInfo.Type.GetErasure());
        x->SetStoragePool(channelInfo.StoragePool);

        x->MutableHistory()->Reserve(channelInfo.History.size());
        for (ui32 j = 0, je = channelInfo.History.size(); j != je; ++j) {
            NKikimrTabletBase::TTabletChannelInfo::THistoryEntry *m = x->MutableHistory()->Add();
            const TTabletChannelInfo::THistoryEntry &entry = channelInfo.History[j];
            m->SetFromGeneration(entry.FromGeneration);
            m->SetGroupID(entry.GroupID);
        }
    }

    if (info.TenantPathId) {
        proto->SetTenantIdOwner(info.TenantPathId.OwnerId);
        proto->SetTenantIdLocalId(info.TenantPathId.LocalPathId);
    }
}

const char* TEvTablet::TEvTabletDead::Str(EReason status) {
    switch (status) {
        TABLET_DEAD_REASON_MAP(ENUM_TO_STRING_IMPL_ITEM)
    default:
        return "TabletReadReasonNotDefined";
    }
}

void TEvTablet::TEvTabletDead::Out(IOutputStream& os, EReason x) {
    switch (x) {
        TABLET_DEAD_REASON_MAP(ENUM_LTLT_IMPL_ITEM);
    default:
        os << static_cast<int>(x);
        return;
    }
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::TEvTablet::TEvTabletStop::EReason, o, x) {
    o << NKikimrTabletBase::TEvTabletStop::EReason_Name(x);
}
