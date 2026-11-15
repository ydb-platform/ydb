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
        Y_ABORT_UNLESS(TErasureType::ErasureNames.contains(x.Type.GetErasure()));

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

namespace NKikimr {

void TEvTablet::TDependencyGraph::Describe(IOutputStream &out) const noexcept {
    out
        << "Deps{" << Snapshot.first << ":" << Snapshot.second
        << " entries " << Entries.size() << "}";
}

TEvTablet::TEvPing::TEvPing(ui64 tabletId, ui64 flags)
{
    Record.SetTabletID(tabletId);
    Record.SetFlags(flags);
}

TEvTablet::TEvPong::TEvPong(ui64 tabletId, ui64 flags)
{
    Record.SetTabletID(tabletId);
    Record.SetFlags(flags);
}

TEvTablet::TEvReadLocalBase::TEvReadLocalBase(const TString& rootKey, bool saveScheme)
{
    if (!rootKey.empty())
        Record.SetRootKey(rootKey);
    if (saveScheme)
        Record.SetSaveScheme(true);
}

TEvTablet::TEvReadLocalBaseResult::TEvReadLocalBaseResult(ui64 origin, const TString& rootKey, bool isError, const TString& docBuffer, const TString* schemeBuffer)
{
    Record.SetOrigin(origin);
    Record.SetRootKey(rootKey);
    Record.SetIsError(isError);
    Record.SetDocBuffer(docBuffer);
    if (schemeBuffer)
        Record.SetScheme(*schemeBuffer);
}

TEvTablet::TEvFollowerAttach::TEvFollowerAttach(ui64 tabletId, ui32 followerAttempt)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
}

TEvTablet::TEvFollowerUpdate::TEvFollowerUpdate(ui64 tabletId, ui32 followerAttempt, ui64 streamCounter)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
    Record.SetStreamCounter(streamCounter);
}

TEvTablet::TEvFollowerAuxUpdate::TEvFollowerAuxUpdate(ui64 tabletId, ui32 followerAttempt, ui64 streamCounter)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
    Record.SetStreamCounter(streamCounter);
}

TEvTablet::TEvFollowerDetach::TEvFollowerDetach(ui64 tabletId, ui32 followerAttempt)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
}

TEvTablet::TEvFollowerDisconnect::TEvFollowerDisconnect(ui64 tabletId, ui32 followerAttempt)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
}

TEvTablet::TEvFollowerRefresh::TEvFollowerRefresh(ui64 tabletId, ui32 generation)
{
    Record.SetTabletId(tabletId);
    Record.SetGeneration(generation);
}

TEvTablet::TEvFollowerGcAck::TEvFollowerGcAck(ui64 tabletId, ui32 followerAttempt, ui32 generation, ui32 step)
{
    Record.SetTabletId(tabletId);
    Record.SetFollowerAttempt(followerAttempt);
    Record.SetGeneration(generation);
    Record.SetStep(step);
}

TEvTablet::TFUpdateBody::TFUpdateBody(const TEvFollowerUpdate &upd)
    : IsSnapshot(upd.Record.GetIsSnapshot())
    , Step(upd.Record.GetStep())
    , NeedFollowerGcAck(upd.Record.HasNeedGCApplyAck() ? upd.Record.GetNeedGCApplyAck() : false)
{
    const auto &r = upd.Record;
    if (r.HasBody())
        EmbeddedBody = r.GetBody();
    if (r.HasAuxPayload())
        AuxPayload = r.GetAuxPayload();

    References.reserve(r.ReferencesIdsSize());
    for (ui32 i = 0, end = r.ReferencesIdsSize(); i != end; ++i) {
        const TLogoBlobID &id = LogoBlobIDFromLogoBlobID(r.GetReferencesIds(i));
        References.emplace_back(std::make_pair(id, r.GetReferences(i)));
    }
}

TEvTablet::TEvTabletStop::TEvTabletStop(ui64 tabletId, EReason reason) {
    Record.SetTabletID(tabletId);
    Record.SetReason(reason);
}

TEvTablet::TEvTabletStateSubscribe::TEvTabletStateSubscribe(ui64 tabletId, ui64 seqNo) {
    Record.SetTabletId(tabletId);
    Record.SetSeqNo(seqNo);
}

TEvTablet::TEvTabletStateUnsubscribe::TEvTabletStateUnsubscribe(ui64 tabletId, ui64 seqNo) {
    Record.SetTabletId(tabletId);
    Record.SetSeqNo(seqNo);
}

} // namespace NKikimr
