#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    using TSpaceMonitor = TBlobDepot::TSpaceMonitor;

    TSpaceMonitor::TSpaceMonitor(TBlobDepot *self)
        : Self(self)
    {}

    TSpaceMonitor::~TSpaceMonitor()
    {}

    void TSpaceMonitor::Handle(TEvBlobStorage::TEvStatusResult::TPtr ev) {
        const ui32 groupId = ev->Cookie;
        const auto it = Groups.find(groupId);
        Y_VERIFY(it != Groups.end());
        TGroupRecord& group = it->second;
        Y_VERIFY(group.StatusRequestInFlight);
        group.StatusRequestInFlight = false;
        auto& msg = *ev->Get();
        if (msg.Status == NKikimrProto::OK) {
            group.StatusFlags = msg.StatusFlags;
            group.ApproximateFreeSpaceShare = msg.ApproximateFreeSpaceShare;
            Self->InvalidateGroupForAllocation(groupId);
        }
    }

    void TSpaceMonitor::Kick() {
        if (Groups.empty()) {
            Init();
        }

        for (auto& [groupId, group] : Groups) {
            if (!group.StatusRequestInFlight) {
                group.StatusRequestInFlight = true;
                SendToBSProxy(Self->SelfId(), groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()), groupId);
            }
        }

        TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvKickSpaceMonitor, 0,
            Self->SelfId(), {}, nullptr, 0));
    }

    void TSpaceMonitor::Init() {
        const auto it = Self->ChannelKinds.find(NKikimrBlobDepot::TChannelKind::Data);
        if (it == Self->ChannelKinds.end()) {
            return; // no data channels?
        }
        const TChannelKind& kind = it->second;
        for (const auto& [channel, group] : kind.ChannelGroups) {
            Groups.try_emplace(group);
        }
    }

    ui64 TSpaceMonitor::GetGroupAllocationWeight(ui32 groupId) const {
        const auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            Y_VERIFY_DEBUG(false);
            return 0;
        }

        const TGroupRecord& group = it->second;
        if (group.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            return 0; // do not write data to this group
        }

        if (!group.ApproximateFreeSpaceShare) { // not collected yet?
            return 1;
        }

        const float weight = group.ApproximateFreeSpaceShare < 0.25
            ? group.ApproximateFreeSpaceShare * 3
            : (group.ApproximateFreeSpaceShare + 2) / 3;

        const bool isCyan = group.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceCyan);

        return weight * 16'777'216.0f * (isCyan ? 0.5f : 1.0f /* cyan penalty */);
    }

    void TSpaceMonitor::SetSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor, float approximateFreeSpaceShare) {
        if (SpaceColor != spaceColor || ApproximateFreeSpaceShare != approximateFreeSpaceShare) {
            SpaceColor = spaceColor;
            ApproximateFreeSpaceShare = approximateFreeSpaceShare;
            Self->OnSpaceColorChange(SpaceColor, ApproximateFreeSpaceShare);
        }
    }

    NKikimrBlobStorage::TPDiskSpaceColor::E TSpaceMonitor::GetSpaceColor() const {
        return SpaceColor;
    }

    float TSpaceMonitor::GetApproximateFreeSpaceShare() const {
        return ApproximateFreeSpaceShare;
    }

    void TBlobDepot::KickSpaceMonitor() {
        SpaceMonitor->Kick();
    }

} // NKikimr::NBlobDepot
