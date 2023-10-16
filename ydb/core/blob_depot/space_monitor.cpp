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
        Y_ABORT_UNLESS(it != Groups.end());
        TGroupRecord& group = it->second;
        Y_ABORT_UNLESS(group.StatusRequestInFlight);
        group.StatusRequestInFlight = false;
        auto& msg = *ev->Get();
        if (msg.Status == NKikimrProto::OK) {
            group.StatusFlags = msg.StatusFlags;
            group.ApproximateFreeSpaceShare = msg.ApproximateFreeSpaceShare;
            Self->InvalidateGroupForAllocation(groupId);

            if (group.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                HandleYellowChannels();
            }
        }
    }

    void TSpaceMonitor::HandleYellowChannels() {
        TVector<ui32> yellowMove, yellowStop;

        for (const auto& [groupId, group] : Groups) {
            if (group.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                yellowMove.insert(yellowMove.end(), group.Channels.begin(), group.Channels.end());
            } else if (group.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                yellowStop.insert(yellowStop.end(), group.Channels.begin(), group.Channels.end());
            }
        }

        Y_ABORT_UNLESS(yellowMove || yellowStop);
        STLOG(PRI_INFO, BLOB_DEPOT, BDT28, "asking to reassign channels", (Id, Self->GetLogId()),
            (YellowMove, FormatList(yellowMove)),
            (YellowStop, FormatList(yellowStop)));
        Self->Executor()->OnYellowChannels(std::move(yellowMove), std::move(yellowStop));
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
            Groups[group].Channels.push_back(channel);
        }
    }

    ui64 TSpaceMonitor::GetGroupAllocationWeight(ui32 groupId, bool stopOnLightYellow) const {
        const auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            Y_DEBUG_ABORT_UNLESS(false);
            return 0;
        }

        const TGroupRecord& group = it->second;
        if (group.StatusFlags.Check(stopOnLightYellow
                ? NKikimrBlobStorage::StatusDiskSpaceLightYellowMove
                : NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            return 0;
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
