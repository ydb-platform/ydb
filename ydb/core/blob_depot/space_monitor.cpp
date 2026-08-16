#include "space_monitor.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TSpaceMonitor = TBlobDepot::TSpaceMonitor;

    static std::optional<NKikimrBlobDepot::ESimpleCounters> GetSpaceColorCounter(
            NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        switch (color) {
            case TColor::GREEN: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_GREEN;
            case TColor::CYAN: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_CYAN;
            case TColor::LIGHT_YELLOW: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_LIGHT_YELLOW;
            case TColor::YELLOW: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_YELLOW;
            case TColor::LIGHT_ORANGE: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_LIGHT_ORANGE;
            case TColor::PRE_ORANGE: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_PRE_ORANGE;
            case TColor::ORANGE: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_ORANGE;
            case TColor::RED: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_RED;
            case TColor::BLACK: return NKikimrBlobDepot::COUNTER_SPACE_COLOR_BLACK;
            default: return std::nullopt;
        }
    }

    void TSpaceMonitor::SwitchSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E oldColor,
            NKikimrBlobStorage::TPDiskSpaceColor::E newColor) {
        if (const auto counter = GetSpaceColorCounter(oldColor)) {
            Self->TabletCounters->Simple()[*counter] = 0;
        }
        if (const auto counter = GetSpaceColorCounter(newColor)) {
            Self->TabletCounters->Simple()[*counter] = 1;
        }
    }

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
        YDB_LOG_INFO("Asking to reassign channels",
            {"marker", "BDT28"},
            {"id", Self->GetLogId()},
            {"yellowMove", FormatList(yellowMove)},
            {"yellowStop", FormatList(yellowStop)});
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

        SwitchSpaceColor(SpaceColor, SpaceColor);
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_APPROXIMATE_FREE_SPACE_SHARE] = static_cast<ui64>(ApproximateFreeSpaceShare * 10000);
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
            const auto oldColor = SpaceColor;
            SpaceColor = spaceColor;
            ApproximateFreeSpaceShare = approximateFreeSpaceShare;
            SwitchSpaceColor(oldColor, SpaceColor);
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_APPROXIMATE_FREE_SPACE_SHARE] =
                static_cast<ui64>(ApproximateFreeSpaceShare * 10000);
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
