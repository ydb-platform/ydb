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

    void TBlobDepot::KickSpaceMonitor() {
        SpaceMonitor->Kick();
    }

} // NKikimr::NBlobDepot
