#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxDisconnectNode : public TTransactionBase<THive> {
protected:
    THolder<TEvInterconnect::TEvNodeDisconnected> Event;

public:
    TTxDisconnectNode(THolder<TEvInterconnect::TEvNodeDisconnected> event, THive* hive)
        : TBase(hive)
        , Event(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_DISCONNECT_NODE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        BLOG_D("THive::TTxDisconnectNode()::Execute");
        TNodeInfo* node = Self->FindNode(Event->NodeId);
        if (node != nullptr) {
            Self->ScheduleUnlockTabletExecution(*node, NKikimrHive::LOCK_LOST_REASON_NODE_DISCONNECTED);
            if (node->BecomeDisconnecting()) {
                THolder<TEvPrivate::TEvProcessDisconnectNode> event = MakeHolder<TEvPrivate::TEvProcessDisconnectNode>();
                event->NodeId = node->Id;
                event->Local = node->Local;
                event->StartTime = TActivationContext::Now();
                for (const auto& t : node->Tablets) {
                    for (TTabletInfo* tablet : t.second) {
                        TLeaderTabletInfo& leader = tablet->GetLeader();
                        TTabletCategoryId tabletCategoryId = leader.Category ? leader.Category->Id : 0;
                        event->Tablets[tabletCategoryId].emplace_back(tablet->GetFullTabletId());
                    }
                }
                Self->ScheduleDisconnectNode(std::move(event));
            } else if (node->IsUnknown()) {
                BLOG_W("THive::TTxDisconnectNode() - killing node " << node->Id);
                Self->KillNode(node->Id, node->Local);
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxDisconnectNode()::Complete");
    }
};

ITransaction* THive::CreateDisconnectNode(THolder<TEvInterconnect::TEvNodeDisconnected> event) {
    return new TTxDisconnectNode(std::move(event), this);
}

} // NHive
} // NKikimr
