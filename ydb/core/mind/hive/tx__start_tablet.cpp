#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxStartTablet : public TTransactionBase<THive> {
    TFullTabletId TabletId;
    TActorId Local;
    ui64 Cookie;
    bool External;
    TSideEffects SideEffects;
    bool Success;

public:
    TTxStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Local(local)
        , Cookie(cookie)
        , External(external)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_START_TABLET; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Success = false;
        SideEffects.Reset(Self->SelfId());
        BLOG_D("THive::TTxStartTablet::Execute Tablet " << TabletId);
        TTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            tablet->BootTime = TActivationContext::Now();
            // finish fast-move operation
            if (tablet->LastNodeId != 0 && tablet->LastNodeId != Local.NodeId()) {
                TNodeInfo* lastNode = Self->FindNode(tablet->LastNodeId);
                if (lastNode != nullptr && lastNode->Local) {
                    tablet->SendStopTablet(lastNode->Local, SideEffects);
                }
                tablet->LastNodeId = 0;
            }
            // increase generation
            if (tablet->IsLeader()) {
                TLeaderTabletInfo& leader = tablet->AsLeader();
                if (leader.IsStarting() || leader.IsBootingSuppressed() && External) {
                    NIceDb::TNiceDb db(txc.DB);
                    leader.IncreaseGeneration();
                    db.Table<Schema::Tablet>().Key(leader.Id).Update<Schema::Tablet::KnownGeneration>(leader.KnownGeneration);
                } else {
                    BLOG_W("THive::TTxStartTablet::Execute Tablet " << leader.ToString() << " (" << leader.StateString() << ") skipped generation increment " << (ui64)leader.State);
                }
            }
            if (tablet->IsLeader()) {
                TLeaderTabletInfo& leader = tablet->AsLeader();
                if (leader.IsStartingOnNode(Local.NodeId()) || leader.IsBootingSuppressed() && External) {
                    if (!leader.DeletedHistory.empty()) {
                        if (!leader.WasAliveSinceCutHistory) {
                            BLOG_ERROR("THive::TTxStartTablet::Execute Tablet " << TabletId << " failed to start after cutting history - will restore history");
                            Self->TabletCounters->Cumulative()[NHive::COUNTER_HISTORY_RESTORED].Increment(leader.DeletedHistory.size());
                            leader.RestoreDeletedHistory(txc);
                        } else {
                            leader.WasAliveSinceCutHistory = false;
                        }
                    }
                    BLOG_D("THive::TTxStartTablet::Execute, Sending TEvBootTablet(" << leader.ToString() << ")"
                            << " to node " << Local.NodeId()
                            << " storage " << leader.TabletStorageInfo->ToString());
                    TFollowerId promotableFollowerId = leader.GetFollowerPromotableOnNode(Local.NodeId());
                    SideEffects.Send(Local,
                                new TEvLocal::TEvBootTablet(*leader.TabletStorageInfo, promotableFollowerId, leader.KnownGeneration),
                                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                                Cookie);
                    Success = true;
                    return true;
                } else {
                    BLOG_W("THive::TTxStartTablet::Execute, ignoring TEvBootTablet(" << leader.ToString() << ") - wrong state or node");
                }
            } else {
                TFollowerTabletInfo& follower = tablet->AsFollower();
                if (follower.IsStartingOnNode(Local.NodeId())) {
                    BLOG_D("THive::TTxStartTablet::Execute, Sending TEvBootTablet(" << follower.ToString() << ")"
                           << " to node " << Local.NodeId()
                           << " storage " << follower.LeaderTablet.TabletStorageInfo->ToString());
                    SideEffects.Send(Local,
                             new TEvLocal::TEvBootTablet(*follower.LeaderTablet.TabletStorageInfo, follower.Id),
                             IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                             Cookie);
                    Success = true;
                    return true;
                } else {
                    BLOG_W("THive::TTxStartTablet::Execute, ignoring TEvBootTablet(" << follower.ToString() << ") - wrong state or node");
                }
            }
            // if anything wrong - attempt to restart the tablet
            if (tablet->InitiateStop(SideEffects)) {
                if (tablet->IsLeader()) {
                    BLOG_NOTICE("THive::TTxStartTablet::Execute, jump-starting tablet " << tablet->ToString());
                    tablet->AsLeader().TryToBoot();
                }
            }
        } else {
            BLOG_W("THive::TTxStartTablet::Execute Tablet " << TabletId << " wasn't found");
        }
        if (External) {
            // Always send some reply for external start requests
            BLOG_W("THive::TTxStartTablet::Execute, Aborting external boot of " << TabletId.first << "." << TabletId.second);
            SideEffects.Send(Local,
                     new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR),
                     0,
                     Cookie);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxStartTablet::Complete Tablet " << TabletId << " SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
        if (Success) {
            Self->UpdateCounterTabletsStarting(+1);
        }
    }
};

ITransaction* THive::CreateStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external) {
    return new TTxStartTablet(tabletId, local, cookie, external, this);
}

} // NHive
} // NKikimr
