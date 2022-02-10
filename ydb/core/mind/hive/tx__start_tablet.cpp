#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxStartTablet : public TTransactionBase<THive> {
    TFullTabletId TabletId;
    TActorId Local;
    ui64 Cookie;
    bool External;
    ui32 KnownGeneration = -1;

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
        BLOG_D("THive::TTxStartTablet::Execute Tablet " << TabletId);
        TTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            if (tablet->IsLeader()) { 
                TLeaderTabletInfo& leader = tablet->AsLeader(); 
                if (leader.IsStarting() || leader.IsBootingSuppressed() && External) { 
                    NIceDb::TNiceDb db(txc.DB);
                    leader.IncreaseGeneration(); 
                    KnownGeneration = leader.KnownGeneration; 
                    db.Table<Schema::Tablet>().Key(leader.Id).Update<Schema::Tablet::KnownGeneration>(leader.KnownGeneration); 
                } else {
                    BLOG_W("THive::TTxStartTablet::Execute Tablet " << TabletId << " skipped generation increment");        
                }
            }
        } else {
            BLOG_W("THive::TTxStartTablet::Execute Tablet " << TabletId << " wasn't found");
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            if (tablet->LastNodeId != 0 && tablet->LastNodeId != Local.NodeId()) {
                TNodeInfo* lastNode = Self->FindNode(tablet->LastNodeId);
                if (lastNode != nullptr && lastNode->Local) {
                    Self->StopTablet(lastNode->Local, tablet->GetFullTabletId());
                }
                tablet->LastNodeId = 0;
            }
            if (tablet->IsLeader()) { 
                TLeaderTabletInfo& leader = tablet->AsLeader(); 
                if (leader.IsStartingOnNode(Local.NodeId()) || leader.IsBootingSuppressed() && External) { 
                    if (KnownGeneration == leader.KnownGeneration) { 
                        BLOG_D("THive::TTxStartTablet::Complete, Sending TEvBootTablet(" << leader.ToString() << ")" 
                               << " to node " << Local.NodeId()
                               << " storage " << leader.TabletStorageInfo->ToString()); 
                        TFollowerId promotableFollowerId = leader.GetFollowerPromotableOnNode(Local.NodeId()); 
                        ctx.Send(Local,
                                 new TEvLocal::TEvBootTablet(*leader.TabletStorageInfo, promotableFollowerId, KnownGeneration), 
                                 IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                                 Cookie);
                        return;
                    } else {
                        BLOG_W("THive::TTxStartTablet::Complete, ignoring outstanding TEvBootTablet(" << leader.ToString() << ") - wrong generation"); 
                    }
                } else {
                    BLOG_W("THive::TTxStartTablet::Complete, ignoring outstanding TEvBootTablet(" << leader.ToString() << ") - wrong state or node"); 
                }
            } else {
                TFollowerTabletInfo& follower = tablet->AsFollower(); 
                if (follower.IsStartingOnNode(Local.NodeId())) { 
                    BLOG_D("THive::TTxStartTablet::Complete, Sending TEvBootTablet(" << follower.ToString() << ")" 
                           << " to node " << Local.NodeId()
                           << " storage " << follower.LeaderTablet.TabletStorageInfo->ToString()); 
                    ctx.Send(Local,
                             new TEvLocal::TEvBootTablet(*follower.LeaderTablet.TabletStorageInfo, follower.Id), 
                             IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                             Cookie);
                    return;
                } else {
                    BLOG_W("THive::TTxStartTablet::Complete, ignoring outstanding TEvBootTablet(" << follower.ToString() << ") - wrong state or node"); 
                }
            }
            // if anything wrong - attempt to restart the tablet
            if (tablet->InitiateStop()) {
                if (tablet->IsLeader()) {
                    BLOG_NOTICE("THive::TTxStartTablet::Complete, jump-starting tablet " << tablet->ToString());
                    tablet->AsLeader().TryToBoot();
                }
            }
        }
        if (External) {
            // Always send some reply for external start requests
            BLOG_W("THive::TTxStartTablet::Complete, Aborting external boot of " << TabletId.first << "." << TabletId.second);
            ctx.Send(Local,
                     new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR),
                     0,
                     Cookie);
        }
    }
};

ITransaction* THive::CreateStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external) {
    return new TTxStartTablet(tabletId, local, cookie, external, this);
}

} // NHive
} // NKikimr
