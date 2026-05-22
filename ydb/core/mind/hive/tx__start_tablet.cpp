#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxStartTablet : public TTransactionBase<THive> {
    TFullTabletId TabletId;
    TActorId Local;
    ui64 Cookie;
    bool External;
    bool BootingSuppressed;
    TSideEffects SideEffects;
    bool Success;

public:
    TTxStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Local(local)
        , Cookie(cookie)
        , External(external)
        , BootingSuppressed(false)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_START_TABLET; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Success = false;
        SideEffects.Reset(Self->SelfId());
        YDB_LOG_DEBUG("THive::TTxStartTablet::Execute Tablet",
            {"GetLogPrefix", GetLogPrefix()},
            {"TabletId", TabletId});
        TTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            NIceDb::TNiceDb db(txc.DB);
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
                BootingSuppressed = leader.IsBootingSuppressed();

                if (BootingSuppressed && External && leader.ChannelProfileNewGroup.any()) {
                    if (leader.State != ETabletState::GroupAssignment) {
                        leader.InitiateAssignTabletGroups();
                    }
                    SideEffects.Send(Local,
                        new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::TRYLATER),
                        0,
                        Cookie);
                    return true;
                }

                if (leader.IsStarting() || BootingSuppressed && External) {
                    leader.IncreaseGeneration();
                    db.Table<Schema::Tablet>().Key(leader.Id).Update<Schema::Tablet::KnownGeneration>(leader.KnownGeneration);
                } else {
                    YDB_LOG_WARN("THive::TTxStartTablet::Execute Tablet ) skipped generation increment",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"leader", leader.ToString()},
                        {"StateString", leader.StateString()},
                        {"State", (ui64)leader.State});
                }
            }
            // reset usage impact estimate on each tablet restart
            tablet->UsageImpact = 0;
            db.Table<Schema::Metrics>().Key(tablet->GetFullTabletId()).Update<Schema::Metrics::UsageImpact>(0.);
            if (tablet->IsLeader()) {
                TLeaderTabletInfo& leader = tablet->AsLeader();
                if (leader.IsStartingOnNode(Local.NodeId()) || BootingSuppressed && External) {
                    if (!leader.DeletedHistory.empty()) {
                        if (!leader.WasAliveSinceCutHistory) {
                            YDB_LOG_ERROR("THive::TTxStartTablet::Execute Tablet failed to start after cutting history - will restore history",
                                {"GetLogPrefix", GetLogPrefix()},
                                {"TabletId", TabletId});
                            Self->TabletCounters->Cumulative()[NHive::COUNTER_HISTORY_RESTORED].Increment(leader.DeletedHistory.size());
                            Self->UpdateCounterTabletChannelHistorySize();
                            leader.RestoreDeletedHistory(txc);
                        } else {
                            leader.WasAliveSinceCutHistory = false;
                        }
                    }
                    YDB_LOG_DEBUG("THive::TTxStartTablet::Execute, Sending TEvBootTablet( to node storage",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"leader", leader.ToString()},
                        {"NodeId", Local.NodeId()},
                        {"TabletStorageInfo", leader.TabletStorageInfo->ToString()});
                    TFollowerId promotableFollowerId = leader.GetFollowerPromotableOnNode(Local.NodeId());
                    SideEffects.Send(Local,
                                new TEvLocal::TEvBootTablet(*leader.TabletStorageInfo, promotableFollowerId, leader.KnownGeneration),
                                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                                Cookie);
                    Success = true;
                    return true;
                } else {
                    YDB_LOG_WARN("THive::TTxStartTablet::Execute, ignoring TEvBootTablet( ) - wrong state or node",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"leader", leader.ToString()});
                }
            } else {
                TFollowerTabletInfo& follower = tablet->AsFollower();
                if (follower.IsStartingOnNode(Local.NodeId())) {
                    YDB_LOG_DEBUG("THive::TTxStartTablet::Execute, Sending TEvBootTablet( to node storage",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"follower", follower.ToString()},
                        {"NodeId", Local.NodeId()},
                        {"TabletStorageInfo", follower.LeaderTablet.TabletStorageInfo->ToString()});
                    SideEffects.Send(Local,
                             new TEvLocal::TEvBootTablet(*follower.LeaderTablet.TabletStorageInfo, follower.Id),
                             IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                             Cookie);
                    Success = true;
                    return true;
                } else {
                    YDB_LOG_WARN("THive::TTxStartTablet::Execute, ignoring TEvBootTablet( ) - wrong state or node",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"follower", follower.ToString()});
                }
            }
            // if anything wrong - attempt to restart the tablet
            if (tablet->InitiateStop(SideEffects)) {
                if (tablet->IsLeader()) {
                    YDB_LOG_NOTICE("THive::TTxStartTablet::Execute, jump-starting tablet",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"tablet", tablet->ToString()});
                    tablet->AsLeader().TryToBoot();
                }
            }
        } else {
            YDB_LOG_WARN("THive::TTxStartTablet::Execute Tablet wasn't found",
                {"GetLogPrefix", GetLogPrefix()},
                {"TabletId", TabletId});
        }
        if (External) {
            // Always send some reply for external start requests
            YDB_LOG_WARN("THive::TTxStartTablet::Execute, Aborting external boot of .",
                {"GetLogPrefix", GetLogPrefix()},
                {"first", TabletId.first},
                {"second", TabletId.second});
            SideEffects.Send(Local,
                     new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR),
                     0,
                     Cookie);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxStartTablet::Complete Tablet",
            {"GetLogPrefix", GetLogPrefix()},
            {"TabletId", TabletId},
            {"SideEffects", SideEffects});
        SideEffects.Complete(ctx);
        bool legitExternalBoot = External && BootingSuppressed;
        if (Success && !legitExternalBoot) {
            Self->UpdateCounterTabletsStarting(+1);
        }
    }
};

ITransaction* THive::CreateStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external) {
    return new TTxStartTablet(tabletId, local, cookie, external, this);
}

} // NHive
} // NKikimr
