#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUpdateTabletStatus : public TTransactionBase<THive> {
    TTabletId TabletId;
    TFollowerId FollowerId;
    const TActorId Local;
    const TEvLocal::TEvTabletStatus::EStatus Status;
    const TEvTablet::TEvTabletDead::EReason Reason;
    ui32 Generation;
    TSideEffects SideEffects;

public:
    TTxUpdateTabletStatus(
            TTabletId tabletId,
            const TActorId &local,
            ui32 generation,
            TFollowerId followerId,
            TEvLocal::TEvTabletStatus::EStatus status,
            TEvTablet::TEvTabletDead::EReason reason,
            THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , FollowerId(followerId)
        , Local(local)
        , Status(status)
        , Reason(reason)
        , Generation(generation)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLET_STATUS; }

    bool IsGoodStatusForPenalties() const {
        switch (Status) {
            case TEvLocal::TEvTabletStatus::StatusBootFailed:
                switch (Reason) {
                    case TEvTablet::TEvTabletDead::EReason::ReasonBootSSError:
                    case TEvTablet::TEvTabletDead::EReason::ReasonBootBSError:
                    case TEvTablet::TEvTabletDead::EReason::ReasonBootSSTimeout:
                        return true;
                    default:
                        break;
                }
                break;

            default:
                break;
        }
        return false;
    }

    TString GetStatus() {
        TStringBuilder str;
        str << (ui32)Status;
        if (Status != TEvLocal::TEvTabletStatus::StatusOk) {
            str << " reason " << Reason;
        }
        return str;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TTabletInfo* tablet = Self->FindTablet(TabletId, FollowerId);
        if (tablet != nullptr) {
            BLOG_D("THive::TTxUpdateTabletStatus::Execute for tablet "
                        << tablet->ToString()
                        << " status "
                        << GetStatus()
                        << " generation "
                        << Generation
                        << " follower "
                        << FollowerId
                        << " from local "
                        << Local);
            NIceDb::TNiceDb db(txc.DB);
            TInstant now = TActivationContext::Now();
            if (Status == TEvLocal::TEvTabletStatus::StatusOk) {
                tablet->Statistics.AddRestartTimestamp(now.MilliSeconds());
                tablet->ActualizeTabletStatistics(now);
                if (tablet->BootTime != TInstant()) {
                    TDuration startTime = now - tablet->BootTime;
                    if (startTime > TDuration::Seconds(30)) {
                        BLOG_W("Tablet " << tablet->GetFullTabletId() << " was starting for " << startTime.Seconds() << " seconds");
                    }
                    Self->TabletCounters->Percentile()[NHive::COUNTER_TABLETS_START_TIME].IncrementFor(startTime.MilliSeconds());
                    Self->UpdateCounterTabletsStarting(-1);
                }
                TNodeInfo* node = Self->FindNode(Local.NodeId());
                if (node == nullptr) {
                    // event from IC about disconnection of the node could overtake events from the node itself because of Pipe Server
                    // KIKIMR-9614
                    return true;
                }
                if (tablet->IsLeader() && Generation < tablet->GetLeader().KnownGeneration) {
                    return true;
                }
                for (const TActorId& actor : tablet->ActorsToNotifyOnRestart) {
                    SideEffects.Send(actor, new TEvPrivate::TEvRestartComplete({TabletId, FollowerId}, "OK"));
                }
                tablet->ActorsToNotifyOnRestart.clear();
                for (const TActorId& actor : Self->ActorsWaitingToMoveTablets) {
                    SideEffects.Send(actor, new TEvPrivate::TEvCanMoveTablets());
                }
                Self->ActorsWaitingToMoveTablets.clear();
                if (tablet->GetLeader().IsDeleting()) {
                    tablet->SendStopTablet(SideEffects);
                    return true;
                }
                tablet->BecomeRunning(Local.NodeId());
                if (tablet->GetLeader().IsLockedToActor()) {
                    // Tablet is locked and shouldn't be running, but we just found out it's running on this node
                    // Ask it to stop using InitiateStop (which uses data saved by BecomeRunning call above)
                    tablet->InitiateStop(SideEffects);
                }
                tablet->BootState = Self->BootStateRunning;
                tablet->Statistics.SetLastAliveTimestamp(now.MilliSeconds());
                if (tablet->IsLeader()) {
                    TLeaderTabletInfo& leader(tablet->AsLeader());
                    leader.KnownGeneration = Generation;
                    db.Table<Schema::Tablet>().Key(TabletId).Update(NIceDb::TUpdate<Schema::Tablet::LeaderNode>(tablet->NodeId),
                                                                    NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(Generation),
                                                                    NIceDb::TUpdate<Schema::Tablet::Statistics>(tablet->Statistics));
                    Self->UpdateTabletFollowersNumber(leader, db, SideEffects);

                    // tablet booted successfully, we may actually cut history now
                    while (!leader.DeletedHistory.empty() && leader.DeletedHistory.front().DeletedAtGeneration < leader.KnownGeneration) {
                        leader.WasAliveSinceCutHistory = true;
                        const auto& entry = leader.DeletedHistory.front();
                        db.Table<Schema::TabletChannelGen>().Key(TabletId, entry.Channel, entry.Entry.FromGeneration).Delete();
                        leader.DeletedHistory.pop();
                    }
                } else {
                    db.Table<Schema::TabletFollowerTablet>().Key(TabletId, FollowerId).Update(
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(tablet->AsFollower().FollowerGroup.Id),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(Local.NodeId()),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(tablet->Statistics));
                }
                for (const TActorId& actor : tablet->ActorsToNotify) {
                    SideEffects.Send(actor, new TEvHive::TEvTabletCreationResult(NKikimrProto::OK, TabletId));
                }
                tablet->ActorsToNotify.clear();
                db.Table<Schema::Tablet>().Key(TabletId).UpdateToNull<Schema::Tablet::ActorsToNotify>();
                tablet->FailedNodeId = 0;
            } else {
                if (Local) {
                    SideEffects.Send(Local, new TEvLocal::TEvDeadTabletAck(std::make_pair(TabletId, FollowerId), Generation));
                }
                if (tablet->IsLeader()) {
                    TLeaderTabletInfo& leader(tablet->AsLeader());
                    if (Generation < leader.KnownGeneration) {
                        return true;
                    }
                    if (leader.GetRestartsPerPeriod(now - Self->GetTabletRestartsPeriod()) >= Self->GetTabletRestartsMaxCount()) {
                        if (IsGoodStatusForPenalties()) {
                            leader.PostponeStart(now + Self->GetPostponeStartPeriod());
                            BLOG_D("THive::TTxUpdateTabletStatus::Execute for tablet " << tablet->ToString()
                                << " postponed start until " << leader.PostponedStart);
                        }
                    }
                }
                if (Local) {
                    if (tablet->IsAliveOnLocal(Local)) {
                        if (tablet->IsLeader()) {
                            TLeaderTabletInfo& leader(tablet->AsLeader());
                            db.Table<Schema::Tablet>().Key(TabletId).Update(NIceDb::TUpdate<Schema::Tablet::LeaderNode>(0),
                                                                            NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(leader.KnownGeneration),
                                                                            NIceDb::TUpdate<Schema::Tablet::Statistics>(tablet->Statistics));
                        } else {
                            db.Table<Schema::TabletFollowerTablet>().Key(TabletId, FollowerId).Update(
                                        NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(tablet->AsFollower().FollowerGroup.Id),
                                        NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0),
                                        NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(tablet->Statistics));
                        }
                        tablet->InitiateStop(SideEffects);
                    }
                    if (IsGoodStatusForPenalties()) {
                        tablet->FailedNodeId = Local.NodeId();
                    }
                }
                switch (tablet->GetLeader().State) {
                case ETabletState::GroupAssignment:
                    //Y_ABORT("Unexpected tablet boot failure during group assignment");
                    // Just ignore it. This is fail from previous generation.
                    return true;
                case ETabletState::StoppingInGroupAssignment:
                case ETabletState::Stopping:
                    if (tablet->IsLeader()) {
                        for (const TActorId& actor : tablet->GetLeader().ActorsToNotify) {
                            SideEffects.Send(actor, new TEvHive::TEvStopTabletResult(NKikimrProto::OK, TabletId));
                        }
                        tablet->GetLeader().ActorsToNotify.clear();
                    }
                    [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME

                case ETabletState::Stopped:
                    Self->ReportStoppedToWhiteboard(tablet->GetLeader());
                    BLOG_D("Report tablet " << tablet->ToString() << " as stopped to Whiteboard");
                    break;
                case ETabletState::BlockStorage:
                    // do nothing - let the tablet die
                    break;
                default:
                    break;
                };
            }
            tablet->GetLeader().TryToBoot();
        }
        Self->ProcessBootQueue(); // it's required to start followers on successful leader start
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxUpdateTabletStatus::Complete TabletId: " << TabletId << " SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateUpdateTabletStatus(
        TTabletId tabletId,
        const TActorId &local,
        ui32 generation,
        TFollowerId followerId,
        TEvLocal::TEvTabletStatus::EStatus status,
        TEvTablet::TEvTabletDead::EReason reason) {
    return new TTxUpdateTabletStatus(tabletId, local, generation, followerId, status, reason, this);
}

} // NHive
} // NKikimr
