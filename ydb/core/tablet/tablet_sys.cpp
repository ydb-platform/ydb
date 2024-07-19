#include "tablet_sys.h"
#include "tablet_tracing_signals.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/stream/str.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_LEVEL
#error log macro definition clash
#endif

#define BLOG_LEVEL(level, stream, marker) LOG_LOG_S(*TlsActivationContext, level, NKikimrServices::TABLET_MAIN, "Tablet: " << TabletID() << " " << stream << " Marker# " << marker)
#define BLOG_D(stream, marker) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "Tablet: " << TabletID() << " " << stream << " Marker# " << marker)
#define BLOG_I(stream, marker) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "Tablet: " << TabletID() << " " << stream << " Marker# " << marker)
#define BLOG_ERROR(stream, marker) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "Tablet: " << TabletID() << " " << stream << " Marker# " << marker)
#define BLOG_TRACE(stream, marker) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "Tablet: " << TabletID() << " " << stream << " Marker# " << marker)


namespace NKikimr {

namespace {

    static constexpr size_t MaxStepsInFlight = 10000;
    static constexpr size_t MaxBytesInFlight = 256 * 1024 * 1024;

    static constexpr TDuration OfflineFollowerWaitFirst = TDuration::Seconds(4);
    static constexpr TDuration OfflineFollowerWaitRetry = TDuration::Seconds(15);

}

ui64 TTablet::TabletID() const {
    return Info->TabletID;
}

void TTablet::NextFollowerAttempt() {
    const ui32 node = FollowerInfo.KnownLeaderID.NodeId();
    if (node && node != SelfId().NodeId()) {
        const TActorId proxy = TActivationContext::InterconnectProxy(node);
        Send(proxy, new TEvents::TEvUnsubscribe);
    }
    FollowerInfo.NextAttempt();
}

void TTablet::ReportTabletStateChange(ETabletState state) {
    const TActorId tabletStateServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
    if (state == TTabletStateInfo::Created || state == TTabletStateInfo::ResolveLeader) {
        Send(tabletStateServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate(TabletID(), FollowerId, state, Info, StateStorageInfo.KnownGeneration, Leader));
    } else {
        Send(tabletStateServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate(TabletID(), FollowerId, state, StateStorageInfo.KnownGeneration));
    }
}

void TTablet::PromoteToCandidate(ui32 gen) {
    if (SuggestedGeneration != 0) {
        if (gen >= SuggestedGeneration)
            return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootRace);
        StateStorageInfo.KnownGeneration = SuggestedGeneration;
    } else {
        StateStorageInfo.KnownGeneration = 1 + Max(gen, StateStorageInfo.KnownGeneration);
    }

    StateStorageInfo.KnownStep = 0;

    Y_DEBUG_ABORT_UNLESS(SetupInfo);
    if (!UserTablet)
        UserTablet = SetupInfo->Apply(Info.Get(), SelfId());
    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnPromoteToCandidate>(StateStorageInfo.KnownGeneration));
    }

    // todo: handle 'proxy not found' case
    Send(StateStorageInfo.ProxyID, new TEvStateStorage::TEvUpdate(TabletID(), 0, SelfId(), UserTablet, StateStorageInfo.KnownGeneration, 0, StateStorageInfo.Signature.Get(), StateStorageInfo.SignatureSz, TEvStateStorage::TProxyOptions::SigAsync));

    Become(&TThis::StateBecomeCandidate);
    ReportTabletStateChange(TTabletStateInfo::Candidate);
}

void TTablet::TabletBlockBlobStorage() {
    Y_ABORT_UNLESS(Info);

    IActor * const x = CreateTabletReqBlockBlobStorage(SelfId(), Info.Get(), StateStorageInfo.KnownGeneration - 1, false);
    TActorId newActorId = Register(x);

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnTabletBlockBlobStorage>(newActorId, StateStorageInfo.KnownGeneration));
    }

    Become(&TThis::StateBlockBlobStorage);
    ReportTabletStateChange(TTabletStateInfo::BlockBlobStorage);
}

void TTablet::TabletRebuildGraph() {
    THolder<NTracing::ITrace> newTrace;
    NTracing::TTraceID rebuildGraphTraceID;
    if (IntrospectionTrace) {
        newTrace = THolder<NTracing::ITrace>(IntrospectionTrace->CreateTrace(NTracing::ITrace::TypeReqRebuildHistoryGraph));
        rebuildGraphTraceID = newTrace->GetSelfID();
    }

    RebuildGraphRequest = Register(CreateTabletReqRebuildHistoryGraph(
        SelfId(), Info.Get(), StateStorageInfo.KnownGeneration - 1, IntrospectionTrace ? newTrace.Release() : nullptr, 0)
    );

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnTabletRebuildGraph>(RebuildGraphRequest, rebuildGraphTraceID));
    }

    Become(&TThis::StateRebuildGraph);
    ReportTabletStateChange(TTabletStateInfo::RebuildGraph);
}

void TTablet::WriteZeroEntry(TEvTablet::TDependencyGraph *graph) {
    // rebuild zero entry
    std::pair<ui32, ui32> snapshot(0, 0);

    ui32 lastGeneration = 0;
    ui32 confirmedStep = 0;
    ui32 lastInGeneration = 0;

    TDeque<TEvTablet::TDependencyGraph::TEntry>::iterator it = graph->Entries.begin();
    TDeque<TEvTablet::TDependencyGraph::TEntry>::iterator end = graph->Entries.end();

    TDeque<TEvTablet::TDependencyGraph::TEntry>::iterator snapIterator = it;
    TDeque<TEvTablet::TDependencyGraph::TEntry>::iterator confirmedIterator = it;

    // find tail (todo: do it in reverse order?)
    for (; it != end; ++it) {
        const ui32 gen = it->Id.first;
        const ui32 step = it->Id.second;

        if (gen > lastGeneration) {
            lastGeneration = gen;

            // do not skip unconfirmed ranges at start of generation except snapshot case
            if (step == 1 || it->IsSnapshot)
                confirmedStep = step;
            else
                confirmedStep = 0;

            confirmedIterator = it;
        }

        Y_ABORT_UNLESS(gen == lastGeneration);

        lastInGeneration = step;

        if (it->IsSnapshot) {
            snapshot = std::pair<ui32, ui32>(gen, step);
            snapIterator = it;
        }

        // if see continuous confirmed range - extend it on next entry
        if (confirmedStep + 1 == step) {
            confirmedStep = step;
            confirmedIterator = it;
        }
    }

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnWriteZeroEntry>(snapshot, lastGeneration, confirmedStep, lastInGeneration));
    }

    // fill tail bitmask (beyond continuous confirmed range) (if any present)

    TAutoPtr<NKikimrTabletBase::TTabletLogEntry> entry(new NKikimrTabletBase::TTabletLogEntry());

    entry->SetSnapshot(MakeGenStepPair(snapshot.first, snapshot.second));
    entry->SetZeroConfirmed(MakeGenStepPair(lastGeneration, confirmedStep));

    {
        const ui32 tailLength = lastInGeneration - confirmedStep;
        entry->SetZeroTailSz(tailLength);
        entry->MutableZeroTailBitmask()->Reserve((tailLength + 63) / 64);

        if (tailLength > 0) {
            ui64 mask = 1;
            ui64 value = 0;

            // if confirmed iterator points to already confirmed step - move iterator on next confirmed entry
            if (confirmedStep)
                ++confirmedIterator;

            for (ui32 i = confirmedStep + 1; i <= lastInGeneration; ++i) {
                Y_DEBUG_ABORT_UNLESS(confirmedIterator != end);
                if (confirmedIterator->Id.second == i) {
                    value |= mask;
                    ++confirmedIterator;
                }

                mask = mask << 1;
                if (mask == 0) {
                    entry->MutableZeroTailBitmask()->Add(value);
                    mask = 1;
                    value = 0;
                }
            }

            if (mask != 1)
                entry->MutableZeroTailBitmask()->Add(value);
        }
    }

    if (snapIterator != graph->Entries.begin())
        graph->Entries.erase(graph->Entries.begin(), snapIterator); // erase head of graph

    Graph.Snapshot = snapshot;

    const TLogoBlobID logid(TabletID(), StateStorageInfo.KnownGeneration, 0, 0, 0, 0);
    TVector<TEvTablet::TLogEntryReference> refs;
    Register(CreateTabletReqWriteLog(SelfId(), logid, entry.Release(), refs, TEvBlobStorage::TEvPut::TacticMinLatency, Info.Get()));

    BLOG_D(" TTablet::WriteZeroEntry. logid# " << logid.ToString(), "TSYS01");

    Become(&TThis::StateWriteZeroEntry);
    ReportTabletStateChange(TTabletStateInfo::WriteZeroEntry);
}

void TTablet::StartActivePhase() {
    Graph.NextEntry = 1;
    Graph.MinFollowerUpdate = 1;

    Send(Launcher, new TEvTablet::TEvRestored(TabletID(), StateStorageInfo.KnownGeneration, UserTablet, false));
    Send(UserTablet, new TEvTablet::TEvRestored(TabletID(), StateStorageInfo.KnownGeneration, UserTablet, false));

    Become(&TThis::StateActivePhase);
    ReportTabletStateChange(TTabletStateInfo::Restored);

    StateStorageGuardian = Register(CreateStateStorageTabletGuardian(TabletID(), SelfId(), UserTablet, StateStorageInfo.KnownGeneration));

    // if nowhere to sync - then declare sync done
    TryFinishFollowerSync();
}

void TTablet::TryPumpWaitingForGc() {
    if (WaitingForGcAck.empty())
        return;

    ui32 minConfirmedGcStep = Max<ui32>();
    for (auto &xpair : LeaderInfo) {
        const TLeaderInfo &followerInfo = xpair.second;
        if (followerInfo.SyncState == EFollowerSyncState::Ignore)
            continue;
        minConfirmedGcStep = Min(followerInfo.ConfirmedGCStep, minConfirmedGcStep);
    }

    while (WaitingForGcAck) {
        auto ackFront = WaitingForGcAck.front();
        const ui32 gcStep = ackFront.first;
        if (gcStep > minConfirmedGcStep)
            break;
        const TInstant commitMoment = ackFront.second;
        const TDuration delay = TActivationContext::Now() - commitMoment ;
        Send(UserTablet, new TEvTablet::TEvFollowerGcApplied(TabletID(), StateStorageInfo.KnownGeneration, gcStep, delay));
        WaitingForGcAck.pop_front();
    }
}

void TTablet::TryFinishFollowerSync() {
    if (InitialFollowerSyncDone)
        return;

    // explicit state check is evil, but parallel flag is even more evil. Conceptually correct way is defining dedicated
    // handlers for active/not-active phases (eg by template<bool>) but would lead to code bloat.
    // So for now this check is here
    if (CurrentStateFunc() != &TThis::StateActivePhase)
        return;

    for (const auto &xpair : LeaderInfo) {
        EFollowerSyncState syncState = xpair.second.SyncState;
        if (syncState == EFollowerSyncState::NeedSync || syncState == EFollowerSyncState::Pending)
            return;
    }

    InitialFollowerSyncDone = true;
    Send(UserTablet, new TEvTablet::TEvFollowerSyncComplete());
}

void TTablet::UpdateStateStorageSignature(TEvStateStorage::TEvUpdateSignature::TPtr &ev) {
    const TEvStateStorage::TEvUpdateSignature *msg = ev->Get();
    StateStorageInfo.MergeSignature(msg->Signature.Get(), msg->Sz);
}

void TTablet::HandlePingBoot(TEvTablet::TEvPing::TPtr &ev) {
    // todo: handle wait-boot flag
    NKikimrTabletBase::TEvPing &record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetTabletID() == TabletID());
    Send(ev->Sender, new TEvTablet::TEvPong(TabletID(), TEvTablet::TEvPong::FlagBoot | TEvTablet::TEvPong::FlagLeader));
}

void TTablet::HandlePingFollower(TEvTablet::TEvPing::TPtr &ev) {
    NKikimrTabletBase::TEvPing &record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetTabletID() == TabletID());
    Send(ev->Sender, new TEvTablet::TEvPong(TabletID(), TEvTablet::TEvPong::FlagFollower));
}

void TTablet::HandleStateStorageLeaderResolve(TEvStateStorage::TEvInfo::TPtr &ev) {
    TEvStateStorage::TEvInfo *msg = ev->Get();

    if (msg->SignatureSz) {
        StateStorageInfo.SignatureSz = msg->SignatureSz;
        StateStorageInfo.Signature.Reset(msg->Signature.Release());
    }

    StateStorageInfo.KnownGeneration = msg->CurrentGeneration;
    StateStorageInfo.KnownStep = msg->CurrentStep;

    if (msg->Status == NKikimrProto::OK && msg->CurrentLeader) {
        FollowerInfo.KnownLeaderID = msg->CurrentLeader;
        Send(FollowerInfo.KnownLeaderID,
                new TEvTablet::TEvFollowerAttach(TabletID(), FollowerInfo.FollowerAttempt),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateFollowerSubscribe);
    } else { // something goes weird, try again a bit later
        NextFollowerAttempt();

        TActivationContext::Schedule(TDuration::MilliSeconds(100),
                new IEventHandle(SelfId(), SelfId(),
                    new TEvTabletBase::TEvFollowerRetry(++FollowerInfo.RetryRound),
                    0, FollowerInfo.FollowerAttempt)
        );
    }
}

void TTablet::HandleFollowerRetry(TEvTabletBase::TEvFollowerRetry::TPtr &ev) {
    if (ev->Cookie != FollowerInfo.FollowerAttempt)
        return;

    BootstrapFollower();
}

void TTablet::HandleByFollower(TEvTabletBase::TEvTryBuildFollowerGraph::TPtr &ev) {
    Y_UNUSED(ev);

    BLOG_TRACE("Follower starting to rebuild history", "TSYS02");
    Y_DEBUG_ABORT_UNLESS(!RebuildGraphRequest);
    RebuildGraphRequest = Register(CreateTabletReqRebuildHistoryGraph(SelfId(), Info.Get(), 0, nullptr, ++FollowerInfo.RebuildGraphCookie));

    // todo: tracing? at least as event
}

void TTablet::HandleByFollower(TEvTabletBase::TEvRebuildGraphResult::TPtr &ev) {
    if (ev->Sender != RebuildGraphRequest || ev->Cookie != FollowerInfo.RebuildGraphCookie || UserTablet) {
        BLOG_D("Outdated TEvRebuildGraphResult ignored", "TSYS03");
        return;
    }

    RebuildGraphRequest = TActorId(); // check consistency??
    TEvTabletBase::TEvRebuildGraphResult *msg = ev->Get();
    BLOG_TRACE("Follower received rebuild history result Status# " << msg->Status, "TSYS04");

    switch (msg->Status) {
    case NKikimrProto::OK:
        {
            UserTablet = SetupInfo->Apply(Info.Get(), SelfId());
            Send(UserTablet,
                new TEvTablet::TEvFBoot(TabletID(), FollowerId, 0,
                    Launcher, msg->DependencyGraph.Get(), Info,
                    ResourceProfiles, TxCacheQuota,
                    std::move(msg->GroupReadBytes),
                    std::move(msg->GroupReadOps)));

            Send(Launcher, new TEvTablet::TEvRestored(TabletID(), StateStorageInfo.KnownGeneration, UserTablet, true));
            BLOG_TRACE("SBoot with rebuilt graph", "TSYS05");
        }
        break;
    case NKikimrProto::NODATA: // any not-positive cases ignored and handled by long retry
    default:
        Schedule(OfflineFollowerWaitRetry, new TEvTabletBase::TEvTryBuildFollowerGraph());
        break;
    }
}

bool TTablet::CheckFollowerUpdate(const TActorId &sender, ui32 attempt, ui64 counter) {
    if (sender != FollowerInfo.KnownLeaderID || attempt != FollowerInfo.FollowerAttempt)
        return false;

    if (counter != FollowerInfo.StreamCounter) {
        Send(FollowerInfo.KnownLeaderID, new TEvTablet::TEvFollowerDetach(TabletID(), FollowerInfo.FollowerAttempt));
        NextFollowerAttempt();
        RetryFollowerBootstrapOrWait();
        return false;
    }

    return true;
}

void TTablet::HandleByFollower(TEvents::TEvUndelivered::TPtr &ev) {
    if (ev->Sender == FollowerInfo.KnownLeaderID) {
        NextFollowerAttempt();
        RetryFollowerBootstrapOrWait();
        return;
    }

    if (ev->Sender == UserTablet && ev->Get()->SourceType == TEvTablet::TEvTabletStop::EventType) {
        return HandleStopped();
    }
}

void TTablet::HandleByFollower(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    if (ev->Get()->NodeId != FollowerInfo.KnownLeaderID.NodeId())
        return;

    BLOG_TRACE("Follower got TEvNodeDisconnected NodeId# " << ev->Get()->NodeId, "TSYS06");
    NextFollowerAttempt();
    RetryFollowerBootstrapOrWait();
}

void TTablet::HandleByFollower(TEvTablet::TEvFollowerDisconnect::TPtr &ev) {
    // sent from PassAway of leader
    if (ev->Sender != FollowerInfo.KnownLeaderID)
        return;

    BLOG_TRACE("Follower got TEvFollowerDisconnect Sender# " << ev->Sender, "TSYS07");
    NextFollowerAttempt();
    RetryFollowerBootstrapOrWait();
}

void TTablet::HandleByFollower(TEvTablet::TEvFollowerRefresh::TPtr &ev) {
    const auto &record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetTabletId() == TabletID());
    if (record.GetGeneration() < ExpandGenStepPair(FollowerInfo.EpochGenStep).first) {
        Send(ev->Sender, new TEvTablet::TEvFollowerDetach(TabletID(), Max<ui32>()));
        return;
    }

    NextFollowerAttempt();

    FollowerInfo.KnownLeaderID = ev->Sender;
    Send(FollowerInfo.KnownLeaderID,
        new TEvTablet::TEvFollowerAttach(TabletID(), FollowerInfo.FollowerAttempt),
        IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

    Become(&TThis::StateFollowerSubscribe);
}

void TTablet::HandleByFollower(TEvTablet::TEvFollowerAuxUpdate::TPtr &ev) {
    const auto &record = ev->Get()->Record;
    if (!CheckFollowerUpdate(ev->Sender, record.GetFollowerAttempt(), record.GetStreamCounter()))
        return;

    Y_ABORT_UNLESS(FollowerInfo.StreamCounter != 0);
    Y_ABORT_UNLESS(UserTablet);

    Send(UserTablet, new TEvTablet::TEvFAuxUpdate(record.GetAuxPayload()));

    ++FollowerInfo.StreamCounter;
}

void TTablet::HandleByFollower(TEvTablet::TEvFollowerUpdate::TPtr &ev) {
    const auto &record = ev->Get()->Record;

    BLOG_TRACE("FollowerUpdate attempt: " << record.GetFollowerAttempt() << ":" << record.GetStreamCounter()
        << ", " << record.GetGeneration() << ":" << record.GetStep(), "TSYS08");

    if (!CheckFollowerUpdate(ev->Sender, record.GetFollowerAttempt(), record.GetStreamCounter()))
        return;

    if (FollowerInfo.StreamCounter == 0) {
        FollowerInfo.RetryRound = 0; // reset retry round counter to enable fast sync with leader

        // first event, must be snapshot
        Y_ABORT_UNLESS(record.GetIsSnapshot());

        // update storage info for case of channel history upgrade
        if (record.HasTabletStorageInfo()) {
            Info = TabletStorageInfoFromProto(record.GetTabletStorageInfo());
        }

        // Drop currently running graph rebuild request
        if (RebuildGraphRequest) {
            Send(RebuildGraphRequest, new TEvents::TEvPoisonPill());
            RebuildGraphRequest = TActorId();
        }

        if (!UserTablet) {
            UserTablet = SetupInfo->Apply(Info.Get(), SelfId());
            Send(Launcher, new TEvTablet::TEvRestored(TabletID(), StateStorageInfo.KnownGeneration, UserTablet, true));
        }

        if (!FollowerStStGuardian)
            FollowerStStGuardian = Register(CreateStateStorageFollowerGuardian(TabletID(), SelfId()));

        FollowerInfo.EpochGenStep = MakeGenStepPair(record.GetGeneration(), record.GetStep());

        Send(UserTablet,
                 new TEvTablet::TEvFBoot(TabletID(), FollowerId, record.GetGeneration(),
                                         Launcher, *ev->Get(), Info,
                                         ResourceProfiles, TxCacheQuota));

        BLOG_TRACE("SBoot attempt: " << FollowerInfo.FollowerAttempt
            << ", " << record.GetGeneration() << ":" << record.GetStep(), "TSYS09");

    } else {
        Y_ABORT_UNLESS(UserTablet);
        Send(UserTablet, new TEvTablet::TEvFUpdate(*ev->Get()));

        BLOG_TRACE("SUpdate attempt: " << FollowerInfo.FollowerAttempt
            << ", " << record.GetGeneration() << ":" << record.GetStep(), "TSYS10");
    }

    ++FollowerInfo.StreamCounter;
}

void TTablet::HandleByFollower(TEvTablet::TEvPromoteToLeader::TPtr &ev) {
    TEvTablet::TEvPromoteToLeader *msg = ev->Get();
    BLOG_TRACE("Follower got TEvPromoteToLeader Sender# " << ev->Sender << " Generation# " << msg->SuggestedGeneration, "TSYS11");

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnFollowerPromoteToLeader>(
            msg->SuggestedGeneration
            , FollowerInfo.KnownLeaderID
            , FollowerStStGuardian));
    }

    Info = msg->TabletStorageInfo;

    // detach from leader
    if (FollowerInfo.KnownLeaderID) {
        Send(FollowerInfo.KnownLeaderID, new TEvTablet::TEvFollowerDetach(TabletID(), FollowerInfo.FollowerAttempt));
        NextFollowerAttempt();
    }

    if (FollowerStStGuardian) {
        Send(FollowerStStGuardian, new TEvents::TEvPoisonPill());
        FollowerStStGuardian = TActorId();
    }

    if (RebuildGraphRequest) {
        Send(RebuildGraphRequest, new TEvents::TEvPoisonPill());
        RebuildGraphRequest = TActorId();
    }

    // setup start info
    SuggestedGeneration = msg->SuggestedGeneration;
    Leader = true;
    FollowerId = 0;
    Bootstrap();
}

void TTablet::HandleByFollower(TEvTablet::TEvFGcAck::TPtr &ev) {
    const TEvTablet::TEvFGcAck *msg = ev->Get();

    if (FollowerInfo.EpochGenStep <= MakeGenStepPair(msg->Generation, msg->Step))
        Send(FollowerInfo.KnownLeaderID, new TEvTablet::TEvFollowerGcAck(TabletID(), FollowerInfo.FollowerAttempt, msg->Generation, msg->Step));
}

TMap<TActorId, TTablet::TLeaderInfo>::iterator
TTablet::EraseFollowerInfo(TMap<TActorId, TLeaderInfo>::iterator followerIt) {
    const ui32 followerNode = followerIt->first.NodeId();

    auto retIt = LeaderInfo.erase(followerIt);

    if (UserTablet) {
        Send(UserTablet, new TEvTablet::TEvFollowerDetached(LeaderInfo.size()));
    }

    TryPumpWaitingForGc();
    TryFinishFollowerSync();

    if (followerNode != SelfId().NodeId()) {
        bool noMoreFollowersOnNode = true;
        for (const auto &xpair : LeaderInfo) {
            if (xpair.first.NodeId() == followerNode) {
                noMoreFollowersOnNode = false;
                break;
            }
        }

        if (noMoreFollowersOnNode)
            Send(TActivationContext::InterconnectProxy(followerNode), new TEvents::TEvUnsubscribe);
    }

    return retIt;
}

TMap<TActorId, TTablet::TLeaderInfo>::iterator TTablet::HandleFollowerConnectionProblem(TMap<TActorId, TLeaderInfo>::iterator followerIt) {
    TLeaderInfo &followerInfo = followerIt->second;
    bool shouldEraseEntry = false;

    switch (followerInfo.SyncState) {
    case EFollowerSyncState::Pending:
    case EFollowerSyncState::Active:
        followerInfo.SyncState = EFollowerSyncState::NeedSync;
        followerInfo.SyncAttempt = 0;
        BLOG_D("HandleFollowerConnectionProblem " << followerIt->first << " moved to NeedSync state", "TSYS12");
        break;
    case EFollowerSyncState::NeedSync:
        if (!followerInfo.SyncCookieHolder && followerInfo.SyncAttempt > 3) {
            shouldEraseEntry = !followerInfo.PresentInList;
            followerInfo.SyncState = EFollowerSyncState::Ignore;
            BLOG_D("HandleFollowerConnectionProblem " << followerIt->first << " moved to Ignore state, shouldEraseEntry# " << shouldEraseEntry, "TSYS13");
        } else {
            BLOG_D("HandleFollowerConnectionProblem " << followerIt->first << " kept in NeedSync state", "TSYS14");
        }
        break;
    case EFollowerSyncState::Ignore:
        BLOG_D("HandleFollowerConnectionProblem " << followerIt->first << " kept in Ignore state", "TSYS15");
        break;
    }

    if (shouldEraseEntry) {
        followerIt = EraseFollowerInfo(followerIt);
    } else {
        TrySyncToFollower(followerIt);
        ++followerIt;
    }

    TryPumpWaitingForGc();
    TryFinishFollowerSync();

    return followerIt;
}

void TTablet::TrySyncToFollower(TMap<TActorId, TLeaderInfo>::iterator followerIt) {
    TLeaderInfo &followerInfo = followerIt->second;
    if (followerInfo.SyncCookieHolder) // already awaiting
        return;

    TDuration delay = TDuration::MilliSeconds(250 + Min<ui32>(3, followerInfo.SyncAttempt) * 250);
    followerInfo.SyncCookieHolder.Reset(new TSchedulerCookieHolder(ISchedulerCookie::Make3Way()));
    auto *schedCookie = followerInfo.SyncCookieHolder->Get();
    Schedule(delay, new TEvTabletBase::TEvTrySyncFollower(followerIt->first, schedCookie), schedCookie);
}

void TTablet::DoSyncToFollower(TMap<TActorId, TLeaderInfo>::iterator followerIt) {
    TLeaderInfo &followerInfo = followerIt->second;

    Send(followerIt->first, new TEvTablet::TEvFollowerRefresh(TabletID(), StateStorageInfo.KnownGeneration), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

    ++followerInfo.SyncAttempt;
    followerInfo.LastSyncAttempt = TActivationContext::Now();
}


void TTablet::HandleByLeader(TEvents::TEvUndelivered::TPtr &ev) {
    auto followerIt = LeaderInfo.find(ev->Sender);
    if (followerIt != LeaderInfo.end()) {
        HandleFollowerConnectionProblem(followerIt);
        return;
    }

    if (ev->Sender == UserTablet && ev->Get()->SourceType == TEvTablet::TEvTabletStop::EventType) {
        return HandleStopped();
    }
}

void TTablet::HandleByLeader(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    // typical number if followers on one node is one. so we don't bother with batched check for unsubscribe
    // and we still need to unsubscribe 'cuz of possible races
    const TEvInterconnect::TEvNodeDisconnected *msg = ev->Get();
    for (auto it = LeaderInfo.begin(); it != LeaderInfo.end(); ) {
        if (it->first.NodeId() == msg->NodeId)
            it = HandleFollowerConnectionProblem(it);
        else
            ++it;
    }
}

void TTablet::HandleByLeader(TEvTablet::TEvFollowerListRefresh::TPtr &ev) {
    auto *msg = ev->Get();
    TMap<TActorId, TLeaderInfo>::iterator infoIt = LeaderInfo.begin();

    for (auto it = msg->FollowerList.begin(), end = msg->FollowerList.end(); it != end; ++it) {
        while (infoIt != LeaderInfo.end() && infoIt->first < *it) {
            if (infoIt->second.SyncState == EFollowerSyncState::Ignore) {
                infoIt = EraseFollowerInfo(infoIt);
            } else {
                infoIt->second.PresentInList = false;
                ++infoIt;
            }
        }

        if (infoIt != LeaderInfo.end() && infoIt->first == *it)
            infoIt->second.PresentInList = true;
    }

    while (infoIt != LeaderInfo.end()) {
        if (infoIt->second.SyncState == EFollowerSyncState::Ignore) {
            infoIt = EraseFollowerInfo(infoIt);
        } else {
            infoIt->second.PresentInList = false;
            ++infoIt;
        }
    }
}

void TTablet::HandleByLeader(TEvTabletBase::TEvTrySyncFollower::TPtr &ev) {
    TEvTabletBase::TEvTrySyncFollower *msg = ev->Get();
    if (msg->CookieHolder.DetachEvent()) {
        auto it = LeaderInfo.find(msg->FollowerId);
        if (it == LeaderInfo.end())
            return;
        it->second.SyncCookieHolder.Destroy();
        DoSyncToFollower(it);
    }
}

void TTablet::HandleByLeader(TEvTablet::TEvFollowerRefresh::TPtr &ev) {
    // could be received by promoted leader
    Send(ev->Sender, new TEvTablet::TEvFollowerDetach(TabletID(), Max<ui32>()));
}

void TTablet::HandleByLeader(TEvTablet::TEvFollowerDetach::TPtr &ev) {
    // could be received by leader from promoted leader, just cleanup and wait for normal termination
    const auto &record = ev->Get()->Record;
    auto followerIt = LeaderInfo.find(ev->Sender);
    if (followerIt == LeaderInfo.end() || followerIt->second.FollowerAttempt != record.GetFollowerAttempt())
        return;
    EraseFollowerInfo(followerIt);
}

void TTablet::HandleByLeader(TEvTablet::TEvFollowerAttach::TPtr &ev) {
    const TActorId followerId = ev->Sender;
    const auto &record = ev->Get()->Record;

    auto followerIt = LeaderInfo.find(followerId);
    if (followerIt != LeaderInfo.end()) {
        // attaching follower known
        Y_ABORT_UNLESS(followerIt->second.FollowerAttempt < record.GetFollowerAttempt() || followerIt->second.FollowerAttempt == Max<ui32>());

        followerIt->second.SyncState = EFollowerSyncState::Pending; // keep ConfirmedGCStep and FromList
    } else {
        if (LeaderInfo.empty()) {
            // Consider sending follower updates starting with the next commit
            Graph.MinFollowerUpdate = Graph.NextEntry;
        }
        auto followerItPair = LeaderInfo.insert(decltype(LeaderInfo)::value_type(ev->Sender, TLeaderInfo(EFollowerSyncState::Pending)));
        Y_ABORT_UNLESS(followerItPair.second);

        followerIt = followerItPair.first;
    }

    TLeaderInfo &followerInfo = followerIt->second;

    followerInfo.FollowerAttempt = record.GetFollowerAttempt();
    followerInfo.StreamCounter = 0;
    followerInfo.SyncAttempt = 0;
    followerInfo.SyncCookieHolder.Destroy();

    if (UserTablet)
        Send(UserTablet, new TEvTablet::TEvNewFollowerAttached(LeaderInfo.size()));
}

void TTablet::HandleByLeader(TEvTablet::TEvFollowerGcAck::TPtr &ev) {
    const TActorId followerId = ev->Sender;
    TLeaderInfo *followerInfo = LeaderInfo.FindPtr(followerId);
    if (!followerInfo || followerInfo->SyncState != EFollowerSyncState::Active)
        return;

    const auto &record = ev->Get()->Record;
    if (record.GetGeneration() != StateStorageInfo.KnownGeneration || followerInfo->FollowerAttempt != record.GetFollowerAttempt())
        return;

    const ui32 step = record.GetStep();
    Y_DEBUG_ABORT_UNLESS(followerInfo->ConfirmedGCStep < step);
    followerInfo->ConfirmedGCStep = Max(step, followerInfo->ConfirmedGCStep);

    TryPumpWaitingForGc();
}

void TTablet::HandleStateStorageInfoResolve(TEvStateStorage::TEvInfo::TPtr &ev) {
    TEvStateStorage::TEvInfo *msg = ev->Get();

    if (msg->SignatureSz) {
        StateStorageInfo.SignatureSz = msg->SignatureSz;
        StateStorageInfo.Signature.Reset(msg->Signature.Release());
    }

    StateStorageInfo.KnownGeneration = msg->CurrentGeneration;
    StateStorageInfo.KnownStep = msg->CurrentStep;

    if (SuggestedGeneration && StateStorageInfo.KnownGeneration >= SuggestedGeneration)
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootSuggestOutdated);

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(
            MakeHolder<NTracing::TOnHandleStateStorageInfoResolve>(
                StateStorageInfo.KnownGeneration
                , StateStorageInfo.KnownStep
                , StateStorageInfo.SignatureSz));
    }

    switch (msg->Status) {
    case NKikimrProto::OK:
        { // enough replicas replied and we have state info now
            Y_ABORT_UNLESS(msg->TabletID == TabletID());

            if (msg->Locked) {
                // tablet already locked, check lock threshold or die
                const ui64 lockedForThreshold = 2 * 1000 * 1000;
                if (msg->LockedFor > lockedForThreshold) {
                    return LockedInitializationPath();
                } else {
                    return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootLocked);
                }
            }

            if (SuggestedGeneration && SuggestedGeneration <= StateStorageInfo.KnownGeneration) {
                return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootRace);
            }

            if (!msg->CurrentLeader || !msg->CurrentGeneration) {
                return LockedInitializationPath();
            }

            BLOG_D("HandleStateStorageInfoResolve, KnownGeneration: " << msg->CurrentGeneration << " Promote", "TSYS16");

            return PromoteToCandidate(0);
        }
    case NKikimrProto::ERROR:
    case NKikimrProto::RACE:
    case NKikimrProto::TIMEOUT:
        return LockedInitializationPath();
    default:
        Y_ABORT();
    }
}

void TTablet::HandleStateStorageInfoLock(TEvStateStorage::TEvInfo::TPtr &ev) {
    const TEvStateStorage::TEvInfo *msg = ev->Get();

    StateStorageInfo.MergeSignature(msg->Signature.Get(), msg->SignatureSz);

    switch (msg->Status) {
    case NKikimrProto::OK:
        { // ok, we had successfully locked state storage for synthetic generation, now find actual one
            StateStorageInfo.Update(msg);

            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TOnHandleStateStorageInfoLock>(
                    StateStorageInfo.KnownGeneration
                    , StateStorageInfo.KnownStep
                    , StateStorageInfo.SignatureSz));
            }

            Register(CreateTabletFindLastEntry(SelfId(), false, Info.Get(), 0, Leader));
            Become(&TThis::StateDiscover);
            ReportTabletStateChange(TTabletStateInfo::Discover);
        }
        return;
    case NKikimrProto::ERROR:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootSSError);
    case NKikimrProto::TIMEOUT:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootSSTimeout);
    case NKikimrProto::RACE:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootRace);
    default:
        Y_ABORT();
    }
}

void TTablet::HandleStateStorageInfoUpgrade(TEvStateStorage::TEvInfo::TPtr &ev) {
    const TEvStateStorage::TEvInfo *msg = ev->Get();

    StateStorageInfo.MergeSignature(msg->Signature.Get(), msg->SignatureSz);

    switch (msg->Status){
    case NKikimrProto::OK:
        { // ok, we marked ourselves as generation owner
            NeedCleanupOnLockedPath = false;
            StateStorageInfo.Update(msg);
            for (const auto &xpair : msg->Followers) {
                if (xpair.first == SelfId())
                    continue;
                if (LeaderInfo.empty()) {
                    // Consider sending follower updates starting with the next commit
                    Graph.MinFollowerUpdate = Graph.NextEntry;
                }
                auto itPair = LeaderInfo.insert(decltype(LeaderInfo)::value_type(xpair.first, TLeaderInfo(EFollowerSyncState::NeedSync)));
                // some followers could be already present by active TEvFollowerAttach
                if (itPair.second)
                    TrySyncToFollower(itPair.first);
            }

            return TabletBlockBlobStorage();
        }
    case NKikimrProto::ERROR:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootSSError);
    case NKikimrProto::TIMEOUT:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootSSTimeout);
    case NKikimrProto::RACE:
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootRace);
    default:
        Y_ABORT();
    }
}

void TTablet::HandleFindLatestLogEntry(TEvTabletBase::TEvFindLatestLogEntryResult::TPtr &ev) {
    TEvTabletBase::TEvFindLatestLogEntryResult *msg = ev->Get();
    switch (msg->Status) {
    case NKikimrProto::OK:
        {
            DiscoveredLastBlocked = msg->BlockedGeneration;
            if (msg->Latest.Generation() > msg->BlockedGeneration + 1) {
                BLOG_ERROR("HandleFindLatestLogEntry inconsistency. LatestGeneration: "
                    <<  msg->Latest.Generation() << ", blocked: " << msg->BlockedGeneration, "TSYS17");
            }

            const ui32 latestKnownGeneration = Max(msg->Latest.Generation(), msg->BlockedGeneration);
            BLOG_D("HandleFindLatestLogEntry, latestKnownGeneration: " << latestKnownGeneration << " Promote", "TSYS18");

            return PromoteToCandidate(latestKnownGeneration);
        }
    case NKikimrProto::NODATA:
        BLOG_D("HandleFindLatestLogEntry, NODATA Promote", "TSYS19");

        DiscoveredLastBlocked = 0;
        return PromoteToCandidate(0);
    default:
        {
            BLOG_ERROR("HandleFindLatestLogEntry, msg->Status: " << NKikimrProto::EReplyStatus_Name(msg->Status), "TSYS20");
            return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootBSError, msg->ErrorReason);
        }
    }
}

void TTablet::HandleBlockBlobStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr &ev) {
    TEvTabletBase::TEvBlockBlobStorageResult *msg = ev->Get();
    switch (msg->Status) {
    case NKikimrProto::OK:
        return TabletRebuildGraph();
    default:
        {
            BLOG_ERROR("HandleBlockBlobStorageResult, msg->Status: "
                    << NKikimrProto::EReplyStatus_Name(msg->Status)
                    << (DiscoveredLastBlocked == Max<ui32>()
                        ? ", not discovered"
                        : Sprintf(", discovered gen was: %u", DiscoveredLastBlocked).c_str()), "TSYS21");

            return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootBSError, msg->ErrorReason);
        }
    }
}

void TTablet::HandleRebuildGraphResult(TEvTabletBase::TEvRebuildGraphResult::TPtr &ev) {
    if (ev->Cookie != 0) // remains from follower past
        return;

    RebuildGraphRequest = TActorId(); // check consistency??

    TEvTabletBase::TEvRebuildGraphResult *msg = ev->Get();
    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnRebuildGraphResult>(msg->Trace.Get()));
    }
    TIntrusivePtr<TEvTablet::TDependencyGraph> graph;
    switch (msg->Status) {
    case NKikimrProto::OK:
        graph = msg->DependencyGraph;
        break;
    case NKikimrProto::NODATA:
        graph = new TEvTablet::TDependencyGraph(std::pair<ui32, ui32>(0, 0));
        break;
    default:
        break;
    }
    switch (msg->Status) {
    case NKikimrProto::OK:
    case NKikimrProto::NODATA:
        WriteZeroEntry(graph.Get());
        Send(UserTablet,
                 new TEvTablet::TEvBoot(TabletID(), StateStorageInfo.KnownGeneration,
                                        graph.Get(), Launcher, Info, ResourceProfiles,
                                        TxCacheQuota,
                                        std::move(msg->GroupReadBytes),
                                        std::move(msg->GroupReadOps)));
        return;
    default:
        {
            BLOG_ERROR("HandleRebuildGraphResult, msg->Status: " << NKikimrProto::EReplyStatus_Name(msg->Status), "TSYS22");
            return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootBSError, msg->ErrorReason);
        }
    }
}

void TTablet::HandleWriteZeroEntry(TEvTabletBase::TEvWriteLogResult::TPtr &ev) {
    TEvTabletBase::TEvWriteLogResult *msg = ev->Get();
    switch (msg->Status) {
    case NKikimrProto::OK:
        return StartActivePhase();
    default:
        {
            BLOG_ERROR("HandleWriteZeroEntry, msg->Status: " << NKikimrProto::EReplyStatus_Name(msg->Status), "TSYS23");
            ReassignYellowChannels(std::move(msg->YellowMoveChannels));
            return CancelTablet(TEvTablet::TEvTabletDead::ReasonBootBSError, msg->ErrorReason); // TODO: detect 'need channel reconfiguration' case
        }
    }
}

void TTablet::Handle(TEvTablet::TEvPing::TPtr &ev) {
    NKikimrTabletBase::TEvPing &record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetTabletID() == TabletID());
    Send(ev->Sender, new TEvTablet::TEvPong(TabletID(), TEvTablet::TEvPong::FlagLeader));
}

void TTablet::HandleByLeader(TEvTablet::TEvTabletActive::TPtr &ev) {
    Y_UNUSED(ev);
    ReportTabletStateChange(TTabletStateInfo::Active);
    Send(Launcher, new TEvTablet::TEvReady(TabletID(), StateStorageInfo.KnownGeneration, UserTablet));
    ActivateTime = AppData()->TimeProvider->Now();
    BLOG_I("Active! Generation: " << StateStorageInfo.KnownGeneration
            <<  ", Type: " << TTabletTypes::TypeToStr((TTabletTypes::EType)Info->TabletType)
            <<  " started in " << (ActivateTime-BoostrapTime).MilliSeconds() << "msec", "TSYS24");

    PipeConnectAcceptor->Activate(SelfId(), UserTablet, true, StateStorageInfo.KnownGeneration);
}

void TTablet::HandleByFollower(TEvTablet::TEvTabletActive::TPtr &ev) {
    Y_UNUSED(ev);
    BLOG_D("Follower TabletStateActive", "TSYS25");

    PipeConnectAcceptor->Activate(SelfId(), UserTablet, false, StateStorageInfo.KnownGeneration);

    Send(FollowerStStGuardian, new TEvTablet::TEvFollowerUpdateState(false, SelfId(), UserTablet));
    ReportTabletStateChange(TTabletStateInfo::Active);
}

TTablet::TLogEntry* TTablet::MakeLogEntry(TEvTablet::TCommitInfo &commitInfo, NKikimrTabletBase::TTabletLogEntry *commitEv) {
    Y_ABORT_UNLESS(commitInfo.TabletID == TabletID() && commitInfo.Generation == StateStorageInfo.KnownGeneration && commitInfo.Step == Graph.NextEntry,
        "commitInfo.TabletID=%ld, tablet=%ld, commitInfo.Generation=%d, KnownGeneration=%d, commitInfo.Step=%d, nextEntry=%d",
        commitInfo.TabletID, TabletID(), commitInfo.Generation, StateStorageInfo.KnownGeneration, commitInfo.Step, Graph.NextEntry);

    const ui32 step = Graph.NextEntry++;

    TLogEntry *entry = new TLogEntry(step, Graph.Confirmed, 0);
    Graph.Queue.push_back(std::unique_ptr<TLogEntry>(entry));
    Graph.Index[step] = entry;
    entry->IsSnapshot = commitInfo.IsSnapshot || commitInfo.IsTotalSnapshot;
    entry->IsTotalSnapshot = commitInfo.IsTotalSnapshot;
    entry->Source = TActorId();

    for (ui32 dependsOn : commitInfo.DependsOn) {
        TGraph::TIndex::iterator it = Graph.Index.find(dependsOn);
        if (it != Graph.Index.end()) {
            if (commitEv)
                commitEv->AddDependsOn(dependsOn);
            it->second->Dependent.push_back(step);
            ++entry->DependenciesLeft;
        }
    }

    return entry;
}

TTablet::TLogEntry* TTablet::FindLogEntry(TEvTablet::TCommitInfo &commitInfo, NKikimrTabletBase::TTabletLogEntry &commitEv) {
    TLogEntry **entryPtr = Graph.Index.FindPtr(commitInfo.Step);
    if (!entryPtr)
        return nullptr;
    TLogEntry *entry = *entryPtr;

    if (entry->IsSnapshot != commitInfo.IsSnapshot || entry->IsTotalSnapshot != commitInfo.IsTotalSnapshot)
        return nullptr;

    for (ui32 dependsOn : commitInfo.DependsOn) {
        TGraph::TIndex::iterator it = Graph.Index.find(dependsOn);
        if (it != Graph.Index.end())
            commitEv.AddDependsOn(dependsOn);
    }

    if (commitEv.DependsOnSize() != entry->DependenciesLeft)
        return nullptr;

    return entry;
}

void TTablet::Handle(TEvTablet::TEvPreCommit::TPtr &ev) {
    TEvTablet::TEvPreCommit *msg = ev->Get();
    MakeLogEntry(*msg, nullptr);
}

void TTablet::Handle(TEvTablet::TEvAux::TPtr &ev) {
    TString& auxUpdate = ev->Get()->FollowerAux;

    if (!Graph.Queue.empty()) {
        Graph.Queue.back()->FollowerAuxUpdates.emplace_back(std::move(auxUpdate));
    } else {
        SpreadFollowerAuxUpdate(auxUpdate);
    }
}

void TTablet::Handle(TEvTablet::TEvCommit::TPtr &ev) {
    if (Graph.StepsInFlight >= MaxStepsInFlight || Graph.BytesInFlight >= MaxBytesInFlight) {
        // Delay commit handling until inflight goes down
        Graph.DelayCommitQueue.push_back(std::move(ev));
        return;
    }

    Y_ABORT_UNLESS(Graph.DelayCommitQueue.empty());
    HandleNext(ev);
}

bool TTablet::HandleNext(TEvTablet::TEvCommit::TPtr &ev) {
    TEvTablet::TEvCommit *msg = ev->Get();

    std::unique_ptr<NKikimrTabletBase::TTabletLogEntry> x(new NKikimrTabletBase::TTabletLogEntry());

    TLogEntry *entry = msg->PreCommited ? FindLogEntry(*msg, *x) : MakeLogEntry(*msg, x.get());

    if (entry == nullptr) {
        CancelTablet(TEvTablet::TEvTabletDead::ReasonInconsistentCommit);
        return false;
    }

    entry->Source = ev->Sender;
    entry->SourceCookie = ev->Cookie;
    entry->WaitFollowerGcAck = msg->WaitFollowerGcAck;

    x->SetSnapshot(MakeGenStepPair(Graph.Snapshot.first, Graph.Snapshot.second));
    x->SetConfirmed(Graph.Confirmed);

    const bool saveFollowerUpdate = !LeaderInfo.empty();
    if (saveFollowerUpdate)
        entry->FollowerUpdate.Reset(new TFollowerUpdate());

    if (entry->IsSnapshot)
        x->SetIsSnapshot(true);
    if (entry->IsTotalSnapshot)
        x->SetIsTotalSnapshot(true);

    x->MutableReferences()->Reserve((i32)(msg->ExternalReferences.size() + msg->References.size()));

    if (saveFollowerUpdate)
        entry->FollowerUpdate->References.reserve(msg->References.size());

    for (TVector<TLogoBlobID>::const_iterator it = msg->ExternalReferences.begin(), end = msg->ExternalReferences.end(); it != end; ++it)
        LogoBlobIDFromLogoBlobID(*it, x->AddReferences());

    for (TVector<TEvTablet::TLogEntryReference>::const_iterator it = msg->References.begin(), end = msg->References.end(); it != end; ++it) {
        const TLogoBlobID &id = it->Id;
        Y_ABORT_UNLESS(id.TabletID() == TabletID() && id.Generation() == StateStorageInfo.KnownGeneration);
        LogoBlobIDFromLogoBlobID(id, x->AddReferences());

        if (saveFollowerUpdate)
            entry->FollowerUpdate->References.push_back(std::make_pair(it->Id, it->Buffer));
    }

    if (!msg->GcDiscovered.empty()) {
        x->MutableGcDiscovered()->Reserve((i32)msg->GcDiscovered.size());
        for (auto &gcx : msg->GcDiscovered)
            LogoBlobIDFromLogoBlobID(gcx, x->AddGcDiscovered());

        if (saveFollowerUpdate)
            entry->FollowerUpdate->GcDiscovered.swap(msg->GcDiscovered);
    }

    if (!msg->GcLeft.empty()) {
        x->MutableGcLeft()->Reserve((i32)msg->GcLeft.size());
        for (auto &gcx : msg->GcLeft)
            LogoBlobIDFromLogoBlobID(gcx, x->AddGcLeft());

        if (saveFollowerUpdate)
            entry->FollowerUpdate->GcLeft.swap(msg->GcLeft);
    }

    if (msg->EmbeddedLogBody) {
        Y_ABORT_UNLESS(x->ReferencesSize() == 0);
        x->SetEmbeddedLogBody(msg->EmbeddedLogBody);

        if (saveFollowerUpdate)
           entry->FollowerUpdate->Body = msg->EmbeddedLogBody;
    }

    if (!msg->EmbeddedMetadata.empty()) {
        auto *m = x->MutableEmbeddedMetadata();
        m->Reserve(msg->EmbeddedMetadata.size());
        for (const auto &meta : msg->EmbeddedMetadata) {
            auto *p = m->Add();
            p->SetKey(meta.Key);
            p->SetData(meta.Data);
        }
    }

    if (saveFollowerUpdate && msg->FollowerAux)
        entry->FollowerUpdate->AuxPayload = msg->FollowerAux;

    entry->ByteSize = x->ByteSizeLong();
    for (const auto& ref : msg->References) {
        entry->ByteSize += ref.Buffer.size();
    }

    if (Y_UNLIKELY(BlobStorageStatus != NKikimrProto::OK)) {
        // Ignore commits that happen after we detect blobstorage problems
        return true;
    }

    const TLogoBlobID logid(TabletID(), StateStorageInfo.KnownGeneration, entry->Step, 0, 0, 0);

    entry->StateStorageConfirmed = true; // todo: do real query against state-storage (optionally?)
    entry->Task = Register(
        CreateTabletReqWriteLog(SelfId(), logid, x.release(), msg->References, msg->CommitTactic, Info.Get(), std::move(ev->TraceId))
    );

    Graph.StepsInFlight += 1;
    Graph.BytesInFlight += entry->ByteSize;
    return true;
}

void TTablet::CheckEntry(TGraph::TIndex::iterator it) {
    ui32 step = it->first;
    TLogEntry *entry = it->second;

    if (!entry->BlobStorageConfirmed || !entry->StateStorageConfirmed || entry->DependenciesLeft > 0)
        return;

    TStackVec<ui32> cleanupQueue;
    do {
        while (!entry && cleanupQueue) {
            it = Graph.Index.find(cleanupQueue.back());
            cleanupQueue.pop_back();
            Y_ABORT_UNLESS(it != Graph.Index.end());
            TLogEntry *ex = it->second;
            if (--ex->DependenciesLeft == 0 && ex->StateStorageConfirmed && ex->BlobStorageConfirmed) {
                step = it->first;
                entry = ex;
            }
        }

        while (entry) {
            Graph.Index.erase(it);
            entry->Commited = true;
            entry->CommitedMoment = TActivationContext::Now();
            Send(entry->Source,
                new TEvTablet::TEvCommitResult(
                    NKikimrProto::OK,
                    TabletID(),
                    StateStorageInfo.KnownGeneration,
                    step,
                    entry->ConfirmedOnSend,
                    std::move(entry->YellowMoveChannels),
                    std::move(entry->YellowStopChannels),
                    std::move(entry->GroupWrittenBytes),
                    std::move(entry->GroupWrittenOps)),
                0, entry->SourceCookie);

            const auto &dependent = entry->Dependent;
            entry = nullptr;
            step = 0;

            for (ui32 i : dependent) {
                if (entry)
                    cleanupQueue.push_back(i);
                else {
                    it = Graph.Index.find(i);
                    Y_ABORT_UNLESS(it != Graph.Index.end());
                    TLogEntry *ex = it->second;
                    if (--ex->DependenciesLeft == 0 && ex->StateStorageConfirmed && ex->BlobStorageConfirmed) {
                        step = it->first;
                        entry = ex;
                    }
                }
            }
        }
    } while (cleanupQueue);
}

void TTablet::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
    Y_ABORT_UNLESS(GcInFly > 0);
    --GcInFly;

    TEvBlobStorage::TEvCollectGarbageResult *msg = ev->Get();

    switch (msg->Status) {
    case NKikimrProto::RACE:
    case NKikimrProto::BLOCKED:
    case NKikimrProto::NO_GROUP:
        // We want to stop after current graph is committed
        if (BlobStorageStatus == NKikimrProto::OK) {
            BlobStorageStatus = msg->Status;
            BlobStorageErrorStep = Graph.NextEntry;
            BlobStorageErrorReason = std::move(msg->ErrorReason);
        }
        break;
    case NKikimrProto::OK:
    default: // silently ignore unrecognized errors (assume temporary)
        if (GcInFly == 0 && GcNextStep != 0) {
            GcLogChannel(std::exchange(GcNextStep, 0));
        }
        return;
    }

    CheckBlobStorageError();
}

void TTablet::GcLogChannel(ui32 step) {
    const ui64 tabletid = TabletID();
    const ui32 gen = StateStorageInfo.KnownGeneration;

    if (GcInFly != 0 || Graph.SyncCommit.SyncStep != 0 && Graph.SyncCommit.SyncStep <= step) {
        if (GcInFlyStep < step) {
            BLOG_D("GcCollect 0 channel postponed, tablet:gen:step => " << gen << ":" << step, "TSYS26");
            GcNextStep = step;
            return;
        }
        BLOG_D("GcCollect 0 channel skipped, tablet:gen:step => " << gen << ":" << step, "TSYS27");
        return;
    }

    BLOG_D("GcCollect 0 channel, tablet:gen:step => " << gen << ":" << step, "TSYS28");

    const TTabletChannelInfo *channelInfo = Info->ChannelInfo(0);
    if (GcCounter == 0) {
        TSet<ui32> alreadySent;
        for (const auto &x : channelInfo->History) {
            const ui32 groupId = x.GroupID;
            if (!alreadySent.insert(groupId).second)
                continue;
            ++GcInFly;
            SendToBSProxy(SelfId(), groupId,
                new TEvBlobStorage::TEvCollectGarbage(
                    tabletid, gen, ++GcCounter, 0,
                    true,
                    gen, step,
                    nullptr, nullptr, TInstant::Max(),
                    false
                )
            );
        }
    } else {
        ++GcInFly;
        SendToBSProxy(SelfId(), channelInfo->LatestEntry()->GroupID,
            new TEvBlobStorage::TEvCollectGarbage(
                tabletid, gen, ++GcCounter, 0,
                true,
                gen, step,
                nullptr, nullptr, TInstant::Max(),
                false
                )
            );
    }
    GcInFlyStep = step;
    GcNextStep = 0;
}

void TTablet::SpreadFollowerAuxUpdate(const TString& auxUpdate) {
    for (auto &xpair : LeaderInfo) {
        SendFollowerAuxUpdate(xpair.second, xpair.first, auxUpdate);
    }
}

void TTablet::SendFollowerAuxUpdate(TLeaderInfo& info, const TActorId& follower, const TString& auxUpdate) {
    if (info.FollowerAttempt == Max<ui32>())
        return;
    if (info.StreamCounter == 0)
        return;

    const ui64 tabletId = TabletID();
    auto notify = MakeHolder<TEvTablet::TEvFollowerAuxUpdate>(tabletId, info.FollowerAttempt, info.StreamCounter);
    notify->Record.SetAuxPayload(auxUpdate);

    Send(follower, notify.Release(), 0, IEventHandle::FlagTrackDelivery);
    ++info.StreamCounter;
}

bool TTablet::ProgressCommitQueue() {
    const ui64 tabletId = TabletID();
    while (!Graph.Queue.empty()) {
        TLogEntry *entry = Graph.Queue.front().get();
        if (!entry->Commited)
            break;

        const ui32 step = entry->Step;

        if (entry->IsSnapshot) {
            Graph.Snapshot = std::pair<ui32, ui32>(StateStorageInfo.KnownGeneration, step);
            GcLogChannel(entry->ConfirmedOnSend);
        }

        if (entry->FollowerUpdate && LeaderInfo && step >= Graph.MinFollowerUpdate) {
            Graph.PostponedFollowerUpdates.emplace_back(std::move(Graph.Queue.front()));
        } else if (entry->WaitFollowerGcAck) {
            Send(UserTablet, new TEvTablet::TEvFollowerGcApplied(tabletId, StateStorageInfo.KnownGeneration, step, TDuration::Max()));
        }

        Graph.Confirmed = step;
        Graph.Queue.pop_front();
    }

    if (CheckBlobStorageError()) {
        return false;
    }

    ProgressFollowerQueue();
    TryFinishFollowerSync();
    return true;
}

void TTablet::ProgressFollowerQueue() {
    const ui32 goodUntil = LeaderInfo ? Graph.ConfirmedCommited : Max<ui32>();

    while (!Graph.PostponedFollowerUpdates.empty()) {
        TLogEntry *entry = Graph.PostponedFollowerUpdates.front().get();
        const ui32 step = entry->Step;
        if (step > goodUntil)
            break;

        auto *sup = entry->FollowerUpdate.Get();

        bool needWaitForFollowerGcAck = false;
        for (auto &xpair : LeaderInfo) {
            if (step < Graph.MinFollowerUpdate) {
                // We cannot be sure follower updates before LeaderInfo became
                // non-empty are contiguous and without any holes. We need
                // to ignore them, as if they didn't exist.
                break;
            }

            TLeaderInfo &followerInfo = xpair.second;

            if (!needWaitForFollowerGcAck) {
                if (followerInfo.SyncState != EFollowerSyncState::Ignore)
                    needWaitForFollowerGcAck = true;
            }

            if (followerInfo.FollowerAttempt == Max<ui32>())
                continue;

            if (followerInfo.SyncState == EFollowerSyncState::Active
                || followerInfo.SyncState == EFollowerSyncState::Pending && entry->IsSnapshot)
            {
                auto notify = MakeHolder<TEvTablet::TEvFollowerUpdate>(TabletID(), followerInfo.FollowerAttempt, followerInfo.StreamCounter);
                auto &record = notify->Record;

                record.SetGeneration(StateStorageInfo.KnownGeneration);
                record.SetStep(step);
                record.SetIsSnapshot(entry->IsSnapshot);
                record.SetNeedGCApplyAck(entry->WaitFollowerGcAck);

                if (sup->Body)
                    record.SetBody(sup->Body);

                if (sup->AuxPayload)
                    record.SetAuxPayload(sup->AuxPayload);

                if (sup->References) {
                    record.MutableReferences()->Reserve(sup->References.size());
                    record.MutableReferencesIds()->Reserve(sup->References.size());

                    for (auto &refpair : sup->References) {
                        record.AddReferences(refpair.second);
                        LogoBlobIDFromLogoBlobID(refpair.first, record.AddReferencesIds());
                    }
                }

                if (followerInfo.StreamCounter == 0) {
                    TabletStorageInfoToProto(*Info, record.MutableTabletStorageInfo());
                    followerInfo.SyncState = EFollowerSyncState::Active;
                }

                const ui32 subscFlag = (followerInfo.StreamCounter == 0) ? IEventHandle::FlagSubscribeOnSession : 0;
                Send(xpair.first, notify.Release(), IEventHandle::FlagTrackDelivery | subscFlag);

                ++xpair.second.StreamCounter;
            }

            for (const TString &x : entry->FollowerAuxUpdates)
                SendFollowerAuxUpdate(xpair.second, xpair.first, x);
        }

        if (entry->WaitFollowerGcAck) {
            if (needWaitForFollowerGcAck) {
                WaitingForGcAck.emplace_back(step, entry->CommitedMoment);
            } else {
                Send(UserTablet, new TEvTablet::TEvFollowerGcApplied(TabletID(), StateStorageInfo.KnownGeneration, step, TDuration::Max()));
            }
        }

        Graph.PostponedFollowerUpdates.pop_front();
    }

    if (Graph.PostponedFollowerUpdates && Graph.Queue.empty() && Graph.SyncCommit.SyncStep == 0) {
        Graph.SyncCommit.SyncStep = Graph.NextEntry - 1;
        if (GcInFly) {
            // Since we always confirm the last commit it should be impossible
            // to ever try to commit inside a garbage collected range.
            Y_DEBUG_ABORT_UNLESS(GcInFlyStep < Graph.SyncCommit.SyncStep);
            Y_DEBUG_ABORT_UNLESS(GcNextStep < Graph.SyncCommit.SyncStep);
        }

        TLogoBlobID entryId(TabletID(), StateStorageInfo.KnownGeneration, Graph.SyncCommit.SyncStep, 0, 0, 1);
        THolder<NKikimrTabletBase::TTabletLogEntry> entry(new NKikimrTabletBase::TTabletLogEntry());

        entry->SetSnapshot(MakeGenStepPair(Graph.Snapshot.first, Graph.Snapshot.second));
        entry->SetConfirmed(Graph.Confirmed);
        entry->SetIsSnapshot(false);
        entry->SetIsTotalSnapshot(false);

        Y_DEBUG_ABORT_UNLESS(Graph.Confirmed == Graph.SyncCommit.SyncStep); // last entry must be confirmed
        Y_DEBUG_ABORT_UNLESS(Graph.SyncCommit.SyncStep > Graph.ConfirmedCommited); // commit should make some progress

        TVector<TEvTablet::TLogEntryReference> refs;
        Register(
            CreateTabletReqWriteLog(SelfId(), entryId, entry.Release(), refs, TEvBlobStorage::TEvPut::ETactic::TacticMinLatency, Info.Get())
        );
    }
}

void TTablet::Handle(TEvTabletPipe::TEvConnect::TPtr& ev) {
    if (PipeConnectAcceptor->IsStopped()) {
        PipeConnectAcceptor->Reject(ev, SelfId(), NKikimrProto::TRYLATER, Leader);
    } else if (PipeConnectAcceptor->IsActive()) {
        PipeConnectAcceptor->Accept(ev, SelfId(), UserTablet, Leader, StateStorageInfo.KnownGeneration);
    } else {
        PipeConnectAcceptor->Enqueue(ev, SelfId());
    }
}

void TTablet::Handle(TEvTabletPipe::TEvServerDestroyed::TPtr& ev) {
    PipeConnectAcceptor->Erase(ev);
}

void TTablet::HandleQueued(TEvTabletPipe::TEvConnect::TPtr& ev) {
    if (PipeConnectAcceptor->IsStopped()) {
        // FIXME: do we really need it?
        PipeConnectAcceptor->Reject(ev, SelfId(), NKikimrProto::TRYLATER, Leader);
    } else {
        PipeConnectAcceptor->Enqueue(ev, SelfId());
    }
}

void TTablet::HandleByFollower(TEvTabletPipe::TEvConnect::TPtr &ev) {
    Y_DEBUG_ABORT_UNLESS(!Leader);
    if (PipeConnectAcceptor->IsActive() && !PipeConnectAcceptor->IsStopped()) {
        PipeConnectAcceptor->Accept(ev, SelfId(), UserTablet, false, StateStorageInfo.KnownGeneration);
    } else {
        PipeConnectAcceptor->Reject(ev, SelfId(), NKikimrProto::TRYLATER, false);
    }
}

void TTablet::Handle(TEvTabletBase::TEvWriteLogResult::TPtr &ev) {
    TEvTabletBase::TEvWriteLogResult *msg = ev->Get();
    const NKikimrProto::EReplyStatus status = msg->Status;
    const TLogoBlobID &logid = msg->EntryId;

    // todo: channel reconfiguration
    switch (status) {
    case NKikimrProto::OK:
    {
        Y_DEBUG_ABORT_UNLESS(logid.Generation() == StateStorageInfo.KnownGeneration && logid.TabletID() == TabletID());
        const ui32 step = logid.Step();

        if (logid.Cookie() == 0) {
            TGraph::TIndex::iterator indexIt = Graph.Index.find(step);

            Y_ABORT_UNLESS(indexIt != Graph.Index.end());

            TLogEntry *entry = indexIt->second;
            entry->BlobStorageConfirmed = true;
            entry->YellowMoveChannels = std::move(msg->YellowMoveChannels);
            entry->YellowStopChannels = std::move(msg->YellowStopChannels);
            entry->GroupWrittenBytes = std::move(msg->GroupWrittenBytes);
            entry->GroupWrittenOps = std::move(msg->GroupWrittenOps);

            Graph.ConfirmedCommited = Max(Graph.ConfirmedCommited, entry->ConfirmedOnSend);
            Graph.StepsInFlight -= 1;
            Graph.BytesInFlight -= entry->ByteSize;

            CheckEntry(indexIt);
        } else {
            Y_DEBUG_ABORT_UNLESS(logid.Cookie() == 1 && step == Graph.SyncCommit.SyncStep);

            Graph.ConfirmedCommited = Max(Graph.ConfirmedCommited, step);
            Graph.SyncCommit.SyncStep = 0;
            if (GcInFly == 0 && GcNextStep != 0) {
                GcLogChannel(std::exchange(GcNextStep, 0));
            }
        }

        if (!ProgressCommitQueue()) {
            return; // we died
        }

        // Send more commits if possible
        while (!Graph.DelayCommitQueue.empty() &&
            Graph.StepsInFlight < MaxStepsInFlight &&
            Graph.BytesInFlight < MaxBytesInFlight)
        {
            auto &nextEv = Graph.DelayCommitQueue.front();
            if (!HandleNext(nextEv)) {
                return; // we died
            }
            Graph.DelayCommitQueue.pop_front();
        }

        return;
    }
    default:
        break;
    }

    if (msg->YellowMoveChannels) {
        ReassignYellowChannels(std::move(msg->YellowMoveChannels));
    }

    // Non-zero cookie causes us to fail on the next step
    const ui32 errorStep = logid.Step() + (logid.Cookie() ? 1 : 0);
    if (BlobStorageStatus == NKikimrProto::OK || errorStep < BlobStorageErrorStep) {
        BlobStorageStatus = status;
        BlobStorageErrorStep = errorStep;
        BlobStorageErrorReason = std::move(msg->ErrorReason);
    }

    CheckBlobStorageError();
}

void TTablet::HandleFeatures(TEvTablet::TEvFeatures::TPtr &ev) {
    SupportedFeatures = ev->Get()->Features;
}

void TTablet::HandleStop(TEvTablet::TEvTabletStop::TPtr &ev) {
    BLOG_D("Received TEvTabletStop from " << ev->Sender << ", reason = " << ev->Get()->GetReason(), "TSYS29");
    StopTablet(ev->Get()->GetReason(), TEvTablet::TEvTabletDead::ReasonPill);
}

void TTablet::HandleStopped() {
    if (DelayedCancelTablet) {
        return CancelTablet(DelayedCancelTablet->Reason, DelayedCancelTablet->Details);
    } else {
        return CancelTablet(TEvTablet::TEvTabletDead::ReasonPill);
    }
}

void TTablet::HandlePoisonPill() {
    return CancelTablet(TEvTablet::TEvTabletDead::ReasonPill);
}

void TTablet::HandleDemoted() {
    StopTablet(TEvTablet::TEvTabletStop::ReasonDemoted, TEvTablet::TEvTabletDead::ReasonDemotedByStateStorage);
}

void TTablet::Handle(TEvTablet::TEvDemoted::TPtr &ev) {
    const auto deadReason =
        ev->Get()->ByIsolation ? TEvTablet::TEvTabletDead::ReasonIsolated
            : TEvTablet::TEvTabletDead::ReasonDemotedByStateStorage;
    const auto stopReason = ev->Get()->ByIsolation
            ? TEvTablet::TEvTabletStop::ReasonIsolated
            : TEvTablet::TEvTabletStop::ReasonDemoted;
    StopTablet(stopReason, deadReason);
}

bool TTablet::CheckBlobStorageError() {
    if (Y_LIKELY(BlobStorageStatus == NKikimrProto::OK)) {
        return false;
    }

    if (!Graph.Queue.empty()) {
        // Check if the head entry is still waiting to be committed
        TLogEntry *entry = Graph.Queue.front().get();
        if (entry->Step < BlobStorageErrorStep && entry->Task) {
            // Commit still inflight, wait for result
            return false;
        }
    }

    if (std::exchange(BlobStorageErrorReported, true)) {
        // Error has already been reported
        return false;
    }

    switch (BlobStorageStatus) {
        case NKikimrProto::RACE:
        case NKikimrProto::BLOCKED:
        case NKikimrProto::NO_GROUP:
            return StopTablet(
                TEvTablet::TEvTabletStop::ReasonStorageBlocked,
                TEvTablet::TEvTabletDead::ReasonDemotedByBlobStorage,
                BlobStorageErrorReason);

        default:
            return StopTablet(
                TEvTablet::TEvTabletStop::ReasonStorageFailure,
                TEvTablet::TEvTabletDead::ReasonBSError,
                BlobStorageErrorReason);
    }
}

bool TTablet::StopTablet(
        TEvTablet::TEvTabletStop::EReason stopReason,
        TEvTablet::TEvTabletDead::EReason deadReason,
        const TString &deadDetails)
{
    if (UserTablet && (SupportedFeatures & TEvTablet::TEvFeatures::GracefulStop)) {
        if (!PipeConnectAcceptor->IsStopped()) {
            PipeConnectAcceptor->Stop(SelfId());

            if (StateStorageGuardian) {
                Send(StateStorageGuardian, new TEvents::TEvPoisonPill());
                StateStorageGuardian = { };
            }

            if (FollowerStStGuardian) {
                Send(FollowerStStGuardian, new TEvents::TEvPoisonPill());
                FollowerStStGuardian = { };
            }
        }

        if (!DelayedCancelTablet) {
            DelayedCancelTablet.ConstructInPlace(deadReason, deadDetails);
        }

        Send(UserTablet, new TEvTablet::TEvTabletStop(TabletID(), stopReason), IEventHandle::FlagTrackDelivery);
        return false;
    }

    CancelTablet(deadReason, deadDetails);
    return true;
}

void TTablet::ReassignYellowChannels(TVector<ui32> &&yellowMoveChannels) {
    if (yellowMoveChannels.empty() || !Info->HiveId) {
        return;
    }

    auto yellowMoveChannelsString = [&]() -> TString {
        TStringBuilder out;
        for (size_t i = 0; i < yellowMoveChannels.size(); ++i) {
            if (i) {
                out << ", ";
            }
            out << yellowMoveChannels[i];
        }
        return std::move(out);
    };

    BLOG_I(
        " Type: " << TTabletTypes::TypeToStr((TTabletTypes::EType)Info->TabletType)
        << ", YellowMoveChannels: " << yellowMoveChannelsString(), "TSYS30");

    Send(MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(
            new TEvHive::TEvReassignTabletSpace(Info->TabletID, std::move(yellowMoveChannels)),
            Info->HiveId,
            /* subscribe */ false));
}

void TTablet::CancelTablet(TEvTablet::TEvTabletDead::EReason reason, const TString &details) {
    BLOG_LEVEL(
        reason == TEvTablet::TEvTabletDead::ReasonPill
            ? NActors::NLog::PRI_NOTICE
            : NActors::NLog::PRI_ERROR,
        " Type: " << TTabletTypes::TypeToStr((TTabletTypes::EType)Info->TabletType)
        << ", EReason: " << TEvTablet::TEvTabletDead::Str(reason)
        << ", SuggestedGeneration: " << SuggestedGeneration
        << ", KnownGeneration: " << StateStorageInfo.KnownGeneration
        << (details ? ", Details: " : "") << details.data(), "TSYS31");

    PipeConnectAcceptor->Detach(SelfId());
    const ui32 reportedGeneration = SuggestedGeneration ? SuggestedGeneration : StateStorageInfo.KnownGeneration;

    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnCancelTablet>(
            this->TabletID()
            , Info->TabletType
            , reason
            , SuggestedGeneration
            , StateStorageInfo.KnownGeneration));
        SendIntrospectionData();
    }

    Send(Launcher, new TEvTablet::TEvTabletDead(TabletID(), reason, reportedGeneration));

    if (UserTablet)
        Send(UserTablet, new TEvTablet::TEvTabletDead(TabletID(), reason, reportedGeneration));

    if (StateStorageGuardian)
        Send(StateStorageGuardian, new TEvents::TEvPoisonPill());

    if (FollowerStStGuardian)
        Send(FollowerStStGuardian, new TEvents::TEvPoisonPill());

    if (RebuildGraphRequest)
        Send(RebuildGraphRequest, new TEvents::TEvPoisonPill());

    TSet<ui32> nodesToUnsubsribe;
    for (auto &xpair : LeaderInfo) {
        Send(xpair.first, new TEvTablet::TEvFollowerDisconnect(TabletID(), xpair.second.FollowerAttempt));
        const ui32 followerNode = xpair.first.NodeId();
        if (followerNode && followerNode != SelfId().NodeId())
            nodesToUnsubsribe.emplace(followerNode);
    }
    LeaderInfo.clear();

    if (FollowerInfo.KnownLeaderID)
        Send(FollowerInfo.KnownLeaderID, new TEvTablet::TEvFollowerDetach(TabletID(), FollowerInfo.FollowerAttempt));

    if (NeedCleanupOnLockedPath)
        Send(StateStorageInfo.ProxyID, new TEvStateStorage::TEvCleanup(TabletID(), SelfId()));

    ReportTabletStateChange(TTabletStateInfo::Dead);

    const ui32 leaderNode = FollowerInfo.KnownLeaderID.NodeId();
    if (leaderNode && leaderNode != SelfId().NodeId())
        nodesToUnsubsribe.emplace(leaderNode);

    for (ui32 x : nodesToUnsubsribe) {
        Send(TActivationContext::InterconnectProxy(x), new TEvents::TEvUnsubscribe());
    }

    PassAway();
}

void TTablet::Handle(TEvTablet::TEvUpdateConfig::TPtr &ev) {
    ResourceProfiles = ev->Get()->ResourceProfiles;
    if (UserTablet)
        TActivationContext::Send(ev->Forward(UserTablet));
}

void TTablet::LockedInitializationPath() {
    const ui32 latestChangeGeneration = SuggestedGeneration ? SuggestedGeneration - 1 : Info->ChannelInfo(0)->LatestEntry()->FromGeneration;

    BLOG_D("LockedInitializationPath", "TSYS32");

    if (StateStorageInfo.KnownGeneration < latestChangeGeneration) {
        StateStorageInfo.KnownGeneration = latestChangeGeneration;
        StateStorageInfo.KnownStep = 0;
    }
    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnLockedInitializationPath>(
            StateStorageInfo.KnownGeneration
            , StateStorageInfo.KnownStep
            , StateStorageInfo.SignatureSz));
    }

    // lock => find latest => update => normal path
    Send(StateStorageInfo.ProxyID, new TEvStateStorage::TEvLock(TabletID(), 0, SelfId(), StateStorageInfo.KnownGeneration + 1, StateStorageInfo.Signature.Get(), StateStorageInfo.SignatureSz, TEvStateStorage::TProxyOptions::SigAsync));

    NeedCleanupOnLockedPath = true;
    Become(&TThis::StateLock);
    ReportTabletStateChange(TTabletStateInfo::Lock);
}

TTablet::TTablet(const TActorId &launcher, TTabletStorageInfo *info, TTabletSetupInfo *setupInfo, bool leader,
                 ui32 suggestedGeneration, ui32 followerId, TResourceProfilesPtr profiles, TSharedQuotaPtr txCacheQuota)
        : TActor(leader ? &TThis::StateBootstrapNormal : &TThis::StateBootstrapFollower)
    , InitialFollowerSyncDone(false)
    , Launcher(launcher)
    , Info(info)
    , SetupInfo(setupInfo)
    , SuggestedGeneration(suggestedGeneration)
    , NeedCleanupOnLockedPath(false)
    , GcCounter(0)
    , PipeConnectAcceptor(NTabletPipe::CreateConnectAcceptor(info->TabletID))
    , Leader(leader)
    , FollowerId(followerId)
    , DiscoveredLastBlocked(Max<ui32>())
    , GcInFly(0)
    , GcInFlyStep(0)
    , GcNextStep(0)
    , ResourceProfiles(profiles)
    , TxCacheQuota(txCacheQuota)
{
    Y_ABORT_UNLESS(!info->Channels.empty() && !info->Channels[0].History.empty());
    Y_ABORT_UNLESS(TTabletTypes::TypeInvalid != info->TabletType);
}

TAutoPtr<IEventHandle> TTablet::AfterRegister(const TActorId &self, const TActorId& parentId) {
    Y_UNUSED(parentId);
    return new IEventHandle(self, self, new TEvents::TEvBootstrap());
}

void TTablet::RetryFollowerBootstrapOrWait() {
    if (FollowerInfo.RetryRound) {
        ReportTabletStateChange(TTabletStateInfo::ResolveLeader);

        TActivationContext::Schedule(TDuration::MilliSeconds(2000), new IEventHandle(
            SelfId(), SelfId(),
            new TEvTabletBase::TEvFollowerRetry(++FollowerInfo.RetryRound),
            0, FollowerInfo.FollowerAttempt));
        Become(&TThis::StateResolveLeader);
    } else {
        FollowerInfo.RetryRound = 1;
        BootstrapFollower();
    }
}

void TTablet::BootstrapFollower() {
    // create guardians right now and schedule offline follower boot
    if (!FollowerStStGuardian) {
        FollowerStStGuardian = Register(CreateStateStorageFollowerGuardian(TabletID(), SelfId()));
        Schedule(OfflineFollowerWaitFirst, new TEvTabletBase::TEvTryBuildFollowerGraph());
    }

    Leader = false;
    BoostrapTime = AppData()->TimeProvider->Now();
    bool enInt = AppData()->EnableIntrospection;
    if (enInt) {
        IntrospectionTrace.Reset(NTracing::CreateTrace(NTracing::ITrace::TypeSysTabletBootstrap));
    }

    StateStorageInfo.ProxyID = MakeStateStorageProxyID();
    Send(StateStorageInfo.ProxyID, new TEvStateStorage::TEvLookup(TabletID(), 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)));
    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnTabletBootstrap>(SuggestedGeneration, false, StateStorageInfo.ProxyID));
    }

    Become(&TThis::StateResolveLeader);
    ReportTabletStateChange(TTabletStateInfo::ResolveLeader);
}

void TTablet::Bootstrap() {
    DiscoveredLastBlocked = Max<ui32>();
    Leader = true;
    BoostrapTime = AppData()->TimeProvider->Now();
    bool enInt = AppData()->EnableIntrospection;
    if (enInt) {
        IntrospectionTrace.Reset(NTracing::CreateTrace(NTracing::ITrace::TypeSysTabletBootstrap));
    }
    ReportTabletStateChange(TTabletStateInfo::Created); // useless?
    StateStorageInfo.ProxyID = MakeStateStorageProxyID();
    Send(StateStorageInfo.ProxyID, new TEvStateStorage::TEvLookup(TabletID(), 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)));
    if (IntrospectionTrace) {
        IntrospectionTrace->Attach(MakeHolder<NTracing::TOnTabletBootstrap>(SuggestedGeneration, true, StateStorageInfo.ProxyID));
    }
    // todo: handle "proxy unknown" case (normal timeouts are handled by proxy)
    PipeConnectAcceptor->Detach(SelfId());
    Become(&TThis::StateResolveStateStorage);
    ReportTabletStateChange(TTabletStateInfo::ResolveStateStorage);
}

void TTablet::ExternalWriteZeroEntry(TTabletStorageInfo *info, ui32 gen, TActorIdentity owner) {
    THolder<NKikimrTabletBase::TTabletLogEntry> entry = MakeHolder<NKikimrTabletBase::TTabletLogEntry>();
    entry->SetSnapshot(MakeGenStepPair(0, 0));
    entry->SetZeroConfirmed(MakeGenStepPair(0, 0));
    entry->SetZeroTailSz(0);
    TLogoBlobID logid(info->TabletID, gen, 0, 0, 0, 0);
    TVector<TEvTablet::TLogEntryReference> refs;
    TActivationContext::Register(CreateTabletReqWriteLog(owner, logid, entry.Release(), refs, TEvBlobStorage::TEvPut::TacticDefault, info));
}

TActorId TTabletSetupInfo::Apply(TTabletStorageInfo *info, TActorIdentity owner) {
    return TActivationContext::Register(Op(owner, info), owner, MailboxType, PoolId);
}

TActorId TTabletSetupInfo::Apply(TTabletStorageInfo *info, const TActorContext &ctx) {
    return Apply(info, TActorIdentity(ctx.SelfID));
}

TActorId TTabletSetupInfo::Tablet(TTabletStorageInfo *info, const TActorId &launcher, const TActorContext &ctx,
                                  ui32 suggestedGeneration, TResourceProfilesPtr profiles, TSharedQuotaPtr txCacheQuota) {
    return ctx.ExecutorThread.RegisterActor(CreateTablet(launcher, info, this, suggestedGeneration, profiles, txCacheQuota),
                                            TabletMailboxType, TabletPoolId);
}

TActorId TTabletSetupInfo::Follower(TTabletStorageInfo *info, const TActorId &launcher, const TActorContext &ctx,
                                 ui32 followerId, TResourceProfilesPtr profiles, TSharedQuotaPtr txCacheQuota) {
    return ctx.ExecutorThread.RegisterActor(CreateTabletFollower(launcher, info, this, followerId, profiles, txCacheQuota),
                                            TabletMailboxType, TabletPoolId);
}

IActor* CreateTablet(const TActorId &launcher, TTabletStorageInfo *info, TTabletSetupInfo *setupInfo,
                     ui32 suggestedGeneration, TResourceProfilesPtr profiles, TSharedQuotaPtr txCacheQuota) {
    return new TTablet(launcher, info, setupInfo, true, suggestedGeneration, 0, profiles, txCacheQuota);
}

IActor* CreateTabletFollower(const TActorId &launcher, TTabletStorageInfo *info, TTabletSetupInfo *setupInfo,
                          ui32 followerId, TResourceProfilesPtr profiles, TSharedQuotaPtr txCacheQuota) {
    return new TTablet(launcher, info, setupInfo, false, 0, followerId, profiles, txCacheQuota);
}

void TTablet::SendIntrospectionData() {
    const TActorId tabletStateServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
    Send(tabletStateServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvIntrospectionData(TabletID(), IntrospectionTrace.Release()));
    IntrospectionTrace.Reset(NTracing::CreateTrace(NTracing::ITrace::TypeSysTabletBootstrap));
}

}
