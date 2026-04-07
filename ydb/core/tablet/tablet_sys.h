#pragma once
#include "defs.h"
#include "tablet_impl.h"
#include "tablet_setup.h"
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/intrlist.h>
#include <util/generic/set.h>

namespace NKikimr {

class TTablet : public TActor<TTablet> {
    using TTabletStateInfo = NKikimrWhiteboard::TTabletStateInfo;
    using ETabletState = TTabletStateInfo::ETabletState;

    struct TRequestAddr {
        TActorId Sender;
        ui64 Cookie;
    };

    struct TStateStorageInfo {
        TActorId ProxyID;

        ui32 KnownGeneration;
        ui32 KnownStep;
        TActorId CurrentLeader;

        TEvStateStorage::TSignature Signature;

        TStateStorageInfo()
            : KnownGeneration(0)
            , KnownStep(0)
        {}

        void Update(const TEvStateStorage::TEvInfo *msg) {
            const ui32 xg = msg->CurrentGeneration;
            const ui32 xs = msg->CurrentStep;
            const TActorId &xm = msg->CurrentLeader;

            if (xg > KnownGeneration) {
                KnownGeneration = xg;
                KnownStep = xs;
                CurrentLeader = xm;
            } else if (xg == KnownGeneration) {
                if (KnownStep < xs)
                    KnownStep = xs;
            } else {
                // happens?
            }
        }
    } StateStorageInfo;

    struct TFollowerUpdate {
        TVector<std::pair<TLogoBlobID, TString>> References;
        TString Body;
        TString AuxPayload;

        TVector<TLogoBlobID> GcDiscovered;
        TVector<TLogoBlobID> GcLeft;
    };

    struct TLogEntry {
        const ui32 Step;
        const ui32 ConfirmedOnSend;

        TVector<ui32> Dependent;
        bool StateStorageConfirmed;
        bool BlobStorageConfirmed;
        ui32 DependenciesLeft;

        bool IsSnapshot;
        bool IsTotalSnapshot;
        bool Commited;
        bool WaitFollowerGcAck;
        TInstant CommitedMoment;

        TActorId Source;
        TActorId Task;

        ui64 SourceCookie;

        THolder<TFollowerUpdate> FollowerUpdate;
        TVector<TString> FollowerAuxUpdates;
        NMetrics::TTabletThroughputRawValue GroupWrittenBytes;
        NMetrics::TTabletIopsRawValue GroupWrittenOps;

        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;

        THashMap<ui32, float> ApproximateFreeSpaceShareByChannel;

        size_t ByteSize;

        TLogEntry(ui32 step, ui32 confirmedOnSend, ui64 sourceCookie)
            : Step(step)
            , ConfirmedOnSend(confirmedOnSend)
            , StateStorageConfirmed(false)
            , BlobStorageConfirmed(false)
            , DependenciesLeft(0)
            , IsSnapshot(false)
            , IsTotalSnapshot(false)
            , Commited(false)
            , WaitFollowerGcAck(false)
            , CommitedMoment(TInstant::Zero())
            , SourceCookie(sourceCookie)
            , ByteSize(0)
        {}
    };

    struct TGraph {
        typedef TDeque<std::unique_ptr<TLogEntry>> TQueueType;
        typedef THashMap<ui32, TLogEntry *> TIndex;

        TQueueType Queue;
        TQueueType PostponedFollowerUpdates;
        TIndex Index;
        TDeque<TEvTablet::TEvCommit::TPtr> DelayCommitQueue;

        ui32 Confirmed;
        ui32 ConfirmedCommited;
        ui32 NextEntry;
        ui32 MinFollowerUpdate;
        ui32 StepsInFlight;
        ui64 BytesInFlight;

        std::pair<ui32, ui32> Snapshot;
        TActorId SnapshotSource;
        ui64 SnapshotCookie = 0;

        struct {
            ui32 SyncStep = 0;
            ui32 Snapshot = 0;
            TActorId SnapshotSource;
            ui64 SnapshotCookie = 0;
        } SyncCommit;

        TGraph()
            : Confirmed(0)
            , ConfirmedCommited(0)
            , NextEntry(0)
            , MinFollowerUpdate(0)
            , StepsInFlight(0)
            , BytesInFlight(0)
        {}
    } Graph;

    struct TFollowerInfo {
        TActorId KnownLeaderID;
        ui32 RetryRound;
        ui32 FollowerAttempt;
        ui64 StreamCounter;
        ui64 EpochGenStep;
        ui64 RebuildGraphCookie;
        ui64 LastCookie = 0;

        TFollowerInfo()
            : RetryRound(0)
            , FollowerAttempt(0)
            , StreamCounter(0)
            , EpochGenStep(Max<ui64>())
            , RebuildGraphCookie(1)
        {}

        void NextAttempt() {
            KnownLeaderID = TActorId();
            // do not touch RetryRound on retries
            ++FollowerAttempt;
            StreamCounter = 0;
            // do not reset EpochGenStep for sync actuality check!
        }
    } FollowerInfo;

    void NextFollowerAttempt();
    void SendFollowerAttach(const TActorId& leader);

    enum class EFollowerSyncState {
        NeedSync, // follower known but not connected, blocks gc
        Pending, // follower connected but stream not yet started, blocks gc
        Active, // follower active, blocks gc
        Ignore, // could not connect to follower for too long. ignore for gc
    };

    struct TLeaderInfo : public TIntrusiveListItem<TLeaderInfo> {
        const TActorId FollowerId;
        TActorId InterconnectSession;
        ui32 FollowerAttempt;
        ui64 StreamCounter;

        EFollowerSyncState SyncState;
        TInstant LastSyncAttempt;
        ui64 SyncAttempt;
        THolder<TSchedulerCookieHolder> SyncCookieHolder;

        ui64 LastCookie = 0;

        ui32 ConfirmedGCStep;
        bool PresentInList;

        explicit TLeaderInfo(const TActorId& followerId, EFollowerSyncState syncState)
            : FollowerId(followerId)
            , FollowerAttempt(Max<ui32>())
            , StreamCounter(0)
            , SyncState(syncState)
            , LastSyncAttempt(TInstant::Zero())
            , SyncAttempt(0)
            , ConfirmedGCStep(0)
            , PresentInList(false)
        {}
    };

    TMap<TActorId, TLeaderInfo> LeaderInfo;
    TDeque<std::pair<ui32, TInstant>> WaitingForGcAck; // step, commitMoment
    bool InitialFollowerSyncDone;

    const TActorId Launcher;
    TActorId UserTablet;
    TActorId StateStorageGuardian;
    TActorId FollowerStStGuardian;
    TIntrusivePtr<TTabletStorageInfo> Info;
    TIntrusivePtr<TTabletSetupInfo> SetupInfo;
    ui32 SuggestedGeneration;
    bool NeedCleanupOnLockedPath;
    ui32 GcCounter;
    THolder<NTabletPipe::IConnectAcceptor> PipeConnectAcceptor;
    TString TabletVersionInfo;
    TInstant BoostrapTime;
    TInstant ActivateTime;
    bool Leader;
    ui32 FollowerId;
    ui32 DiscoveredLastBlocked;
    ui32 GcInFly;
    ui32 GcInFlyStep;
    ui32 GcConfirmedStep;
    ui32 GcNextStep;

    // retry failed GC logic
    ui32 GcTryCounter;
    TBackoffTimer GcBackoffTimer;
    bool GcPendingRetry;
    ui32 GcFailCount;

    TEvTablet::TEvGcForStepAckRequest::TPtr GcForStepAckRequest;
    TResourceProfilesPtr ResourceProfiles;
    TSharedQuotaPtr TxCacheQuota;
    THolder<NTracing::ITrace> IntrospectionTrace;
    TActorId RebuildGraphRequest;

    // Delayed cancellation reason
    struct TDelayedCancelTablet {
        const TEvTablet::TEvTabletDead::EReason Reason;
        const TString Details;

        explicit TDelayedCancelTablet(
                TEvTablet::TEvTabletDead::EReason reason,
                TString details = TString())
            : Reason(reason)
            , Details(std::move(details))
        { }
    };

    TMaybe<TDelayedCancelTablet> DelayedCancelTablet;

    // Optional supported features, by default none
    ui32 SupportedFeatures = TEvTablet::TEvFeatures::None;

    // Optional delayed blob storage status
    NKikimrProto::EReplyStatus BlobStorageStatus = NKikimrProto::OK;
    ui32 BlobStorageErrorStep = Max<ui32>();
    TString BlobStorageErrorReason;
    bool BlobStorageErrorReported = false;

    // Leader confirmation requests
    THashMap<ui64, TRequestAddr> ConfirmLeaderRequests;
    ui64 ConfirmLeaderCounter = 0;

    struct TTabletStateSubscriber : public TIntrusiveListItem<TTabletStateSubscriber> {
        TActorId ActorId;
        ui64 Cookie;
        ui64 SeqNo;
        TActorId InterconnectSession;
    };

    struct TInterconnectSession {
        TActorId ActorId;
        TIntrusiveList<TTabletStateSubscriber> TabletStateSubscribers;
        TIntrusiveList<TLeaderInfo> Followers;
        bool Connected = false;
    };

    struct TInterconnectPending {
        ui64 LastCookie = 0;
        TIntrusiveList<TLeaderInfo> Followers;
    };

    THashMap<TActorId, TTabletStateSubscriber> TabletStateSubscribers;
    THashMap<TActorId, TInterconnectSession> InterconnectSessions;
    THashMap<ui32, TInterconnectPending> InterconnectPending;
    ui64 LastInterconnectSubscribeCookie = 0;

    TMessageRelevanceOwner Relevance = std::make_shared<TMessageRelevanceTracker>();

    ui64 TabletID() const;

    void ReportTabletStateChange(ETabletState state);
    void PromoteToCandidate(ui32 gen);
    void TabletBlockBlobStorage();
    void TabletRebuildGraph();
    void WriteZeroEntry(TEvTablet::TDependencyGraph *graph);

    void StartActivePhase();
    void UpdateStateStorageSignature(TEvStateStorage::TEvUpdateSignature::TPtr &ev);
    void TryFinishFollowerSync();
    void TryPumpWaitingForGc();

    void HandlePingBoot(TEvTablet::TEvPing::TPtr &ev);
    void HandlePingFollower(TEvTablet::TEvPing::TPtr &ev);
    void HandleStateStorageInfoResolve(TEvStateStorage::TEvInfo::TPtr &ev);
    void HandleStateStorageLeaderResolve(TEvStateStorage::TEvInfo::TPtr &ev);
    void HandleFollowerRetry(TEvTabletBase::TEvFollowerRetry::TPtr &ev);
    void HandleByFollower(TEvTabletBase::TEvTryBuildFollowerGraph::TPtr &ev);
    void HandleByFollower(TEvTabletBase::TEvRebuildGraphResult::TPtr &ev);

    void HandleStateStorageInfoLock(TEvStateStorage::TEvInfo::TPtr &ev);

    void HandleStateStorageInfoUpgrade(TEvStateStorage::TEvInfo::TPtr &ev);
    void HandleFindLatestLogEntry(TEvTabletBase::TEvFindLatestLogEntryResult::TPtr &ev);
    void HandleBlockBlobStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr &ev);

    void HandleByFollower(TEvTablet::TEvFollowerDisconnect::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvFollowerUpdate::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvFollowerAuxUpdate::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvFollowerRefresh::TPtr &ev);
    void HandleByFollower(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void HandleByFollower(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleByFollower(TEvents::TEvUndelivered::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvPromoteToLeader::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvFGcAck::TPtr &ev);
    void HandleByFollower(TEvTablet::TEvTabletActive::TPtr &ev);
    void HandleByFollower(TEvTabletPipe::TEvConnect::TPtr &ev);

    void HandleByLeader(TEvTablet::TEvFollowerAttach::TPtr &ev);
    void HandleByLeader(TEvTablet::TEvFollowerDetach::TPtr &ev);
    void HandleByLeader(TEvTablet::TEvFollowerRefresh::TPtr &ev);
    void HandleByLeader(TEvTablet::TEvFollowerListRefresh::TPtr &ev);
    void HandleByLeader(TEvTabletBase::TEvTrySyncFollower::TPtr &ev);
    void HandleByLeader(TEvTablet::TEvFollowerGcAck::TPtr &ev);
    void HandleByLeader(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void HandleByLeader(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleByLeader(TEvents::TEvUndelivered::TPtr &ev);

    void HandleRebuildGraphResult(TEvTabletBase::TEvRebuildGraphResult::TPtr &ev);
    void HandleWriteZeroEntry(TEvTabletBase::TEvWriteLogResult::TPtr &ev);

    void Handle(TEvTablet::TEvPing::TPtr &ev);
    void HandleByLeader(TEvTablet::TEvTabletActive::TPtr &ev);

    bool CheckFollowerUpdate(const TActorId &actorId, ui32 attempt, ui64 counter);

    TLogEntry* MakeLogEntry(TEvTablet::TCommitInfo &commitInfo, NKikimrTabletBase::TTabletLogEntry *commitEv);
    TLogEntry* FindLogEntry(TEvTablet::TCommitInfo &commitInfo, NKikimrTabletBase::TTabletLogEntry &commitEv);

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev);
    void Handle(TEvTablet::TEvPreCommit::TPtr &ev);

    void Handle(TEvTablet::TEvGcForStepAckRequest::TPtr& ev);
    void Handle(TEvTabletBase::TEvLogGcRetry::TPtr& ev);

    void Handle(TEvTablet::TEvConfirmLeader::TPtr &ev);
    void Handle(TEvBlobStorage::TEvGetBlockResult::TPtr &ev);
    void Handle(TEvTablet::TEvCommit::TPtr &ev);
    bool HandleNext(TEvTablet::TEvCommit::TPtr &ev);
    void Handle(TEvTablet::TEvAux::TPtr &ev);
    void CheckEntry(TGraph::TIndex::iterator it);

    // next funcs return next correct iterator
    TMap<TActorId, TLeaderInfo>::iterator EraseFollowerInfo(TMap<TActorId, TLeaderInfo>::iterator followerIt);
    TMap<TActorId, TLeaderInfo>::iterator HandleFollowerConnectionProblem(TMap<TActorId, TLeaderInfo>::iterator followerIt, bool permanent = false);
    void HandleFollowerDisconnect(TLeaderInfo* follower);

    void TrySyncToFollower(TMap<TActorId, TLeaderInfo>::iterator followerIt);
    void DoSyncToFollower(TMap<TActorId, TLeaderInfo>::iterator followerIt);

    void GcLogChannel(ui32 step);
    void RetryGcRequests();

    bool ProgressCommitQueue();
    void ProgressFollowerQueue();
    void ProgressSendSyncCommit();
    void SpreadFollowerAuxUpdate(const TString& auxUpdate);
    void SendFollowerAuxUpdate(TLeaderInfo& info, const TActorId& follower, const TString& auxUpdate);

    void Handle(TEvTabletPipe::TEvConnect::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDestroyed::TPtr& ev);
    void HandleQueued(TEvTabletPipe::TEvConnect::TPtr& ev);

    void Handle(TEvTabletBase::TEvWriteLogResult::TPtr &ev);

    void HandleFeatures(TEvTablet::TEvFeatures::TPtr &ev);
    void HandleStop(TEvTablet::TEvTabletStop::TPtr &ev);
    void HandleStopped();
    void HandlePoisonPill();
    void HandleDemoted();
    void Handle(TEvTablet::TEvDemoted::TPtr &ev);
    bool CheckBlobStorageError();
    bool StopTablet(TEvTablet::TEvTabletStop::EReason stopReason,
                    TEvTablet::TEvTabletDead::EReason deadReason,
                    const TString &deadDetails = TString());
    void ReassignYellowChannels(TVector<ui32> &&yellowChannels);
    void CancelTablet(TEvTablet::TEvTabletDead::EReason reason, const TString &details = TString());

    void Handle(TEvTablet::TEvUpdateConfig::TPtr &ev);

    void LockedInitializationPath();
    void Bootstrap();
    void BootstrapFollower();
    void RetryFollowerBootstrapOrWait();

    void SendIntrospectionData();

    void Handle(TEvTablet::TEvTabletStateSubscribe::TPtr& ev);
    void Handle(TEvTablet::TEvTabletStateUnsubscribe::TPtr& ev);
    void SendTabletStateUpdate(const TTabletStateSubscriber& subscriber, NKikimrTabletBase::TEvTabletStateUpdate::EState state);
    void SendTabletStateUpdates(NKikimrTabletBase::TEvTabletStateUpdate::EState state);
    TInterconnectSession& SubscribeInterconnectSession(const TActorId& sessionId);
    void InterconnectSessionConnected(const TActorId& sessionId, ui32 nodeId, ui64 cookie);
    void InterconnectSessionDisconnected(const TActorId& sessionId);
    void InterconnectSessionDisconnected(const TActorId& sessionId, ui32 nodeId, ui64 cookie);
    void TabletStateUndelivered(const TActorId& actorId, ui64 cookie);
    void SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags = 0, ui64 cookie = 0);

    void StartRecovery();
    void Handle(TEvTablet::TEvCompleteRecoveryBoot::TPtr& ev);
    void HandleEmptyZeroEntry(TEvTabletBase::TEvWriteLogResult::TPtr& ev);
    void Handle(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev);

    STATEFN(StateResolveStateStorage) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvInfo, HandleStateStorageInfoResolve);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateLock) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvInfo, HandleStateStorageInfoLock);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateDiscover) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvFindLatestLogEntryResult, HandleFindLatestLogEntry);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateBecomeCandidate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvInfo, HandleStateStorageInfoUpgrade);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateBlockBlobStorage) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvBlockBlobStorageResult, HandleBlockBlobStorageResult);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateRebuildGraph) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvRebuildGraphResult, HandleRebuildGraphResult);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateWriteZeroEntry) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvWriteLogResult, HandleWriteZeroEntry);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateActivePhase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvCommit, Handle);
            hFunc(TEvTablet::TEvConfirmLeader, Handle);
            hFunc(TEvTablet::TEvAux, Handle);
            hFunc(TEvTablet::TEvPreCommit, Handle);
            hFunc(TEvTablet::TEvTabletActive, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerGcAck, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerListRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTabletBase::TEvWriteLogResult, Handle);
            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, Handle);
            hFunc(TEvTablet::TEvDemoted, Handle);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            hFunc(TEvTabletPipe::TEvConnect, Handle);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            hFunc(TEvBlobStorage::TEvGetBlockResult, Handle);
            hFunc(TEvTablet::TEvGcForStepAckRequest, Handle);
            sFunc(TEvTabletBase::TEvLogGcRetry, RetryGcRequests);
        }
    }

    STATEFN(StateRecovery) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvCompleteRecoveryBoot, Handle);
            hFunc(TEvTabletBase::TEvWriteLogResult, HandleEmptyZeroEntry);
            hFunc(TEvTabletBase::TEvDeleteTabletResult, Handle);

            hFunc(TEvTabletPipe::TEvConnect, Handle);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);

            hFunc(TEvStateStorage::TEvUpdateSignature, UpdateStateStorageSignature);
            hFunc(TEvTablet::TEvPing, HandlePingBoot);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            cFunc(TEvStateStorage::TEvReplicaLeaderDemoted::EventType, HandleDemoted);
            hFunc(TEvTablet::TEvFollowerAttach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerDetach, HandleByLeader);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByLeader);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTrySyncFollower, HandleByLeader);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByLeader);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByLeader);
            hFunc(TEvents::TEvUndelivered, HandleByLeader);
        }
    }

    STATEFN(StateBootstrapNormal) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvBootstrap::EventType, Bootstrap);
        default:
            Y_ABORT();
        }
    }

    STATEFN(StateBootstrapFollower) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvBootstrap::EventType, BootstrapFollower);
        default:
            Y_ABORT();
        }
    }

    STATEFN(StateResolveLeader) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvPromoteToLeader, HandleByFollower);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByFollower);
            hFunc(TEvTablet::TEvTabletActive, HandleByFollower);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvStateStorage::TEvInfo, HandleStateStorageLeaderResolve);
            hFunc(TEvTabletBase::TEvFollowerRetry, HandleFollowerRetry);
            hFunc(TEvTabletBase::TEvTryBuildFollowerGraph, HandleByFollower);
            hFunc(TEvTabletBase::TEvRebuildGraphResult, HandleByFollower);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByFollower);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByFollower);
            hFunc(TEvents::TEvUndelivered, HandleByFollower);
            hFunc(TEvTabletPipe::TEvConnect, Handle);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
        }
    }

    STATEFN(StateFollowerSubscribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvPromoteToLeader, HandleByFollower);
            hFunc(TEvTablet::TEvFollowerDisconnect, HandleByFollower);
            hFunc(TEvTablet::TEvFollowerUpdate, HandleByFollower);
            hFunc(TEvTablet::TEvFollowerAuxUpdate, HandleByFollower);
            hFunc(TEvTablet::TEvFollowerRefresh, HandleByFollower);
            hFunc(TEvTablet::TEvFGcAck, HandleByFollower);
            hFunc(TEvTablet::TEvTabletActive, HandleByFollower);
            hFunc(TEvTablet::TEvUpdateConfig, Handle);
            hFunc(TEvTabletBase::TEvTryBuildFollowerGraph, HandleByFollower);
            hFunc(TEvTabletBase::TEvRebuildGraphResult, HandleByFollower);
            hFunc(TEvTablet::TEvTabletStateSubscribe, Handle);
            hFunc(TEvTablet::TEvTabletStateUnsubscribe, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleByFollower);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleByFollower);
            hFunc(TEvents::TEvUndelivered, HandleByFollower);
            hFunc(TEvTabletPipe::TEvConnect, HandleByFollower);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
            hFunc(TEvTablet::TEvFeatures, HandleFeatures);
            hFunc(TEvTablet::TEvTabletStop, HandleStop);
            cFunc(TEvTablet::TEvTabletStopped::EventType, HandleStopped);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_ACTOR;
    }

    TTablet(
            const TActorId &launcher,
            TTabletStorageInfo *info,
            TTabletSetupInfo *setupInfo,
            bool leader,
            ui32 suggestedGeneration, // when leader == true
            ui32 followerID, // when leader == false
            TResourceProfilesPtr profiles = nullptr,
            TSharedQuotaPtr txCacheQuota = nullptr
            );

    TAutoPtr<IEventHandle> AfterRegister(const TActorId &self, const TActorId &parentId) override;
    static void ExternalWriteZeroEntry(TTabletStorageInfo *info, ui32 gen, TActorIdentity owner, TMessageRelevanceWatcher relevance);
};

}
