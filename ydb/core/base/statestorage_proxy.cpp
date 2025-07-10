#include "statestorage_impl.h"
#include "tabletid.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

#include <util/digest/city.h>
#include <util/generic/xrange.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)

namespace NKikimr {

// make configurable, here is no sense in too low ttl
const static ui64 StateStorageRequestTimeout = 30 * 1000 * 1000;

class TStateStorageProxyRequest : public TActor<TStateStorageProxyRequest> {
    TIntrusivePtr<TStateStorageInfo> Info;

    const bool UseInterconnectSubscribes;
    ui64 TabletID;
    ui64 Cookie;
    TEvStateStorage::TProxyOptions ProxyOptions;
    ui32 SuggestedGeneration;
    ui32 SuggestedStep;
    TActorId SuggestedLeader;
    TActorId SuggestedLeaderTablet;
    TActorId Source;

    ui32 Replicas;
    THolder<TStateStorageInfo::TSelection> ReplicaSelection;
    TEvStateStorage::TSignature Signature;
    THashSet<TActorId> UndeliveredReplicas;

    TStateStorageInfo::TSelection::EStatus ReplyStatus;
    ui32 RepliesMerged;
    ui32 RepliesAfterReply;
    ui32 SignaturesMerged;

    TActorId ReplyLeader;
    TActorId ReplyLeaderTablet;
    ui32 ReplyGeneration;
    ui32 ReplyStep;
    bool ReplyLocked;
    ui64 ReplyLockedFor;

    TMap<TActorId, TActorId> Followers;

    const ui32 RingGroupIndex;
    bool NotifyRingGroupProxy;

    void SelectRequestReplicas(TStateStorageInfo *info) {
        THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
        info->SelectReplicas(TabletID, selection.Get(), RingGroupIndex);
        Replicas = selection->Sz;
        ReplicaSelection = std::move(selection);
    }

    template<typename T>
    void SendRequest(const T &op) {
        Y_ABORT_UNLESS(ReplicaSelection && ReplicaSelection->SelectedReplicas && ReplicaSelection->Sz);

        const ui32 sendFlags = IEventHandle::FlagTrackDelivery | (UseInterconnectSubscribes ? IEventHandle::FlagSubscribeOnSession : 0);
        for (ui64 cookie = 0; cookie < ReplicaSelection->Sz; ++cookie) {
            auto replicaId = ReplicaSelection->SelectedReplicas[cookie];
            Send(replicaId, op(cookie, replicaId), sendFlags, cookie);
        }
    }

    void PassAway() override {
        if (UseInterconnectSubscribes && ReplicaSelection) {
            const ui32 selfNode = SelfId().NodeId();
            for (ui32 i = 0; i < ReplicaSelection->Sz; ++i) {
                const ui32 node = ReplicaSelection->SelectedReplicas[i].NodeId();
                if (node != selfNode) {
                    Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
                }
            }
        }
        if (NotifyRingGroupProxy)
            Send(Source, new TEvStateStorage::TEvRingGroupPassAway());
        TActor::PassAway();
    }

    void Reply(NKikimrProto::EReplyStatus status) {
        Send(Source, new TEvStateStorage::TEvInfo(status, TabletID, Cookie, ReplyLeader, ReplyLeaderTablet, ReplyGeneration, ReplyStep, ReplyLocked, ReplyLockedFor, Signature, Followers));
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        Reply(status);
        PassAway();
    }

    void ReplyAndSig(NKikimrProto::EReplyStatus status) {
        Reply(status);
        if (ProxyOptions.SigWaitMode == ProxyOptions.SigAsync && RepliesMerged != Replicas)
            Become(&TThis::StateUpdateSig);
        else
            PassAway();
    }

    struct TCloneUpdateEventOp {
        const TEvStateStorage::TEvUpdate * const Ev;
        const bool UpdateLeaderTablet;
        ui64 ClusterStateGeneration;
        ui64 ClusterStateGuid;

        TCloneUpdateEventOp(const TEvStateStorage::TEvUpdate *ev, ui64 clusterStateGeneration, ui64 clusterStateGuid)
            : Ev(ev)
            , UpdateLeaderTablet(!!ev->ProposedLeaderTablet)
            , ClusterStateGeneration(clusterStateGeneration)
            , ClusterStateGuid(clusterStateGuid)
        {}

        IEventBase* operator()(ui64 cookie, TActorId replicaId) const {
            THolder<TEvStateStorage::TEvReplicaUpdate> req(new TEvStateStorage::TEvReplicaUpdate());
            req->Record.SetSignature(Ev->Signature.GetReplicaSignature(replicaId));
            req->Record.SetTabletID(Ev->TabletID);
            req->Record.SetClusterStateGeneration(ClusterStateGeneration);
            req->Record.SetClusterStateGuid(ClusterStateGuid);
            ActorIdToProto(Ev->ProposedLeader, req->Record.MutableProposedLeader());

            if (UpdateLeaderTablet)
                ActorIdToProto(Ev->ProposedLeaderTablet, req->Record.MutableProposedLeaderTablet());

            req->Record.SetProposedGeneration(Ev->ProposedGeneration);
            req->Record.SetProposedStep(Ev->ProposedStep);
            req->Record.SetCookie(cookie);
            return req.Release();
        }
    };

    struct TCloneLockEventOp {
        const TEvStateStorage::TEvLock * const Ev;
        ui64 ClusterStateGeneration;
        ui64 ClusterStateGuid;

        TCloneLockEventOp(const TEvStateStorage::TEvLock *ev, ui64 clusterStateGeneration, ui64 clusterStateGuid)
            : Ev(ev)
            , ClusterStateGeneration(clusterStateGeneration)
            , ClusterStateGuid(clusterStateGuid)
        {}

        IEventBase* operator()(ui64 cookie, TActorId replicaId) const {
            THolder<TEvStateStorage::TEvReplicaLock> req(new TEvStateStorage::TEvReplicaLock());
            req->Record.SetSignature(Ev->Signature.GetReplicaSignature(replicaId));
            req->Record.SetTabletID(Ev->TabletID);
            req->Record.SetClusterStateGeneration(ClusterStateGeneration);
            req->Record.SetClusterStateGuid(ClusterStateGuid);
            ActorIdToProto(Ev->ProposedLeader, req->Record.MutableProposedLeader());
            req->Record.SetProposedGeneration(Ev->ProposedGeneration);
            req->Record.SetCookie(cookie);

            return req.Release();
        }
    };

    void MergeNodeError(ui32 node) {
        for (ui32 cookie = 0; cookie < ReplicaSelection->Sz; ++cookie) {
            auto replicaId = ReplicaSelection->SelectedReplicas[cookie];
            if (replicaId.NodeId() == node)
                MergeConnectionError(cookie);
        }
    }

    void MergeConnectionError(ui64 cookie) {
        Y_ABORT_UNLESS(cookie < Replicas);
        auto replicaId = ReplicaSelection->SelectedReplicas[cookie];
        if (!Signature.HasReplicaSignature(replicaId) && UndeliveredReplicas.insert(replicaId).second) {
            ++RepliesMerged;
            ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusUnavailable, &ReplyStatus, cookie, false);
        }
    }

    bool CheckConfigVersion(TActorId &sender, TEvStateStorage::TEvReplicaInfo *ev) {
        const auto &record = ev->Record;
        const ui64 clusterStateGeneration = record.GetClusterStateGeneration();
        const ui64 clusterStateGuid = record.GetClusterStateGuid();
        if (Info->ClusterStateGeneration < clusterStateGeneration ||
            (Info->ClusterStateGeneration == clusterStateGeneration && Info->ClusterStateGuid != clusterStateGuid)) {
            BLOG_D("StateStorageProxy TEvNodeWardenNotifyConfigMismatch: Info->ClusterStateGeneration=" << Info->ClusterStateGeneration << " clusterStateGeneration=" << clusterStateGeneration <<" Info->ClusterStateGuid=" << Info->ClusterStateGuid << " clusterStateGuid=" << clusterStateGuid);
            if (NotifyRingGroupProxy) {
                Send(Source, new TEvStateStorage::TEvConfigVersionInfo(clusterStateGeneration, clusterStateGuid));
            }
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new NStorage::TEvNodeWardenNotifyConfigMismatch(sender.NodeId(), clusterStateGeneration, clusterStateGuid));
            ReplyAndDie(NKikimrProto::ERROR);
            return false;
        }
        return true;
    }

    void MergeReply(TActorId &sender, TEvStateStorage::TEvReplicaInfo *ev) {
        const auto &record = ev->Record;
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        const ui64 cookie = record.GetCookie();

        if (!CheckConfigVersion(sender, ev)) {
            return;
        }

        Y_ABORT_UNLESS(cookie < Replicas);
        auto replicaId = ReplicaSelection->SelectedReplicas[cookie];

        if (Signature.HasReplicaSignature(replicaId)) {
            BLOG_ERROR("TStateStorageProxyRequest::MergeReply duplicated TEvReplicaInfo cookie:" << cookie
                << " replica:" << replicaId << " signature:" << Signature.GetReplicaSignature(replicaId) << " ev: " << ev->ToString());
            return;
        }
        UndeliveredReplicas.erase(replicaId);
        Signature.SetReplicaSignature(replicaId, ev->Record.GetSignature());
        ++RepliesMerged;
        ++SignaturesMerged;

        switch (status) {
        case NKikimrProto::OK: {
            const ui32 gen = record.GetCurrentGeneration();
            const ui32 step = record.GetCurrentStep();
            const TActorId leader = ActorIdFromProto(record.GetCurrentLeader());

            if (gen < ReplyGeneration || (gen == ReplyGeneration && step < ReplyStep)) {
                ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusOutdated, &ReplyStatus, cookie, false);
            } else {
                const bool reset = gen > ReplyGeneration || step > ReplyStep || leader != ReplyLeader;
                const TActorId replyLeaderTablet = ActorIdFromProto(record.GetCurrentLeaderTablet());

                ReplyGeneration = gen;
                ReplyStep = step;

                if (ReplyLeader != leader) {
                    ReplyLeader = leader;
                    ReplyLeaderTablet = replyLeaderTablet;
                } else if (!ReplyLeaderTablet) {
                    ReplyLeaderTablet = replyLeaderTablet;
                } else {
                    Y_ABORT_UNLESS(ReplyLeaderTablet == replyLeaderTablet || !replyLeaderTablet);
                }

                // todo: accurate handling of locked flag
                ReplyLocked = (reset ? false : ReplyLocked) || record.GetLocked();
                ReplyLockedFor = reset ? record.GetLockedFor() : Max(ReplyLockedFor, record.GetLockedFor());

                ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusOk, &ReplyStatus, cookie, reset);
            }
            break;
        }
        // NOTE: replicas currently reply with ERROR when there is no data for the tablet
        case NKikimrProto::ERROR:
        case NKikimrProto::NODATA:
            ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusNoInfo, &ReplyStatus, cookie, false);
            break;
        default:
            Y_ABORT();
        }

        for (ui32 i = 0, end = record.FollowerSize(); i < end; ++i) {
            Followers[ActorIdFromProto(record.GetFollower(i))] = ActorIdFromProto(record.GetFollowerTablet(i));
        }
    }

    template<typename TEv>
    void PrepareInit(TEv *ev) {
        TabletID = ev->TabletID;
        Cookie = ev->Cookie;
        ProxyOptions = ev->ProxyOptions;
        SelectRequestReplicas(Info.Get());
    }

    // request setup

    void HandleInit(TEvStateStorage::TEvLookup::TPtr &ev) {
        TEvStateStorage::TEvLookup *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ringGroup:" << RingGroupIndex << " ev: " << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg);
        SendRequest([this](ui64 cookie, TActorId /*replica*/) { return new TEvStateStorage::TEvReplicaLookup(TabletID, cookie, Info->ClusterStateGeneration, Info->ClusterStateGuid); });

        Become(&TThis::StateLookup, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
    }

    void HandleInit(TEvStateStorage::TEvUpdate::TPtr &ev) {
        TEvStateStorage::TEvUpdate *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ringGroup:" << RingGroupIndex << " ev: " << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg);

        SuggestedLeader = msg->ProposedLeader;
        SuggestedLeaderTablet = msg->ProposedLeaderTablet;
        SuggestedGeneration = msg->ProposedGeneration;
        SuggestedStep = msg->ProposedStep;

        TCloneUpdateEventOp op(msg, Info->ClusterStateGeneration, Info->ClusterStateGuid);
        SendRequest(op);
        Become(&TThis::StateUpdate, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
    }

    void HandleInit(TEvStateStorage::TEvLock::TPtr &ev) {
        TEvStateStorage::TEvLock *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ringGroup:" << RingGroupIndex << " ev: " << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg);

        SuggestedLeader = msg->ProposedLeader;
        SuggestedGeneration = msg->ProposedGeneration;
        SuggestedStep = 0;

        TCloneLockEventOp op(msg, Info->ClusterStateGeneration, Info->ClusterStateGuid);
        SendRequest(op);
        Become(&TThis::StateUpdate, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
    }

    // lookup handling

    void HandleLookupTimeout() {
        BLOG_D("ProxyRequest::HandleLookupTimeout");
        switch (ReplyStatus) {
        case TStateStorageInfo::TSelection::StatusUnknown:
            ReplyAndDie(NKikimrProto::TIMEOUT);
            return;
        case TStateStorageInfo::TSelection::StatusOk:
            ReplyAndDie(NKikimrProto::OK);
            return;
        case TStateStorageInfo::TSelection::StatusNoInfo:
            ReplyAndDie(NKikimrProto::NODATA);
            return;
        case TStateStorageInfo::TSelection::StatusOutdated:
            ReplyAndDie(NKikimrProto::RACE);
            return;
        case TStateStorageInfo::TSelection::StatusUnavailable:
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }
        Y_DEBUG_ABORT_UNLESS(false);
        PassAway();
    }

    void CheckLookupReply() {
        const ui32 majority = (Replicas / 2 + 1);
        const bool allowReply = ProxyOptions.SigWaitMode == ProxyOptions.SigNone
            || (ProxyOptions.SigWaitMode == ProxyOptions.SigAsync && SignaturesMerged >= majority)
            || RepliesMerged == Replicas;

        if (allowReply) {
            switch (ReplyStatus) {
            case TStateStorageInfo::TSelection::StatusUnknown:
                return; // not yet ready, do nothing
            case TStateStorageInfo::TSelection::StatusOk:
                ReplyAndSig(NKikimrProto::OK);
                return;
            case TStateStorageInfo::TSelection::StatusNoInfo:
                if (RepliesMerged == Replicas) { // for negative response always waits for full reply set to avoid herding of good replicas by fast retry cycle
                    ReplyAndSig(NKikimrProto::NODATA);
                }
                return;
            case TStateStorageInfo::TSelection::StatusOutdated:
                ReplyAndSig(NKikimrProto::RACE);
                return;
            case TStateStorageInfo::TSelection::StatusUnavailable:
                ReplyAndSig(NKikimrProto::ERROR);
                return;
            }
        }
    }

    void HandleLookup(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        const ui32 node = ev->Get()->NodeId;
        MergeNodeError(node);
        CheckLookupReply();
    }

    void HandleLookup(TEvents::TEvUndelivered::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        MergeConnectionError(ev->Cookie);
        CheckLookupReply();
    }

    void HandleLookup(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();
        MergeReply(ev->Sender, msg);
        CheckLookupReply();
    }

    // update handling

    void HandleUpdateTimeout() {
        BLOG_D("ProxyRequest::HandleUpdateTimeout ringGroup:" << RingGroupIndex);
        switch (ReplyStatus) {
        case TStateStorageInfo::TSelection::StatusUnknown:
            ReplyAndDie(NKikimrProto::TIMEOUT);
            return;
        case TStateStorageInfo::TSelection::StatusOk:
            {
                const bool race = (ReplyLeader != SuggestedLeader || ReplyGeneration != SuggestedGeneration);
                const NKikimrProto::EReplyStatus status = race ? NKikimrProto::RACE : NKikimrProto::OK;
                ReplyAndDie(status);
            }
            return;
        case TStateStorageInfo::TSelection::StatusNoInfo:
        case TStateStorageInfo::TSelection::StatusUnavailable:
            // Note: StatusNoInfo shouldn't really happen for update queries
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        case TStateStorageInfo::TSelection::StatusOutdated:
            ReplyAndDie(NKikimrProto::RACE);
            return;
        }
        Y_DEBUG_ABORT_UNLESS(false);
        PassAway();
    }

    void CheckUpdateReply() {
        const bool allowReply = ProxyOptions.SigWaitMode != ProxyOptions.SigSync || RepliesMerged == Replicas;

        if (allowReply) {
            switch (ReplyStatus) {
            case TStateStorageInfo::TSelection::StatusUnknown:
                return;
            case TStateStorageInfo::TSelection::StatusOk:
            {
                const bool race = (ReplyLeader != SuggestedLeader || ReplyGeneration != SuggestedGeneration); // step overrun is consumed
                const NKikimrProto::EReplyStatus status = race ? NKikimrProto::RACE : NKikimrProto::OK;
                ReplyAndSig(status);
            }
            return;
            case TStateStorageInfo::TSelection::StatusNoInfo:
            case TStateStorageInfo::TSelection::StatusUnavailable:
                // Note: StatusNoInfo shouldn't really happen for update queries
                ReplyAndSig(NKikimrProto::ERROR);
                return;
            case TStateStorageInfo::TSelection::StatusOutdated:
                ReplyAndSig(NKikimrProto::RACE);
                return;
            }
        }
    }

    void HandleUpdate(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        const ui32 node = ev->Get()->NodeId;
        MergeNodeError(node);
        CheckUpdateReply();
    }

    void HandleUpdate(TEvents::TEvUndelivered::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        MergeConnectionError(ev->Cookie);
        CheckUpdateReply();
    }

    void HandleUpdate(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());
        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();
        MergeReply(ev->Sender, msg);
        CheckUpdateReply();
    }

    // async wait for full signature

    void MergeSigNodeError(ui32 node) {
        for (ui32 i = 0; i < ReplicaSelection->Sz; ++i) {
            const auto replicaId = ReplicaSelection->SelectedReplicas[i];
            if (replicaId.NodeId() == node) {
                if (!Signature.HasReplicaSignature(replicaId) && UndeliveredReplicas.insert(replicaId).second) {
                    ++RepliesAfterReply;
                }
            }
        }
    }

    void UpdateSigFor(ui64 cookie, ui64 sig) {
        Y_ABORT_UNLESS(cookie < Replicas);
        const auto replicaId = ReplicaSelection->SelectedReplicas[cookie];
        if ((sig == Max<ui64>() && UndeliveredReplicas.insert(replicaId).second) || !Signature.HasReplicaSignature(replicaId)) {
            if (sig != Max<ui64>()) {
                Signature.SetReplicaSignature(replicaId, sig);
            }
            ++RepliesAfterReply;
            ++SignaturesMerged;

            if (RepliesMerged + RepliesAfterReply == Replicas) {
                Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature));
                return PassAway();
            }
        }
    }

    void HandleUpdateSig(TEvents::TEvUndelivered::TPtr &ev) {
        const ui64 cookie = ev->Cookie;
        BLOG_D("ProxyRequest::HandleUpdateSig undelivered ringGroup:" << RingGroupIndex << " for: " << cookie);
        return UpdateSigFor(cookie, Max<ui64>());
    }

    void HandleUpdateSig(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const ui32 node = ev->Get()->NodeId;
        BLOG_D("ProxyRequest::HandleUpdateSig ringGroup:" << RingGroupIndex << " node disconnected: " << node);
        MergeSigNodeError(node);

        if (RepliesMerged + RepliesAfterReply == Replicas) {
            Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature));
            return PassAway();
        }
    }

    void HandleUpdateSig(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdateSig ringGroup:" << RingGroupIndex << " ev: " << ev->Get()->ToString());

        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();

        CheckConfigVersion(ev->Sender, msg);

        const ui64 cookie = msg->Record.GetCookie();
        Y_ABORT_UNLESS(cookie < Replicas);
        const auto replicaId = ReplicaSelection->SelectedReplicas[cookie];
        if (Signature.HasReplicaSignature(replicaId)) {
            BLOG_ERROR("TStateStorageProxyRequest::HandleUpdateSig duplicated TEvReplicaInfo cookie:" << cookie
                << " replica:" << replicaId << " signature:" << Signature.GetReplicaSignature(replicaId) << " ev: " << ev->ToString());
            return;
        }
        UndeliveredReplicas.erase(replicaId);
        return UpdateSigFor(cookie, msg->Record.GetSignature());
    }

    void HandleUpdateSigTimeout() {
        BLOG_D("ProxyRequest::HandleUpdateSigTimeout ringGroup:" << RingGroupIndex << " RepliesAfterReply# " << (ui32)RepliesAfterReply);
        if (RepliesAfterReply > 0)
            Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY_REQUEST;
    }

    TStateStorageProxyRequest(const TIntrusivePtr<TStateStorageInfo> &info, ui32 ringGroupIndex, bool notifyRingGroupProxy = true)
        : TActor(&TThis::StateInit)
        , Info(info)
        , UseInterconnectSubscribes(true)
        , TabletID(0)
        , Cookie(0)
        , SuggestedGeneration(0)
        , SuggestedStep(0)
        , Replicas(0)
        , ReplyStatus(TStateStorageInfo::TSelection::StatusUnknown)
        , RepliesMerged(0)
        , RepliesAfterReply(0)
        , SignaturesMerged(0)
        , ReplyGeneration(0)
        , ReplyStep(0)
        , ReplyLocked(false)
        , ReplyLockedFor(0)
        , RingGroupIndex(ringGroupIndex)
        , NotifyRingGroupProxy(notifyRingGroupProxy)
    {}

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvLookup, HandleInit);
            hFunc(TEvStateStorage::TEvUpdate, HandleInit);
            hFunc(TEvStateStorage::TEvLock, HandleInit);
            default:
                BLOG_W("ProxyRequest::StateInit unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                break;
        }
    }

    // main lookup

    STATEFN(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, HandleLookup);
            hFunc(TEvents::TEvUndelivered, HandleLookup);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleLookup);
            cFunc(TEvents::TSystem::Wakeup, HandleLookupTimeout);
            default:
                BLOG_W("ProxyRequest::StateLookup unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                break;
        }
    }

    // both update and lock
    STATEFN(StateUpdate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, HandleUpdate);
            hFunc(TEvents::TEvUndelivered, HandleUpdate);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleUpdate);
            cFunc(TEvents::TSystem::Wakeup, HandleUpdateTimeout);
            default:
                BLOG_W("ProxyRequest::StateUpdate unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                break;
        }
    }

    // already replied, async signature update
    STATEFN(StateUpdateSig) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, HandleUpdateSig);
            hFunc(TEvents::TEvUndelivered, HandleUpdateSig);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleUpdateSig);
            cFunc(TEvents::TSystem::Wakeup, HandleUpdateSigTimeout);
            default:
                BLOG_W("ProxyRequest::StateUpdateSig unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                break;
        }
    }
};

class TStateStorageRingGroupProxyRequest : public TActorBootstrapped<TStateStorageRingGroupProxyRequest> {
    TIntrusivePtr<TStateStorageInfo> Info;
    THashMap<TActorId, ui32> RingGroupActors;
    THashMap<ui32, TActorId> RingGroupActorsByIndex;

    TActorId Source;
    THashSet<TActorId> Replies;
    ui32 RingGroupPassAwayCounter;
    bool WaitAllReplies;

    ui64 TabletID;
    ui64 Cookie;
    TActorId CurrentLeader;
    TActorId CurrentLeaderTablet;
    ui32 CurrentGeneration;
    ui32 CurrentStep;
    bool Locked;
    ui64 LockedFor;
    TMap<TActorId, TActorId> Followers;

    TEvStateStorage::TSignature Signature;


    void HandleInit(TEvStateStorage::TEvLookup::TPtr &ev) {
        TEvStateStorage::TEvLookup *msg = ev->Get();
        Source = ev->Sender;
        WaitAllReplies = msg->ProxyOptions.SigWaitMode != msg->ProxyOptions.SigNone;
        BLOG_D("RingGroupProxyRequest::HandleInit ev: " << msg->ToString());
        for (ui32 ringGroupIndex = 0; ringGroupIndex < Info->RingGroups.size(); ++ringGroupIndex) {
            const auto &ringGroup = Info->RingGroups[ringGroupIndex];
            if ((!WaitAllReplies && ringGroup.WriteOnly) || ringGroup.State == ERingGroupState::DISCONNECTED
                || ringGroup.State == ERingGroupState::NOT_SYNCHRONIZED) {
                continue;
            }
            auto actorId = RegisterWithSameMailbox(new TStateStorageProxyRequest(Info, ringGroupIndex));
            RingGroupActors[actorId] = ringGroupIndex;
            RingGroupActorsByIndex[ringGroupIndex] = actorId;
            Send(actorId, new TEvStateStorage::TEvLookup(*msg));
        }
    }

    template<class T>
    void HandleInit(T::TPtr &ev) {
        T *msg = ev->Get();
        Source = ev->Sender;
        WaitAllReplies = true;
        BLOG_D("RingGroupProxyRequest::HandleInit ev: " << msg->ToString());
        for (ui32 ringGroupIndex = 0; ringGroupIndex < Info->RingGroups.size(); ++ringGroupIndex) {
            const auto &ringGroup = Info->RingGroups[ringGroupIndex];
            if (ringGroup.State == ERingGroupState::DISCONNECTED || ringGroup.State == ERingGroupState::NOT_SYNCHRONIZED) {
                continue;
            }
            auto actorId = RegisterWithSameMailbox(new TStateStorageProxyRequest(Info, ringGroupIndex));
            RingGroupActors[actorId] = ringGroupIndex;
            RingGroupActorsByIndex[ringGroupIndex] = actorId;
            Send(actorId, new T(*msg));
        }
    }

    void ProcessEvInfo(ui32 ringGroupIdx, TEvStateStorage::TEvInfo *msg) {
        if (!Info->RingGroups[ringGroupIdx].WriteOnly && Info->RingGroups[ringGroupIdx].State == ERingGroupState::PRIMARY) {
            // TODO: if ringGroups return different results? Y_ABORT("StateStorage ring groups are not synchronized");
            TabletID = msg->TabletID;
            Cookie = msg->Cookie;
            CurrentLeader = msg->CurrentLeader;
            CurrentLeaderTablet = msg->CurrentLeaderTablet;
            CurrentGeneration = msg->CurrentGeneration;
            CurrentStep = msg->CurrentStep;
            Locked = msg->Locked;
            LockedFor = msg->LockedFor;
            for (auto& [k,v] : msg->Followers) {
                Followers[k] = v;
            }
        }
        Signature.Merge(msg->Signature);
    }

    void Reply(NKikimrProto::EReplyStatus status) {
        auto* msg = new TEvStateStorage::TEvInfo(status, TabletID, Cookie, CurrentLeader, CurrentLeaderTablet, CurrentGeneration, CurrentStep, Locked, LockedFor, Signature, Followers);
        BLOG_D("RingGroupProxyRequest::Reply ev: " << msg->ToString());
        Send(Source, msg);
    }

    bool ShouldReply() {
        bool reply = !WaitAllReplies;
        if(!reply) {
            for(ui32 i : xrange(Info->RingGroups.size())) {
                auto& rg = Info->RingGroups[i];
                if(!rg.WriteOnly && RingGroupActorsByIndex.contains(i) && !Replies.contains(RingGroupActorsByIndex[i])) {
                    return reply;
                }
            }
            return true;
        }
        return reply;
    }

    void HandleResult(TEvStateStorage::TEvInfo::TPtr &ev) {
        TEvStateStorage::TEvInfo *msg = ev->Get();
        Replies.insert(ev->Sender);
        ProcessEvInfo(RingGroupActors[ev->Sender], msg);
        BLOG_D("RingGroupProxyRequest::HandleTEvInfo ev: " << msg->ToString());
        if (ShouldReply()) {
            Reply(msg->Status);
        }
    }

    void HandleResult(TEvStateStorage::TEvUpdateSignature::TPtr &ev) {
        TEvStateStorage::TEvUpdateSignature *msg = ev->Get();
        Signature.Merge(msg->Signature);
        Send(Source, new TEvStateStorage::TEvUpdateSignature(msg->TabletID, Signature));
    }

    void HandleConfigVersion(TEvStateStorage::TEvConfigVersionInfo::TPtr &ev) {
        TEvStateStorage::TEvConfigVersionInfo *msg = ev->Get();
        if (Info->ClusterStateGeneration < msg->ClusterStateGeneration ||
            (Info->ClusterStateGeneration == msg->ClusterStateGeneration && Info->ClusterStateGuid != msg->ClusterStateGuid)) {
            Reply(NKikimrProto::ERROR);
            PassAway();
        }
    }

    void Timeout() {
        PassAway();
    }

    void Handle(TEvStateStorage::TEvRingGroupPassAway::TPtr& /*ev*/) {
        RingGroupPassAwayCounter++;
        if (RingGroupPassAwayCounter >= Info->RingGroups.size()) {
            PassAway();
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY_REQUEST;
    }

    TStateStorageRingGroupProxyRequest(TIntrusivePtr<TStateStorageInfo> info)
        : Info(info)
        , RingGroupPassAwayCounter(0)
    {
    }

    void Bootstrap() {
        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateInit);
    }

    STRICT_STFUNC(StateInit,
        hFunc(TEvStateStorage::TEvLookup, HandleInit);
        hFunc(TEvStateStorage::TEvUpdate, HandleInit<TEvStateStorage::TEvUpdate>);
        hFunc(TEvStateStorage::TEvLock, HandleInit<TEvStateStorage::TEvLock>);

        hFunc(TEvStateStorage::TEvInfo, HandleResult);
        hFunc(TEvStateStorage::TEvUpdateSignature, HandleResult);
        hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        hFunc(TEvStateStorage::TEvRingGroupPassAway, Handle);
        cFunc(TEvents::TSystem::Wakeup, Timeout);
    )

};

class TStateStorageDumpRequest : public TActorBootstrapped<TStateStorageDumpRequest> {
protected:
    const TActorId Sender;
    TIntrusivePtr<TStateStorageInfo> Info;
    TList<TActorId> AllReplicas;
    TAutoPtr<TEvStateStorage::TEvResponseReplicasDumps> Response;
    ui64 UndeliveredCount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TStateStorageDumpRequest(const TActorId &sender, const TIntrusivePtr<TStateStorageInfo> &info)
        : Sender(sender)
        , Info(info)
        , UndeliveredCount(0)
    {}

    void SendResponse() {
        Send(Sender, Response.Release());
        PassAway();
    }

    void Bootstrap() {
        Response = new TEvStateStorage::TEvResponseReplicasDumps();
        AllReplicas = Info->SelectAllReplicas();
        if (!AllReplicas.empty()) {
            Response->ReplicasDumps.reserve(AllReplicas.size());
            for (const TActorId &replica : AllReplicas) {
                Send(replica, new TEvStateStorage::TEvReplicaDumpRequest(), IEventHandle::FlagTrackDelivery);
            }
            Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
            Become(&TThis::StateRequestedDumps);
        } else {
            SendResponse();
        }
    }

    STATEFN(StateRequestedDumps) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaDump, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void OnResponseReceived() {
        if (Response->ReplicasDumps.size() + UndeliveredCount >= AllReplicas.size())
            SendResponse();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &) {
        ++UndeliveredCount;
        OnResponseReceived();
    }

    void Handle(TEvStateStorage::TEvReplicaDump::TPtr &ev) {
        Response->ReplicasDumps.emplace_back(std::make_pair(ev->Sender, ev->Release()));
        OnResponseReceived();
    }

    void Timeout() {
        SendResponse();
    }
};

class TStateStorageDeleteRequest : public TActorBootstrapped<TStateStorageDeleteRequest> {
protected:
    const TActorId Sender;
    TIntrusivePtr<TStateStorageInfo> Info;
    TList<TActorId> AllReplicas;
    ui32 Count;
    ui32 UndeliveredCount;
    ui64 TabletID;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TStateStorageDeleteRequest(const TActorId &sender, const TIntrusivePtr<TStateStorageInfo> &info, ui64 tabletId)
        : Sender(sender)
        , Info(info)
        , Count(0)
        , UndeliveredCount(0)
        , TabletID(tabletId)
    {}

    void SendResponse() {
        Send(Sender, new TEvStateStorage::TEvDeleteResult(TabletID, Count > AllReplicas.size() / 2 ? NKikimrProto::OK : NKikimrProto::ERROR));
        PassAway();
    }

    void Bootstrap() {
        AllReplicas = Info->SelectAllReplicas();
        if (!AllReplicas.empty()) {
            for (const TActorId &replica : AllReplicas) {
                Send(replica, new TEvStateStorage::TEvReplicaDelete(TabletID, Info->ClusterStateGeneration, Info->ClusterStateGuid), IEventHandle::FlagTrackDelivery);
            }
            Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
            Become(&TThis::StateRequestedDelete);
        } else {
            SendResponse();
        }
    }

    STATEFN(StateRequestedDelete) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void OnResponseReceived() {
        if (Count + UndeliveredCount >= AllReplicas.size())
            SendResponse();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &) {
        ++UndeliveredCount;
        OnResponseReceived();
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        ++Count;
        auto &record = ev->Get()->Record;
        const ui64 clusterStateGeneration = record.GetClusterStateGeneration();
        const ui64 clusterStateGuid = record.GetClusterStateGuid();
        if (Info->ClusterStateGeneration < clusterStateGeneration ||
            (Info->ClusterStateGeneration == clusterStateGeneration && Info->ClusterStateGuid != clusterStateGuid)) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new NStorage::TEvNodeWardenNotifyConfigMismatch(ev->Sender.NodeId(), clusterStateGeneration, clusterStateGuid));
            SendResponse();
            return;
        }
        OnResponseReceived();
    }

    void Timeout() {
        SendResponse();
    }
};

class TStateStorageProxy : public TActor<TStateStorageProxy> {
    TIntrusivePtr<TStateStorageInfo> Info;
    TIntrusivePtr<TStateStorageInfo> BoardInfo;
    TIntrusivePtr<TStateStorageInfo> SchemeBoardInfo;

    THashMap<std::tuple<TActorId, ui64>, std::tuple<ui64, TIntrusivePtr<TStateStorageInfo> TThis::*>> Subscriptions;
    THashSet<std::tuple<TActorId, ui64>> SchemeBoardSubscriptions;

    void Handle(TEvStateStorage::TEvRequestReplicasDumps::TPtr &ev) {
        TActivationContext::Register(new TStateStorageDumpRequest(ev->Sender, Info));
    }

    void Handle(TEvStateStorage::TEvDelete::TPtr &ev) {
        TActivationContext::Register(new TStateStorageDeleteRequest(ev->Sender, Info, ev->Get()->TabletID));
    }

    void SpreadCleanupRequest(const TStateStorageInfo::TSelection &selection, ui64 tabletId, TActorId proposedLeader) {
        for (ui32 i = 0; i < selection.Sz; ++i)
            Send(selection.SelectedReplicas[i], new TEvStateStorage::TEvReplicaCleanup(tabletId, proposedLeader, Info->ClusterStateGeneration, Info->ClusterStateGuid));
    }

    void Handle(TEvStateStorage::TEvCleanup::TPtr &ev) {
        const auto *msg = ev->Get();
        for (size_t ringGroupIdx = 0; ringGroupIdx < Info->RingGroups.size(); ++ringGroupIdx) {
            THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
            Info->SelectReplicas(msg->TabletID, selection.Get(), ringGroupIdx);
            SpreadCleanupRequest(*selection, msg->TabletID, msg->ProposedLeader);
        }
    }

    void Handle(TEvStateStorage::TEvResolveReplicas::TPtr &ev) {
        if (ev->Get()->Subscribe) {
            Subscriptions.try_emplace(std::make_tuple(ev->Sender, ev->Cookie), ev->Get()->TabletID, &TThis::Info);
        }
        ResolveReplicas(ev, ev->Get()->TabletID, Info);
    }

    void HandleUnsubscribe(STATEFN_SIG) {
        const auto key = std::make_tuple(ev->Sender, ev->Cookie);
        Subscriptions.erase(key);
        SchemeBoardSubscriptions.erase(key);
    }

    void Handle(TEvStateStorage::TEvResolveBoard::TPtr &ev) {
        if (!BoardInfo) {
            Send(ev->Sender, new TEvStateStorage::TEvResolveReplicasList(), 0, ev->Cookie);
            return;
        }

        const auto *msg = ev->Get();
        const ui64 pathHash = CityHash64(msg->Path);

        if (msg->Subscribe) {
            Subscriptions.try_emplace(std::make_tuple(ev->Sender, ev->Cookie), pathHash, &TThis::BoardInfo);
        }

        ResolveReplicas(ev, pathHash, BoardInfo);
    }

    void Handle(TEvStateStorage::TEvResolveSchemeBoard::TPtr &ev) {
        if (!SchemeBoardInfo) {
            Send(ev->Sender, new TEvStateStorage::TEvResolveReplicasList(), 0, ev->Cookie);
            return;
        }

        const auto *msg = ev->Get();

        ui64 fakeTabletId;
        switch (msg->KeyType) {
        case TEvStateStorage::TEvResolveSchemeBoard::KeyTypePath:
            fakeTabletId = CityHash64(msg->Path);
            break;

        case TEvStateStorage::TEvResolveSchemeBoard::KeyTypePathId:
            fakeTabletId = msg->PathId.Hash();
            break;

        default:
            Y_ABORT("unreachable");
        }

        if (ev->Get()->Subscribe) {
            Subscriptions.try_emplace(std::make_tuple(ev->Sender, ev->Cookie), fakeTabletId, &TThis::SchemeBoardInfo);
        }

        ResolveReplicas(ev, fakeTabletId, SchemeBoardInfo);
    }

    void Handle(TEvStateStorage::TEvListSchemeBoard::TPtr &ev) {
        if (ev->Get()->Subscribe) {
            SchemeBoardSubscriptions.emplace(std::make_tuple(ev->Sender, ev->Cookie));
        }
        Send(ev->Sender, new TEvStateStorage::TEvListSchemeBoardResult(SchemeBoardInfo), 0, ev->Cookie);
    }

    void Handle(TEvStateStorage::TEvListStateStorage::TPtr &ev) {
        Send(ev->Sender, new TEvStateStorage::TEvListStateStorageResult(Info), 0, ev->Cookie);
    }

    void Handle(TEvStateStorage::TEvUpdateGroupConfig::TPtr &ev) {
        auto *msg = ev->Get();
        Info = msg->GroupConfig;
        BoardInfo = msg->BoardConfig;
        SchemeBoardInfo = msg->SchemeBoardConfig;

        for (const auto& [key, value] : Subscriptions) {
            const auto& [sender, cookie] = key;
            const auto& [tabletId, ptr] = value;
            struct { TActorId Sender; ui64 Cookie; } ev{sender, cookie};
            ResolveReplicas(&ev, tabletId, this->*ptr);
        }
        for (const auto& [sender, cookie] : SchemeBoardSubscriptions) {
            Send(sender, new TEvStateStorage::TEvListSchemeBoardResult(SchemeBoardInfo), 0, cookie);
        }
    }

    void Handle(TEvStateStorage::TEvRingGroupPassAway::TPtr& /*ev*/) {
        // Do nothng
    }

    template<typename TEventPtr>
    void ResolveReplicas(const TEventPtr &ev, ui64 tabletId, const TIntrusivePtr<TStateStorageInfo> &info) const {
        TAutoPtr<TEvStateStorage::TEvResolveReplicasList> reply(new TEvStateStorage::TEvResolveReplicasList());
        reply->ClusterStateGeneration = info->ClusterStateGeneration;
        reply->ClusterStateGuid = info->ClusterStateGuid;
        reply->ReplicaGroups.reserve(info->RingGroups.size());
        for (ui32 ringGroupIndex : xrange(info->RingGroups.size())) {
            if (info->RingGroups[ringGroupIndex].State == ERingGroupState::DISCONNECTED) {
                continue;
            }
            THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
            info->SelectReplicas(tabletId, selection.Get(), ringGroupIndex);
            reply->ReplicaGroups.resize(reply->ReplicaGroups.size() + 1);
            auto &rg = reply->ReplicaGroups.back();
            rg.WriteOnly = info->RingGroups[ringGroupIndex].WriteOnly;
            rg.State = info->RingGroups[ringGroupIndex].State;
            rg.Replicas.insert(rg.Replicas.end(), selection->SelectedReplicas.Get(), selection->SelectedReplicas.Get() + selection->Sz);
        }
        reply->ConfigContentHash = info->ContentHash();
        Send(ev->Sender, reply.Release(), 0, ev->Cookie);
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY;
    }

    TStateStorageProxy(
            const TIntrusivePtr<TStateStorageInfo> &info,
            const TIntrusivePtr<TStateStorageInfo> &boardInfo,
            const TIntrusivePtr<TStateStorageInfo> &schemeBoardInfo)
        : TActor(&TThis::StateInit)
        , Info(info)
        , BoardInfo(boardInfo)
        , SchemeBoardInfo(schemeBoardInfo)
    {}

    STATEFN(StateInit) {
        BLOG_TRACE("Proxy::StateInit ev type# " << ev->GetTypeRewrite() << " event: "
            << ev->ToString());

        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvRequestReplicasDumps, Handle);
            hFunc(TEvStateStorage::TEvDelete, Handle);
            hFunc(TEvStateStorage::TEvCleanup, Handle);
            hFunc(TEvStateStorage::TEvResolveReplicas, Handle);
            hFunc(TEvStateStorage::TEvResolveBoard, Handle);
            hFunc(TEvStateStorage::TEvResolveSchemeBoard, Handle);
            hFunc(TEvStateStorage::TEvListSchemeBoard, Handle);
            hFunc(TEvStateStorage::TEvListStateStorage, Handle);
            hFunc(TEvStateStorage::TEvUpdateGroupConfig, Handle);
            hFunc(TEvStateStorage::TEvRingGroupPassAway, Handle);
            fFunc(TEvents::TSystem::Unsubscribe, HandleUnsubscribe);
        default:
            if (Info->RingGroups.size() > 1)
                TActivationContext::Forward(ev, RegisterWithSameMailbox(new TStateStorageRingGroupProxyRequest(Info)));
            else
                TActivationContext::Forward(ev, RegisterWithSameMailbox(new TStateStorageProxyRequest(Info, 0, false)));
            break;
        }
    }
};

class TStateStorageProxyStub : public TActor<TStateStorageProxyStub> {
    std::deque<std::unique_ptr<IEventHandle>> PendingQ;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY_STUB;
    }

    TStateStorageProxyStub()
        : TActor(&TThis::StateFunc)
    {}

    STFUNC(StateFunc) {
        if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
            for (auto& q : PendingQ) {
                TActivationContext::Send(q->Forward(ev->Sender));
            }
            PassAway();
        } else {
            PendingQ.emplace_back(ev.Release());
        }
    }
};

IActor* CreateStateStorageProxy(
    const TIntrusivePtr<TStateStorageInfo> &info,
    const TIntrusivePtr<TStateStorageInfo> &board,
    const TIntrusivePtr<TStateStorageInfo> &schemeBoard
) {
    return new TStateStorageProxy(info, board, schemeBoard);
}

IActor* CreateStateStorageProxyStub() {
    return new TStateStorageProxyStub();
}

}
