#include "statestorage_impl.h"
#include "tabletid.h"

#include <ydb/core/base/compile_time_flags.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/helpers/flow_controlled_queue.h>

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
    TIntrusivePtr<TStateStorageInfo> FlowControlledInfo;

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
    TArrayHolder<ui64> Signature;

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

    void SelectRequestReplicas(TStateStorageInfo *info) {
        THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
        info->SelectReplicas(TabletID, selection.Get());
        Replicas = selection->Sz;
        ReplicaSelection = std::move(selection);
        Signature.Reset(new ui64[Replicas]);
        Fill(Signature.Get(), Signature.Get() + Replicas, 0);
    }

    template<typename T>
    void SendRequest(const T &op) {
        Y_ABORT_UNLESS(ReplicaSelection && ReplicaSelection->SelectedReplicas && ReplicaSelection->Sz);

        ui64 cookie = 0;
        const ui32 sendFlags = IEventHandle::FlagTrackDelivery | (UseInterconnectSubscribes ? IEventHandle::FlagSubscribeOnSession : 0);
        for (ui32 i = 0; i < ReplicaSelection->Sz; ++i, ++cookie)
            Send(ReplicaSelection->SelectedReplicas[i], op(cookie), sendFlags, cookie);
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

        TActor::PassAway();
    }

    void Reply(NKikimrProto::EReplyStatus status) {
        Send(Source, new TEvStateStorage::TEvInfo(status, TabletID, Cookie, ReplyLeader, ReplyLeaderTablet, ReplyGeneration, ReplyStep, ReplyLocked, ReplyLockedFor, Signature.Get(), Replicas, Followers));
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
        mutable ui32 Idx;

        TCloneUpdateEventOp(const TEvStateStorage::TEvUpdate *ev)
            : Ev(ev)
            , UpdateLeaderTablet(!!ev->ProposedLeaderTablet)
            , Idx(0)
        {}

        IEventBase* operator()(ui64 cookie) const {
            THolder<TEvStateStorage::TEvReplicaUpdate> req(new TEvStateStorage::TEvReplicaUpdate());
            req->Record.SetTabletID(Ev->TabletID);
            ActorIdToProto(Ev->ProposedLeader, req->Record.MutableProposedLeader());

            if (UpdateLeaderTablet)
                ActorIdToProto(Ev->ProposedLeaderTablet, req->Record.MutableProposedLeaderTablet());

            req->Record.SetProposedGeneration(Ev->ProposedGeneration);
            req->Record.SetProposedStep(Ev->ProposedStep);
            req->Record.SetSignature(Ev->Signature[Idx]);

            ++Idx;
            req->Record.SetCookie(cookie);

            return req.Release();
        }
    };

    struct TCloneLockEventOp {
        const TEvStateStorage::TEvLock * const Ev;
        mutable ui32 Idx;

        TCloneLockEventOp(const TEvStateStorage::TEvLock *ev)
            : Ev(ev)
            , Idx(0)
        {}

        IEventBase* operator()(ui64 cookie) const {
            THolder<TEvStateStorage::TEvReplicaLock> req(new TEvStateStorage::TEvReplicaLock());
            req->Record.SetTabletID(Ev->TabletID);
            ActorIdToProto(Ev->ProposedLeader, req->Record.MutableProposedLeader());
            req->Record.SetProposedGeneration(Ev->ProposedGeneration);
            req->Record.SetSignature(Ev->Signature[Idx]);

            ++Idx;
            req->Record.SetCookie(cookie);

            return req.Release();
        }
    };

    void MergeNodeError(ui32 node) {
        ui64 cookie = 0;
        for (ui32 i = 0; i < ReplicaSelection->Sz; ++i, ++cookie) {
            const ui32 replicaNode = ReplicaSelection->SelectedReplicas[i].NodeId();
            if (replicaNode == node)
                MergeConnectionError(cookie);
        }
    }

    void MergeConnectionError(ui64 cookie) {
        Y_ABORT_UNLESS(cookie < Replicas);

        if (Signature[cookie] == 0) {
            Signature[cookie] = Max<ui64>();
            ++RepliesMerged;

            ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusNoInfo, &ReplyStatus, cookie, false);
        }
    }

    void MergeReply(TEvStateStorage::TEvReplicaInfo *ev) {
        const auto &record = ev->Record;
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        const ui64 cookie = record.GetCookie();

        Y_ABORT_UNLESS(cookie < Replicas);
        Y_ABORT_UNLESS(Signature[cookie] == 0 || Signature[cookie] == Max<ui64>());
        Signature[cookie] = ev->Record.GetSignature();
        ++RepliesMerged;
        ++SignaturesMerged;

        if (status == NKikimrProto::OK) {
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
        } else if (status == NKikimrProto::ERROR) {
            ReplicaSelection->MergeReply(TStateStorageInfo::TSelection::StatusNoInfo, &ReplyStatus, cookie, false);
        } else {
            Y_ABORT();
        }

        for (ui32 i = 0, end = record.FollowerSize(); i < end; ++i) {
            Followers[ActorIdFromProto(record.GetFollower(i))] = ActorIdFromProto(record.GetFollowerTablet(i));
        }
    }

    template<typename TEv>
    void PrepareInit(TEv *ev, bool allowFlowControlled) {
        TabletID = ev->TabletID;
        Cookie = ev->Cookie;
        ProxyOptions = ev->ProxyOptions;

        if (allowFlowControlled && FlowControlledInfo.Get() && KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP)
            SelectRequestReplicas(FlowControlledInfo.Get());
        else
            SelectRequestReplicas(Info.Get());
    }

    // request setup

    void HandleInit(TEvStateStorage::TEvLookup::TPtr &ev) {
        TEvStateStorage::TEvLookup *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ev: " << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg, true);
        SendRequest([this](ui64 cookie) { return new TEvStateStorage::TEvReplicaLookup(TabletID, cookie); });

        Become(&TThis::StateLookup, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
    }

    void HandleInit(TEvStateStorage::TEvUpdate::TPtr &ev) {
        TEvStateStorage::TEvUpdate *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ev: %s" << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg, false);

        SuggestedLeader = msg->ProposedLeader;
        SuggestedLeaderTablet = msg->ProposedLeaderTablet;
        SuggestedGeneration = msg->ProposedGeneration;
        SuggestedStep = msg->ProposedStep;

        if (msg->SignatureSz == Replicas) {
            TCloneUpdateEventOp op(msg);
            SendRequest(op);
            Become(&TThis::StateUpdate, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
        } else {
            // wrong signature, reply with no-info (but correct signature count)
            ReplyAndDie(NKikimrProto::ERROR);
        }
    }

    void HandleInit(TEvStateStorage::TEvLock::TPtr &ev) {
        TEvStateStorage::TEvLock *msg = ev->Get();
        BLOG_D("ProxyRequest::HandleInit ev: " << msg->ToString());
        Source = ev->Sender;

        PrepareInit(msg, false);

        SuggestedLeader = msg->ProposedLeader;
        SuggestedGeneration = msg->ProposedGeneration;
        SuggestedStep = 0;

        if (msg->SignatureSz == Replicas) {
            TCloneLockEventOp op(msg);
            SendRequest(op);
            Become(&TThis::StateUpdate, TDuration::MicroSeconds(StateStorageRequestTimeout), new TEvents::TEvWakeup());
        } else { // wrong signature, reply with no-info (but correct signature count)
            ReplyAndDie(NKikimrProto::ERROR);
        }
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
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        case TStateStorageInfo::TSelection::StatusOutdated:
            ReplyAndDie(NKikimrProto::RACE);
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
                if (RepliesMerged == Replicas) // for negative response always waits for full reply set to avoid herding of good replicas by fast retry cycle
                    ReplyAndSig(NKikimrProto::ERROR);
                return;
            case TStateStorageInfo::TSelection::StatusOutdated:
                ReplyAndSig(NKikimrProto::RACE);
                return;
            }
        }
    }

    void HandleLookup(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ev: " << ev->Get()->ToString());
        const ui32 node = ev->Get()->NodeId;
        MergeNodeError(node);
        CheckLookupReply();
    }

    void HandleLookup(TEvents::TEvUndelivered::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ev: " << ev->Get()->ToString());
        const ui64 cookie = ev->Cookie;
        MergeConnectionError(cookie);
        CheckLookupReply();
    }

    void HandleLookup(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleLookup ev: " << ev->Get()->ToString());
        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();
        MergeReply(msg);
        CheckLookupReply();
    }

    // update handling

    void HandleUpdateTimeout() {
        BLOG_D("ProxyRequest::HandleUpdateTimeout");
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
                // should not happens for update queries
                ReplyAndSig(NKikimrProto::ERROR);
                return;
            case TStateStorageInfo::TSelection::StatusOutdated:
                ReplyAndSig(NKikimrProto::RACE);
                return;
            }
        }
    }

    void HandleUpdate(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ev: " << ev->Get()->ToString());
        const ui32 node = ev->Get()->NodeId;
        MergeNodeError(node);
        CheckUpdateReply();
    }

    void HandleUpdate(TEvents::TEvUndelivered::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ev: " << ev->Get()->ToString());
        const ui64 cookie = ev->Cookie;
        MergeConnectionError(cookie);
        CheckUpdateReply();
    }

    void HandleUpdate(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdate ev: " << ev->Get()->ToString());
        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();
        MergeReply(msg);
        CheckUpdateReply();
    }

    // async wait for full signature

    void MergeSigNodeError(ui32 node) {
        for (ui32 i = 0; i < ReplicaSelection->Sz; ++i) {
            const ui32 replicaNode = ReplicaSelection->SelectedReplicas[i].NodeId();
            if (replicaNode == node) {
                if (Signature[i] == 0) {
                    Signature[i] = Max<ui64>();
                    ++RepliesAfterReply;
                }
            }
        }
    }

    void UpdateSigFor(ui64 cookie, ui64 sig) {
        Y_ABORT_UNLESS(cookie < Replicas);

        if (Signature[cookie] == 0) {
            Signature[cookie] = sig;
            ++RepliesAfterReply;
            ++SignaturesMerged;

            if (RepliesMerged + RepliesAfterReply == Replicas) {
                Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature.Get(), Replicas));
                return PassAway();
            }
        }
    }

    void HandleUpdateSig(TEvents::TEvUndelivered::TPtr &ev) {
        const ui64 cookie = ev->Cookie;
        BLOG_D("ProxyRequest::HandleUpdateSig undelivered for: " << cookie);

        return UpdateSigFor(cookie, Max<ui64>());
    }

    void HandleUpdateSig(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const ui32 node = ev->Get()->NodeId;
        BLOG_D("ProxyRequest::HandleUpdateSig node disconnected: " << node);

        MergeSigNodeError(node);

        if (RepliesMerged + RepliesAfterReply == Replicas) {
            Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature.Get(), Replicas));
            return PassAway();
        }
    }

    void HandleUpdateSig(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        BLOG_D("ProxyRequest::HandleUpdateSig ev: " << ev->Get()->ToString());

        TEvStateStorage::TEvReplicaInfo *msg = ev->Get();
        const ui64 cookie = msg->Record.GetCookie();
        Y_ABORT_UNLESS(cookie < Replicas);
        Y_ABORT_UNLESS(Signature[cookie] == 0 || Signature[cookie] == Max<ui64>());

        return UpdateSigFor(cookie, msg->Record.GetSignature());
    }

    void HandleUpdateSigTimeout() {
        BLOG_D("ProxyRequest::HandleUpdateSigTimeout RepliesAfterReply# " << (ui32)RepliesAfterReply);
        if (RepliesAfterReply > 0)
            Send(Source, new TEvStateStorage::TEvUpdateSignature(TabletID, Signature.Get(), Replicas));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY_REQUEST;
    }

    TStateStorageProxyRequest(const TIntrusivePtr<TStateStorageInfo> &info, const TIntrusivePtr<TStateStorageInfo> &flowControlledInfo)
        : TActor(&TThis::StateInit)
        , Info(info)
        , FlowControlledInfo(flowControlledInfo)
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
                Send(replica, new TEvStateStorage::TEvReplicaDelete(TabletID), IEventHandle::FlagTrackDelivery);
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

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &) {
        ++Count;
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

    TIntrusivePtr<TStateStorageInfo> FlowControlledInfo;

    TMap<TActorId, TActorId> ReplicaProbes;

    THashMap<std::tuple<TActorId, ui64>, ui64> Subscriptions;

    void Handle(TEvStateStorage::TEvRequestReplicasDumps::TPtr &ev) {
        TActivationContext::Register(new TStateStorageDumpRequest(ev->Sender, Info));
    }

    void Handle(TEvStateStorage::TEvDelete::TPtr &ev) {
        TActivationContext::Register(new TStateStorageDeleteRequest(ev->Sender, Info, ev->Get()->TabletID));
    }

    void SpreadCleanupRequest(const TStateStorageInfo::TSelection &selection, ui64 tabletId, TActorId proposedLeader) {
        for (ui32 i = 0; i < selection.Sz; ++i)
            Send(selection.SelectedReplicas[i], new TEvStateStorage::TEvReplicaCleanup(tabletId, proposedLeader));
    }

    void Handle(TEvStateStorage::TEvCleanup::TPtr &ev) {
        const auto *msg = ev->Get();
        THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
        Info->SelectReplicas(msg->TabletID, selection.Get());
        SpreadCleanupRequest(*selection, msg->TabletID, msg->ProposedLeader);
    }

    void Handle(TEvStateStorage::TEvResolveReplicas::TPtr &ev) {
        if (ev->Get()->Subscribe) {
            Subscriptions.emplace(std::make_tuple(ev->Sender, ev->Cookie), ev->Get()->TabletID);
        }
        ResolveReplicas(ev, ev->Get()->TabletID, Info);
    }

    void HandleUnsubscribe(STATEFN_SIG) {
        Subscriptions.erase(std::make_tuple(ev->Sender, ev->Cookie));
    }

    void Handle(TEvStateStorage::TEvResolveBoard::TPtr &ev) {
        if (!BoardInfo) {
            Send(ev->Sender, new TEvStateStorage::TEvResolveReplicasList(), 0, ev->Cookie);
            return;
        }

        const auto *msg = ev->Get();
        const ui64 pathHash = CityHash64(msg->Path);

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

        ResolveReplicas(ev, fakeTabletId, SchemeBoardInfo);
    }

    void Handle(TEvStateStorage::TEvListSchemeBoard::TPtr &ev) {
        if (!SchemeBoardInfo) {
            Send(ev->Sender, new TEvStateStorage::TEvListSchemeBoardResult(nullptr), 0, ev->Cookie);
            return;
        }

        Send(ev->Sender, new TEvStateStorage::TEvListSchemeBoardResult(SchemeBoardInfo), 0, ev->Cookie);
    }

    void Handle(TEvStateStorage::TEvListStateStorage::TPtr &ev) {
        if (!Info) {
            Send(ev->Sender, new TEvStateStorage::TEvListStateStorageResult(nullptr), 0, ev->Cookie);
            return;
        }

        Send(ev->Sender, new TEvStateStorage::TEvListStateStorageResult(Info), 0, ev->Cookie);
    }

    void Handle(TEvStateStorage::TEvReplicaProbeSubscribe::TPtr &ev) {
        const auto *msg = ev->Get();

        if (!KIKIMR_ALLOW_SSREPLICA_PROBES) {
            Send(ev->Sender, new TEvStateStorage::TEvReplicaProbeConnected(msg->ReplicaId));
            return;
        }

        auto it = ReplicaProbes.find(msg->ReplicaId);
        if (it == ReplicaProbes.end()) {
            Send(ev->Sender, new TEvStateStorage::TEvReplicaProbeDisconnected(msg->ReplicaId));
            return;
        }

        TActivationContext::Send(ev->Forward(it->second));
    }

    void Handle(TEvStateStorage::TEvReplicaProbeUnsubscribe::TPtr &ev) {
        if (!KIKIMR_ALLOW_SSREPLICA_PROBES)
            return;

        const auto *msg = ev->Get();

        auto it = ReplicaProbes.find(msg->ReplicaId);
        if (it != ReplicaProbes.end()) {
            TActivationContext::Send(ev->Forward(it->second));
        }
    }

    void Handle(TEvStateStorage::TEvUpdateGroupConfig::TPtr &ev) {
        auto *msg = ev->Get();
        TIntrusivePtr<TStateStorageInfo> old = Info;

        Info = msg->GroupConfig;
        BoardInfo = msg->BoardConfig;
        SchemeBoardInfo = msg->SchemeBoardConfig;

        RegisterDerivedServices(TlsActivationContext->ExecutorThread.ActorSystem, old.Get());

        for (const auto& [key, tabletId] : Subscriptions) {
            const auto& [sender, cookie] = key;
            struct { TActorId Sender; ui64 Cookie; } ev{sender, cookie};
            ResolveReplicas(&ev, tabletId, Info);
        }
    }

    void RegisterDerivedServices(TActorSystem *sys, const TStateStorageInfo *old) {
        RegisterReplicaProbes(sys);
        RegisterFlowContolled(sys, old);
    }

    void RegisterReplicaProbes(TActorSystem *sys) {
        if (!KIKIMR_ALLOW_SSREPLICA_PROBES)
            return;

        for (auto &xpair : ReplicaProbes)
            sys->Send(xpair.second, new TEvents::TEvPoisonPill());
        ReplicaProbes.clear();

        for (auto &ring : Info->Rings)
            for (const TActorId replicaId : ring.Replicas) {
                const TActorId probeId = sys->Register(CreateStateStorageReplicaProbe(replicaId));
                ReplicaProbes.emplace(replicaId, probeId);
            }
    }

    template<typename TEventPtr>
    void ResolveReplicas(const TEventPtr &ev, ui64 tabletId, const TIntrusivePtr<TStateStorageInfo> &info) const {
        THolder<TStateStorageInfo::TSelection> selection(new TStateStorageInfo::TSelection());
        info->SelectReplicas(tabletId, selection.Get());

        TAutoPtr<TEvStateStorage::TEvResolveReplicasList> reply(new TEvStateStorage::TEvResolveReplicasList());
        reply->Replicas.insert(reply->Replicas.end(), selection->SelectedReplicas.Get(), selection->SelectedReplicas.Get() + selection->Sz);
        reply->ConfigContentHash = info->ContentHash();
        Send(ev->Sender, reply.Release(), 0, ev->Cookie);
    }

    void RegisterFlowContolled(TActorSystem *sys, const TStateStorageInfo *old) {
        if (!KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP)
            return;

        TIntrusivePtr<TStateStorageInfo> updated = new TStateStorageInfo();
        updated->NToSelect = Info->NToSelect;
        updated->Rings.resize(Info->Rings.size());

        const bool checkOldInfo = FlowControlledInfo && old
            && updated->NToSelect == FlowControlledInfo->NToSelect
            && updated->Rings.size() == FlowControlledInfo->Rings.size();

        ui32 ringIdx = 0;
        for (const ui32 ringsSz = Info->Rings.size(); ringIdx < ringsSz; ++ringIdx) {
            const bool checkRing = checkOldInfo && (FlowControlledInfo->Rings[ringIdx].Replicas.size() == Info->Rings[ringIdx].Replicas.size());

            TStateStorageInfo::TRing &ctring = updated->Rings[ringIdx];
            TStateStorageInfo::TRing *fcring = checkRing ? &FlowControlledInfo->Rings[ringIdx] : nullptr;
            const auto &srcring = Info->Rings[ringIdx];
            const auto *oldring = checkRing ? &old->Rings[ringIdx] : nullptr;

            ctring.Replicas.resize(srcring.Replicas.size());
            ui32 replicaIdx = 0;
            for (const ui32 srcSize = srcring.Replicas.size(); replicaIdx < srcSize; ++replicaIdx) {
                if (checkRing && srcring.Replicas[replicaIdx] == oldring->Replicas[replicaIdx]) {
                    ctring.Replicas[replicaIdx] = fcring->Replicas[replicaIdx];
                    fcring->Replicas[replicaIdx] = TActorId();
                } else {
                    if (fcring && replicaIdx < fcring->Replicas.size())
                        Send(fcring->Replicas[replicaIdx], new TEvents::TEvPoison());

                    TFlowControlledQueueConfig flowConfig;
                    flowConfig.MaxAllowedInFly = 10000;
                    flowConfig.TargetDynamicRate = 250000;

                    ctring.Replicas[replicaIdx] = sys->Register(
                        CreateFlowControlledRequestQueue(srcring.Replicas[replicaIdx], NKikimrServices::TActivity::SS_PROXY_REQUEST, flowConfig),
                        TMailboxType::ReadAsFilled
                    );
                }
            }
            if (fcring) {
                for (const ui32 fcSize = fcring->Replicas.size(); replicaIdx < fcSize; ++replicaIdx) {
                    Send(fcring->Replicas[replicaIdx], new TEvents::TEvPoison());
                }
            }
        }
        if (FlowControlledInfo) {
            for (const ui32 oldSize = FlowControlledInfo->Rings.size(); ringIdx < oldSize; ++ringIdx) {
                for (TActorId outdated : FlowControlledInfo->Rings[oldSize].Replicas)
                    Send(outdated, new TEvents::TEvPoison());
            }
        }

        FlowControlledInfo = std::move(updated);
    }

    void Registered(TActorSystem* sys, const TActorId&) {
        RegisterDerivedServices(sys, nullptr);
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
            hFunc(TEvStateStorage::TEvReplicaProbeSubscribe, Handle);
            hFunc(TEvStateStorage::TEvReplicaProbeUnsubscribe, Handle);
            fFunc(TEvents::TSystem::Unsubscribe, HandleUnsubscribe);
        default:
            TActivationContext::Forward(ev, RegisterWithSameMailbox(new TStateStorageProxyRequest(Info, FlowControlledInfo)));
            break;
        }
    }
};

class TStateStorageProxyStub : public TActor<TStateStorageProxyStub> {

    void Handle(TEvStateStorage::TEvLookup::TPtr &ev) {
        BLOG_D("ProxyStub::Handle ev: " << ev->Get()->ToString());
        const TEvStateStorage::TEvLookup *msg = ev->Get();
        const ui64 tabletId = msg->TabletID;
        const ui64 cookie = msg->Cookie;

        Send(ev->Sender, new TEvStateStorage::TEvInfo(
            NKikimrProto::ERROR,
            tabletId, cookie, TActorId(), TActorId(), 0, 0, false, 0,
            nullptr, 0, TMap<TActorId, TActorId>()));
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY_STUB;
    }

    TStateStorageProxyStub()
        : TActor(&TThis::StateFunc)
    {}

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvLookup, Handle);
            default:
                BLOG_W("ProxyStub::StateFunc unexpected event type# "
                    << ev->GetTypeRewrite()
                    << " event: "
                    << ev->ToString());
                break;
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
