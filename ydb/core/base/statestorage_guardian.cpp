#include "statestorage_impl.h"
#include "statestorage_guardian_impl.h"
#include "tabletid.h"
#include "tablet.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/compile_time_flags.h>
#include <ydb/core/protos/services.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>

#include <util/generic/algorithm.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NStateStorageGuardian {

struct TGuardedInfo;
struct TFollowerInfo;

struct TEvPrivate {
    enum EEv {
        EvRefreshFollowerState = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvRefreshFollowerState : public TEventLocal<TEvRefreshFollowerState, EvRefreshFollowerState> {
        TIntrusiveConstPtr<TFollowerInfo> FollowerInfo;

        TEvRefreshFollowerState(const TIntrusivePtr<TFollowerInfo> &info)
            : FollowerInfo(info)
        {}
    };
};

struct TGuardedInfo : public TAtomicRefCount<TGuardedInfo> {
    const ui64 TabletID;
    const TActorId Leader;
    const TActorId TabletLeader;
    const ui32 Generation;

    TGuardedInfo(ui64 tabletId, const TActorId &leader, const TActorId &tabletLeader, ui32 generation)
        : TabletID(tabletId)
        , Leader(leader)
        , TabletLeader(tabletLeader)
        , Generation(generation)
    {}
};

struct TFollowerInfo : public TAtomicRefCount<TGuardedInfo> {
    const ui64 TabletID;
    const TActorId Follower;
    const TActorId Tablet; 
    const bool IsCandidate;

    TFollowerInfo(ui64 tabletId, TActorId follower, TActorId tablet, bool isCandidate)
        : TabletID(tabletId)
        , Follower(follower)
        , Tablet(tablet)
        , IsCandidate(isCandidate)
    {}
};

class TReplicaGuardian : public TActorBootstrapped<TReplicaGuardian> {
    TIntrusiveConstPtr<TGuardedInfo> Info;
    const TActorId Replica; 
    const TActorId Guard; 

    ui64 Signature;
    TInstant DowntimeFrom;

    void PassAway() override {
        if (Replica.NodeId() != SelfId().NodeId())
            Send(TActivationContext::InterconnectProxy(Replica.NodeId()), new TEvents::TEvUnsubscribe);

        if (KIKIMR_ALLOW_SSREPLICA_PROBES) {
            const TActorId ssProxyId = MakeStateStorageProxyID(StateStorageGroupFromTabletID(Info->TabletID)); 
            Send(ssProxyId, new TEvStateStorage::TEvReplicaProbeUnsubscribe(Replica));
        }

        TActor::PassAway();
    }

    void RequestInfo() {
        if (KIKIMR_ALLOW_SSREPLICA_PROBES) {
            const TActorId ssProxyId = MakeStateStorageProxyID(StateStorageGroupFromTabletID(Info->TabletID)); 
            Send(ssProxyId, new TEvStateStorage::TEvReplicaProbeSubscribe(Replica));
            Become(&TThis::StateLookup);
        } else {
            MakeRequest();
        }
    }

    void MakeRequest() {
        Send(Replica, new TEvStateStorage::TEvReplicaLookup(Info->TabletID, 0), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateLookup);
    }

    void UpdateInfo() {
        TAutoPtr<TEvStateStorage::TEvReplicaUpdate> req(new TEvStateStorage::TEvReplicaUpdate());
        req->Record.SetTabletID(Info->TabletID);
        ActorIdToProto(Info->Leader, req->Record.MutableProposedLeader());
        ActorIdToProto(Info->TabletLeader, req->Record.MutableProposedLeaderTablet());
        req->Record.SetProposedGeneration(Info->Generation);
        req->Record.SetProposedStep(0);
        req->Record.SetSignature(Signature);
        req->Record.SetIsGuardian(true);

        Send(Replica, req.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateUpdate);
    }

    void Gone() {
        Send(Guard, new TEvents::TEvGone());
        PassAway();
    }

    void SomeSleep() {
        const TInstant now = TActivationContext::Now();
        if (DowntimeFrom > now) {
            DowntimeFrom = now;
        } else if (DowntimeFrom + TDuration::Seconds(15) < now) {
            return Gone();
        }

        Become(&TThis::StateSleep, TDuration::MilliSeconds(250), new TEvents::TEvWakeup());
    }

    void Demoted() {
        Send(Info->Leader, new TEvTablet::TEvDemoted(false));
        return PassAway();
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        const auto &record = ev->Get()->Record;
        const auto status = record.GetStatus();
        Signature = record.GetSignature();

        DowntimeFrom = TInstant::Max();

        if (status == NKikimrProto::OK) {
            const ui32 gen = record.GetCurrentGeneration();

            if (gen > Info->Generation) {
                return Demoted();
            } else if (gen == Info->Generation) {
                const TActorId leader = ActorIdFromProto(record.GetCurrentLeader());
                const TActorId tabletLeader = ActorIdFromProto(record.GetCurrentLeaderTablet());
                if (!leader || leader == Info->Leader && !tabletLeader) {
                    return UpdateInfo();
                } else if (leader != Info->Leader || tabletLeader != Info->TabletLeader) {
                    return Demoted(); // hack around cluster restarts
                } else {
                    Become(&TThis::StateCalm);
                    Send(Guard, ev->Release().Release());
                    return;
                }
            } else {
                return UpdateInfo(); // what about locked-state?
            }
        } else if (status == NKikimrProto::ERROR) {
            return UpdateInfo();
        } else {
            Y_FAIL();
        }
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_REPLICA_GUARDIAN;
    }

    TReplicaGuardian(TGuardedInfo *info, TActorId replica, TActorId guard) 
        : Info(info)
        , Replica(replica)
        , Guard(guard)
        , Signature(0)
        , DowntimeFrom(TInstant::Max())
    {}

    void Bootstrap() {
        RequestInfo();
    }

    STATEFN(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaProbeConnected::EventType, MakeRequest);
            cFunc(TEvStateStorage::TEvReplicaProbeDisconnected::EventType, Gone);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateCalm) { // info is correct, wait for disconnect event
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            cFunc(TEvents::TEvUndelivered::EventType, RequestInfo);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, RequestInfo);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateSleep) { // not-connected, sleeping for retry
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            cFunc(TEvents::TEvWakeup::EventType, RequestInfo);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateUpdate) { // waiting for update result
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

class TFollowerGuardian : public TActorBootstrapped<TFollowerGuardian> {
    TIntrusiveConstPtr<TFollowerInfo> Info;
    const TActorId Replica; 
    const TActorId Guard; 

    TInstant DowntimeFrom;

    void RefreshInfo(TEvPrivate::TEvRefreshFollowerState::TPtr &ev) {
        Info = ev->Get()->FollowerInfo;
    }

    void UpdateInfo(TEvPrivate::TEvRefreshFollowerState::TPtr &ev) {
        RefreshInfo(ev);
        UpdateInfo();
    }

    void UpdateInfo() {
        if (KIKIMR_ALLOW_SSREPLICA_PROBES) {
            const TActorId ssProxyId = MakeStateStorageProxyID(StateStorageGroupFromTabletID(Info->TabletID)); 
            Send(ssProxyId, new TEvStateStorage::TEvReplicaProbeSubscribe(Replica));
            Become(&TThis::StateCalm);
        } else {
            MakeRequest();
        }
    }

    void MakeRequest() {
        Send(
            Replica,
            new TEvStateStorage::TEvReplicaRegFollower(Info->TabletID, Info->Follower, Info->Tablet, Info->IsCandidate),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateCalm);
    }

    void SomeSleep() {
        const TInstant now = TActivationContext::Now();
        if (DowntimeFrom > now) {
            DowntimeFrom = now;
        } else if (DowntimeFrom + TDuration::Seconds(15) < now) {
            return Gone();
        }

        Become(&TThis::StateSleep, TDuration::MilliSeconds(250), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        Send(Replica, new TEvStateStorage::TEvReplicaUnregFollower(Info->TabletID, Info->Follower));
        if (Replica.NodeId() != SelfId().NodeId())
            Send(TActivationContext::InterconnectProxy(Replica.NodeId()), new TEvents::TEvUnsubscribe());

        if (KIKIMR_ALLOW_SSREPLICA_PROBES) {
            const TActorId ssProxyId = MakeStateStorageProxyID(StateStorageGroupFromTabletID(Info->TabletID)); 
            Send(ssProxyId, new TEvStateStorage::TEvReplicaProbeUnsubscribe(Replica));
        }

        TActor::PassAway();
    }

    void Gone() {
        Send(Guard, new TEvents::TEvGone());
        PassAway();
    }

    void Ping() {
        DowntimeFrom = TInstant::Max();
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_REPLICA_GUARDIAN;
    }

    TFollowerGuardian(TFollowerInfo *info, const TActorId replica, const TActorId guard)
        : Info(info)
        , Replica(replica)
        , Guard(guard)
        , DowntimeFrom(TInstant::Max())
    {}

    void Bootstrap() {
        UpdateInfo();
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshFollowerState, UpdateInfo);
            cFunc(TEvStateStorage::TEvReplicaProbeConnected::EventType, MakeRequest);
            cFunc(TEvStateStorage::TEvReplicaProbeDisconnected::EventType, Gone);
            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);
            cFunc(TEvTablet::TEvPing::EventType, Ping);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
        }
    }

    STATEFN(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshFollowerState, RefreshInfo);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvWakeup::EventType, UpdateInfo);

            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
        }
    }
};

class TTabletGuardian : public TActorBootstrapped<TTabletGuardian> {
    TIntrusivePtr<TGuardedInfo> Info;
    TIntrusivePtr<TFollowerInfo> FollowerInfo;

    TVector<std::pair<TActorId, TActorId>> ReplicaGuardians; // replica -> guardian, position dependant so vector 
    ui32 ReplicasOnlineThreshold;

    THolder<TFollowerTracker> FollowerTracker;

    TActorId Launcher() const { 
        return Info ? Info->Leader : FollowerInfo->Follower;
    }

    void HandlePoison() {
        for (const auto &xpair : ReplicaGuardians)
            Send(xpair.second, new TEvents::TEvPoison());

        return PassAway();
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev) {
        const TVector<TActorId> &replicasList = ev->Get()->Replicas; 
        Y_VERIFY(!replicasList.empty(), "must not happens, guardian must be created over active tablet");

        const ui32 replicaSz = replicasList.size();

        TVector<std::pair<TActorId, TActorId>> updatedReplicaGuardians; 
        updatedReplicaGuardians.reserve(replicaSz);

        const bool inspectCurrent = (ReplicaGuardians.size() == replicaSz);
        if (!inspectCurrent) {
            for (const auto &xpair : ReplicaGuardians) {
                if (xpair.second)
                    Send(xpair.second, new TEvents::TEvPoison());
            }
            ReplicaGuardians.clear();
        }

        for (ui32 idx : xrange(replicasList.size())) {
            const TActorId replica = replicasList[idx]; 

            if (inspectCurrent && ReplicaGuardians[idx].first == replica && ReplicaGuardians[idx].second) {
                updatedReplicaGuardians.emplace_back(ReplicaGuardians[idx]);
                ReplicaGuardians[idx].second = TActorId(); 
            } else {
                if (Info)
                    updatedReplicaGuardians.emplace_back(replica, RegisterWithSameMailbox(new TReplicaGuardian(Info.Get(), replica, SelfId())));
                else
                    updatedReplicaGuardians.emplace_back(replica, RegisterWithSameMailbox(new TFollowerGuardian(FollowerInfo.Get(), replica, SelfId())));
            }
        }

        for (const auto &xpair : ReplicaGuardians) {
            if (xpair.second)
                Send(xpair.second, new TEvents::TEvPoison());
        }

        ReplicaGuardians.swap(updatedReplicaGuardians);
        ReplicasOnlineThreshold = (ReplicaGuardians.size() == 1) ? 0 : 1;

        if (!FollowerTracker || !inspectCurrent) // would notify on first change
            FollowerTracker.Reset(new TFollowerTracker(replicaSz));

        Become(&TThis::StateCalm);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        Y_UNUSED(ev);
        Y_FAIL("must not happens, guardian must be created over active tablet");
    }

    bool ReplicaDown(TActorId guardian) { 
        ui32 replicasOnline = 0;
        bool ret = false;

        for (auto it = ReplicaGuardians.begin(), end = ReplicaGuardians.end(); it != end; ++it) {
            if (it->second == guardian) {
                it->second = TActorId(); 
                ret = true;
            } else if (it->second) {
                ++replicasOnline;
            }
        }

        if (replicasOnline == ReplicasOnlineThreshold) {
            Send(Launcher(), new TEvTablet::TEvDemoted(true));
            HandlePoison();

            // we are dead now
            return false;
        }

        return ret; // true on erase, false on outdated notify
    }

    void SendResolveRequest(TDuration delay) {
        const ui64 tabletId = Info ? Info->TabletID : FollowerInfo->TabletID;
        const ui64 stateStorageGroup = StateStorageGroupFromTabletID(tabletId);
        const TActorId proxyActorID = MakeStateStorageProxyID(stateStorageGroup); 

        if (delay == TDuration::Zero()) {
            Send(proxyActorID, new TEvStateStorage::TEvResolveReplicas(tabletId), IEventHandle::FlagTrackDelivery);
        } else {
            TActivationContext::Schedule(
                delay,
                new IEventHandle(proxyActorID, SelfId(), new TEvStateStorage::TEvResolveReplicas(tabletId), IEventHandle::FlagTrackDelivery)
            );
        }

        Become(&TThis::StateResolve);
    }

    void HandleGoneResolve(TEvents::TEvGone::TPtr &ev) {
        // already resolving so no more action needed, just refresh active replica list
        ReplicaDown(ev->Sender);
    }

    void HandleGoneCalm(TEvents::TEvGone::TPtr &ev) {
        if (ReplicaDown(ev->Sender)) {
            const ui64 rndDelay = AppData()->RandomProvider->GenRand() % 150;
            SendResolveRequest(TDuration::MilliSeconds(150 + rndDelay));
        }
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        Y_VERIFY(FollowerTracker);

        const NKikimrStateStorage::TEvInfo &record = ev->Get()->Record;
        const TActorId guardian = ev->Sender; 
        for (ui32 idx : xrange(ReplicaGuardians.size())) {
            if (ReplicaGuardians[idx].second != guardian)
                continue;

            TVector<TActorId> reported; 
            reported.reserve(record.FollowerSize() + record.FollowerCandidatesSize());
            for (const auto &x : record.GetFollower()) {
                reported.emplace_back(ActorIdFromProto(x)); 
            }

            for (const auto &x : record.GetFollowerCandidates()) {
                reported.emplace_back(ActorIdFromProto(x)); 
            }

            Sort(reported);
            if (FollowerTracker->Merge(idx, reported)) {
                const auto &merged = FollowerTracker->GetMerged();

                // reuse reported so in many cases no allocation happens
                reported.clear();
                reported.reserve(merged.size());

                for (const auto &xpair : merged) {
                    reported.emplace_back(xpair.first);
                }

                Send(Launcher(), new TEvTablet::TEvFollowerListRefresh(std::move(reported)));
            }

            break;
        }
    }

    bool RefreshFollowerInfo(TEvTablet::TEvFollowerUpdateState::TPtr &ev) {
        const auto *msg = ev->Get();
        const ui64 tabletId = FollowerInfo->TabletID;

        Y_VERIFY(msg->FollowerActor == FollowerInfo->Follower);

        const bool hasChanges = msg->TabletActor != FollowerInfo->Tablet || msg->IsCandidate != FollowerInfo->IsCandidate;
        if (hasChanges) {
            FollowerInfo = new TFollowerInfo(
                tabletId,
                msg->FollowerActor,
                msg->TabletActor,
                msg->IsCandidate
            );
        }

        return hasChanges;
    }

    void UpdateFollowerInfo(TEvTablet::TEvFollowerUpdateState::TPtr &ev) {
        if (!RefreshFollowerInfo(ev))
            return;

        for (auto &xpair : ReplicaGuardians) {
            const TActorId guardian = xpair.second; 
            Send(guardian, new TEvPrivate::TEvRefreshFollowerState(FollowerInfo));
        }
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_TABLET_GUARDIAN;
    }

    TTabletGuardian(TGuardedInfo *info)
        : Info(info)
        , ReplicasOnlineThreshold(0)
    {}

    TTabletGuardian(TFollowerInfo *info)
        : FollowerInfo(info)
        , ReplicasOnlineThreshold(0)
    {}

    void Bootstrap() {
        SendResolveRequest(TDuration::Zero());
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvFollowerUpdateState, UpdateFollowerInfo);
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvGone, HandleGoneResolve);
            cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
            cFunc(TEvTablet::TEvTabletDead::EventType, HandlePoison);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvFollowerUpdateState, UpdateFollowerInfo);
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvGone, HandleGoneCalm);
            cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
            cFunc(TEvTablet::TEvTabletDead::EventType, HandlePoison);
        }
    }
};

}

IActor* CreateStateStorageTabletGuardian(ui64 tabletId, const TActorId &leader, const TActorId &tabletLeader, ui32 generation) {
    TIntrusivePtr<NStateStorageGuardian::TGuardedInfo> info = new NStateStorageGuardian::TGuardedInfo(tabletId, leader, tabletLeader, generation);
    return new NStateStorageGuardian::TTabletGuardian(info.Get());
}

IActor* CreateStateStorageFollowerGuardian(ui64 tabletId, const TActorId &follower) {
    TIntrusivePtr<NStateStorageGuardian::TFollowerInfo> followerInfo = new NStateStorageGuardian::TFollowerInfo(tabletId, follower, TActorId(), true);
    return new NStateStorageGuardian::TTabletGuardian(followerInfo.Get());
}

}
