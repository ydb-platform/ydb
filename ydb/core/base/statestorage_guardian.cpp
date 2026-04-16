#include "statestorage_impl.h"
#include "statestorage_guardian_impl.h"
#include "tabletid.h"
#include "tablet.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

#include <util/generic/algorithm.h>
#include <util/generic/xrange.h>


#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)

namespace NKikimr {
namespace NStateStorageGuardian {

struct TGuardedInfo;
struct TFollowerInfo;

struct TEvPrivate {
    enum EEv {
        EvRefreshFollowerState = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvReplicaMissing,

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

    struct TEvReplicaMissing : public TEventLocal<TEvReplicaMissing, EvReplicaMissing> {
        const bool Missing;

        explicit TEvReplicaMissing(bool missing)
            : Missing(missing)
        { }
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

struct TFollowerInfo : public TAtomicRefCount<TFollowerInfo> {
    const ui64 TabletID;
    const TActorId Follower;
    const TActorId Tablet;
    const ui32 FollowerId;
    const bool IsCandidate;

    TFollowerInfo(ui64 tabletId, ui32 followerId, TActorId follower, TActorId tablet, bool isCandidate)
        : TabletID(tabletId)
        , Follower(follower)
        , Tablet(tablet)
        , FollowerId(followerId)
        , IsCandidate(isCandidate)
    {}
};

template<typename TDerived>
class TBaseGuardian : public TActorBootstrapped<TDerived> {
protected:
    const TActorId Replica;
    const TActorId Guard;
    ui64 ClusterStateGeneration;
    ui64 ClusterStateGuid;

    TInstant DowntimeFrom = TInstant::Max();
    ui64 LastCookie = 0;
    bool ReplicaMissingReported = false;
    TMonotonic LastReplicaMissing = TMonotonic::Max();

    TBaseGuardian(TActorId replica, TActorId guard, ui64 clusterStateGeneration, ui64 clusterStateGuid)
        : Replica(replica)
        , Guard(guard)
        , ClusterStateGeneration(clusterStateGeneration)
        , ClusterStateGuid(clusterStateGuid)
    {}

    void Gone() {
        this->Send(Guard, new TEvents::TEvGone());
        PassAway();
    }

    void PassAway() override {
        if (Replica.NodeId() != this->SelfId().NodeId())
            this->Send(TActivationContext::InterconnectProxy(Replica.NodeId()), new TEvents::TEvUnsubscribe);

        TActorBootstrapped<TDerived>::PassAway();
    }

    void ReplicaMissing(bool value) {
        if (ReplicaMissingReported < value) {
            const TMonotonic now = TActivationContext::Monotonic();
            if (LastReplicaMissing == TMonotonic::Max()) {
                // this if the first time in row we report replica missing
                LastReplicaMissing = now;
            } else {
                // make it actually "missing" only after a specific amount of time
                value = LastReplicaMissing + TDuration::Seconds(3) < now;
            }
        } else if (value < ReplicaMissingReported) {
            LastReplicaMissing = TMonotonic::Max();
        }
        if (value != ReplicaMissingReported) {
            this->Send(Guard, new TEvPrivate::TEvReplicaMissing(value));
            ReplicaMissingReported = value;
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Cookie == LastCookie) {
            ReplicaMissing(true);
            SomeSleep();
        }
    }

    void HandleThenSomeSleep(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        if (ev->Cookie == LastCookie) {
            ++LastCookie;
            SomeSleep();
        }
    }

    void HandleThenRequestInfo(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        if (ev->Cookie == LastCookie) {
            ++LastCookie;
            static_cast<TDerived&>(*this).RequestInfo();
        }
    }

    void SomeSleep() {
        const TInstant now = TActivationContext::Now();
        if (DowntimeFrom > now) {
            DowntimeFrom = now;
        } else if (DowntimeFrom + TDuration::Seconds(15) < now) {
            return Gone();
        }

        this->Become(&TDerived::StateSleep, TDuration::MilliSeconds(250), new TEvents::TEvWakeup());
    }

    void HandleConfigVersion(TEvStateStorage::TEvConfigVersionInfo::TPtr &ev) {
        TEvStateStorage::TEvConfigVersionInfo *msg = ev->Get();
        ClusterStateGeneration = msg->ClusterStateGeneration;
        ClusterStateGuid = msg->ClusterStateGuid;
    }

    void CheckConfigVersion(const TActorId &selfId, const TActorId &sender, const auto *msg) {
        ui64 msgGeneration = msg->Record.GetClusterStateGeneration();
        ui64 msgGuid = msg->Record.GetClusterStateGuid();
        if (ClusterStateGeneration < msgGeneration || (ClusterStateGeneration == msgGeneration && ClusterStateGuid != msgGuid)) {
            BLOG_D("Guardian TEvNodeWardenNotifyConfigMismatch: ClusterStateGeneration=" << ClusterStateGeneration << " msgGeneration=" << msgGeneration <<" ClusterStateGuid=" << ClusterStateGuid << " msgGuid=" << msgGuid);
            this->Send(MakeBlobStorageNodeWardenID(selfId.NodeId()),
                new NStorage::TEvNodeWardenNotifyConfigMismatch(sender.NodeId(), msgGeneration, msgGuid));
        }
    }
};

class TReplicaGuardian : public TBaseGuardian<TReplicaGuardian> {
    TIntrusiveConstPtr<TGuardedInfo> Info;

    ui64 Signature;

    friend class TBaseGuardian;

    void RequestInfo() {
        MakeRequest();
    }

    void MakeRequest() {
        ui64 cookie = ++LastCookie;
        Send(Replica, new TEvStateStorage::TEvReplicaLookup(Info->TabletID, cookie, ClusterStateGeneration, ClusterStateGuid), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie);
        Become(&TThis::StateLookup);
    }

    void UpdateInfo() {
        ui64 cookie = ++LastCookie;
        TAutoPtr<TEvStateStorage::TEvReplicaUpdate> req(new TEvStateStorage::TEvReplicaUpdate());
        req->Record.SetTabletID(Info->TabletID);
        req->Record.SetCookie(cookie);
        req->Record.SetClusterStateGeneration(ClusterStateGeneration);
        req->Record.SetClusterStateGuid(ClusterStateGuid);
        ActorIdToProto(Info->Leader, req->Record.MutableProposedLeader());
        ActorIdToProto(Info->TabletLeader, req->Record.MutableProposedLeaderTablet());
        req->Record.SetProposedGeneration(Info->Generation);
        req->Record.SetProposedStep(0);
        req->Record.SetSignature(Signature);
        req->Record.SetIsGuardian(true);

        Send(Replica, req.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie);
        Become(&TThis::StateUpdate);
    }

    void Demoted() {
        Send(Info->Leader, new TEvTablet::TEvDemoted(false));
        return PassAway();
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        const auto &record = ev->Get()->Record;
        if (record.GetCookie() && record.GetCookie() != LastCookie) {
            // Ignore outdated results
            return;
        }

        CheckConfigVersion(SelfId(), ev->Sender, ev->Get());

        const auto status = record.GetStatus();
        Signature = record.GetSignature();
        DowntimeFrom = TInstant::Max();
        ReplicaMissing(false);

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
            Y_ABORT();
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_REPLICA_GUARDIAN;
    }

    TReplicaGuardian(TGuardedInfo *info, TActorId replica, TActorId guard, ui64 clusterStateGeneration, ui64 clusterStateGuid)
        : TBaseGuardian(replica, guard, clusterStateGeneration, clusterStateGuid)
        , Info(info)
        , Signature(0)
    {}

    void Bootstrap() {
        RequestInfo();
    }

    STATEFN(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            hFunc(TEvents::TEvUndelivered, TBaseGuardian::Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleThenSomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }

    STATEFN(StateCalm) { // info is correct, wait for disconnect event
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            hFunc(TEvents::TEvUndelivered, TBaseGuardian::Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleThenRequestInfo);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }

    STATEFN(StateSleep) { // not-connected, sleeping for retry
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            cFunc(TEvents::TEvWakeup::EventType, RequestInfo);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }

    STATEFN(StateUpdate) { // waiting for update result
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            hFunc(TEvents::TEvUndelivered, TBaseGuardian::Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleThenSomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }
};

class TFollowerGuardian : public TBaseGuardian<TFollowerGuardian> {
    TIntrusiveConstPtr<TFollowerInfo> Info;

    void RefreshInfo(TEvPrivate::TEvRefreshFollowerState::TPtr &ev) {
        Info = ev->Get()->FollowerInfo;
    }

    void UpdateInfo(TEvPrivate::TEvRefreshFollowerState::TPtr &ev) {
        RefreshInfo(ev);
        UpdateInfo();
    }

    void UpdateInfo() {
        MakeRequest();
    }

    void MakeRequest() {
        ui64 cookie = ++LastCookie;
        Send(
            Replica,
            new TEvStateStorage::TEvReplicaRegFollower(Info->TabletID, Info->FollowerId, Info->Follower, Info->Tablet, Info->IsCandidate, ClusterStateGeneration, ClusterStateGuid),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            cookie);
        Become(&TThis::StateCalm);
    }

    void PassAway() override {
        Send(Replica, new TEvStateStorage::TEvReplicaUnregFollower(Info->TabletID, Info->FollowerId, Info->Follower, ClusterStateGeneration, ClusterStateGuid));
        TBaseGuardian::PassAway();
    }

    void Ping() {
        DowntimeFrom = TInstant::Max();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_REPLICA_GUARDIAN;
    }

    TFollowerGuardian(TFollowerInfo *info, const TActorId replica, const TActorId guard, ui64 clusterStateGeneration, ui64 clusterStateGuid)
        : TBaseGuardian(replica, guard, clusterStateGeneration, clusterStateGuid)
        , Info(info)
    {}

    void Bootstrap() {
        UpdateInfo();
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshFollowerState, UpdateInfo);
            hFunc(TEvents::TEvUndelivered, TBaseGuardian::Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleThenSomeSleep);
            cFunc(TEvTablet::TEvPing::EventType, Ping);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }

    STATEFN(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshFollowerState, RefreshInfo);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvWakeup::EventType, UpdateInfo);

            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, Gone);
            hFunc(TEvStateStorage::TEvConfigVersionInfo, HandleConfigVersion);
        }
    }
};

class TTabletGuardian : public TActorBootstrapped<TTabletGuardian> {
    TIntrusivePtr<TGuardedInfo> Info;
    TIntrusivePtr<TFollowerInfo> FollowerInfo;

    TVector<std::pair<TActorId, TActorId>> ReplicaGuardians; // replica -> guardian, position dependant so vector
    THashSet<TActorId> MissingReplicas;
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

    void PassAway() override {
        const TActorId proxyActorID = MakeStateStorageProxyID();
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, proxyActorID, SelfId(), nullptr, 0));
        TActorBootstrapped::PassAway();
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev) {
        const TVector<TActorId> &replicasList = ev->Get()->GetPlainReplicas();
        ui64 clusterStateGeneration = ev->Get()->ClusterStateGeneration;
        ui64 clusterStateGuid = ev->Get()->ClusterStateGuid;
        Y_ABORT_UNLESS(!replicasList.empty(), "must not happens, guardian must be created over active tablet");

        const ui32 replicaSz = replicasList.size();

        TVector<std::pair<TActorId, TActorId>> updatedReplicaGuardians;
        updatedReplicaGuardians.reserve(replicaSz);

        for (ui32 idx : xrange(replicasList.size())) {
            const TActorId replica = replicasList[idx];
            bool found = false;
            for (auto& p : ReplicaGuardians)
                if (p.first == replica && p.second) {
                    updatedReplicaGuardians.emplace_back(p);
                    Send(p.second, new TEvStateStorage::TEvConfigVersionInfo(clusterStateGeneration, clusterStateGuid));
                    p.second = TActorId();
                    found = true;
                    break;
                }
            if (!found) {
                if (Info)
                    updatedReplicaGuardians.emplace_back(replica, RegisterWithSameMailbox(new TReplicaGuardian(Info.Get(), replica, SelfId(), clusterStateGeneration, clusterStateGuid)));
                else
                    updatedReplicaGuardians.emplace_back(replica, RegisterWithSameMailbox(new TFollowerGuardian(FollowerInfo.Get(), replica, SelfId(), clusterStateGeneration, clusterStateGuid)));
            }
        }
        for (const auto &xpair : ReplicaGuardians) {
            if (xpair.second) {
                Send(xpair.second, new TEvents::TEvPoison());
            }
        }
        ReplicaGuardians.swap(updatedReplicaGuardians);
        ReplicasOnlineThreshold = (ReplicaGuardians.size() == 1) ? 0 : 1;

        FollowerTracker.Reset(new TFollowerTracker(replicaSz));

        Become(&TThis::StateCalm);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        Y_UNUSED(ev);
        Y_ABORT("must not happens, guardian must be created over active tablet");
    }

    ui32 CountOnlineReplicas() const {
        ui32 replicasOnline = 0;

        for (auto& pr : ReplicaGuardians) {
            if (pr.second && !MissingReplicas.contains(pr.second)) {
                ++replicasOnline;
            }
        }

        return replicasOnline;
    }

    bool ValidateOnlineReplicasOrDie() {
        ui32 replicasOnline = CountOnlineReplicas();

        if (replicasOnline <= ReplicasOnlineThreshold) {
            Send(Launcher(), new TEvTablet::TEvDemoted(true));
            HandlePoison();
            return false;
        }

        return true;
    }

    bool ReplicaDown(TActorId guardian) {
        bool ret = false;

        for (auto it = ReplicaGuardians.begin(), end = ReplicaGuardians.end(); it != end; ++it) {
            if (it->second == guardian) {
                it->second = TActorId();
                ret = true;
                break;
            }
        }

        if (ret && !ValidateOnlineReplicasOrDie()) {
            // we are dead now
            return false;
        }

        return ret; // true on erase, false on outdated notify
    }

    void SendResolveRequest(TDuration delay, bool initial) {
        const ui64 tabletId = Info ? Info->TabletID : FollowerInfo->TabletID;
        const TActorId proxyActorID = MakeStateStorageProxyID();

        if (delay == TDuration::Zero()) {
            Send(proxyActorID, new TEvStateStorage::TEvResolveReplicas(tabletId, initial), IEventHandle::FlagTrackDelivery);
        } else {
            TActivationContext::Schedule(
                delay,
                new IEventHandle(proxyActorID, SelfId(), new TEvStateStorage::TEvResolveReplicas(tabletId, initial), IEventHandle::FlagTrackDelivery)
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
            SendResolveRequest(TDuration::MilliSeconds(150 + rndDelay), false);
        }
    }

    void Handle(TEvPrivate::TEvReplicaMissing::TPtr &ev) {
        auto* msg = ev->Get();

        if (msg->Missing) {
            MissingReplicas.insert(ev->Sender);
            ValidateOnlineReplicasOrDie();
        } else {
            MissingReplicas.erase(ev->Sender);
        }
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev) {
        Y_ABORT_UNLESS(FollowerTracker);

        const NKikimrStateStorage::TEvInfo &record = ev->Get()->Record;
        const TActorId guardian = ev->Sender;
        for (ui32 idx : xrange(ReplicaGuardians.size())) {
            if (ReplicaGuardians[idx].second != guardian)
                continue;

            TVector<TActorId> reported;

            // NOTE: The Follower, FollowerTablet and FollowerCandidate fields
            //       are deprecated and will be removed from the API. The new code
            //       should use the FollowerInfo field. For compatibility with older nodes,
            //       the code here tries to use the both the new field and the old fields.
            if (record.FollowerInfoSize() > 0) {
                reported.reserve(record.FollowerInfoSize());

                for (const auto& followerInfo : record.GetFollowerInfo()) {
                    reported.emplace_back(ActorIdFromProto(followerInfo.GetFollower()));
                }
            } else {
                // TODO: Remove the code, which handles the old fields
                reported.reserve(record.FollowerSize() + record.FollowerCandidatesSize());
                for (const auto &x : record.GetFollower()) {
                    reported.emplace_back(ActorIdFromProto(x));
                }

                for (const auto &x : record.GetFollowerCandidates()) {
                    reported.emplace_back(ActorIdFromProto(x));
                }
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

        Y_ABORT_UNLESS(msg->FollowerActor == FollowerInfo->Follower);

        const bool hasChanges = msg->TabletActor != FollowerInfo->Tablet || msg->IsCandidate != FollowerInfo->IsCandidate;
        if (hasChanges) {
            FollowerInfo = new TFollowerInfo(
                tabletId,
                FollowerInfo->FollowerId,
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
        SendResolveRequest(TDuration::Zero(), true);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvFollowerUpdateState, UpdateFollowerInfo);
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvGone, HandleGoneResolve);
            hFunc(TEvPrivate::TEvReplicaMissing, Handle);
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
            hFunc(TEvPrivate::TEvReplicaMissing, Handle);
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

IActor* CreateStateStorageFollowerGuardian(ui64 tabletId, ui32 followerId, const TActorId &follower) {
    TIntrusivePtr<NStateStorageGuardian::TFollowerInfo> followerInfo = new NStateStorageGuardian::TFollowerInfo(tabletId, followerId, follower, TActorId(), true);
    return new NStateStorageGuardian::TTabletGuardian(followerInfo.Get());
}

}
