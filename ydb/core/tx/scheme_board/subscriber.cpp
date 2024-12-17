#include "events.h"
#include "events_internal.h"
#include "helpers.h"
#include "monitorable_actor.h"
#include "subscriber.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/protos/scheme_board.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/string/cast.h>

namespace NKikimr {

using EDeletionPolicy = ESchemeBoardSubscriberDeletionPolicy;

namespace NSchemeBoard {

#define SBS_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)
#define SBS_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)
#define SBS_LOG_I(stream) SB_LOG_I(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)
#define SBS_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)
#define SBS_LOG_W(stream) SB_LOG_W(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)
#define SBS_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_SUBSCRIBER, "[" << LogPrefix() << "]" << this->SelfId() << "[" << Path << "] " << stream)

namespace {

    template <typename T, typename U>
    typename std::enable_if_t<!std::is_same_v<T, U>, bool> IsSame(const T&, const U&) {
        return false;
    }

    template <typename T, typename U>
    typename std::enable_if_t<std::is_same_v<T, U>, bool> IsSame(const T& t, const U& u) {
        return t == u;
    }

    template <typename TPath>
    bool IsValidNotification(const TPath& path, const NKikimrSchemeBoard::TEvNotify& record) {
        bool valid = false;

        if (record.HasPath()) {
            valid = IsSame(path, record.GetPath());
        }

        if (!valid && (record.HasPathOwnerId() && record.HasLocalPathId())) {
            valid = IsSame(path, TPathId(record.GetPathOwnerId(), record.GetLocalPathId()));
        }

        return valid;
    }

    struct TPathVersion {
        TPathId PathId;
        ui64 Version;

        TPathVersion()
            : PathId(TPathId())
            , Version(0)
        {
        }

        explicit TPathVersion(const TPathId& pathId, const ui64 version)
            : PathId(pathId)
            , Version(version)
        {
        }

        static TPathVersion FromNotify(const NKikimrSchemeBoard::TEvNotify& record) {
            TPathId pathId;
            if (record.HasPathOwnerId() && record.HasLocalPathId()) {
                pathId = TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
            }

            return TPathVersion(pathId, record.GetVersion());
        }

        TString ToString() const {
            TString result;
            TStringOutput out(result);
            Out(out);
            return result;
        }

        void Out(IOutputStream& o) const {
            if (!*this) {
                PathId.Out(o);
            } else {
                o << "(PathId: " << PathId.ToString() << ", Version: " << Version << ")";
            }
        }

        bool operator<(const TPathVersion& x) const {
            return PathId != x.PathId ? PathId < x.PathId : Version < x.Version;
        }
        bool operator>(const TPathVersion& x) const {
            return x < *this;
        }
        bool operator<=(const TPathVersion& x) const {
            return PathId != x.PathId ? PathId < x.PathId : Version <= x.Version;
        }
        bool operator>=(const TPathVersion& x) const {
            return x <= *this;
        }
        bool operator==(const TPathVersion& x) const {
            return PathId == x.PathId && Version == x.Version;
        }
        bool operator!=(const TPathVersion& x) const {
            return PathId != x.PathId || Version != x.Version;
        }
        operator bool() const {
            return bool(PathId);
        }
    };

    // TNotifyResponse isolates changes of how NKikimrSchemeBoard::TEvNotify is filled
    // by previous and recent replica implementation.
    //
    // TNotifyResponse wouldn't be even needed if not for backward compatibility support.
    // Consider removing compatibility support at version stable-25-1.
    struct TNotifyResponse {
        NKikimrSchemeBoard::TEvNotify Notify;
        TPathId SubdomainPathId;
        TSet<ui64> PathAbandonedTenantsSchemeShards;
        TMaybe<NKikimrScheme::TEvDescribeSchemeResult> DescribeSchemeResult;

        static TNotifyResponse FromNotify(NKikimrSchemeBoard::TEvNotify&& record) {
            // PathSubdomainPathId's absence is a marker that input message was sent
            // from the older replica implementation

            if (record.HasPathSubdomainPathId()) {
                // Sender implementation is as recent as ours.
                // Just copy two fields from the notify message (this branch is practically a stub)

                auto subdomainPathId = PathIdFromPathId(record.GetPathSubdomainPathId());
                auto pathAbandonedTenantsSchemeShards = TSet<ui64>(
                    record.GetPathAbandonedTenantsSchemeShards().begin(),
                    record.GetPathAbandonedTenantsSchemeShards().end()
                );
                return TNotifyResponse{
                    .Notify = std::move(record),
                    .SubdomainPathId = std::move(subdomainPathId),
                    .PathAbandonedTenantsSchemeShards = std::move(pathAbandonedTenantsSchemeShards),
                };

            } else {
                // Sender implementation is older then ours.
                // Extract two essential fields from the payload, hold deserialized proto,
                // drop original payload to save on memory

                // Move DescribeSchemeResult blob out of the input message.
                auto data = std::move(*record.MutableDescribeSchemeResultSerialized());
                record.ClearDescribeSchemeResultSerialized();

                // it's inconvenient to use arena here
                auto proto = DeserializeDescribeSchemeResult(data);

                return TNotifyResponse{
                    .Notify = std::move(record),
                    .SubdomainPathId = NSchemeBoard::GetDomainId(proto),
                    .PathAbandonedTenantsSchemeShards = NSchemeBoard::GetAbandonedSchemeShardIds(proto),
                    .DescribeSchemeResult = std::move(proto),
                };
            }
        }

        NKikimrScheme::TEvDescribeSchemeResult GetDescribeSchemeResult() {
            if (DescribeSchemeResult) {
                return *DescribeSchemeResult;
            } else {
                // it's inconvenient to use arena here
                return DeserializeDescribeSchemeResult(Notify.GetDescribeSchemeResultSerialized());
            }
        }
    };

    struct TState {
        bool Deleted = false;
        bool Strong = false;
        TPathVersion Version;
        TDomainId DomainId;
        TSet<ui64> AbandonedSchemeShards;

        TState() = default;

    private:
        explicit TState(const TPathVersion& version, const TDomainId& domainId, const TSet<ui64>& abandonedSchemeShards)
            : Deleted(false)
            , Strong(true)
            , Version(version)
            , DomainId(domainId)
            , AbandonedSchemeShards(abandonedSchemeShards)
        {
        }

        explicit TState(bool strong, const TPathVersion& version)
            : Deleted(true)
            , Strong(strong)
            , Version(version)
        {
        }

    public:
        static TState FromNotify(const TNotifyResponse& notifyResponse) {
            const auto& record = notifyResponse.Notify;
            const TPathVersion& pathVersion = TPathVersion::FromNotify(record);
            if (!record.GetIsDeletion()) {
                return TState(
                    pathVersion,
                    notifyResponse.SubdomainPathId,
                    notifyResponse.PathAbandonedTenantsSchemeShards
                );
            } else {
                return TState(record.GetStrong(), pathVersion);
            }
        }

        TString ToString() const {
            TString result;
            TStringOutput out(result);
            Out(out);
            return result;
        }

        void Out(IOutputStream& o) const {
            o << "{"
                << " Deleted: " << Deleted
                << " Strong: " << Strong
                << " Version: " << Version
                << " DomainId: " << DomainId
                << " AbandonedSchemeShards: " << "there are " << AbandonedSchemeShards.size() << " elements";
            if (AbandonedSchemeShards.size() > 0) {
                o << ", first is " << *AbandonedSchemeShards.begin();
            }
            if (AbandonedSchemeShards.size() > 1) {
                o << ", last is " << *AbandonedSchemeShards.rbegin();
            }
            o << " }";
        }

        bool LessThan(const TState& other, TString& reason) const {
            if (!Strong && other.Strong) {
                reason = "Update to strong state";
                return true;
            }

            if (!other.Version) {
                reason = "Ignore empty state";
                return false;
            }

            if (!Version) {
                reason = "Update to non-empty state";
                return true;
            }

            if (Version.PathId.OwnerId == other.Version.PathId.OwnerId) {
                if (other.Version <= Version) {
                    reason = "Path was already updated";
                    return false;
                }

                reason = "Path was updated to new version";
                return true;
            }

            if (!DomainId && Deleted) {
                if (other.Version <= Version) {
                    reason = "Path was already deleted";
                    return false;
                }

                reason = "Path was updated to new version";
                return true;
            }

            // it is only because we need to manage undo of upgrade subdomain, finally remove it

            if (Version.PathId == other.DomainId) { // Update from TSS, GSS->TSS
                if (AbandonedSchemeShards.contains(other.Version.PathId.OwnerId)) { // TSS is ignored, present GSS reverted that TSS
                    reason = "Update was ignored, GSS implicitly banned that TSS";
                    return false;
                }

                reason = "Path was updated as a replacement from TSS, GSS->TSS";
                return true;
            }

            if (DomainId == other.Version.PathId) { // Update from GSS, TSS->GSS
                if (other.AbandonedSchemeShards.contains(Version.PathId.OwnerId)) { // GSS reverts TSS
                    reason = "Path was updated as a replacement from GSS, GSS implicitly reverts TSS";
                    return true;
                }

                reason = "Update was ignored, TSS is preserved";
                return false;
            }

            if (DomainId == other.DomainId) {
                if (other.Version <= Version) {
                    reason = "Path was already updated";
                    return false;
                }

                reason = "Path was updated to new version";
                return true;
            } else  if (DomainId < other.DomainId) {
                reason = "New domain is detected, it is newer path then we know";
                return true;
            } else {
                reason = "Totally ignore the update";
                return false;
            }

            Y_FAIL_S("Unknown update"
                << ": state# " << *this
                << ", other state# " << other);
        }

        bool LessThan(const TState& other) const {
            TString unused;
            return LessThan(other, unused);
        }

    };

    struct TEvPrivate {
        enum EEv {
            EvReplicaMissing = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSwitchReplica,

            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvReplicaMissing : public TEventLocal<TEvReplicaMissing, EvReplicaMissing> {
            // empty
        };
    };

} // anonymous

template <typename TPath, typename TDerived>
class TReplicaSubscriber: public TMonitorableActor<TDerived> {
    void Handle(NInternalEvents::TEvNotify::TPtr& ev) {
        auto& record = *ev->Get()->MutableRecord();

        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        this->Send(ev->Sender, new NInternalEvents::TEvNotifyAck(record.GetVersion()));

        if (!IsValidNotification(Path, record)) {
            SBS_LOG_E("Suspicious " << ev->Get()->ToString()
                << ": sender# " << ev->Sender);
            return;
        }

        this->Send(Parent, ev->Release().Release(), 0, ev->Cookie);
    }

    void Handle(NInternalEvents::TEvSyncVersionRequest::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        CurrentSyncRequest = ev->Cookie;
        this->Send(Replica, ev->Release().Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(NInternalEvents::TEvSyncVersionResponse::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        if (ev->Cookie != CurrentSyncRequest) {
            return;
        }

        this->Send(Parent, ev->Release().Release(), 0, ev->Cookie);
        CurrentSyncRequest = 0;
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(this->SelfId(), this->ActorActivityType());
        auto& record = *response->Record.MutableReplicaSubscriberResponse();

        ActorIdToProto(Parent, record.MutableParent());
        ActorIdToProto(Replica, record.MutableReplica());

        record.SetDomainOwnerId(DomainOwnerId);
        record.SetCurrentSyncRequest(CurrentSyncRequest);

        if constexpr (std::is_same_v<TPath, TString>) {
            record.SetPath(Path);
        } else {
            record.MutablePathId()->SetOwnerId(Path.OwnerId);
            record.MutablePathId()->SetLocalPathId(Path.LocalPathId);
        }

        this->Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        // We notify parent that this replica is missing, but we stay alive
        // until the node is disconnected, in which case we assume the node
        // may reboot and actor is launched.
        this->Send(Parent, new TEvPrivate::TEvReplicaMissing);
    }

    void PassAway() override {
        if (Replica.NodeId() != this->SelfId().NodeId()) {
            this->Send(MakeInterconnectProxyId(Replica.NodeId()), new TEvents::TEvUnsubscribe());
        }

        this->Send(Replica, new NInternalEvents::TEvUnsubscribe(Path));
        this->Send(Parent, new TEvents::TEvGone());

        TMonitorableActor<TDerived>::PassAway();
    }

    NJson::TJsonMap MonAttributes() const override {
        return {
            {"Parent", TMonitorableActor<TDerived>::PrintActorIdAttr(NKikimrServices::TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, Parent)},
            {"Replica", TMonitorableActor<TDerived>::PrintActorIdAttr(NKikimrServices::TActivity::SCHEME_BOARD_REPLICA_ACTOR, Replica)},
            {"Path", ToString(Path)},
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "replica"sv;
    }

    explicit TReplicaSubscriber(
            const TActorId& parent,
            const TActorId& replica,
            const TPath& path,
            const ui64 domainOwnerId)
        : Parent(parent)
        , Replica(replica)
        , Path(path)
        , DomainOwnerId(domainOwnerId)
        , CurrentSyncRequest(0)
    {
    }

    void Bootstrap(const TActorContext&) {
        TMonitorableActor<TDerived>::Bootstrap();

        this->Send(Replica, new NInternalEvents::TEvSubscribe(Path, DomainOwnerId),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        this->Become(&TDerived::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternalEvents::TEvNotify, Handle);
            hFunc(NInternalEvents::TEvSyncVersionRequest, Handle);
            hFunc(NInternalEvents::TEvSyncVersionResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, PassAway);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    using TBase = TReplicaSubscriber<TPath, TDerived>;

private:
    const TActorId Parent;
    const TActorId Replica;
    const TPath Path;
    const ui64 DomainOwnerId;

    ui64 CurrentSyncRequest;

}; // TReplicaSubscriber

class TReplicaSubscriberByPath: public TReplicaSubscriber<TString, TReplicaSubscriberByPath> {
public:
    using TBase::TBase;
};

class TReplicaSubscriberByPathId: public TReplicaSubscriber<TPathId, TReplicaSubscriberByPathId> {
public:
    using TBase::TBase;
};

template <typename TPath, typename TDerived, typename TReplicaDerived>
class TSubscriberProxy: public TMonitorableActor<TDerived> {
    void Handle(NInternalEvents::TEvNotify::TPtr& ev) {
        if (ev->Sender != ReplicaSubscriber) {
            return;
        }

        this->Send(Parent, ev->Release().Release(), 0, ev->Cookie);
        Delay = DefaultDelay;
    }

    void Handle(NInternalEvents::TEvSyncVersionRequest::TPtr& ev) {
        if (!ReplicaMissing) {
            CurrentSyncRequest = ev->Cookie;
            this->Send(ReplicaSubscriber, ev->Release().Release(), 0, ev->Cookie);
        } else {
            this->Send(Parent, new NInternalEvents::TEvSyncVersionResponse(0, true), 0, ev->Cookie);
        }
    }

    void HandleSleep(NInternalEvents::TEvSyncVersionRequest::TPtr& ev) {
        this->Send(Parent, new NInternalEvents::TEvSyncVersionResponse(0, true), 0, ev->Cookie);
    }

    void Handle(NInternalEvents::TEvSyncVersionResponse::TPtr& ev) {
        if (ev->Sender != ReplicaSubscriber || ev->Cookie != CurrentSyncRequest) {
            return;
        }

        this->Send(Parent, ev->Release().Release(), 0, ev->Cookie);
        CurrentSyncRequest = 0;
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(this->SelfId(), this->ActorActivityType());
        auto& record = *response->Record.MutableSubscriberProxyResponse();

        ActorIdToProto(Parent, record.MutableParent());
        ActorIdToProto(Replica, record.MutableReplica());
        ActorIdToProto(ReplicaSubscriber, record.MutableReplicaSubscriber());

        record.SetDomainOwnerId(DomainOwnerId);
        record.SetCurrentSyncRequest(CurrentSyncRequest);

        if constexpr (std::is_same_v<TPath, TString>) {
            record.SetPath(Path);
        } else {
            record.MutablePathId()->SetOwnerId(Path.OwnerId);
            record.MutablePathId()->SetLocalPathId(Path.LocalPathId);
        }

        this->Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void OnReplicaFailure() {
        if (CurrentSyncRequest) {
            this->Send(Parent, new NInternalEvents::TEvSyncVersionResponse(0, true), 0, CurrentSyncRequest);
            CurrentSyncRequest = 0;
        }

        this->Send(Parent, new NInternalEvents::TEvNotifyBuilder(Path, true));
    }

    void Handle(TEvents::TEvGone::TPtr& ev) {
        if (ev->Sender != ReplicaSubscriber) {
            return;
        }

        if (!ReplicaMissing) {
            OnReplicaFailure();
        }

        ReplicaSubscriber = TActorId();
        ReplicaMissing = false;

        this->Become(&TDerived::StateSleep, Delay, new TEvents::TEvWakeup());
        Delay = Min(Delay * 2, MaxDelay);
    }

    void Handle(TEvPrivate::TEvReplicaMissing::TPtr& ev) {
        if (ev->Sender != ReplicaSubscriber) {
            return;
        }

        if (!ReplicaMissing) {
            OnReplicaFailure();

            ReplicaMissing = true;
        }
    }

    void PassAway() override {
        if (ReplicaSubscriber) {
            this->Send(ReplicaSubscriber, new TEvents::TEvPoisonPill());
        }

        TMonitorableActor<TDerived>::PassAway();
    }

    NJson::TJsonMap MonAttributes() const override {
        return {
            {"Parent", TMonitorableActor<TDerived>::PrintActorIdAttr(NKikimrServices::TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR, Parent)},
            {"ReplicaIndex", TStringBuilder() << ReplicaIndex << '/' << TotalReplicas},
            {"Path", ToString(Path)},
        };
    }

    void HandleSwitchReplica(STATEFN_SIG) {
        Replica = ev->Sender;
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, ReplicaSubscriber, this->SelfId(), nullptr, 0));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "proxy"sv;
    }

    explicit TSubscriberProxy(
            const TActorId& parent,
            const ui32 replicaIndex,
            const ui32 totalReplicas,
            const TActorId& replica,
            const TPath& path,
            const ui64 domainOwnerId)
        : Parent(parent)
        , ReplicaIndex(replicaIndex)
        , TotalReplicas(totalReplicas)
        , Replica(replica)
        , Path(path)
        , DomainOwnerId(domainOwnerId)
        , Delay(DefaultDelay)
        , CurrentSyncRequest(0)
    {
    }

    void Bootstrap(const TActorContext&) {
        TMonitorableActor<TDerived>::Bootstrap();

        ReplicaSubscriber = this->RegisterWithSameMailbox(new TReplicaDerived(this->SelfId(), Replica, Path, DomainOwnerId));
        this->Become(&TDerived::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternalEvents::TEvNotify, Handle);
            hFunc(NInternalEvents::TEvSyncVersionRequest, Handle);
            hFunc(NInternalEvents::TEvSyncVersionResponse, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            hFunc(TEvents::TEvGone, Handle);
            hFunc(TEvPrivate::TEvReplicaMissing, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);

            fFunc(TEvPrivate::EvSwitchReplica, HandleSwitchReplica);
        }
    }

    STFUNC(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternalEvents::TEvSyncVersionRequest, HandleSleep);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            CFunc(TEvents::TEvWakeup::EventType, Bootstrap);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);

            fFunc(TEvPrivate::EvSwitchReplica, HandleSwitchReplica);
        }
    }

    using TBase = TSubscriberProxy<TPath, TDerived, TReplicaDerived>;

private:
    const TActorId Parent;
    const ui32 ReplicaIndex;
    const ui32 TotalReplicas;
    TActorId Replica;
    const TPath Path;
    const ui64 DomainOwnerId;

    TActorId ReplicaSubscriber;
    TDuration Delay;

    ui64 CurrentSyncRequest;
    bool ReplicaMissing = false;

    static constexpr TDuration DefaultDelay = TDuration::MilliSeconds(10);
    static constexpr TDuration MaxDelay = TDuration::Seconds(5);

}; // TSubscriberProxy

class TSubscriberProxyByPath: public TSubscriberProxy<TString, TSubscriberProxyByPath, TReplicaSubscriberByPath> {
public:
    using TBase::TBase;
};

class TSubscriberProxyByPathId: public TSubscriberProxy<TPathId, TSubscriberProxyByPathId, TReplicaSubscriberByPathId> {
public:
    using TBase::TBase;
};

template <typename TPath, typename TDerived, typename TProxyDerived>
class TSubscriber: public TMonitorableActor<TDerived> {
    template <typename TNotify, typename... Args>
    static THolder<TNotify> BuildNotify(const NKikimrSchemeBoard::TEvNotify& record, Args&&... args) {
        THolder<TNotify> notify;

        TString path;
        TPathId pathId;

        if (record.HasPath()) {
            path = record.GetPath();
        }
        if (record.HasPathOwnerId() && record.HasLocalPathId()) {
            pathId = TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
        }

        Y_ABORT_UNLESS(path || pathId);

        if (!pathId) {
            notify = MakeHolder<TNotify>(path, std::forward<Args>(args)...);
        } else if (!path) {
            notify = MakeHolder<TNotify>(pathId, std::forward<Args>(args)...);
        } else {
            notify = MakeHolder<TNotify>(path, pathId, std::forward<Args>(args)...);
        }

        return notify;
    }

    TMap<TActorId, TState>::const_iterator SelectStateImpl() const {
        Y_ABORT_UNLESS(!States.empty());

        auto it = States.begin();
        auto newest = it;

        while (++it != States.end()) {
            if (newest->second.LessThan(it->second)) {
                newest = it;
            }
        }

        return newest;
    }

    const TState& SelectState() const {
        auto it = SelectStateImpl();
        Y_ABORT_UNLESS(it != States.end());
        return it->second;
    }

    TNotifyResponse* SelectResponse() {
        Y_ABORT_UNLESS(IsMajorityReached());

        auto newest = SelectStateImpl();
        Y_ABORT_UNLESS(newest != States.end());

        auto it = InitialResponses.find(newest->first);
        Y_ABORT_UNLESS(it != InitialResponses.end());

        return &it->second;
    }

    bool IsMajorityReached() const {
        return InitialResponses.size() > (Proxies.size() / 2);
    }

    void EnqueueSyncRequest(NInternalEvents::TEvSyncRequest::TPtr& ev) {
        DelayedSyncRequest = Max(DelayedSyncRequest, ev->Cookie);
    }

    bool MaybeRunVersionSync() {
        if (!DelayedSyncRequest) {
            return false;
        }

        CurrentSyncRequest = DelayedSyncRequest;
        DelayedSyncRequest = 0;

        Y_ABORT_UNLESS(PendingSync.empty());
        for (const auto& [proxy, replica] : Proxies) {
            this->Send(proxy, new NInternalEvents::TEvSyncVersionRequest(Path), 0, CurrentSyncRequest);
            PendingSync.emplace(proxy);
        }

        return true;
    }

    void Handle(NInternalEvents::TEvNotify::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        if (!IsValidNotification(Path, ev->Get()->GetRecord())) {
            SBS_LOG_E("Suspicious " << ev->Get()->ToString()
                << ": sender# " << ev->Sender);
            return;
        }

        // TEvNotify message is consumed here, can't be used after this point
        TNotifyResponse notifyResponse = TNotifyResponse::FromNotify(std::move(*ev->Get()->MutableRecord()));
        TNotifyResponse* selectedNotify = &notifyResponse;

        States[ev->Sender] = TState::FromNotify(notifyResponse);
        if (!IsMajorityReached()) {
            InitialResponses[ev->Sender] = std::move(notifyResponse);
            if (IsMajorityReached()) {
                MaybeRunVersionSync();
                selectedNotify = SelectResponse();
            } else {
                return;
            }
        }

        const TState& newestState = SelectState();
        TString reason;

        if (!State) {
            SBS_LOG_N("Set up state"
                << ": owner# " << Owner
                << ", state# " << newestState);
            State = newestState;
        } else if (State->LessThan(newestState, reason)) {
            SBS_LOG_N("" << reason
                << ": owner# " << Owner
                << ", state# " << *State
                << ", new state# " << newestState);
            State = newestState;
        } else if (!State->Deleted && newestState.Deleted && newestState.Strong) {
            SBS_LOG_N("Path was deleted"
                << ": owner# " << Owner
                << ", state# " << *State
                << ", new state# " << newestState);
            State = newestState;
        } else {
            SBS_LOG_I("" << reason
                << ": owner# " << Owner
                << ", state# " << *State
                << ", other state# " << newestState);
            return;
        }

        const auto& record = selectedNotify->Notify;

        if (!record.GetIsDeletion()) {
            NKikimrScheme::TEvDescribeSchemeResult proto = selectedNotify->GetDescribeSchemeResult();
            this->Send(Owner, BuildNotify<TSchemeBoardEvents::TEvNotifyUpdate>(record, std::move(proto)));
        } else {
            this->Send(Owner, BuildNotify<TSchemeBoardEvents::TEvNotifyDelete>(record, record.GetStrong()));
        }
    }

    void Handle(NInternalEvents::TEvSyncRequest::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        if (ev->Sender != Owner) {
            SBS_LOG_W("Suspicious " << ev->Get()->ToString()
                << ": sender# " << ev->Sender
                << ", owner# " << Owner);
            return;
        }

        EnqueueSyncRequest(ev);

        if (PendingSync || !IsMajorityReached()) {
            return;
        }

        Y_ABORT_UNLESS(MaybeRunVersionSync());
    }

    void Handle(NInternalEvents::TEvSyncVersionResponse::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        if (ev->Cookie != CurrentSyncRequest) {
            SBS_LOG_D("Sync cookie mismatch"
                << ": sender# " << ev->Sender
                << ", cookie# " << ev->Cookie
                << ", current cookie# " << CurrentSyncRequest);
            return;
        }

        auto it = PendingSync.find(ev->Sender);
        if (it == PendingSync.end()) {
            SBS_LOG_D("Unexpected sync response"
                << ": sender# " << ev->Sender
                << ", cookie# " << ev->Cookie);
            return;
        }

        PendingSync.erase(it);
        Y_ABORT_UNLESS(!ReceivedSync.contains(ev->Sender));
        ReceivedSync[ev->Sender] = ev->Get()->Record.GetPartial();

        ui32 successes = 0;
        ui32 failures = 0;
        for (const auto& [_, partial] : ReceivedSync) {
            if (!partial) {
                ++successes;
            } else {
                ++failures;
            }
        }

        const ui32 size = Proxies.size();
        const ui32 half = size / 2;
        if (successes <= half && failures <= half && (successes + failures) < size) {
            SBS_LOG_D("Sync is in progress"
                << ": cookie# " << ev->Cookie
                << ", size# " << size
                << ", half# " << half
                << ", successes# " << successes
                << ", faulires# " << failures);
            return;
        }

        const bool partial = !(successes > half);
        const TString done = TStringBuilder() << "Sync is done"
            << ": cookie# " << ev->Cookie
            << ", size# " << size
            << ", half# " << half
            << ", successes# " << successes
            << ", faulires# " << failures
            << ", partial# " << partial;

        if (!partial) {
            SBS_LOG_D(done);
        } else {
            SBS_LOG_W(done);
        }

        this->Send(Owner, new NInternalEvents::TEvSyncResponse(Path, partial), 0, ev->Cookie);

        PendingSync.clear();
        ReceivedSync.clear();

        MaybeRunVersionSync();
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr& ev) {
        SBS_LOG_D("Handle " << ev->Get()->ToString());

        const auto& replicas = ev->Get()->Replicas;

        if (replicas.empty()) {
            Y_ABORT_UNLESS(Proxies.empty());
            SBS_LOG_E("Subscribe on unconfigured SchemeBoard");
            this->Become(&TDerived::StateCalm);
            return;
        }

        Y_ABORT_UNLESS(Proxies.empty() || Proxies.size() == replicas.size());

        if (Proxies.empty()) {
            for (size_t i = 0; i < replicas.size(); ++i) {
                Proxies.emplace_back(this->RegisterWithSameMailbox(new TProxyDerived(this->SelfId(), i, replicas.size(),
                    replicas[i], Path, DomainOwnerId)), replicas[i]);
            }
        } else {
            for (size_t i = 0; i < replicas.size(); ++i) {
                if (auto& [proxy, replica] = Proxies[i]; replica != replicas[i]) {
                    TActivationContext::Send(new IEventHandle(TEvPrivate::EvSwitchReplica, 0, proxy, replicas[i], nullptr, 0));
                    replica = replicas[i];
                }
            }
        }

        this->Become(&TDerived::StateWork);
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(this->SelfId(), this->ActorActivityType());
        auto& record = *response->Record.MutableSubscriberResponse();

        ActorIdToProto(Owner, record.MutableOwner());

        record.SetDomainOwnerId(DomainOwnerId);
        record.SetDelayedSyncRequest(DelayedSyncRequest);
        record.SetCurrentSyncRequest(CurrentSyncRequest);

        auto fillState = [](const auto& from, auto& to) {
            to.SetDeleted(from.Deleted);
            to.SetStrong(from.Strong);
            to.SetVersion(from.Version.Version);

            to.MutablePathId()->SetOwnerId(from.Version.PathId.OwnerId);
            to.MutablePathId()->SetLocalPathId(from.Version.PathId.LocalPathId);

            to.MutableDomainId()->SetOwnerId(from.DomainId.OwnerId);
            to.MutableDomainId()->SetLocalPathId(from.DomainId.LocalPathId);

            for (const auto tabletId : from.AbandonedSchemeShards) {
                to.AddAbandonedSchemeShards(tabletId);
            }
        };

        for (const auto& [proxy, state] : States) {
            auto& proxyState = *record.AddProxyStates();

            ActorIdToProto(proxy, proxyState.MutableProxy());
            fillState(state, *proxyState.MutableState());
        }

        if (State) {
            fillState(*State, *record.MutableState());
        }

        if constexpr (std::is_same_v<TPath, TString>) {
            record.SetPath(Path);
        } else {
            record.MutablePathId()->SetOwnerId(Path.OwnerId);
            record.MutablePathId()->SetLocalPathId(Path.LocalPathId);
        }

        this->Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void HandleUndelivered() {
        SBS_LOG_E("Subscribe on unavailable SchemeBoard");
        this->Become(&TDerived::StateCalm);
    }

    void PassAway() override {
        for (const auto& [proxy, replica] : Proxies) {
            this->Send(proxy, new TEvents::TEvPoisonPill());
        }

        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeStateStorageProxyID(),
            this->SelfId(), nullptr, 0));

        TMonitorableActor<TDerived>::PassAway();
    }

    NJson::TJsonMap MonAttributes() const override {
        return {
            {"Owner", ToString(Owner)},
            {"Path", ToString(Path)},
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "main"sv;
    }

    explicit TSubscriber(
            const TActorId& owner,
            const TPath& path,
            const ui64 domainOwnerId)
        : Owner(owner)
        , Path(path)
        , DomainOwnerId(domainOwnerId)
        , DelayedSyncRequest(0)
        , CurrentSyncRequest(0)
    {
    }

    void Bootstrap(const TActorContext&) {
        TMonitorableActor<TDerived>::Bootstrap();

        const TActorId proxy = MakeStateStorageProxyID();
        this->Send(proxy, new TEvStateStorage::TEvResolveSchemeBoard(Path, true), IEventHandle::FlagTrackDelivery);
        this->Become(&TDerived::StateResolve);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternalEvents::TEvSyncRequest, EnqueueSyncRequest); // from owner (cache)

            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);

            hFunc(NInternalEvents::TEvNotify, Handle);
            hFunc(NInternalEvents::TEvSyncRequest, Handle); // from owner (cache)
            hFunc(NInternalEvents::TEvSyncVersionResponse, Handle); // from proxies

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    using TBase = TSubscriber<TPath, TDerived, TProxyDerived>;

private:
    const TActorId Owner;
    const TPath Path;
    const ui64 DomainOwnerId;

    std::vector<std::tuple<TActorId, TActorId>> Proxies;
    TMap<TActorId, TState> States;
    TMap<TActorId, TNotifyResponse> InitialResponses;
    TMaybe<TState> State;

    ui64 DelayedSyncRequest;
    ui64 CurrentSyncRequest;
    TSet<TActorId> PendingSync;
    TMap<TActorId, bool> ReceivedSync;

}; // TSubscriber

class TSubscriberByPath: public TSubscriber<TString, TSubscriberByPath, TSubscriberProxyByPath> {
public:
    using TBase::TBase;
};

class TSubscriberByPathId: public TSubscriber<TPathId, TSubscriberByPathId, TSubscriberProxyByPathId> {
public:
    using TBase::TBase;
};

} // NSchemeBoard

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path
) {
    auto *domain = AppData()->DomainsInfo->GetDomain();
    ui64 domainOwnerId = domain->SchemeRoot;
    return CreateSchemeBoardSubscriber(owner, path, domainOwnerId);
}

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path,
    const ui64 domainOwnerId
) {
    return new NSchemeBoard::TSubscriberByPath(owner, path, domainOwnerId);
}

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TPathId& pathId,
    const ui64 domainOwnerId
) {
    return new NSchemeBoard::TSubscriberByPathId(owner, pathId, domainOwnerId);
}

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TString& path,
    const EDeletionPolicy deletionPolicy
) {
    Y_UNUSED(deletionPolicy);
    return new NSchemeBoard::TSubscriberByPath(owner, path, 0);
}

IActor* CreateSchemeBoardSubscriber(
    const TActorId& owner,
    const TPathId& pathId,
    const EDeletionPolicy deletionPolicy
) {
    Y_UNUSED(deletionPolicy);
    return new NSchemeBoard::TSubscriberByPathId(owner, pathId, 0);
}

} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NSchemeBoard::TPathVersion, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NSchemeBoard::TState, o, x) {
    return x.Out(o);
}
