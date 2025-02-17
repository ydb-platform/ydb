#include "statestorage_impl.h"
#include "tabletid.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/random_provider/random_provider.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/xrange.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOARD_LOOKUP, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOARD_LOOKUP, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BOARD_LOOKUP, stream)

namespace NKikimr {

class TBoardLookupActor : public TActorBootstrapped<TBoardLookupActor> {
    const TString Path;
    const TActorId Owner;
    const ui64 Cookie;
    const EBoardLookupMode Mode;
    const bool Subscriber;
    TBoardRetrySettings BoardRetrySettings;

    static constexpr int MAX_REPLICAS_COUNT_EXP = 32; // Replicas.size() <= 2**MAX_REPLICAS_COUNT_EXP

    struct TEvPrivate {
        enum EEv {
            EvReconnectReplicas = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvReconnectReplicas :
                public TEventLocal<TEvReconnectReplicas, EEv::EvReconnectReplicas> {
            ui32 ReplicaIdx;

            TEvReconnectReplicas(ui32 replicaIdx) : ReplicaIdx(replicaIdx) {
            }
        };
    };

    enum class EReplicaState {
        Unknown,
        NotAvailable,
        NoInfo,
        Ready,
        Reconnect,
    };

    struct TReplica {
        TActorId Replica;
        EReplicaState State = EReplicaState::Unknown;
        THashSet<TActorId> Infos;
        bool IsScheduled = false;
        ui32 ReconnectNumber = 0;
        NMonotonic::TMonotonic LastReconnectAt = TMonotonic::Zero();
        TDuration CurrentDelay = TDuration::Zero();
    };

    TVector<TReplica> Replicas;

    TMap<TActorId, TEvStateStorage::TBoardInfoEntry> Info;
    THashMap<TActorId, THashSet<ui32>> InfoReplicas;

    ui32 WaitForReplicasToSuccess;

    const TDuration& GetCurrentDelay(TReplica& replica) {
        if (replica.CurrentDelay == TDuration::Zero()) {
            replica.CurrentDelay = BoardRetrySettings.StartDelayMs;
        }
        return replica.CurrentDelay;
    }

    TDuration GetReconnectDelayForReplica(TReplica& replica) {
        auto newDelay = replica.CurrentDelay;
        newDelay *= 2;
        if (newDelay > BoardRetrySettings.MaxDelayMs) {
            newDelay = BoardRetrySettings.MaxDelayMs;
        }
        newDelay *= AppData()->RandomProvider->Uniform(50, 200);
        newDelay /= 100;
        replica.CurrentDelay = newDelay;
        return replica.CurrentDelay;
    }

    struct {
        ui32 Replied = 0;
        ui32 NoInfo = 0;
        ui32 HasInfo = 0;
        ui32 NotAvailable = 0;
    } Stats;

    void PassAway() override {
        for (const auto &replica : Replicas) {
            if (Subscriber) {
                Send(replica.Replica, new TEvStateStorage::TEvReplicaBoardUnsubscribe());
            }
            if (replica.Replica.NodeId() != SelfId().NodeId()) {
                Send(TActivationContext::InterconnectProxy(replica.Replica.NodeId()), new TEvents::TEvUnsubscribe());
            }
        }
        TActor::PassAway();
    }

    void NotAvailable() {
        if (CurrentStateFunc() != &TThis::StateSubscribe) {
            Send(Owner, new TEvStateStorage::TEvBoardInfo(
                TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable, Path), 0, Cookie);
        } else {
            Send(Owner,
                new TEvStateStorage::TEvBoardInfoUpdate(
                    TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable, Path
                ), 0, Cookie
            );
        }
        return PassAway();
    }

    void CheckCompletion() {
        if (CurrentStateFunc() != &TThis::StateSubscribe) {
            if ((!Subscriber && Stats.HasInfo == WaitForReplicasToSuccess) ||
                    (Subscriber && Stats.HasInfo + Stats.NoInfo == WaitForReplicasToSuccess)) {
                auto reply = MakeHolder<TEvStateStorage::TEvBoardInfo>(
                    TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
                reply->InfoEntries = std::move(Info);
                Send(Owner, std::move(reply), 0, Cookie);
                if (Subscriber) {
                    Become(&TThis::StateSubscribe);
                    return;
                }
                return PassAway();
            }

            if (!Subscriber) {
                if (Stats.Replied == Replicas.size()) {
                    return NotAvailable();
                }
            } else {
                if (Stats.NotAvailable > (Replicas.size() - WaitForReplicasToSuccess)) {
                    return NotAvailable();
                }
            }
        } else {
            if (Stats.NotAvailable > (Replicas.size() - WaitForReplicasToSuccess)) {
                return NotAvailable();
            }
        }
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev) {
        auto *msg = ev->Get();

        if (msg->Replicas.empty()) {
            BLOG_ERROR("lookup on unconfigured statestorage board service");
            return NotAvailable();
        }

        Replicas.resize(msg->Replicas.size());
        for (auto idx : xrange(msg->Replicas.size())) {
            const TActorId &replica = msg->Replicas[idx];
            Send(replica,
                new TEvStateStorage::TEvReplicaBoardLookup(Path, Subscriber),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                EncodeCookie(idx, 0));
            Replicas[idx].Replica = replica;
            Replicas[idx].State = EReplicaState::Unknown;
        }

        switch (Mode) {
        case EBoardLookupMode::First:
        case EBoardLookupMode::FirstNonEmptyDoubleTime:
            WaitForReplicasToSuccess = 1;
            break;
        case EBoardLookupMode::Second:
        case EBoardLookupMode::SecondNonEmptyDoubleTime:
            WaitForReplicasToSuccess = Min<ui32>(2, Replicas.size());
            break;
        case EBoardLookupMode::Majority:
        case EBoardLookupMode::MajorityDoubleTime:
        case EBoardLookupMode::Subscription:
            WaitForReplicasToSuccess = (Replicas.size() / 2 + 1);
            break;
        default:
            Y_ABORT("unsupported mode");
        }

        Become(&TThis::StateLookup);
    }

    void Handle(TEvStateStorage::TEvReplicaBoardInfoUpdate::TPtr &ev) {
        const auto [idx, reconnectNumber] = DecodeCookie(ev->Cookie);

        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];

        if (reconnectNumber != replica.ReconnectNumber) {
            return;
        }
        if (replica.State != EReplicaState::Ready && replica.State != EReplicaState::NoInfo) {
            return;
        }

        const auto &record = ev->Get()->Record;
        auto& info = record.GetInfo();
        const TActorId oid = ActorIdFromProto(info.GetOwner());

        auto& replicas = InfoReplicas[oid];
        if (info.GetDropped()) {
            replicas.erase(idx);
            replica.Infos.erase(oid);
        } else {
            replicas.insert(idx);
            replica.Infos.insert(oid);
        }

        if (CurrentStateFunc() == &TThis::StateSubscribe) {
            std::optional<TEvStateStorage::TBoardInfoEntry> update;
            if (info.GetDropped()) {
                if (!replicas.empty()) {
                    return;
                }
                InfoReplicas.erase(oid);
                Info.erase(oid);
                update = { "", true };
            } else {
                auto& currentInfo = Info[oid];
                if (currentInfo.Payload != info.GetPayload()) {
                    currentInfo.Payload = info.GetPayload();
                    update = { info.GetPayload(), false };
                }
            }
            if (update.has_value()) {
                auto reply = MakeHolder<TEvStateStorage::TEvBoardInfoUpdate>(
                    TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
                reply->Updates = { { oid, std::move(update.value()) } };
                Send(Owner, std::move(reply), 0, Cookie);
            }
        } else {
            if (info.GetDropped()) {
                if (!replicas.empty()) {
                    return;
                }
                InfoReplicas.erase(oid);
                Info.erase(oid);
            } else {
                Info[oid].Payload = info.GetPayload();
            }
        }
    }

    void Handle(TEvStateStorage::TEvReplicaBoardInfo::TPtr &ev) {
        const auto [idx, reconnectNumber] = DecodeCookie(ev->Cookie);

        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];

        if (reconnectNumber != replica.ReconnectNumber) {
            return;
        }
        if (replica.State != EReplicaState::Unknown && replica.State != EReplicaState::Reconnect) {
            return;
        }

        const auto &record = ev->Get()->Record;

        if (replica.State == EReplicaState::Unknown) {
            Stats.Replied++;
        } else {
            Y_ABORT_UNLESS(Stats.NotAvailable);
            Stats.NotAvailable--;
        }

        if (record.GetDropped()) {
            replica.State = EReplicaState::NoInfo;
            ++Stats.NoInfo;
        } else {
            Y_DEBUG_ABORT_UNLESS(record.GetInfo().size());
            replica.State = EReplicaState::Ready;
            ++Stats.HasInfo;

            bool isStateSubscribe = (CurrentStateFunc() == &TThis::StateSubscribe);
            TMap<TActorId, TEvStateStorage::TBoardInfoEntry> updates;

            for (const auto &x : record.GetInfo()) {
                const TActorId oid = ActorIdFromProto(x.GetOwner());

                auto& currentInfo = Info[oid];
                if (currentInfo.Payload != x.GetPayload()) {
                    currentInfo.Payload = x.GetPayload();
                    if (isStateSubscribe) {
                        updates[oid] = {x.GetPayload(), false};
                    }
                }

                InfoReplicas[oid].insert(idx);
                replica.Infos.insert(oid);
            }

            if (isStateSubscribe && !updates.empty()) {
                auto reply = MakeHolder<TEvStateStorage::TEvBoardInfoUpdate>(
                    TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
                reply->Updates = std::move(updates);
                Send(Owner, std::move(reply), 0, Cookie);
            }
        }

        CheckCompletion();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        for (ui32 idx = 0; idx < Replicas.size(); idx++) {
            auto& replica = Replicas[idx];
            if (replica.Replica.NodeId() == nodeId) {
                if (replica.State == EReplicaState::Unknown) {
                    ++Stats.Replied;
                }
                if (replica.State != EReplicaState::NotAvailable) {
                    if (replica.State == EReplicaState::Ready) {
                        Y_ABORT_UNLESS(Stats.HasInfo);
                        Stats.HasInfo--;
                    } else if (replica.State == EReplicaState::NoInfo) {
                        Y_ABORT_UNLESS(Stats.NoInfo);
                        Stats.NoInfo--;
                    }
                    if (replica.State != EReplicaState::Reconnect) {
                        ++Stats.NotAvailable;
                    }
                    replica.State = EReplicaState::NotAvailable;
                }

                ClearInfosByReplica(idx);
                replica.Infos.clear();

                ReconnectReplica(idx);
            }
        }

        CheckCompletion();
    }

    void Handle(TEvStateStorage::TEvReplicaShutdown::TPtr &ev) {
        const auto [idx, reconnectNumber] = DecodeCookie(ev->Cookie);

        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];

        if (reconnectNumber != replica.ReconnectNumber) {
            return;
        }

        if (replica.State != EReplicaState::Ready && replica.State != EReplicaState::NoInfo) {
            return;
        }

        if (replica.State == EReplicaState::Ready) {
            Y_ABORT_UNLESS(Stats.HasInfo);
            --Stats.HasInfo;
        } else if (replica.State == EReplicaState::NoInfo) {
            Y_ABORT_UNLESS(Stats.NoInfo);
            --Stats.NoInfo;
        }

        replica.State = EReplicaState::NotAvailable;
        ++Stats.NotAvailable;

        ClearInfosByReplica(idx);
        replica.Infos.clear();

        CheckCompletion();
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        auto *msg = ev->Get();
        if (msg->SourceType != TEvStateStorage::TEvReplicaBoardLookup::EventType)
            return;

        const auto [idx, reconnectNumber] = DecodeCookie(ev->Cookie);

        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];

        if (reconnectNumber != replica.ReconnectNumber) {
            return;
        }
        if (replica.State != EReplicaState::Reconnect && replica.State != EReplicaState::Unknown) {
            return;
        }

        if (replica.State == EReplicaState::Unknown) {
            ++Stats.Replied;
            ++Stats.NotAvailable;
        }

        replica.State = EReplicaState::NotAvailable;

        ClearInfosByReplica(idx);
        replica.Infos.clear();

        CheckCompletion();
    }

    void ReconnectReplica(ui32 replicaIdx, bool fromReconnect = false) {
        auto& replica = Replicas[replicaIdx];

        if (!Subscriber) {
            return;
        }
        if (replica.IsScheduled || replica.State != EReplicaState::NotAvailable) {
            return;
        }

        auto now = TlsActivationContext->Monotonic();
        if (now - replica.LastReconnectAt < GetCurrentDelay(replica)) {
            auto at = replica.LastReconnectAt + GetReconnectDelayForReplica(replica);
            replica.IsScheduled = true;
            Schedule(at - now, new TEvPrivate::TEvReconnectReplicas(replicaIdx));
            return;
        }
        if (!fromReconnect) {
            auto delay = TDuration::Seconds(1);
            delay *= AppData()->RandomProvider->Uniform(10, 200);
            delay /= 100;
            replica.IsScheduled = true;
            Schedule(delay, new TEvPrivate::TEvReconnectReplicas(replicaIdx));
            return;
        }

        replica.ReconnectNumber++;
        replica.State = EReplicaState::Reconnect;
        Send(replica.Replica,
            new TEvStateStorage::TEvReplicaBoardLookup(Path, Subscriber),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            EncodeCookie(replicaIdx, replica.ReconnectNumber));

        replica.LastReconnectAt = now;
    }

    void Handle(TEvPrivate::TEvReconnectReplicas::TPtr& ev) {
        const auto& idx = ev->Get()->ReplicaIdx;
        Replicas[idx].IsScheduled = false;
        ReconnectReplica(idx, true);
    }

    std::pair<ui64, ui64> DecodeCookie(ui64 cookie) {
        return {((1ULL << MAX_REPLICAS_COUNT_EXP) - 1) & cookie, cookie >> MAX_REPLICAS_COUNT_EXP};
    }

    ui64 EncodeCookie(ui64 idx, ui64 reconnectNumber) {
        return idx | (reconnectNumber << MAX_REPLICAS_COUNT_EXP);
    }

    void ClearInfosByReplica(ui32 replicaIdx) {
        bool isStateSubscribe = (CurrentStateFunc() == &TThis::StateSubscribe);
        TMap<TActorId, TEvStateStorage::TBoardInfoEntry> updates;

        const auto& replica = Replicas[replicaIdx];
        for (auto infoId : replica.Infos) {
            auto infoReplicasIt = InfoReplicas.find(infoId);
            if (infoReplicasIt == InfoReplicas.end()) {
                continue;
            }
            infoReplicasIt->second.erase(replicaIdx);
            if (infoReplicasIt->second.empty()) {
                if (isStateSubscribe) {
                    auto& update = updates[infoId];
                    update.Dropped = true;
                }
                InfoReplicas.erase(infoId);
                Info.erase(infoId);
            }
        }
        if (isStateSubscribe && !updates.empty()) {
            auto reply = MakeHolder<TEvStateStorage::TEvBoardInfoUpdate>(
                TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
            reply->Updates = std::move(updates);
            Send(Owner, std::move(reply), 0, Cookie);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_LOOKUP_ACTOR;
    }

    TBoardLookupActor(
        const TString &path, TActorId owner, EBoardLookupMode mode,
        TBoardRetrySettings boardRetrySettings, ui64 cookie = 0)
        : Path(path)
        , Owner(owner)
        , Cookie(cookie)
        , Mode(mode)
        , Subscriber(Mode == EBoardLookupMode::Subscription)
        , BoardRetrySettings(std::move(boardRetrySettings))
        , WaitForReplicasToSuccess(0)
    {}

    void Bootstrap() {
        const TActorId proxyId = MakeStateStorageProxyID();
        Send(proxyId, new TEvStateStorage::TEvResolveBoard(Path), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateResolve);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvUndelivered::EventType, NotAvailable);
        }
    }

    STATEFN(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvReplicaBoardInfoUpdate, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvStateStorage::TEvReplicaShutdown, Handle);
            hFunc(TEvPrivate::TEvReconnectReplicas, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateSubscribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvReplicaBoardInfoUpdate, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvStateStorage::TEvReplicaShutdown, Handle);
            hFunc(TEvPrivate::TEvReconnectReplicas, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBoardLookupActor(
        const TString &path, const TActorId &owner, EBoardLookupMode mode,
        TBoardRetrySettings boardRetrySettings, ui64 cookie) {
    return new TBoardLookupActor(path, owner, mode, std::move(boardRetrySettings), cookie);
}

}
