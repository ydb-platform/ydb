#include "statestorage_impl.h"
#include "tabletid.h"
#include <ydb/core/protos/services.pb.h>
#include <library/cpp/actors/core/interconnect.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>

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
    const EBoardLookupMode Mode;
    const ui32 StateStorageGroupId;
    const bool Subscriber;

    enum class EReplicaState {
        Unknown,
        NotAvailable,
        NoInfo,
        Ready,
    };

    struct TReplica {
        TActorId Replica;
        EReplicaState State = EReplicaState::Unknown;
        THashSet<TActorId> Infos;
    };

    TVector<TReplica> Replicas;

    TMap<TActorId, TEvStateStorage::TEvBoardInfo::TInfoEntry> Info;
    THashMap<TActorId, THashSet<ui32>> InfoReplicas;

    ui32 WaitForReplicasToSuccess;

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
            Send(Owner, new TEvStateStorage::TEvBoardInfo(TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable, Path));
        } else {
            Send(Owner,
                new TEvStateStorage::TEvBoardInfoUpdate(
                    TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable, Path
                )
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
                Send(Owner, std::move(reply));
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
            BLOG_ERROR("lookup on unconfigured statestorage board service " << StateStorageGroupId);
            return NotAvailable();
        }

        Replicas.resize(msg->Replicas.size());
        for (auto idx : xrange(msg->Replicas.size())) {
            const TActorId &replica = msg->Replicas[idx];
            Send(replica,
                new TEvStateStorage::TEvReplicaBoardLookup(Path, TActorId(), Subscriber),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, idx);
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
            Y_FAIL("unsupported mode");
        }

        Become(&TThis::StateLookup);
    }

    void Handle(TEvStateStorage::TEvReplicaBoardInfoUpdate::TPtr &ev) {
        const auto &record = ev->Get()->Record;
        const ui32 idx = ev->Cookie;
        if (idx >= Replicas.size()) {
            return;
        }
        auto &replica = Replicas[idx];
        if (replica.State == EReplicaState::NotAvailable) {
            return;
        }

        replica.State = EReplicaState::Ready;

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
            TEvStateStorage::TEvBoardInfoUpdate::TInfoEntryUpdate update;
            update.Owner = oid;
            if (info.GetDropped()) {
                if (!replicas.empty()) {
                    return;
                }
                InfoReplicas.erase(oid);
                Info.erase(oid);
                update.Dropped = true;
            } else {
                if (Info[oid].Payload != info.GetPayload()) {
                    Info[oid].Payload = info.GetPayload();
                    update.Payload = std::move(info.GetPayload());
                }
            }

            auto reply = MakeHolder<TEvStateStorage::TEvBoardInfoUpdate>(
                TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
            reply->Update = std::move(update);
            Send(Owner, std::move(reply));
        } else {
            if (info.GetDropped()) {
                if (!replicas.empty()) {
                    return;
                }
                InfoReplicas.erase(oid);
                Info.erase(oid);
            } else {
                Info[oid].Payload = std::move(info.GetPayload());
            }
        }
    }

    void Handle(TEvStateStorage::TEvReplicaBoardInfo::TPtr &ev) {
        const auto &record = ev->Get()->Record;
        const ui32 idx = ev->Cookie;
        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];
        if (replica.State != EReplicaState::Unknown)
            return;
        ++Stats.Replied;
        if (record.GetDropped()) {
            replica.State = EReplicaState::NoInfo;
            ++Stats.NoInfo;
        } else {
            Y_VERIFY_DEBUG(record.GetInfo().size());
            replica.State = EReplicaState::Ready;
            ++Stats.HasInfo;

            for (auto &x : record.GetInfo()) {
                const TActorId oid = ActorIdFromProto(x.GetOwner());
                Info[oid].Payload = std::move(x.GetPayload());
                InfoReplicas[oid].insert(idx);
                replica.Infos.insert(oid);
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
                    replica.State = EReplicaState::NotAvailable;
                    ++Stats.NotAvailable;
                }

                for (auto infoId : replica.Infos) {
                    InfoReplicas[infoId].erase(idx);
                }
            }
        }

        CheckCompletion();
    }

    void Handle(TEvStateStorage::TEvReplicaShutdown::TPtr &ev) {
        const ui32 idx = ev->Cookie;
        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];
        Y_VERIFY(replica.Replica == ev->Sender);

        if (replica.State == EReplicaState::Unknown) {
            ++Stats.Replied;
        }
        if (replica.State != EReplicaState::NotAvailable) {
            replica.State = EReplicaState::NotAvailable;
            ++Stats.NotAvailable;
        }

        for (auto infoId : replica.Infos) {
            InfoReplicas[infoId].erase(idx);
        }

        CheckCompletion();
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        auto *msg = ev->Get();
        if (msg->SourceType != TEvStateStorage::TEvReplicaBoardLookup::EventType)
            return;
        const ui32 idx = ev->Cookie;
        if (idx >= Replicas.size())
            return;
        auto &replica = Replicas[idx];
        if (replica.State != EReplicaState::Unknown)
            return;
        replica.State = EReplicaState::NotAvailable;
        ++Stats.Replied;
        ++Stats.NotAvailable;

        for (auto infoId : replica.Infos) {
            InfoReplicas[infoId].erase(idx);
        }

        CheckCompletion();
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_LOOKUP_ACTOR;
    }

    TBoardLookupActor(const TString &path, TActorId owner, EBoardLookupMode mode, ui32 groupId)
        : Path(path)
        , Owner(owner)
        , Mode(mode)
        , StateStorageGroupId(groupId)
        , Subscriber(Mode == EBoardLookupMode::Subscription)
    {}

    void Bootstrap() {
        const TActorId proxyId = MakeStateStorageProxyID(StateStorageGroupId);
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
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateSubscribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaBoardInfoUpdate, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvStateStorage::TEvReplicaShutdown, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBoardLookupActor(const TString &path, const TActorId &owner, ui32 groupId, EBoardLookupMode mode) {
    return new TBoardLookupActor(path, owner, mode, groupId);
}

}
