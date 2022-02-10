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

    enum class EReplicaState {
        Unknown,
        NotAvailable,
        NoInfo,
        Ready,
    };

    struct TReplica {
        TActorId Replica;
        EReplicaState State = EReplicaState::Unknown;
    };

    TVector<TReplica> Replicas;
    TMap<TActorId, TEvStateStorage::TEvBoardInfo::TInfoEntry> Info;

    ui32 WaitForReplicasToSuccess;

    struct {
        ui32 Replied = 0;
        ui32 NoInfo = 0;
        ui32 HasInfo = 0;
    } Stats;

    void PassAway() override {
        for (const auto &replica : Replicas)
            if (replica.Replica.NodeId() != SelfId().NodeId())
                Send(TActivationContext::InterconnectProxy(replica.Replica.NodeId()), new TEvents::TEvUnsubscribe());
        TActor::PassAway();
    }

    void NotAvailable() {
        Send(Owner, new TEvStateStorage::TEvBoardInfo(TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable, Path));
        return PassAway();
    }

    void CheckCompletion() {
        if (Stats.HasInfo == WaitForReplicasToSuccess) {
            auto reply = MakeHolder<TEvStateStorage::TEvBoardInfo>(TEvStateStorage::TEvBoardInfo::EStatus::Ok, Path);
            reply->InfoEntries = std::move(Info);
            Send(Owner, std::move(reply));

            return PassAway();
        }

        if (Stats.Replied == Replicas.size())
            return NotAvailable();
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
            Send(replica, new TEvStateStorage::TEvReplicaBoardLookup(Path, TActorId(), false), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, idx);
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
            WaitForReplicasToSuccess = (Replicas.size() / 2 + 1);
            break;
        default:
            Y_FAIL("unsupported mode");
        }

        Become(&TThis::StateLookup);
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
                Info[oid].Payload = x.GetPayload();
            }
        }

        CheckCompletion();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        for (auto &replica : Replicas) {
            if (replica.Replica.NodeId() == nodeId && replica.State == EReplicaState::Unknown) {
                replica.State = EReplicaState::NotAvailable;
                ++Stats.Replied;
                ++Stats.NoInfo;
            }
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
        ++Stats.NoInfo;

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
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBoardLookupActor(const TString &path, const TActorId &owner, ui32 groupId, EBoardLookupMode mode, bool sub, bool useNodeSubsriptions) {
    Y_UNUSED(useNodeSubsriptions);
    Y_VERIFY(!sub, "subscribe mode for board lookup not implemented yet");
    return new TBoardLookupActor(path, owner, mode, groupId);
}

}
