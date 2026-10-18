#include "events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <algorithm>

namespace NKikimr::NSql {

namespace {

struct TParticipant {
    ui64 TabletId = 0;
    TVector<TDeferredWrite> Writes;
    TVector<NKikimrDataEvents::TLock> Locks;
    bool Prepared = false;
    bool Done = false;
    ui32 OverloadRetries = 0;
    ui32 InternalRetries = 0;
};

class TCommitActor : public NActors::TActorBootstrapped<TCommitActor> {
public:
    TCommitActor(const NActors::TActorId& replyTo, ui64 txId, TVector<TDeferredWrite> writes, TVector<NKikimrDataEvents::TLock> locks, ui32 internalErrorRetries)
        : ReplyTo(replyTo)
        , TxId(txId)
        , Writes(std::move(writes))
        , Locks(std::move(locks))
        , InternalErrorRetries(internalErrorRetries)
    {
    }

    void Bootstrap() {
        if (!TxId) {
            Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
            Become(&TCommitActor::StateWork);
            return;
        }
        Start();
    }

    void Start() {
        for (const TDeferredWrite& write : Writes) {
            TParticipant& participant = Find(write.TabletId);
            participant.Writes.push_back(write);
        }
        for (const auto& lock : Locks) {
            if (!lock.GetDataShard()) {
                continue;
            }
            Find(lock.GetDataShard()).Locks.push_back(lock);
        }
        if (Participants.empty()) {
            Succeed();
            return;
        }
        ui32 writers = 0;
        for (const TParticipant& participant : Participants) {
            if (!participant.Writes.empty()) {
                ++writers;
                if (!Arbiter) {
                    Arbiter = participant.TabletId;
                }
            }
        }
        if (writers < 5) {
            Arbiter = 0;
        }
        Distributed = Participants.size() > 1;
        if (!Distributed) {
            SendImmediate(Participants[0]);
            return;
        }
        for (TParticipant& participant : Participants) {
            SendPrepare(participant);
        }
        Become(&TCommitActor::StateWork);
    }

    TParticipant& Find(ui64 tabletId) {
        for (TParticipant& participant : Participants) {
            if (participant.TabletId == tabletId) {
                return participant;
            }
        }
        Participants.push_back({});
        Participants.back().TabletId = tabletId;
        return Participants.back();
    }

    void Remember(TString sqlState, TString message) {
        if (!HasError) {
            ErrorState = std::move(sqlState);
            ErrorMessage = std::move(message);
            HasError = true;
        }
    }

    void FailNow() {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->SqlState = HasError ? ErrorState : TString("XX000");
        result->Message = HasError ? ErrorMessage : TString("internal error");
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Succeed() {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "COMMIT";
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void AddWrites(NEvents::TDataEvents::TEvWrite& event, const TParticipant& participant) {
        auto writer = NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(event);
        for (const TDeferredWrite& write : participant.Writes) {
            const ui64 index = writer.AddDataToPayload(TString(write.Payload));
            const TTableId table(write.OwnerId, write.LocalPathId, write.SchemaVersion);
            event.AddOperation(static_cast<NKikimrDataEvents::TEvWrite::TOperation::EOperationType>(write.Operation), table, write.ColumnIds, index, NKikimrDataEvents::FORMAT_CELLVEC);
        }
    }

    void FillCommitLocks(NEvents::TDataEvents::TEvWrite& event, const TParticipant& participant) {
        const bool withLocks = !participant.Locks.empty() || Distributed;
        if (!withLocks) {
            return;
        }
        auto* locks = event.Record.MutableLocks();
        locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        if (Arbiter) {
            locks->SetArbiterShard(Arbiter);
        }
        for (const TParticipant& shard : Participants) {
            locks->AddSendingShards(shard.TabletId);
            locks->AddReceivingShards(shard.TabletId);
        }
        for (const auto& lock : participant.Locks) {
            *locks->AddLocks() = lock;
        }
    }

    void SendTo(ui64 tabletId, std::unique_ptr<NEvents::TDataEvents::TEvWrite> event) {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(event.release(), tabletId, true));
        if (FindTablet(tabletId) == Linked.end()) {
            Linked.push_back(tabletId);
        }
    }

    TVector<ui64>::iterator FindTablet(ui64 tabletId) {
        return std::find(Linked.begin(), Linked.end(), tabletId);
    }

    void SendImmediate(TParticipant& participant) {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        AddWrites(*event, participant);
        FillCommitLocks(*event, participant);
        SendTo(participant.TabletId, std::move(event));
        Become(&TCommitActor::StateWork);
    }

    void SendPrepare(TParticipant& participant) {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE);
        AddWrites(*event, participant);
        FillCommitLocks(*event, participant);
        SendTo(participant.TabletId, std::move(event));
    }

    void SendRollback(const TParticipant& participant) {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        auto* locks = event->Record.MutableLocks();
        locks->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
        for (const auto& lock : participant.Locks) {
            *locks->AddLocks() = lock;
        }
        SendTo(participant.TabletId, std::move(event));
    }

    bool AllPrepared() const {
        for (const TParticipant& participant : Participants) {
            if (!participant.Prepared) {
                return false;
            }
        }
        return !Participants.empty();
    }

    bool AllDone() const {
        for (const TParticipant& participant : Participants) {
            if (participant.Prepared && !participant.Done) {
                return false;
            }
        }
        return true;
    }

    void Propose() {
        if (!Coordinator || MinStep > MaxStep) {
            Remember("XX000", "internal error");
            FailNow();
            return;
        }
        Phase = EPhase::Planned;
        auto request = std::make_unique<TEvTxProxy::TEvProposeTransaction>();
        request->Record.SetCoordinatorID(Coordinator);
        auto* tx = request->Record.MutableTransaction();
        tx->SetTxId(TxId);
        tx->SetExecLevel(0);
        tx->SetMinStep(MinStep);
        tx->SetMaxStep(MaxStep);
        tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
        for (const TParticipant& participant : Participants) {
            auto* affected = tx->MutableAffectedSet()->Add();
            affected->SetTabletId(participant.TabletId);
            ui32 flags = 0;
            if (!participant.Writes.empty()) {
                flags |= TEvTxProxy::TEvProposeTransaction::AffectedWrite;
            }
            if (!participant.Locks.empty() || participant.Writes.empty()) {
                flags |= TEvTxProxy::TEvProposeTransaction::AffectedRead;
            }
            affected->SetFlags(flags);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.release(), Coordinator, true));
        if (FindTablet(Coordinator) == Linked.end()) {
            Linked.push_back(Coordinator);
        }
    }

    void FinishIfReady() {
        if (!AllDone()) {
            return;
        }
        if (HasError) {
            FailNow();
            return;
        }
        Succeed();
    }

    TString StatusState(NKikimrDataEvents::TEvWriteResult::EStatus status) const {
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_CONSTRAINT_VIOLATION:
                return "23505";
            case EStatus::STATUS_OVERLOADED:
                return "53000";
            case EStatus::STATUS_BAD_REQUEST:
                return "22023";
            case EStatus::STATUS_LOCKS_BROKEN:
                return "40001";
            default:
                return "XX000";
        }
    }

    TString StatusText(NKikimrDataEvents::TEvWriteResult::EStatus status) const {
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_CONSTRAINT_VIOLATION:
                return "duplicate key value violates unique constraint";
            case EStatus::STATUS_OVERLOADED:
                return "overloaded";
            case EStatus::STATUS_BAD_REQUEST:
                return "bad request";
            case EStatus::STATUS_LOCKS_BROKEN:
                return "could not serialize access";
            default:
                return "internal error";
        }
    }

    void NotePrepare(const NKikimrDataEvents::TEvWriteResult& record) {
        if (record.DomainCoordinatorsSize() < 1) {
            Remember("XX000", "internal error");
            return;
        }
        const ui64 coordinator = record.GetDomainCoordinators(0);
        if (!Coordinator) {
            Coordinator = coordinator;
        } else if (Coordinator != coordinator) {
            Remember("XX000", "internal error");
        }
        MinStep = std::max(MinStep, record.GetMinStep());
        MaxStep = std::min(MaxStep, record.GetMaxStep());
    }

    bool RetryPrepare(TParticipant& participant, NKikimrDataEvents::TEvWriteResult::EStatus status) {
        if (AnyPrepared) {
            return false;
        }
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        if (status == EStatus::STATUS_OVERLOADED && participant.OverloadRetries < 16) {
            ++participant.OverloadRetries;
            Schedule(TDuration::MilliSeconds(50), new NActors::TEvents::TEvWakeup());
            RetryTablet = participant.TabletId;
            return true;
        }
        if (status == EStatus::STATUS_INTERNAL_ERROR && participant.InternalRetries < InternalErrorRetries) {
            ++participant.InternalRetries;
            if (Distributed) {
                SendPrepare(participant);
            } else {
                SendImmediate(participant);
            }
            return true;
        }
        return false;
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        if (!TxId) {
            Remember("XX000", "internal error");
            FailNow();
            return;
        }
        Start();
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (Replied) {
            return;
        }
        const auto& record = ev->Get()->Record;
        TParticipant* participant = nullptr;
        for (TParticipant& candidate : Participants) {
            if (candidate.TabletId == record.GetOrigin()) {
                participant = &candidate;
                break;
            }
        }
        if (!participant && Participants.size() == 1) {
            participant = &Participants[0];
        }
        if (!participant) {
            return;
        }
        const auto status = record.GetStatus();
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        if (!Distributed) {
            if (status == EStatus::STATUS_COMPLETED) {
                participant->Prepared = true;
                participant->Done = true;
                Succeed();
                return;
            }
            if (RetryPrepare(*participant, status)) {
                return;
            }
            Remember(StatusState(status), StatusText(status));
            FailNow();
            return;
        }
        if (Phase == EPhase::Prepare) {
            if (status == EStatus::STATUS_PREPARED) {
                participant->Prepared = true;
                AnyPrepared = true;
                NotePrepare(record);
                if (HasError) {
                    FailNow();
                    return;
                }
                if (AllPrepared()) {
                    Propose();
                }
                return;
            }
            if (RetryPrepare(*participant, status)) {
                return;
            }
            Remember(StatusState(status), StatusText(status));
            if (!AnyPrepared) {
                FailNow();
                return;
            }
            WantAbort = true;
            participant->Prepared = false;
            Propose();
            return;
        }
        if (status == EStatus::STATUS_COMPLETED || status == EStatus::STATUS_ABORTED || status == EStatus::STATUS_LOCKS_BROKEN) {
            participant->Done = true;
            if (status != EStatus::STATUS_COMPLETED || WantAbort) {
                if (status != EStatus::STATUS_COMPLETED) {
                    Remember(status == EStatus::STATUS_LOCKS_BROKEN ? TString("40001") : TString("XX000"),
                        status == EStatus::STATUS_LOCKS_BROKEN ? TString("could not serialize access") : TString("internal error"));
                }
            }
            FinishIfReady();
            return;
        }
        Remember(StatusState(status), StatusText(status));
        participant->Done = true;
        FinishIfReady();
    }

    void HandlePropose(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev) {
        if (Replied || Phase != EPhase::Planned) {
            return;
        }
        const auto status = ev->Get()->GetStatus();
        if (static_cast<ui32>(status) < static_cast<ui32>(TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted)) {
            Remember("XX000", "internal error");
            for (TParticipant& participant : Participants) {
                if (participant.Prepared) {
                    participant.Done = true;
                }
            }
            FailNow();
        }
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        if (Phase != EPhase::Prepare || !RetryTablet) {
            return;
        }
        const ui64 tabletId = RetryTablet;
        RetryTablet = 0;
        for (TParticipant& participant : Participants) {
            if (participant.TabletId == tabletId && !participant.Prepared) {
                if (Distributed) {
                    SendPrepare(participant);
                } else {
                    SendImmediate(participant);
                }
            }
        }
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (Replied) {
            return;
        }
        Remember("08006", "connection failure");
        if (!AnyPrepared) {
            FailNow();
            return;
        }
        for (TParticipant& participant : Participants) {
            if (participant.TabletId == ev->Get()->TabletId) {
                participant.Done = true;
            }
        }
        FinishIfReady();
    }

    void Unlink() {
        for (const ui64 tabletId : Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(tabletId));
        }
        Linked.clear();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleAllocate);
            hFunc(NEvents::TDataEvents::TEvWriteResult, HandleWrite);
            hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandlePropose);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    enum class EPhase {
        Prepare,
        Planned,
    };

    const NActors::TActorId ReplyTo;
    ui64 TxId = 0;
    const TVector<TDeferredWrite> Writes;
    const TVector<NKikimrDataEvents::TLock> Locks;
    const ui32 InternalErrorRetries;
    TVector<TParticipant> Participants;
    TVector<ui64> Linked;
    EPhase Phase = EPhase::Prepare;
    bool Distributed = false;
    bool AnyPrepared = false;
    bool WantAbort = false;
    bool HasError = false;
    bool Replied = false;
    ui64 Arbiter = 0;
    ui64 Coordinator = 0;
    ui64 MinStep = 0;
    ui64 MaxStep = Max<ui64>();
    ui64 RetryTablet = 0;
    TString ErrorState;
    TString ErrorMessage;
};

class TRollbackActor : public NActors::TActorBootstrapped<TRollbackActor> {
public:
    TRollbackActor(const NActors::TActorId& replyTo, ui64 txId, TVector<NKikimrDataEvents::TLock> locks)
        : ReplyTo(replyTo)
        , TxId(txId)
        , Locks(std::move(locks))
    {
    }

    void Bootstrap() {
        TVector<ui64> shards;
        for (const auto& lock : Locks) {
            if (!lock.GetDataShard()) {
                continue;
            }
            if (std::find(shards.begin(), shards.end(), lock.GetDataShard()) == shards.end()) {
                shards.push_back(lock.GetDataShard());
            }
        }
        if (shards.empty()) {
            Succeed();
            return;
        }
        Pending = shards.size();
        for (const ui64 shard : shards) {
            auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            auto* locks = event->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
            for (const auto& lock : Locks) {
                if (lock.GetDataShard() == shard) {
                    *locks->AddLocks() = lock;
                }
            }
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(event.release(), shard, true));
            Linked.push_back(shard);
        }
        Become(&TRollbackActor::StateWork);
    }

    void Succeed() {
        if (Replied) {
            return;
        }
        Replied = true;
        for (const ui64 tabletId : Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(tabletId));
        }
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "ROLLBACK";
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr&) {
        if (Pending > 0) {
            --Pending;
        }
        if (Pending == 0) {
            Succeed();
        }
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        if (Pending > 0) {
            --Pending;
        }
        if (Pending == 0) {
            Succeed();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TDataEvents::TEvWriteResult, HandleWrite);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const ui64 TxId;
    const TVector<NKikimrDataEvents::TLock> Locks;
    TVector<ui64> Linked;
    ui32 Pending = 0;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateCommitActor(const NActors::TActorId& replyTo, ui64 txId, TVector<TDeferredWrite> writes, TVector<NKikimrDataEvents::TLock> locks, ui32 internalErrorRetries) {
    return new TCommitActor(replyTo, txId, std::move(writes), std::move(locks), internalErrorRetries);
}

NActors::IActor* CreateRollbackActor(const NActors::TActorId& replyTo, ui64 txId, TVector<NKikimrDataEvents::TLock> locks) {
    return new TRollbackActor(replyTo, txId, std::move(locks));
}

} // namespace NKikimr::NSql
