#include "events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSql {

namespace {

class TCommitActor : public NActors::TActorBootstrapped<TCommitActor> {
public:
    TCommitActor(const NActors::TActorId& replyTo, TVector<TDeferredWrite> writes, ui32 internalErrorRetries)
        : ReplyTo(replyTo)
        , Writes(std::move(writes))
        , InternalErrorRetries(internalErrorRetries)
    {
    }

    void Bootstrap() {
        if (Writes.empty()) {
            Succeed();
            return;
        }
        TabletId = Writes[0].TabletId;
        for (const TDeferredWrite& write : Writes) {
            if (write.TabletId != TabletId) {
                Fail("0A000", "not supported");
                return;
            }
        }
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        Become(&TCommitActor::StateWork);
    }

    void Fail(TString sqlState, TString message) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
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

    void Write() {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        auto writer = NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*event);
        for (const TDeferredWrite& write : Writes) {
            const ui64 index = writer.AddDataToPayload(TString(write.Payload));
            const TTableId table(write.OwnerId, write.LocalPathId, write.SchemaVersion);
            event->AddOperation(static_cast<NKikimrDataEvents::TEvWrite::TOperation::EOperationType>(write.Operation), table, write.ColumnIds, index, NKikimrDataEvents::FORMAT_CELLVEC);
            Linked = true;
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(event.release(), TabletId, true));
    }

    void Unlink() {
        if (Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Linked = false;
        }
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        if (!TxId) {
            Fail("XX000", "internal error");
            return;
        }
        Write();
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto status = ev->Get()->Record.GetStatus();
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_COMPLETED:
                Succeed();
                return;
            case EStatus::STATUS_OVERLOADED:
                if (OverloadRetries >= 16) {
                    Fail("53000", "overloaded");
                    return;
                }
                ++OverloadRetries;
                Schedule(TDuration::MilliSeconds(50), new NActors::TEvents::TEvWakeup());
                return;
            case EStatus::STATUS_INTERNAL_ERROR:
                if (InternalRetries >= InternalErrorRetries) {
                    Fail("XX000", "internal error");
                    return;
                }
                ++InternalRetries;
                Write();
                return;
            case EStatus::STATUS_CONSTRAINT_VIOLATION:
                Fail("23505", "duplicate key value violates unique constraint");
                return;
            case EStatus::STATUS_BAD_REQUEST:
                Fail("22023", "bad request");
                return;
            default:
                Fail("XX000", "internal error");
                return;
        }
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        Write();
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleAllocate);
            hFunc(NEvents::TDataEvents::TEvWriteResult, HandleWrite);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TVector<TDeferredWrite> Writes;
    const ui32 InternalErrorRetries;
    ui64 TabletId = 0;
    ui64 TxId = 0;
    bool Linked = false;
    bool Replied = false;
    ui32 OverloadRetries = 0;
    ui32 InternalRetries = 0;
};

} // namespace

NActors::IActor* CreateCommitActor(const NActors::TActorId& replyTo, TVector<TDeferredWrite> writes, ui32 internalErrorRetries) {
    return new TCommitActor(replyTo, std::move(writes), internalErrorRetries);
}

} // namespace NKikimr::NSql
