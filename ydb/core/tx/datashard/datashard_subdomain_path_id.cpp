#include "datashard_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NDataShard {

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(10);

class TDataShard::TFindSubDomainPathIdActor : public TActorBootstrapped<TFindSubDomainPathIdActor> {
    using TBase = TActorBootstrapped<TFindSubDomainPathIdActor>;

public:
    TFindSubDomainPathIdActor(const TActorId& parent, ui64 tabletId, ui64 schemeShardId, bool delayFirstRequest)
        : Parent(parent)
        , TabletId(tabletId)
        , SchemeShardId(schemeShardId)
        , DelayNextRequest(delayFirstRequest)
    { }

    void Bootstrap() {
        if (DelayNextRequest) {
            // Wait up to a large delay, so requests from shards spread over time
            auto delay = TDuration::MicroSeconds(RandomNumber(MaxFindSubDomainPathIdDelay.MicroSeconds()));
            Timer = CreateLongTimer(TActivationContext::AsActorContext(), delay,
                new IEventHandleFat(SelfId(), SelfId(), new TEvents::TEvWakeup));
            Become(&TThis::StateSleep);
        } else {
            DelayNextRequest = true;
            WakeUp();
        }
    }

    void PassAway() override {
        if (Timer) {
            Send(Timer, new TEvents::TEvPoison);
        }
        NTabletPipe::CloseAndForgetClient(SelfId(), SchemeShardPipe);
        TBase::PassAway();
    }

private:
    STFUNC(StateSleep) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            sFunc(TEvents::TEvWakeup, WakeUp);
        }
    }

    void WakeUp() {
        Timer = { };
        SchemeShardPipe = Register(NTabletPipe::CreateClient(SelfId(), SchemeShardId));
        NTabletPipe::SendData(SelfId(), SchemeShardPipe,
            new NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathId(TabletId));
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathIdResult, Handle);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            // We could not connect to schemeshard, try again
            SchemeShardPipe = { };
            Bootstrap();
            return;
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->ClientId == SchemeShardPipe) {
            // We lost connection to schemeshard, try again
            SchemeShardPipe = { };
            Bootstrap();
            return;
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathIdResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SUCCESS) {
            // The request failed for some reason, we just stop trying in that case
            PassAway();
            return;
        }

        Send(Parent, new TEvPrivate::TEvSubDomainPathIdFound(msg->Record.GetSchemeShardId(), msg->Record.GetSubDomainPathId()));
        PassAway();
    }

private:
    const TActorId Parent;
    const ui64 TabletId;
    const ui64 SchemeShardId;
    bool DelayNextRequest;
    TActorId Timer;
    TActorId SchemeShardPipe;
};

void TDataShard::StopFindSubDomainPathId() {
    if (FindSubDomainPathIdActor) {
        Send(FindSubDomainPathIdActor, new TEvents::TEvPoison);
        FindSubDomainPathIdActor = { };
    }
}

void TDataShard::StartFindSubDomainPathId(bool delayFirstRequest) {
    if (!FindSubDomainPathIdActor &&
        CurrentSchemeShardId != 0 &&
        CurrentSchemeShardId != INVALID_TABLET_ID &&
        (!SubDomainPathId || SubDomainPathId->OwnerId != CurrentSchemeShardId))
    {
        FindSubDomainPathIdActor = Register(new TFindSubDomainPathIdActor(SelfId(), TabletID(), CurrentSchemeShardId, delayFirstRequest));
    }
}

class TDataShard::TTxPersistSubDomainPathId : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxPersistSubDomainPathId(TDataShard* self, ui64 schemeShardId, ui64 localPathId)
        : TTransactionBase(self)
        , SchemeShardId(schemeShardId)
        , LocalPathId(localPathId)
    { }

    TTxType GetTxType() const override { return TXTYPE_PERSIST_SUBDOMAIN_PATH_ID; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Self->CurrentSchemeShardId == SchemeShardId &&
            !Self->SubDomainPathId || Self->SubDomainPathId->OwnerId != SchemeShardId)
        {
            Self->PersistSubDomainPathId(SchemeShardId, LocalPathId, txc);
            Self->StartWatchingSubDomainPathId();
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 SchemeShardId;
    const ui64 LocalPathId;
};

void TDataShard::Handle(TEvPrivate::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (FindSubDomainPathIdActor == ev->Sender) {
        FindSubDomainPathIdActor = { };
    }

    Execute(new TTxPersistSubDomainPathId(this, msg->SchemeShardId, msg->LocalPathId), ctx);
}

void TDataShard::StopWatchingSubDomainPathId() {
    if (WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        WatchingSubDomainPathId.reset();
    }
}

void TDataShard::StartWatchingSubDomainPathId() {
    if (!SubDomainPathId || SubDomainPathId->OwnerId != CurrentSchemeShardId) {
        return;
    }

    if (WatchingSubDomainPathId && *WatchingSubDomainPathId != *SubDomainPathId) {
        StopWatchingSubDomainPathId();
    }

    if (!WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(*SubDomainPathId));
        WatchingSubDomainPathId = *SubDomainPathId;
    }
}

class TDataShard::TTxPersistSubDomainOutOfSpace : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxPersistSubDomainOutOfSpace(TDataShard* self, bool outOfSpace)
        : TTransactionBase(self)
        , OutOfSpace(outOfSpace)
    { }

    TTxType GetTxType() const override { return TXTYPE_PERSIST_SUBDOMAIN_OUT_OF_SPACE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        if (Self->SubDomainOutOfSpace != OutOfSpace) {
            Self->PersistSys(db, Schema::Sys_SubDomainOutOfSpace, ui64(OutOfSpace ? 1 : 0));
            Self->SubDomainOutOfSpace = OutOfSpace;
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const bool OutOfSpace;
};

void TDataShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (SubDomainPathId && msg->PathId == *SubDomainPathId) {
        const bool outOfSpace = msg->Result->GetPathDescription()
            .GetDomainDescription()
            .GetDomainState()
            .GetDiskQuotaExceeded();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Discovered subdomain " << msg->PathId << " state, outOfSpace = " << outOfSpace
            << " at datashard " << TabletID());

        Execute(new TTxPersistSubDomainOutOfSpace(this, outOfSpace), ctx);
    }
}

} // namespace NDataShard
} // namespace NKikimr
