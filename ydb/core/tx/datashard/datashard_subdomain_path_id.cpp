#include "datashard_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NDataShard {

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(10);

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
        FindSubDomainPathIdActor = Register(CreateFindSubDomainPathIdActor(SelfId(), TabletID(), CurrentSchemeShardId, delayFirstRequest, MaxFindSubDomainPathIdDelay));
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

void TDataShard::Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
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
