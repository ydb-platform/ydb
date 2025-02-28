#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TTxPersistSubDomainOutOfSpace : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPersistSubDomainOutOfSpace(TColumnShard* self, bool outOfSpace)
        : TTransactionBase(self)
        , OutOfSpace(outOfSpace)
    { }

    TTxType GetTxType() const override { return TXTYPE_PERSIST_SUBDOMAIN_OUT_OF_SPACE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        if (Self->SpaceWatcher->SubDomainOutOfSpace != OutOfSpace) {
            Schema::SaveSpecialValue(db, Schema::EValueIds::SubDomainOutOfSpace, ui64(OutOfSpace ? 1 : 0));
            Self->SpaceWatcher->SubDomainOutOfSpace = OutOfSpace;
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const bool OutOfSpace;
};

class TTxPersistSubDomainPathId : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPersistSubDomainPathId(TColumnShard* self, ui64 localPathId)
        : TTransactionBase(self)
        , LocalPathId(localPathId)
    { }

    TTxType GetTxType() const override { return TXTYPE_PERSIST_SUBDOMAIN_PATH_ID; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->SpaceWatcher->SubDomainPathId) {
            Self->SpaceWatcher->PersistSubDomainPathId(LocalPathId, txc);
            Self->SpaceWatcher->StartWatchingSubDomainPathId();
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 LocalPathId;
};

void TSpaceWatcher::PersistSubDomainPathId(ui64 localPathId,
                                               NTabletFlatExecutor::TTransactionContext &txc) {
    SubDomainPathId = localPathId;
    NIceDb::TNiceDb db(txc.DB);
    Schema::SaveSpecialValue(db, Schema::EValueIds::SubDomainLocalPathId, localPathId);
}

void TSpaceWatcher::StopWatchingSubDomainPathId() {
    if (WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        WatchingSubDomainPathId.reset();
    }
}

void TSpaceWatcher::StartWatchingSubDomainPathId() {
    if (!SubDomainPathId) {
        return;
    }

    if (!WatchingSubDomainPathId) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("started_watching_subdomain", *SubDomainPathId);
        Self->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(TPathId(Self->CurrentSchemeShardId, *SubDomainPathId)));
        WatchingSubDomainPathId = *SubDomainPathId;
    }
}

void TSpaceWatcher::Handle(NActors::TEvents::TEvPoison::TPtr& , const TActorContext& ctx) {
    Die(ctx);
}

void TColumnShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("notify_subdomain", msg->PathId);
    const bool outOfSpace = msg->Result->GetPathDescription()
        .GetDomainDescription()
        .GetDomainState()
        .GetDiskQuotaExceeded();

    Execute(new TTxPersistSubDomainOutOfSpace(this, outOfSpace), ctx);
}

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(10);

void TSpaceWatcher::StartFindSubDomainPathId(bool delayFirstRequest) {
    if (!FindSubDomainPathIdActor &&
        Self->CurrentSchemeShardId != 0 &&
        (!SubDomainPathId))
    {
        FindSubDomainPathIdActor = Register(CreateFindSubDomainPathIdActor(SelfId(), Self->TabletID(), Self->CurrentSchemeShardId, delayFirstRequest, MaxFindSubDomainPathIdDelay));
    }
}


void TSpaceWatcher::Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (FindSubDomainPathIdActor == ev->Sender) {
        FindSubDomainPathIdActor = { };
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "subdomain_found")("scheme_shard_id", msg->SchemeShardId)("local_path_id", msg->LocalPathId);
    Self->Execute(new TTxPersistSubDomainPathId(Self, msg->LocalPathId), ctx);
}

}
