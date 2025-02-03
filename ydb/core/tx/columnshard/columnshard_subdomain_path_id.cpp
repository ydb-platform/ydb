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

        if (Self->SubDomainOutOfSpace != OutOfSpace) {
            Schema::SaveSpecialValue(db, Schema::EValueIds::SubDomainOutOfSpace, ui64(OutOfSpace ? 1 : 0));
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

class TTxPersistSubDomainPathId : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPersistSubDomainPathId(TColumnShard* self, ui64 localPathId)
        : TTransactionBase(self)
        , LocalPathId(localPathId)
    { }

    TTxType GetTxType() const override { return TXTYPE_PERSIST_SUBDOMAIN_PATH_ID; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->SubDomainPathId) {
            Self->PersistSubDomainPathId(LocalPathId, txc);
            Self->StartWatchingSubDomainPathId();
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 LocalPathId;
};

void TColumnShard::PersistSubDomainPathId(ui64 localPathId,
                                               NTabletFlatExecutor::TTransactionContext &txc) {
    SubDomainPathId = localPathId;
    NIceDb::TNiceDb db(txc.DB);
    Schema::SaveSpecialValue(db, Schema::EValueIds::SubDomainLocalPathId, localPathId);
}

void TColumnShard::StopWatchingSubDomainPathId() {
    if (WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        WatchingSubDomainPathId.reset();
    }
}

void TColumnShard::StartWatchingSubDomainPathId() {
    if (!SubDomainPathId) {
        return;
    }

    if (!WatchingSubDomainPathId) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("started_watching_subdomain", *SubDomainPathId);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(TPathId(CurrentSchemeShardId, *SubDomainPathId)));
        WatchingSubDomainPathId = *SubDomainPathId;
    }
}

void TColumnShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("notify_subdomain", msg->PathId);
    if (SubDomainPathId && msg->PathId.LocalPathId == *SubDomainPathId) {
        const bool outOfSpace = msg->Result->GetPathDescription()
            .GetDomainDescription()
            .GetDomainState()
            .GetDiskQuotaExceeded();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_COLUMNSHARD,
            "Discovered subdomain " << msg->PathId << " state, outOfSpace = " << outOfSpace
            << " at columnshard " << TabletID());

        Execute(new TTxPersistSubDomainOutOfSpace(this, outOfSpace), ctx);
    }
}

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(10);

void TColumnShard::StartFindSubDomainPathId(bool delayFirstRequest) {
    if (!FindSubDomainPathIdActor &&
        (!SubDomainPathId))
    {
        FindSubDomainPathIdActor = Register(CreateFindSubDomainPathIdActor(SelfId(), TabletID(), CurrentSchemeShardId, delayFirstRequest, MaxFindSubDomainPathIdDelay));
    }
}


void TColumnShard::Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (FindSubDomainPathIdActor == ev->Sender) {
        FindSubDomainPathIdActor = { };
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "subdomain_found")("scheme_shard_id", msg->SchemeShardId)("local_path_id", msg->LocalPathId);
    Execute(new TTxPersistSubDomainPathId(this, msg->LocalPathId), ctx);
}

}
