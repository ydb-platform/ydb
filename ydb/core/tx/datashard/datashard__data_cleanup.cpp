#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShard::TTxDataCleanup : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvForceDataCleanup::TPtr Ev;
    std::unique_ptr<TEvDataShard::TEvForceDataCleanupResult> Response;

public:
    TTxDataCleanup(TDataShard* ds, TEvDataShard::TEvForceDataCleanup::TPtr ev)
        : TBase(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_DATA_CLEANUP; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Ev->Get()->Record;

        if (!Self->IsStateActive()) {
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                "DataCleanup tx at non-ready tablet " << Self->TabletID()
                << " state " << Self->State
                << ", requested from " << Ev->Sender);
            Response = std::make_unique<TEvDataShard::TEvForceDataCleanupResult>(
                record.GetDataCleanupGeneration(),
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::FAILED);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        ui64 lastGen = 0;
        if (!Self->SysGetUi64(db, Schema::Sys_DataCleanupCompletedGeneration, lastGen)) {
            return false;
        }

        if (lastGen >= record.GetDataCleanupGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "DataCleanup of tablet# " << Self->TabletID()
                << " for requested generation " << record.GetDataCleanupGeneration()
                << ", requested from# " << Ev->Sender
                << " already completed"
                << ", last persisted DataCleanup generation: " << lastGen);
            Response = std::make_unique<TEvDataShard::TEvForceDataCleanupResult>(
                lastGen,
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
            return true;
        }

        if (Self->GetSnapshotManager().RemoveExpiredSnapshots(ctx.Now(), txc)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "DataCleanup of tablet# " << Self->TabletID()
                << ": expired snapshots removed");
        }
        Self->OutReadSets.Cleanup(db, ctx);

        Self->Executor()->CleanupData(Ev->Get()->Record.GetDataCleanupGeneration());
        Self->DataCleanupWaiters.insert({Ev->Get()->Record.GetDataCleanupGeneration(), Ev->Sender});
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Response) {
            ctx.Send(Ev->Sender, std::move(Response));
        }
    }
};

class TDataShard::TTxCompleteDataCleanup : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    ui64 DataCleanupGeneration;

public:
    TTxCompleteDataCleanup(TDataShard* ds, ui64 dataCleanupGeneration)
        : TBase(ds)
        , DataCleanupGeneration(dataCleanupGeneration)
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_DATA_CLEANUP; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSys(db, Schema::Sys_DataCleanupCompletedGeneration, DataCleanupGeneration);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto waiterIt = Self->DataCleanupWaiters.begin();
        while (waiterIt != Self->DataCleanupWaiters.end() && waiterIt->first <= DataCleanupGeneration) {
            auto response = MakeHolder<TEvDataShard::TEvForceDataCleanupResult>(
                DataCleanupGeneration,
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
            ctx.Send(waiterIt->second, std::move(response));
            waiterIt = Self->DataCleanupWaiters.erase(waiterIt);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Updated last DataCleanup of tablet# "<< Self->TabletID()
            << ", last persisted DataCleanup generation: " << DataCleanupGeneration);
    }
};

void TDataShard::Handle(TEvDataShard::TEvForceDataCleanup::TPtr& ev, const TActorContext& ctx) {
    Executor()->Execute(new TTxDataCleanup(this, ev), ctx);
}

void TDataShard::DataCleanupComplete(ui64 dataCleanupGeneration, const TActorContext& ctx) {
    Executor()->Execute(new TTxCompleteDataCleanup(this, dataCleanupGeneration), ctx);
}

} // namespace NKikimr::NDataShard
