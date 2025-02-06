#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShard::TTxDataCleanup : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvForceDataCleanup::TPtr Ev;

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
            auto response = MakeHolder<TEvDataShard::TEvForceDataCleanupResult>(
                record.GetDataCleanupGeneration(),
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::FAILED);
            ctx.Send(Ev->Sender, std::move(response));
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        ui64 lastGen = 0;
        auto hasLastGen = Self->SysGetUi64(db, Schema::Sys_DataCleanupGeneration, lastGen);

        if (hasLastGen && lastGen >= record.GetDataCleanupGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "DataCleanup of tablet# " << Self->TabletID()
                << " for requested generation " << record.GetDataCleanupGeneration()
                << ", requested from# " << Ev->Sender
                << " already completed"
                << ", last persisted DataCleanup generation: " << lastGen);
            auto response = MakeHolder<TEvDataShard::TEvForceDataCleanupResult>(
                lastGen,
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
            ctx.Send(Ev->Sender, std::move(response));
            return true;
        }

        Self->Executor()->CleanupData(record.GetDataCleanupGeneration());
        Self->DataCleanupWaiters.emplace_back(Ev->Sender);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
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
        Self->PersistSys(db, Schema::Sys_DataCleanupGeneration, DataCleanupGeneration);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // requested generations of all DataCleanupWaiters less or equal to dataCleanupGeneration
        for (const auto& waiter : Self->DataCleanupWaiters) {
            auto response = MakeHolder<TEvDataShard::TEvForceDataCleanupResult>(
                DataCleanupGeneration,
                Self->TabletID(),
                NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
            ctx.Send(waiter, std::move(response));
        }
        Self->DataCleanupWaiters.clear();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Updated last DataCleanupof tablet# "<< Self->TabletID()
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
