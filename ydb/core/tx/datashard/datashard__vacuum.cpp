#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShard::TTxVacuum : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvVacuum::TPtr Ev;
    std::unique_ptr<TEvDataShard::TEvVacuumResult> Response;

public:
    TTxVacuum(TDataShard* ds, TEvDataShard::TEvVacuum::TPtr ev)
        : TBase(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_VACUUM; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Ev->Get()->Record;

        if (!Self->IsStateActive()) {
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                "Vacuum tx at non-ready tablet " << Self->TabletID()
                << " state " << Self->State
                << ", requested from " << Ev->Sender);
            Response = std::make_unique<TEvDataShard::TEvVacuumResult>(
                record.GetVacuumGeneration(),
                Self->TabletID(),
                NKikimrTxDataShard::TEvVacuumResult::WRONG_SHARD_STATE);
            return true;
        }

        if (Self->Executor()->HasLoanedParts()) {
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                "Vacuum of tablet# " << Self->TabletID()
                << ": has borrowed parts"
                << ", requested from " << Ev->Sender);
            Response = std::make_unique<TEvDataShard::TEvVacuumResult>(
                record.GetVacuumGeneration(),
                Self->TabletID(),
                NKikimrTxDataShard::TEvVacuumResult::BORROWED);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        ui64 lastGen = 0;
        if (!Self->SysGetUi64(db, Schema::Sys_VacuumCompletedGeneration, lastGen)) {
            return false;
        }

        if (lastGen >= record.GetVacuumGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Vacuum of tablet# " << Self->TabletID()
                << " for requested generation " << record.GetVacuumGeneration()
                << ", requested from# " << Ev->Sender
                << " already completed"
                << ", last persisted Vacuum generation: " << lastGen);
            Response = std::make_unique<TEvDataShard::TEvVacuumResult>(
                lastGen,
                Self->TabletID(),
                NKikimrTxDataShard::TEvVacuumResult::OK);
            return true;
        }

        if (Self->GetSnapshotManager().RemoveExpiredSnapshots(ctx.Now(), txc)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Vacuum of tablet# " << Self->TabletID()
                << ": expired snapshots removed");
        }
        Self->OutReadSets.Cleanup(db, ctx);

        Self->Executor()->StartVacuum(Ev->Get()->Record.GetVacuumGeneration());
        Self->VacuumWaiters.insert({Ev->Get()->Record.GetVacuumGeneration(), Ev->Sender});
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Response) {
            ctx.Send(Ev->Sender, std::move(Response));
        }
    }
};

class TDataShard::TTxCompleteVacuum : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    ui64 VacuumGeneration;

public:
    TTxCompleteVacuum(TDataShard* ds, ui64 vacuumGeneration)
        : TBase(ds)
        , VacuumGeneration(vacuumGeneration)
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_VACUUM; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSys(db, Schema::Sys_VacuumCompletedGeneration, VacuumGeneration);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto waiterIt = Self->VacuumWaiters.begin();
        while (waiterIt != Self->VacuumWaiters.end() && waiterIt->first <= VacuumGeneration) {
            auto response = MakeHolder<TEvDataShard::TEvVacuumResult>(
                VacuumGeneration,
                Self->TabletID(),
                NKikimrTxDataShard::TEvVacuumResult::OK);
            ctx.Send(waiterIt->second, std::move(response));
            waiterIt = Self->VacuumWaiters.erase(waiterIt);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Updated last Vacuum of tablet# "<< Self->TabletID()
            << ", last persisted Vacuum generation: " << VacuumGeneration);
    }
};

void TDataShard::Handle(TEvDataShard::TEvVacuum::TPtr& ev, const TActorContext& ctx) {
    Executor()->Execute(new TTxVacuum(this, ev), ctx);
}

void TDataShard::VacuumComplete(ui64 vacuumGeneration, const TActorContext& ctx) {
    Executor()->Execute(new TTxCompleteVacuum(this, vacuumGeneration), ctx);
}

} // namespace NKikimr::NDataShard
