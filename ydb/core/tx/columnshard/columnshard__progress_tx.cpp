#include "columnshard_impl.h"
#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

class TColumnShard::TTxProgressTx : public TTransactionBase<TColumnShard> {
private:
    struct TEvent {
        TActorId Target;
        ui64 Cookie;
        THolder<IEventBase> Event;

        TEvent(TActorId target, ui64 cookie, THolder<IEventBase> event)
            : Target(target)
            , Cookie(cookie)
            , Event(std::move(event))
        { }
    };

public:
    TTxProgressTx(TColumnShard* self)
        : TTransactionBase(self)
    { }

    TTxType GetTxType() const override { return TXTYPE_PROGRESS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_S_DEBUG("TTxProgressTx.Execute at tablet " << Self->TabletID());
        Y_VERIFY(Self->ProgressTxInFlight);

        NIceDb::TNiceDb db(txc.DB);

        // Process deadline queue and cleanup expired transactions
        if (Self->HaveOutdatedTxs()) {
            size_t removedCount = 0;
            ui64 outdatedStep = Self->GetOutdatedStep();
            while (Self->DeadlineQueue) {
                auto it = Self->DeadlineQueue.begin();
                if (outdatedStep < it->MaxStep) {
                    // This transaction has a chance to be planned
                    break;
                }
                ui64 txId = it->TxId;
                LOG_S_DEBUG("Removing outdated txId " << txId << " max step " << it->MaxStep << " outdated step "
                    << outdatedStep << " at tablet " << Self->TabletID());
                Self->DeadlineQueue.erase(it);
                Self->RemoveTx(txc.DB, txId);
                ++removedCount;
            }
            if (removedCount > 0) {
                // We cannot continue with this transaction, start a new transaction
                Self->Execute(new TTxProgressTx(Self), ctx);
                return true;
            }
        }

        // Process a single transaction at the front of the queue
        if (Self->PlanQueue) {
            ui64 step;
            ui64 txId;
            {
                auto it = Self->PlanQueue.begin();
                step = it->Step;
                txId = it->TxId;
                Self->PlanQueue.erase(it);
            }

            auto& txInfo = Self->BasicTxInfo.at(txId);
            switch (txInfo.TxKind) {
                case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
                    auto& meta = Self->AltersInFlight.at(txId);
                    Self->RunSchemaTx(meta.Body, TRowVersion(step, txId), txc);
                    Self->ProtectSchemaSeqNo(meta.Body.GetSeqNo(), txc);
                    for (TActorId subscriber : meta.NotifySubscribers) {
                        TxEvents.emplace_back(subscriber, 0,
                            MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(Self->TabletID(), txId));
                    }
                    Self->AltersInFlight.erase(txId);
                    Self->InitS3Actors(ctx, false);
                    break;
                }
                case NKikimrTxColumnShard::TX_KIND_COMMIT: {
                    const auto& meta = Self->CommitsInFlight.at(txId);

                    TBlobGroupSelector dsGroupSelector(Self->Info());
                    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

                    // CacheInserted -> CacheCommitted
                    for (auto& writeId : meta.WriteIds) {
                        Self->BatchCache.Commit(writeId);
                    }

                    auto counters = Self->InsertTable->Commit(dbTable, step, txId, meta.MetaShard, meta.WriteIds);
                    Self->IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
                    Self->IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
                    Self->IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);

                    if (meta.MetaShard == 0) {
                        for (TWriteId writeId : meta.WriteIds) {
                            Self->RemoveLongTxWrite(db, writeId, txId);
                        }
                    }
                    Self->CommitsInFlight.erase(txId);
                    Self->UpdateInsertTableCounters();
                    StartBackgroundActivities = true;
                    break;
                }
                default: {
                    Y_FAIL("Unexpected TxKind");
                }
            }

            // Currently transactions never fail and there are no dependencies between them
            auto txKind = txInfo.TxKind;
            auto status = NKikimrTxColumnShard::SUCCESS;
            auto result = MakeHolder<TEvColumnShard::TEvProposeTransactionResult>(Self->TabletID(), txKind, txId, status);
            result->Record.SetStep(step);
            TxEvents.emplace_back(txInfo.Source, txInfo.Cookie, std::move(result));

            Self->BasicTxInfo.erase(txId);
            Schema::EraseTxInfo(db, txId);

            Self->RescheduleWaitingReads();
        }

        Self->ProgressTxInFlight = false;
        if (Self->PlanQueue) {
            Self->EnqueueProgressTx(ctx);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_DEBUG("TTxProgressTx.Complete at tablet " << Self->TabletID());

        for (auto& rec : TxEvents) {
            ctx.Send(rec.Target, rec.Event.Release(), 0, rec.Cookie);
        }

        Self->UpdateBlobMangerCounters();
        if (Self->BlobManager->CanCollectGarbage()) {
            Self->Execute(Self->CreateTxRunGc(), ctx);
        }

        if (StartBackgroundActivities) {
            Self->EnqueueBackgroundActivities(false, true);
        }
    }

private:
    TVector<TEvent> TxEvents;
    bool StartBackgroundActivities{false};
};

void TColumnShard::EnqueueProgressTx(const TActorContext& ctx) {
    if (!ProgressTxInFlight) {
        ProgressTxInFlight = true;
        Execute(new TTxProgressTx(this), ctx);
    }
}

}
