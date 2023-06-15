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

    struct TResultEvent {
        TBasicTxInfo TxInfo;
        NKikimrTxColumnShard::EResultStatus Status;

        TResultEvent(TBasicTxInfo&& txInfo, NKikimrTxColumnShard::EResultStatus status)
            : TxInfo(std::move(txInfo))
            , Status(status)
        {}

        THolder<IEventBase> MakeEvent(ui64 tabletId) const {
            auto result = MakeHolder<TEvColumnShard::TEvProposeTransactionResult>(
                tabletId, TxInfo.TxKind, TxInfo.TxId, Status);
            result->Record.SetStep(TxInfo.PlanStep);
            return result;
        }
    };

    enum class ETriggerActivities {
        NONE,
        POST_INSERT,
        POST_SCHEMA
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
        if (!Self->PlanQueue.empty()) {
            ui64 step;
            ui64 txId;
            {
                auto node = Self->PlanQueue.extract(Self->PlanQueue.begin());
                auto& item = node.value();
                step = item.Step;
                txId = item.TxId;
                Self->RunningQueue.emplace(std::move(item));
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
                    Trigger = ETriggerActivities::POST_SCHEMA;
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

                    auto pathExists = [&](ui64 pathId) {
                        auto it = Self->Tables.find(pathId);
                        return it != Self->Tables.end() && !it->second.IsDropped();
                    };

                    auto counters = Self->InsertTable->Commit(dbTable, step, txId, meta.MetaShard, meta.WriteIds,
                                                              pathExists);
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
                    Trigger = ETriggerActivities::POST_INSERT;
                    break;
                }
                default: {
                    Y_FAIL("Unexpected TxKind");
                }
            }

            // Currently transactions never fail and there are no dependencies between them
            txInfo.PlanStep = step;
            TxResults.emplace_back(TResultEvent(std::move(txInfo), NKikimrTxColumnShard::SUCCESS));

            Self->BasicTxInfo.erase(txId);
            Schema::EraseTxInfo(db, txId);

            Self->RescheduleWaitingReads();
        }

        Self->ProgressTxInFlight = false;
        if (!Self->PlanQueue.empty()) {
            Self->EnqueueProgressTx(ctx);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_DEBUG("TTxProgressTx.Complete at tablet " << Self->TabletID());

        for (auto& rec : TxEvents) {
            ctx.Send(rec.Target, rec.Event.Release(), 0, rec.Cookie);
        }

        for (auto& res : TxResults) {
            TPlanQueueItem txItem(res.TxInfo.PlanStep, res.TxInfo.TxId);
            Self->RunningQueue.erase(txItem);

            auto event = res.MakeEvent(Self->TabletID());
            ctx.Send(res.TxInfo.Source, event.Release(), 0, res.TxInfo.Cookie);
        }

        Self->ScheduleNextGC(ctx);

        switch (Trigger) {
            case ETriggerActivities::POST_INSERT:
                Self->EnqueueBackgroundActivities(false, TBackgroundActivity::Indexation());
                break;
            case ETriggerActivities::POST_SCHEMA:
                Self->EnqueueBackgroundActivities();
                break;
            case ETriggerActivities::NONE:
            default:
                break;
        }
    }

private:
    std::vector<TResultEvent> TxResults;
    std::vector<TEvent> TxEvents;
    ETriggerActivities Trigger{ETriggerActivities::NONE};
};

void TColumnShard::EnqueueProgressTx(const TActorContext& ctx) {
    if (!ProgressTxInFlight) {
        ProgressTxInFlight = true;
        Execute(new TTxProgressTx(this), ctx);
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvCheckPlannedTransaction::TPtr& ev, const TActorContext& ctx) {
    auto& record = Proto(ev->Get());
    ui64 step = record.GetStep();
    ui64 txId = record.GetTxId();
    LOG_S_DEBUG("CheckTransaction planStep " << step << " txId " << txId << " at tablet " << TabletID());

    TPlanQueueItem frontTx(LastPlannedStep, 0);
    if (!RunningQueue.empty()) {
        frontTx = TPlanQueueItem(RunningQueue.begin()->Step, RunningQueue.begin()->TxId);
    } else if (!PlanQueue.empty()) {
        frontTx = TPlanQueueItem(PlanQueue.begin()->Step, PlanQueue.begin()->TxId);
    }

    bool finished = step < frontTx.Step || (step == frontTx.Step && txId < frontTx.TxId);
    if (finished) {
        auto txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;
        auto status = NKikimrTxColumnShard::SUCCESS;
        auto result = MakeHolder<TEvColumnShard::TEvProposeTransactionResult>(TabletID(), txKind, txId, status);
        result->Record.SetStep(step);

        ctx.Send(ev->Get()->GetSource(), result.Release());
    }

    // For now do not return result for not finished tx. It would be sent in TTxProgressTx::Complete()
}

}
