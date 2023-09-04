#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

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
        TTxController::TBasicTxInfo TxInfo;
        NKikimrTxColumnShard::EResultStatus Status;

        TResultEvent(TTxController::TBasicTxInfo&& txInfo, NKikimrTxColumnShard::EResultStatus status)
            : TxInfo(std::move(txInfo))
            , Status(status)
        {}

        std::unique_ptr<IEventBase> MakeEvent(ui64 tabletId) const {
            if (TxInfo.TxKind ==  NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE) {
                auto result = NEvents::TDataEvents::TEvWriteResult::BuildCommited(TxInfo.TxId);
                return result;
            } else {
                auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
                tabletId, TxInfo.TxKind, TxInfo.TxId, Status);
                result->Record.SetStep(TxInfo.PlanStep);
                return result;
            }
        }
    };

    enum class ETriggerActivities {
        NONE,
        POST_INSERT,
        POST_SCHEMA
    };

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxProgressTx[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }

public:
    TTxProgressTx(TColumnShard* self)
        : TTransactionBase(self)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    TTxType GetTxType() const override { return TXTYPE_PROGRESS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());
        Y_VERIFY(Self->ProgressTxInFlight);

        size_t removedCount = Self->ProgressTxController.CleanExpiredTxs(txc);
        if (removedCount > 0) {
            // We cannot continue with this transaction, start a new transaction
            Self->Execute(new TTxProgressTx(Self), ctx);
            return true;
        }

        // Process a single transaction at the front of the queue
        auto plannedItem = Self->ProgressTxController.StartPlannedTx();
        if (!!plannedItem) {
            ui64 step = plannedItem->PlanStep;
            ui64 txId = plannedItem->TxId;

            TTxController::TBasicTxInfo txInfo = *plannedItem;
            switch (txInfo.TxKind) {
                case NKikimrTxColumnShard::TX_KIND_SCHEMA:
                {
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

                    auto pathExists = [&](ui64 pathId) {
                        return Self->TablesManager.HasTable(pathId);
                    };

                    auto counters = Self->InsertTable->Commit(dbTable, step, txId, meta.WriteIds,
                                                              pathExists);
                    Self->IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
                    Self->IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
                    Self->IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);

                    NIceDb::TNiceDb db(txc.DB);
                    for (TWriteId writeId : meta.WriteIds) {
                        Self->RemoveLongTxWrite(db, writeId, txId);
                    }
                    Self->CommitsInFlight.erase(txId);
                    Self->UpdateInsertTableCounters();
                    Trigger = ETriggerActivities::POST_INSERT;
                    break;
                }
                case NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE: {
                    NOlap::TSnapshot snapshot(step, txId);
                    Y_VERIFY(Self->OperationsManager.CommitTransaction(*Self, txId, txc, snapshot));
                    Trigger = ETriggerActivities::POST_INSERT;
                    break;
                }
                default: {
                    Y_FAIL("Unexpected TxKind");
                }
            }

            // Currently transactions never fail and there are no dependencies between them
            TxResults.emplace_back(TResultEvent(std::move(txInfo), NKikimrTxColumnShard::SUCCESS));

            Self->ProgressTxController.FinishPlannedTx(txId, txc);
            Self->RescheduleWaitingReads();
        }

        Self->ProgressTxInFlight = false;
        if (!!Self->ProgressTxController.GetPlannedTx()) {
            Self->EnqueueProgressTx(ctx);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

        for (auto& rec : TxEvents) {
            ctx.Send(rec.Target, rec.Event.Release(), 0, rec.Cookie);
        }

        for (auto& res : TxResults) {
            Self->ProgressTxController.CompleteRunningTx(TTxController::TPlanQueueItem(res.TxInfo.PlanStep, res.TxInfo.TxId));

            auto event = res.MakeEvent(Self->TabletID());
            ctx.Send(res.TxInfo.Source, event.release(), 0, res.TxInfo.Cookie);
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
    const ui32 TabletTxNo;
};

void TColumnShard::EnqueueProgressTx(const TActorContext& ctx) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "EnqueueProgressTx")("tablet_id", TabletID());
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

    auto frontTx = ProgressTxController.GetFrontTx();
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
