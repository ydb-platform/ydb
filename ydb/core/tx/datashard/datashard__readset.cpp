#include "datashard_txs.h"

#include <ydb/library/actors/core/monotonic_provider.h>

namespace NKikimr::NDataShard {

    TDataShard::TTxReadSet::TTxReadSet(TDataShard *self, TEvTxProcessing::TEvReadSet::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool TDataShard::TTxReadSet::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxReadSet::Execute at " << Self->TabletID() << " got read set: "
                    << Ev->Get()->ToString().data());

        DoExecute(txc, ctx);

        if (Ack || NoDataReply) {
            // Only leader is allowed to ack readsets, start confirming as early
            // as possible, so we handle both read-only and read-write acks.
            AckTs = AppData(ctx)->MonotonicTimeProvider->Now();
            Self->Executor()->ConfirmReadOnlyLease(AckTs);
        } else {
            // We won't need the event in Complete
            Ev.Reset();
        }

        return true;
    }

    void TDataShard::TTxReadSet::DoExecute(TTransactionContext &txc, const TActorContext &ctx) {
        auto state = Self->State;
        Y_ABORT_UNLESS(state != TShardState::Unknown
                 && state != TShardState::Uninitialized
                 && state != TShardState::Readonly,
                 "State %" PRIu32 " event %s", state, Ev->Get()->ToString().data());

        const auto& msg = *Ev->Get();
        const auto& record = msg.Record;

        if (!(record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_ACK)) {
            Ack = MakeAck(ctx);
        }

        if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET) {
            if (!Self->ProcessReadSetExpectation(Ev)) {
                NoDataReply = MakeNoDataReply(ctx);
            }

            // Note: expect + no data is pure notification, avoid further processing
            if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                return;
            }
        }

        if (!Self->IsStateActive()) {
            /// @warning Ack and allow sender to forget readset.
            /// It's possible till readsets can't passwthough splits-merges or other shard mutations.
            LOG_WARN(ctx, NKikimrServices::TX_DATASHARD,
                "Allow sender to lose readset, state %" PRIu32 " at %" PRIu64 " %s",
                state, Self->TabletID(), msg.ToString().data());
            return;
        }

        bool saved = Self->Pipeline.SaveInReadSet(msg, Ack, txc, ctx);
        if (!saved) { // delayed. Do not ack
            Y_ABORT_UNLESS(!Ack);
        }
    }

    THolder<IEventHandle> TDataShard::TTxReadSet::MakeAck(const TActorContext& ctx) {
        return THolder(new IEventHandle(Ev->Sender, ctx.SelfID,
                                new TEvTxProcessing::TEvReadSetAck(*Ev->Get(), Self->TabletID())));
    }

    THolder<IEventHandle> TDataShard::TTxReadSet::MakeNoDataReply(const TActorContext& ctx) {
        const auto& record = Ev->Get()->Record;
        auto event = MakeHolder<TEvTxProcessing::TEvReadSet>(
            record.GetStep(),
            record.GetTxId(),
            record.GetTabletDest(),
            record.GetTabletSource(),
            Self->TabletID());
        event->Record.SetFlags(NKikimrTx::TEvReadSet::FLAG_NO_DATA | NKikimrTx::TEvReadSet::FLAG_NO_ACK);
        return THolder(new IEventHandle(Ev->Sender, ctx.SelfID, event.Release()));
    }

    void TDataShard::TTxReadSet::Complete(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxReadSet::Complete at " << Self->TabletID());

        // If it was read set for non-active tx we should send ACK back after successful save in DB
        // Note that, active tx will send "delayed" ACK after tx complete
        if (Ack || NoDataReply) {
            LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
                      "Send RS %s at %" PRIu64 " %s",
                      Ack && NoDataReply ? "Ack+Reply" : Ack ? "Ack" : "Reply",
                      Self->TabletID(), Ev->Get()->ToString().data());

            struct TSendState : public TThrRefBase {
                TDataShard* Self;
                THolder<IEventHandle> Ack;
                THolder<IEventHandle> NoDataReply;

                TSendState(TDataShard* self,
                        THolder<IEventHandle>&& ack,
                        THolder<IEventHandle>&& noDataReply)
                    : Self(self)
                    , Ack(std::move(ack))
                    , NoDataReply(std::move(noDataReply))
                { }
            };

            Self->Executor()->ConfirmReadOnlyLease(
                AckTs,
                [state = MakeIntrusive<TSendState>(Self, std::move(Ack), std::move(NoDataReply))] {
                    if (state->Ack) {
                        state->Self->IncCounter(COUNTER_ACK_SENT);
                        TActivationContext::Send(std::move(state->Ack));
                    }
                    if (state->NoDataReply) {
                        TActivationContext::Send(std::move(state->NoDataReply));
                    }
                });
        }
    }

} // namespace NKikimr::NDataShard
