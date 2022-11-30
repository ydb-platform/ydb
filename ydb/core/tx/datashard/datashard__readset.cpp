#include "datashard_txs.h"

namespace NKikimr {

namespace NDataShard {

    TDataShard::TTxReadSet::TTxReadSet(TDataShard *self, TEvTxProcessing::TEvReadSet::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool TDataShard::TTxReadSet::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxReadSet::Execute at " << Self->TabletID() << " got read set: "
                    << Ev->Get()->ToString().data());

        DoExecute(txc, ctx);

        if (Ack) {
            // Only leader is allowed to ack readsets, start confirming as early
            // as possible, so we handle both read-only and read-write acks.
            AckTs = AppData(ctx)->MonotonicTimeProvider->Now();
            Self->Executor()->ConfirmReadOnlyLease(AckTs);
        }

        return true;
    }

    void TDataShard::TTxReadSet::DoExecute(TTransactionContext &txc, const TActorContext &ctx) {
        auto state = Self->State;
        Y_VERIFY(state != TShardState::Unknown
                 && state != TShardState::Uninitialized
                 && state != TShardState::Readonly,
                 "State %" PRIu32 " event %s", state, Ev->Get()->ToString().data());

        if (!(Ev->Get()->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_ACK)) {
            Ack = MakeAck(ctx);
        }

        // TODO: handle FLAG_EXPECT_READSET for missing transactions and inactive states

        if (!Self->IsStateActive()) {
            /// @warning Ack and allow sender to forget readset.
            /// It's possible till readsets can't passwthough splits-merges or other shard mutations.
            LOG_WARN(ctx, NKikimrServices::TX_DATASHARD,
                "Allow sender to lose readset, state %" PRIu32 " at %" PRIu64 " %s",
                state, Self->TabletID(), Ev->Get()->ToString().data());
            return;
        }

        bool saved = Self->Pipeline.SaveInReadSet(*Ev->Get(), Ack, txc, ctx);
        if (!saved) { // delayed. Do not ack
            Y_VERIFY(!Ack);
            Ev.Reset();
        }
    }

    THolder<IEventHandle> TDataShard::TTxReadSet::MakeAck(const TActorContext& ctx) {
        return THolder(new IEventHandle(Ev->Sender, ctx.SelfID,
                                new TEvTxProcessing::TEvReadSetAck(*Ev->Get(), Self->TabletID())));
    }

    void TDataShard::TTxReadSet::Complete(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxReadSet::Complete at " << Self->TabletID());

        // If it was read set for non-active tx we should send ACK back after successful save in DB
        // Note that, active tx will send "delayed" ACK after tx complete
        if (Ack) {
            LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
                      "Send RS Ack at %" PRIu64 " %s",
                      Self->TabletID(), Ev->Get()->ToString().data());

            struct TSendState : public TThrRefBase {
                TDataShard* Self;
                THolder<IEventHandle> Ack;

                TSendState(TDataShard* self, THolder<IEventHandle>&& ack)
                    : Self(self)
                    , Ack(std::move(ack))
                { }
            };

            Self->Executor()->ConfirmReadOnlyLease(AckTs, [state = MakeIntrusive<TSendState>(Self, std::move(Ack))] {
                TActivationContext::Send(std::move(state->Ack));
                state->Self->IncCounter(COUNTER_ACK_SENT);
            });
        }
    }
}

}
