#include "datashard_txs.h"

#include <ydb/library/actors/core/monotonic_provider.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr::NDataShard {

    TDataShard::TTxReadSet::TTxReadSet(TDataShard *self, TEvTxProcessing::TEvReadSet::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool TDataShard::TTxReadSet::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxReadSet::Execute, got read set",
            {"tabletId", Self->TabletID()},
            {"readsetDetails", Ev->Get()->ToString().data()});

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
        Y_ENSURE(state != TShardState::Unknown
                 && state != TShardState::Uninitialized
                 && state != TShardState::Readonly,
                 "State " << state << " event " << Ev->Get()->ToString());

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
            YDB_LOG_WARN_CTX(ctx, "Allowing sender to lose readset",
                {"state", state},
                {"tabletId", Self->TabletID()},
                {"msg", msg});
            return;
        }

        bool saved = Self->Pipeline.SaveInReadSet(msg, Ack, txc, ctx);
        if (!saved) { // delayed. Do not ack
            Y_ENSURE(!Ack);
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
        YDB_LOG_DEBUG_CTX(ctx, "TTxReadSet::Complete",
            {"tabletId", Self->TabletID()});

        // If it was read set for non-active tx we should send ACK back after successful save in DB
        // Note that, active tx will send "delayed" ACK after tx complete
        if (Ack || NoDataReply) {
            YDB_LOG_DEBUG_CTX(ctx, "Sending read set response",
                {"replyType", Ack && NoDataReply ? "Ack+Reply" : Ack ? "Ack" : "Reply"},
                {"tabletId", Self->TabletID()},
                {"eventString", Ev->Get()->ToString().data()});

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
