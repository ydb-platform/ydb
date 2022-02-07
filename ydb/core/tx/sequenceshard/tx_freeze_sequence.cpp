#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxFreezeSequence : public TTxBase {
        explicit TTxFreezeSequence(TSelf* self, TEvSequenceShard::TEvFreezeSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_FREEZE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxFreezeSequence.Execute"
                << " PathId# " << pathId);

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvFreezeSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxFreezeSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvFreezeSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxFreezeSequence.Execute SEQUENCE_NOT_FOUND"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active:
                    break;
                case Schema::ESequenceState::Frozen:
                    break;
                case Schema::ESequenceState::Moved: {
                    SetResult(NKikimrTxSequenceShard::TEvFreezeSequenceResult::SEQUENCE_MOVED);
                    Result->Record.SetMovedTo(sequence.MovedTo);
                    SLOG_T("TTxFreezeSequence.Execute SEQUENCE_MOVED"
                        << " PathId# " << pathId
                        << " MovedTo# " << sequence.MovedTo);
                    return true;
                }
            }

            NIceDb::TNiceDb db(txc.DB);
            sequence.State = Schema::ESequenceState::Frozen;
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Sequences::State>(sequence.State));

            SetResult(NKikimrTxSequenceShard::TEvFreezeSequenceResult::SUCCESS);
            Result->Record.SetMinValue(sequence.MinValue);
            Result->Record.SetMaxValue(sequence.MaxValue);
            Result->Record.SetStartValue(sequence.StartValue);
            Result->Record.SetNextValue(sequence.NextValue);
            Result->Record.SetNextUsed(sequence.NextUsed);
            Result->Record.SetCache(sequence.Cache);
            Result->Record.SetIncrement(sequence.Increment);
            Result->Record.SetCycle(sequence.Cycle);
            SLOG_N("TTxFreezeSequence.Execute SUCCESS"
                << " PathId# " << pathId);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxFreezeSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvFreezeSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvFreezeSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvFreezeSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvFreezeSequenceResult> Result;
    };


    void TSequenceShard::Handle(TEvSequenceShard::TEvFreezeSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxFreezeSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
