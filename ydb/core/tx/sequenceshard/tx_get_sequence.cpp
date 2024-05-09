#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxGetSequence : public TTxBase {
        explicit TTxGetSequence(TSelf* self, TEvSequenceShard::TEvGetSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_GET_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            Y_UNUSED(txc);

            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxGetSequence.Execute"
                << " PathId# " << pathId);

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvGetSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxGetSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvGetSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxGetSequence.Execute SEQUENCE_NOT_FOUND"
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
                    SetResult(NKikimrTxSequenceShard::TEvGetSequenceResult::SEQUENCE_MOVED);
                    Result->Record.SetMovedTo(sequence.MovedTo);
                    SLOG_T("TTxGetSequence.Execute SEQUENCE_MOVED"
                        << " PathId# " << pathId
                        << " MovedTo# " << sequence.MovedTo);
                    return true;
                }
            }

            SetResult(NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS);
            Result->Record.SetMinValue(sequence.MinValue);
            Result->Record.SetMaxValue(sequence.MaxValue);
            Result->Record.SetStartValue(sequence.StartValue);
            Result->Record.SetNextValue(sequence.NextValue);
            Result->Record.SetNextUsed(sequence.NextUsed);
            Result->Record.SetCache(sequence.Cache);
            Result->Record.SetIncrement(sequence.Increment);
            Result->Record.SetCycle(sequence.Cycle);
            SLOG_N("TTxGetSequence.Execute SUCCESS"
                << " PathId# " << pathId);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxGetSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvGetSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvGetSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvGetSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvGetSequenceResult> Result;
    };


    void TSequenceShard::Handle(TEvSequenceShard::TEvGetSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxGetSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
