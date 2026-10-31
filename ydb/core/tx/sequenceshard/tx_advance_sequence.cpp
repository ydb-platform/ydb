#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxAdvanceSequence : public TTxBase {
        explicit TTxAdvanceSequence(TSelf* self, TEvSequenceShard::TEvAdvanceSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_ADVANCE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxAdvanceSequence.Execute"
                << " PathId# " << pathId
                << " ObservedMin# " << msg->Record.GetObservedMin()
                << " ObservedMax# " << msg->Record.GetObservedMax());

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxAdvanceSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxAdvanceSequence.Execute SEQUENCE_NOT_FOUND"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active:
                    break;
                case Schema::ESequenceState::Frozen: {
                    SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_FROZEN);
                    SLOG_T("TTxAdvanceSequence.Execute SEQUENCE_FROZEN"
                        << " PathId# " << pathId);
                    return true;
                }
                case Schema::ESequenceState::Moved: {
                    SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_MOVED);
                    Result->Record.SetMovedTo(sequence.MovedTo);
                    SLOG_T("TTxAdvanceSequence.Execute SEQUENCE_MOVED"
                        << " PathId# " << pathId
                        << " MovedTo# " << sequence.MovedTo);
                    return true;
                }
            }

            i64 nextValue = sequence.NextValue;
            bool nextUsed = sequence.NextUsed;
            const bool changed = Advance(sequence,
                msg->Record.GetObservedMin(), msg->Record.GetObservedMax(), nextValue, nextUsed);

            if (changed) {
                SLOG_D("TTxAdvanceSequence.Execute advancing"
                    << " PathId# " << pathId
                    << " NextValue# " << sequence.NextValue
                    << " -> " << nextValue
                    << " NextUsed# " << sequence.NextUsed
                    << " -> " << nextUsed);

                sequence.NextValue = nextValue;
                sequence.NextUsed = nextUsed;

                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::NextValue>(sequence.NextValue),
                    NIceDb::TUpdate<Schema::Sequences::NextUsed>(sequence.NextUsed));
            }

            SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SUCCESS);
            Result->Record.SetNextValue(sequence.NextValue);
            Result->Record.SetNextUsed(sequence.NextUsed);
            SLOG_T("TTxAdvanceSequence.Execute SUCCESS"
                << " PathId# " << pathId
                << " Changed# " << changed
                << " NextValue# " << sequence.NextValue
                << " NextUsed# " << sequence.NextUsed);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxAdvanceSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvAdvanceSequenceResult(status, Self->TabletID()));
        }

    private:
        // Advance moves the sequence in the direction of increment.
        // There is basic Cycle support, but it's not used in real life and assumes that
        // a single Advance() call doesn't wrap around the range.
        static bool Advance(const TSequence& sequence, i64 observedMin, i64 observedMax,
                i64& nextValue, bool& nextUsed)
        {
            i64 increment = sequence.Increment;
            if (increment == 0) {
                increment = 1;
            }

            if (observedMin < sequence.MinValue) {
                observedMin = sequence.MinValue;
            }
            if (observedMax > sequence.MaxValue) {
                observedMax = sequence.MaxValue;
            }

            // Check if already advanced past the last value
            const i64 mid = (sequence.MinValue/2 + sequence.MaxValue/2);
            if (sequence.Cycle && increment > 0 && nextValue >= mid && observedMax < mid) {
                // Wrapped forward
                nextValue = sequence.MinValue;
            } else if (sequence.Cycle && increment < 0 && nextValue < mid && observedMin > mid) {
                // Wrapped backward
                nextValue = sequence.MaxValue;
            } else if (increment > 0 && (observedMax < nextValue || observedMax == nextValue && nextUsed) ||
                increment < 0 && (observedMin > nextValue || observedMin == nextValue && nextUsed)) {
                // Already advanced
                return false;
            }

            // Align with % increment
            if (increment > 0) {
                const ui64 rem = (ui64(observedMax) - ui64(nextValue)) % ui64(increment);
                nextValue = i64(ui64(observedMax) - rem);
            } else {
                const ui64 rem = (ui64(nextValue) - ui64(observedMin)) % (ui64(0) - ui64(increment));
                nextValue = i64(ui64(observedMin) + rem);
            }
            nextUsed = true;
            return true;
        }

        TEvSequenceShard::TEvAdvanceSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvAdvanceSequenceResult> Result;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvAdvanceSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxAdvanceSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
