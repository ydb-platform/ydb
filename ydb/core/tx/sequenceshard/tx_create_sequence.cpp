#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxCreateSequence : public TTxBase {
        explicit TTxCreateSequence(TSelf* self, TEvSequenceShard::TEvCreateSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_CREATE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxCreateSequence.Execute"
                << " PathId# " << pathId
                << " Record# " << msg->Record.ShortDebugString());

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvCreateSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxCreateSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            if (Self->Sequences.contains(pathId)) {
                SetResult(NKikimrTxSequenceShard::TEvCreateSequenceResult::SEQUENCE_ALREADY_EXISTS);
                SLOG_T("TTxCreateSequence.Execute SEQUENCE_ALREADY_EXISTS"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = Self->Sequences[pathId];
            sequence.PathId = pathId;
            if (msg->Record.OptionalIncrement_case() == NKikimrTxSequenceShard::TEvCreateSequence::kIncrement) {
                sequence.Increment = msg->Record.GetIncrement();
                if (sequence.Increment == 0) {
                    sequence.Increment = 1;
                }
            }
            if (sequence.Increment > 0) {
                sequence.MinValue = 1;
                sequence.MaxValue = Max<i64>();
            } else {
                sequence.MinValue = Min<i64>();
                sequence.MaxValue = -1;
            }
            if (msg->Record.OptionalMinValue_case() == NKikimrTxSequenceShard::TEvCreateSequence::kMinValue) {
                sequence.MinValue = msg->Record.GetMinValue();
            }
            if (msg->Record.OptionalMaxValue_case() == NKikimrTxSequenceShard::TEvCreateSequence::kMaxValue) {
                sequence.MaxValue = msg->Record.GetMaxValue();
            }
            if (msg->Record.OptionalStartValue_case() == NKikimrTxSequenceShard::TEvCreateSequence::kStartValue) {
                sequence.StartValue = msg->Record.GetStartValue();
            } else {
                if (sequence.Increment > 0) {
                    sequence.StartValue = sequence.MinValue;
                } else {
                    sequence.StartValue = sequence.MaxValue;
                }
            }

            bool frozen = msg->Record.GetFrozen();
            if (frozen) {
                sequence.State = Schema::ESequenceState::Frozen;
            }

            if (msg->Record.OptionalCycle_case() == NKikimrTxSequenceShard::TEvCreateSequence::kCycle) {
                sequence.Cycle = msg->Record.GetCycle();
            }


            if (msg->Record.HasSetVal()) {
                sequence.NextValue = msg->Record.GetSetVal().GetNextValue();
                sequence.NextUsed = msg->Record.GetSetVal().GetNextUsed();
            } else {
                sequence.NextUsed = false;
                sequence.NextValue = sequence.StartValue;
            }

            if (msg->Record.OptionalCache_case() == NKikimrTxSequenceShard::TEvCreateSequence::kCache) {
                sequence.Cache = msg->Record.GetCache();
                if (sequence.Cache < 1) {
                    sequence.Cache = 1;
                }
            }

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Sequences::MinValue>(sequence.MinValue),
                NIceDb::TUpdate<Schema::Sequences::MaxValue>(sequence.MaxValue),
                NIceDb::TUpdate<Schema::Sequences::StartValue>(sequence.StartValue),
                NIceDb::TUpdate<Schema::Sequences::NextValue>(sequence.NextValue),
                NIceDb::TUpdate<Schema::Sequences::NextUsed>(sequence.NextUsed),
                NIceDb::TUpdate<Schema::Sequences::Cache>(sequence.Cache),
                NIceDb::TUpdate<Schema::Sequences::Increment>(sequence.Increment),
                NIceDb::TUpdate<Schema::Sequences::Cycle>(sequence.Cycle),
                NIceDb::TUpdate<Schema::Sequences::State>(sequence.State));
            SetResult(NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            SLOG_N("TTxCreateSequence.Execute SUCCESS"
                << " PathId# " << pathId
                << " MinValue# " << sequence.MinValue
                << " MaxValue# " << sequence.MaxValue
                << " StartValue# " << sequence.StartValue
                << " Cache# " << sequence.Cache
                << " Increment# " << sequence.Increment
                << " Cycle# " << (sequence.Cycle ? "true" : "false")
                << " State# " << (frozen ? "Frozen" : "Active"));
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxCreateSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvCreateSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvCreateSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvCreateSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvCreateSequenceResult> Result;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvCreateSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxCreateSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
