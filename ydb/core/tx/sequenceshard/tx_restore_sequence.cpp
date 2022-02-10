#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxRestoreSequence : public TTxBase {
        explicit TTxRestoreSequence(TSelf* self, TEvSequenceShard::TEvRestoreSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_RESTORE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxRestoreSequence.Execute"
                << " PathId# " << pathId
                << " Record# " << msg->Record.ShortDebugString());

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvRestoreSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxRestoreSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            NIceDb::TNiceDb db(txc.DB);

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                // Auto-create new sequence with specified settings
                auto& sequence = Self->Sequences[pathId];
                sequence.PathId = pathId;
                sequence.MinValue = msg->Record.GetMinValue();
                sequence.MaxValue = msg->Record.GetMaxValue();
                sequence.StartValue = msg->Record.GetStartValue();
                sequence.NextValue = msg->Record.GetNextValue();
                sequence.NextUsed = msg->Record.GetNextUsed();
                sequence.Cache = msg->Record.GetCache();
                sequence.Increment = msg->Record.GetIncrement();
                sequence.Cycle = msg->Record.GetCycle();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::MinValue>(sequence.MinValue),
                    NIceDb::TUpdate<Schema::Sequences::MaxValue>(sequence.MaxValue),
                    NIceDb::TUpdate<Schema::Sequences::StartValue>(sequence.StartValue),
                    NIceDb::TUpdate<Schema::Sequences::NextValue>(sequence.NextValue),
                    NIceDb::TUpdate<Schema::Sequences::NextUsed>(sequence.NextUsed),
                    NIceDb::TUpdate<Schema::Sequences::Cache>(sequence.Cache),
                    NIceDb::TUpdate<Schema::Sequences::Increment>(sequence.Increment),
                    NIceDb::TUpdate<Schema::Sequences::Cycle>(sequence.Cycle));

                SetResult(NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS);
                SLOG_T("TTxRestoreSequence.Execute SUCCESS"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active: {
                    SetResult(NKikimrTxSequenceShard::TEvRestoreSequenceResult::SEQUENCE_ALREADY_ACTIVE);
                    SLOG_T("TTxRestoreSequence.Execute SEQUENCE_ALREADY_ACTIVE"
                        << " PathId# " << pathId);
                    return true;
                }
                case Schema::ESequenceState::Frozen:
                    break;
                case Schema::ESequenceState::Moved:
                    break;
            }

            sequence.MinValue = msg->Record.GetMinValue();
            sequence.MaxValue = msg->Record.GetMaxValue();
            sequence.StartValue = msg->Record.GetStartValue();
            sequence.NextValue = msg->Record.GetNextValue();
            sequence.NextUsed = msg->Record.GetNextUsed();
            sequence.Cache = msg->Record.GetCache();
            sequence.Increment = msg->Record.GetIncrement();
            sequence.Cycle = msg->Record.GetCycle();
            sequence.State = Schema::ESequenceState::Active;
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

            SetResult(NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS);
            SLOG_N("TTxRestoreSequence.Execute SUCCESS"
                << " PathId# " << pathId
                << " Record# " << msg->Record.ShortDebugString());
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxRestoreSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvRestoreSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvRestoreSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvRestoreSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvRestoreSequenceResult> Result;
    };


    void TSequenceShard::Handle(TEvSequenceShard::TEvRestoreSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxRestoreSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
