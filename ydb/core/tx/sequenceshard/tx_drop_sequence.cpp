#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxDropSequence : public TTxBase {
        explicit TTxDropSequence(TSelf* self, TEvSequenceShard::TEvDropSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_DROP_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            SLOG_T("TTxDropSequence.Execute"
                << " PathId# " << pathId);

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxDropSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxDropSequence.Execute SEQUENCE_NOT_FOUND"
                    << " PathId# " << pathId);
                return true;
            }

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
            Self->Sequences.erase(it);

            SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::SUCCESS);
            SLOG_N("TTxDropSequence.Execute SUCCESS"
                << " PathId# " << pathId);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxDropSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvDropSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvDropSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvDropSequenceResult> Result;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvDropSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxDropSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
