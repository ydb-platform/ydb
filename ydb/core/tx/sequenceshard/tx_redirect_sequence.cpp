#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxRedirectSequence : public TTxBase {
        explicit TTxRedirectSequence(TSelf* self, TEvSequenceShard::TEvRedirectSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_REDIRECT_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();
            auto redirectTo = msg->Record.GetRedirectTo();

            SLOG_T("TTxRedirectSequence.Execute"
                << " PathId# " << pathId
                << " RedirectTo# " << redirectTo);

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxRedirectSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxRedirectSequence.Execute SEQUENCE_NOT_FOUND"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active:
                    break;
                case Schema::ESequenceState::Frozen:
                    break;
                case Schema::ESequenceState::Moved:
                    break;
            }

            NIceDb::TNiceDb db(txc.DB);
            sequence.State = Schema::ESequenceState::Moved;
            sequence.MovedTo = redirectTo;
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Sequences::State>(sequence.State),
                NIceDb::TUpdate<Schema::Sequences::MovedTo>(sequence.MovedTo));

            SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::SUCCESS);
            SLOG_N("TTxRedirectSequence.Execute SUCCESS"
                << " PathId# " << pathId
                << " RedirectTo# " << redirectTo);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxRedirectSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvRedirectSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvRedirectSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvRedirectSequenceResult> Result;
    };


    void TSequenceShard::Handle(TEvSequenceShard::TEvRedirectSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxRedirectSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
