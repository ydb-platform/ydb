#include "sequenceshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SEQUENCESHARD

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

            YDB_LOG_TRACE("TTxRedirectSequence.Execute",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId},
                {"RedirectTo", redirectTo});

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::PIPE_OUTDATED);
                YDB_LOG_TRACE("TTxRedirectSequence.Execute PIPE_OUTDATED",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvRedirectSequenceResult::SEQUENCE_NOT_FOUND);
                YDB_LOG_TRACE("TTxRedirectSequence.Execute SEQUENCE_NOT_FOUND",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
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
            YDB_LOG_NOTICE("TTxRedirectSequence.Execute SUCCESS",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId},
                {"RedirectTo", redirectTo});
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            YDB_LOG_TRACE("TTxRedirectSequence.Complete",
                {"LogPrefix", LogPrefix});

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
