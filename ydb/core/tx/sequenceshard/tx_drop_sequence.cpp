#include "sequenceshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SEQUENCESHARD

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

            YDB_LOG_TRACE("TTxDropSequence.Execute",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId});

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::PIPE_OUTDATED);
                YDB_LOG_TRACE("TTxDropSequence.Execute PIPE_OUTDATED",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::SEQUENCE_NOT_FOUND);
                YDB_LOG_TRACE("TTxDropSequence.Execute SEQUENCE_NOT_FOUND",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
                return true;
            }

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
            Self->Sequences.erase(it);

            SetResult(NKikimrTxSequenceShard::TEvDropSequenceResult::SUCCESS);
            YDB_LOG_NOTICE("TTxDropSequence.Execute SUCCESS",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId});
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            YDB_LOG_TRACE("TTxDropSequence.Complete",
                {"LogPrefix", LogPrefix});

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
