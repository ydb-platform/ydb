#include "sequenceshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SEQUENCESHARD

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxUpdateSequence : public TTxBase {
        explicit TTxUpdateSequence(TSelf* self, TEvSequenceShard::TEvUpdateSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_UPDATE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();

            YDB_LOG_TRACE("TTxUpdateSequence.Execute",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId},
                {"Record", msg->Record.ShortDebugString()});

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::PIPE_OUTDATED);
                YDB_LOG_TRACE("TTxUpdateSequence.Execute PIPE_OUTDATED",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::SEQUENCE_NOT_FOUND);
                YDB_LOG_TRACE("TTxUpdateSequence.Execute SEQUENCE_NOT_FOUND",
                    {"LogPrefix", LogPrefix},
                    {"PathId", pathId});
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active:
                    break;
                case Schema::ESequenceState::Frozen: {
                    SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::SEQUENCE_FROZEN);
                    YDB_LOG_TRACE("TTxUpdateSequence.Execute SEQUENCE_FROZEN",
                        {"LogPrefix", LogPrefix},
                        {"PathId", pathId});
                    return true;
                }
                case Schema::ESequenceState::Moved: {
                    SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::SEQUENCE_MOVED);
                    Result->Record.SetMovedTo(sequence.MovedTo);
                    YDB_LOG_TRACE("TTxUpdateSequence.Execute SEQUENCE_MOVED",
                        {"LogPrefix", LogPrefix},
                        {"PathId", pathId},
                        {"MovedTo", sequence.MovedTo});
                    return true;
                }
            }

            NIceDb::TNiceDb db(txc.DB);

            if (msg->Record.ChangeMinValue_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kMinValue) {
                sequence.MinValue = msg->Record.GetMinValue();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::MinValue>(sequence.MinValue));
            }
            if (msg->Record.ChangeMaxValue_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kMaxValue) {
                sequence.MaxValue = msg->Record.GetMaxValue();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::MaxValue>(sequence.MaxValue));
            }
            if (msg->Record.ChangeStartValue_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kStartValue) {
                sequence.StartValue = msg->Record.GetStartValue();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::StartValue>(sequence.StartValue));
            }
            if (msg->Record.ChangeNextValue_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kNextValue) {
                sequence.NextValue = msg->Record.GetNextValue();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::NextValue>(sequence.NextValue));
            }
            if (msg->Record.ChangeNextUsed_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kNextUsed) {
                sequence.NextUsed = msg->Record.GetNextUsed();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::NextUsed>(sequence.NextUsed));
            }
            if (msg->Record.ChangeCache_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kCache) {
                sequence.Cache = msg->Record.GetCache();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::Cache>(sequence.Cache));
            }
            if (msg->Record.ChangeIncrement_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kIncrement) {
                sequence.Increment = msg->Record.GetIncrement();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::Increment>(sequence.Increment));
            }
            if (msg->Record.ChangeCycle_case() == NKikimrTxSequenceShard::TEvUpdateSequence::kCycle) {
                sequence.Cycle = msg->Record.GetCycle();
                db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Sequences::Cycle>(sequence.Cycle));
            }

            SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::SUCCESS);
            YDB_LOG_TRACE("TTxUpdateSequence.Execute SUCCESS",
                {"LogPrefix", LogPrefix},
                {"PathId", pathId});
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            YDB_LOG_TRACE("TTxUpdateSequence.Complete",
                {"LogPrefix", LogPrefix});

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvUpdateSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvUpdateSequenceResult(status, Self->TabletID()));
            Result->Record.SetTxId(Ev->Get()->Record.GetTxId());
            Result->Record.SetTxPartId(Ev->Get()->Record.GetTxPartId());
        }

        TEvSequenceShard::TEvUpdateSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvUpdateSequenceResult> Result;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvUpdateSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxUpdateSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
