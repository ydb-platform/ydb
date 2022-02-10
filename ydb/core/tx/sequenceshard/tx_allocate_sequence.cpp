#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxAllocateSequence : public TTxBase {
        explicit TTxAllocateSequence(TSelf* self, TEvSequenceShard::TEvAllocateSequence::TPtr&& ev)
            : TTxBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_ALLOCATE_SEQUENCE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto* msg = Ev->Get();

            auto pathId = msg->GetPathId();
            auto cache = msg->Record.GetCache();

            SLOG_T("TTxAllocateSequence.Execute"
                << " PathId# " << pathId
                << " Cache# " << cache);

            if (!Self->CheckPipeRequest(Ev->Recipient)) {
                SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::PIPE_OUTDATED);
                SLOG_T("TTxAllocateSequence.Execute PIPE_OUTDATED"
                    << " PathId# " << pathId);
                return true;
            }

            auto it = Self->Sequences.find(pathId);
            if (it == Self->Sequences.end()) {
                SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_NOT_FOUND);
                SLOG_T("TTxAllocateSequence.Execute SEQUENCE_NOT_FOUND"
                    << " PathId# " << pathId);
                return true;
            }

            auto& sequence = it->second;
            switch (sequence.State) {
                case Schema::ESequenceState::Active:
                    break;
                case Schema::ESequenceState::Frozen: {
                    SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_FROZEN);
                    SLOG_T("TTxAllocateSequence.Execute SEQUENCE_FROZEN"
                        << " PathId# " << pathId);
                    return true;
                }
                case Schema::ESequenceState::Moved: {
                    SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_MOVED);
                    Result->Record.SetMovedTo(sequence.MovedTo);
                    SLOG_T("TTxAllocateSequence.Execute SEQUENCE_MOVED"
                        << " PathId# " << pathId
                        << " MovedTo# " << sequence.MovedTo);
                    return true;
                }
            }

            // Note: will have granularity >= 1
            if (cache == 0) {
                cache = sequence.Cache;
                if (cache == 0) {
                    cache = 1;
                }
            }

            auto res = TryToAllocate(sequence, cache);
            if (res.second == 0) {
                // Cannot allocate even a single value
                SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_OVERFLOW);
                SLOG_T("TTxAllocateSequence.Execute SEQUENCE_OVERFLOW"
                    << " PathId# " << pathId);
                return true;
            }

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Sequences>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Sequences::NextValue>(sequence.NextValue),
                NIceDb::TUpdate<Schema::Sequences::NextUsed>(sequence.NextUsed));

            SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
            Result->Record.SetAllocationStart(res.first);
            Result->Record.SetAllocationCount(res.second);
            Result->Record.SetAllocationIncrement(sequence.Increment);
            SLOG_T("TTxAllocateSequence.Execute SUCCESS"
                << " PathId# " << pathId
                << " AllocationStart# " << Result->Record.GetAllocationStart()
                << " AllocationCount# " << Result->Record.GetAllocationCount()
                << " AllocationIncrement# " << Result->Record.GetAllocationIncrement());
            return true;
        }

        std::pair<i64, ui64> TryToAllocate(TSequence& sequence, ui64 cache) {
            std::pair<i64, ui64> res = { 0, 0 };

            res.first = sequence.NextValue;

            if (sequence.Increment > 0) {
                ui64 delta = sequence.Increment;

                if (sequence.NextUsed) {
                    if (res.first < sequence.MaxValue && ui64(sequence.MaxValue) - ui64(res.first) >= delta) {
                        res.first += delta;
                    } else {
                        // overflow, either report error or cycle from min value
                        if (!sequence.Cycle) {
                            return res;
                        }
                        res.first = sequence.MinValue;
                    }
                }

                if (res.first > sequence.MaxValue) {
                    return res;
                }

                // see how many values would it take to reach MaxValue
                res.second = (ui64(sequence.MaxValue) - ui64(res.first)) / delta;
                if (res.second > cache) {
                    res.second = cache;
                }

                i64 next = res.first + delta * res.second;
                if (res.second < cache && next == sequence.MaxValue) {
                    // we want to also consume the MaxValue
                    sequence.NextValue = next;
                    sequence.NextUsed = true;
                    ++res.second;
                } else if (res.second > 0) {
                    sequence.NextValue = next;
                    sequence.NextUsed = false;
                }
            } else {
                ui64 delta = -sequence.Increment;

                if (sequence.NextUsed) {
                    if (res.first > sequence.MinValue && ui64(res.first) - ui64(sequence.MinValue) >= delta) {
                        res.first -= delta;
                    } else {
                        // overflow, either report error or cycle from max value
                        if (!sequence.Cycle) {
                            return res;
                        }
                        res.first = sequence.MaxValue;
                    }
                }

                if (res.first < sequence.MinValue) {
                    return res;
                }

                // see how many values would it take to reach MinValue
                res.second = (ui64(res.first) - ui64(sequence.MinValue)) / delta;
                if (res.second > cache) {
                    res.second = cache;
                }

                i64 next = res.first - delta * res.second;
                if (res.second < cache && next == sequence.MinValue) {
                    // we want to also consume the MinValue
                    sequence.NextValue = next;
                    sequence.NextUsed = true;
                    ++res.second;
                } else if (res.second > 0) {
                    sequence.NextValue = next;
                    sequence.NextUsed = false;
                }
            }

            return res;
        }

        void Complete(const TActorContext& ctx) override {
            SLOG_T("TTxAllocateSequence.Complete");

            if (Result) {
                ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
            }
        }

        void SetResult(NKikimrTxSequenceShard::TEvAllocateSequenceResult::EStatus status) {
            Result.Reset(new TEvSequenceShard::TEvAllocateSequenceResult(status, Self->TabletID()));
        }

        TEvSequenceShard::TEvAllocateSequence::TPtr Ev;
        THolder<TEvSequenceShard::TEvAllocateSequenceResult> Result;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvAllocateSequence::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxAllocateSequence(this, std::move(ev)), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
