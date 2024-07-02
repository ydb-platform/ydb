#pragma once

#include "flat_row_eggs.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {
namespace NTable {

    class TRowState {
    public:
        TRowState(size_t slots = 0)
        {
            Init(slots);
        }

        void Init(size_t slots)
        {
            Rop = ERowOp::Absent;
            Need_ = 0;
            Left_ = slots;
            Cells.assign(slots, { });
            State.assign(slots, { });
        }

        void Reset(TArrayRef<const TCell> cellDefaults)
        {
            Rop = ERowOp::Absent;
            Need_ = 0;
            Left_ = cellDefaults.size();
            State.assign(Left_, { });
            Cells.assign(cellDefaults.begin(), cellDefaults.end());
        }

        ui32 Need() const noexcept { return Need_; }
        ui32 Left() const noexcept { return Left_; }
        TPos Size() const noexcept { return Cells.size(); }

        bool operator==(ERowOp rop) const noexcept
        {
            return Rop == rop;
        }

        bool operator!=(ERowOp rop) const noexcept
        {
            return Rop != rop;
        }

        ERowOp GetRowState() const {
            return Rop;
        }

        bool Touch(ERowOp op) noexcept
        {
            Y_ABORT_UNLESS(!(Rop == ERowOp::Erase || Rop == ERowOp::Reset),
                "Sequence for row state is already finalized");

            switch (op) {
                case ERowOp::Upsert:
                case ERowOp::Reset:
                    Rop = op;
                    return Left_ > 0; /* process current row when needed */
                case ERowOp::Erase:
                    Rop = (Rop == ERowOp::Absent ? ERowOp::Erase : ERowOp::Reset);
                    return false; /* current row shouldn't be processed */
                default:
                    Y_ABORT("Unexpected row rolling operation code: %" PRIu8, ui8(op));
            }
        }

        void Set(TPos on, TCellOp code, const TCell &cell) noexcept
        {
            Y_ABORT_UNLESS(State[on] == ECellOp::Empty, "Updating cell that already has a value assigned");

            if (Y_UNLIKELY(code == ECellOp::Empty)) {
                // Source column is not set, nothing to update
                // N.B. doesn't really happen in practice
                return;
            }

            Y_ABORT_UNLESS(Left_ > 0, "Cells update counter is out of sync");
            --Left_;

            if (Y_UNLIKELY(code == ECellOp::Reset)) {
                // Setting cell to a schema default value
                // N.B. doesn't really happen in practice
                State[on] = code;
            } else if (code != ECellOp::Null || code != ELargeObj::Inline) {
                State[on] = code;
                Cells[on] = cell;

                /* Required but not materialized external blobs are stored as
                    ECellOp::Null with non-inline ELargeObj storage. Upper layers may
                    check completeness of loaded row later.
                 */

                Need_ += bool(code == ECellOp::Null && code != ELargeObj::Inline);
            } else {
                State[on] = ECellOp::Set;
                Cells[on] = { };
            }
        }

        void Merge(const TRowState& other) noexcept {
            Y_ABORT_UNLESS(!(Rop == ERowOp::Erase || Rop == ERowOp::Reset),
                "Sequence for row state is already finalized");

            if (Y_UNLIKELY(other.Rop == ERowOp::Absent)) {
                return;
            }

            if (Touch(other.Rop)) {
                Y_DEBUG_ABORT_UNLESS(State.size() == other.State.size());
                for (TPos i = 0; i < other.State.size(); ++i) {
                    if (State[i] != ECellOp::Empty || other.State[i] == ECellOp::Empty) {
                        continue;
                    }
                    Set(i, other.State[i], other.Cells[i]);
                }
            }
        }

        TArrayRef<const TCell> operator*() const noexcept
        {
            return Cells;
        }

        const TCell& Get(TPos pos) const noexcept
        {
            return Cells[pos];
        }

        TCellOp GetCellOp(TPos pos) const noexcept
        {
            return State[pos];
        }

        bool IsFinalized() const noexcept
        {
            return (Rop == ERowOp::Upsert && Left_ == 0)
                    || Rop == ERowOp::Erase
                    || Rop == ERowOp::Reset;
        }

        bool IsFinalized(TPos pos) const noexcept
        {
            return GetCellOp(pos) != ECellOp::Empty;
        }

    private:
        ERowOp Rop = ERowOp::Absent;
        ui32 Need_ = 0;     /* Dangled cells state      */
        ui32 Left_ = 0;     /* Cells with unknown state */
        TStackVec<TCellOp, 64> State;
        TSmallVec<TCell> Cells;
    };

}
}
