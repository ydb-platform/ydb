#pragma once

#include "flat_row_state.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NTable {

    /* Deep copy of a TRowState that owns its cell data. The source row may
        point into page memory that is gone by the time the copy is used, so
        anything that needs to keep a row around (e.g. buffering, delta scans)
        stores it as TSavedRow instead.
     */
    struct TSavedRow {
        ERowOp Op = ERowOp::Absent;
        TString Buf;
        struct TSlot { ui32 Offset = 0; ui32 Size = 0; TCellOp CellOp; };
        TVector<TSlot> Slots;

        explicit operator bool() const noexcept {
            return Op != ERowOp::Absent;
        }

        void Save(const TRowState& row) {
            Op = row.GetRowState();
            Slots.resize(row.Size());
            Buf.clear();
            for (ui32 i = 0; i < row.Size(); i++) {
                Slots[i].CellOp = row.GetCellOp(i);
                const auto& cell = row.Get(i);
                Slots[i].Offset = Buf.size();
                if (!cell.IsNull()) {
                    Slots[i].Size = cell.Size();
                    Buf.append(cell.Data(), cell.Size());
                } else {
                    Slots[i].CellOp = ECellOp::Null;
                    Slots[i].Size = 0;
                }
            }
        }

        bool IsNull(size_t i) const {
            return i >= Slots.size() ||
                Slots[i].CellOp != ECellOp::Set;
        }

        TStringBuf GetBuf(size_t i) const {
            if (!IsNull(i)) {
                return TStringBuf(Buf.data() + Slots[i].Offset, Slots[i].Size);
            }
            return TStringBuf();
        }

        void Restore(TRowState& out) const {
            out.Init(Slots.size());
            out.Touch(Op);
            if (Op == ERowOp::Erase) {
                return;
            }
            for (ui32 i = 0; i < Slots.size(); i++) {
                if (Slots[i].CellOp == ECellOp::Set) {
                    out.Set(i, Slots[i].CellOp, TCell(Buf.data() + Slots[i].Offset, Slots[i].Size));
                }
            }
        }
    };

}
}
