#pragma once

#include "rows.h"

#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_row_misc.h>
#include <ydb/core/tablet_flat/flat_row_column.h>
#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_update_op.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/vector.h>
#include <util/generic/utility.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    const NScheme::TTypeRegistry* DbgRegistry();
    TString PrintValue(const TCell& r, NScheme::TTypeId typeId);

    class TRowTool {
    public:
        using TState = TRowState;

        struct TSplit {
            explicit TSplit(ui32 items)
            {
                Key.reserve(items);
            }

            TVector<TRawTypeValue> Key;
            TVector<TUpdateOp> Ops;
        };

        TRowTool(const TRowScheme &scheme)
            : Scheme(scheme)
        {

        }

        TVector<TRawTypeValue> LookupKey(const TRow &tagged) const
        {
            return Split(tagged, true, false).Key;
        }

        TVector<TCell> KeyCells(const TRow &tagged) const
        {
            TCelled celled(LookupKey(tagged), *Scheme.Keys, false);

            return { celled.Cells, celled.Cells + celled.Size };
        }

        TSplit Split(const TRow &tagged, bool weak, bool values) const
        {
            TSplit pair((*tagged) ? Scheme.Keys->Size() : 0);

            ui32 foundKeyCols = 0;

            /* For lookups (lookup = true) absense of tail columns in key is
                interpreted as +inf, thus to support this semantic grow key
                with nulls only when need to store new value from TRow. But
                empty key has special meanings in context of ESeek modes.
             */

            for (auto &value: *tagged) {
                auto * const info = ColFor(value);

                TRawTypeValue raw(value.Cell.AsRef(), value.Type);

                if (info->IsKey()) {
                    Y_ABORT_UNLESS(value.Op == ECellOp::Set || value.Op == ECellOp::Null);

                    if (info->Key >= Scheme.Keys->Size()) {
                        ythrow yexception()
                            << "Cell at " << info->Key << " is out of"
                            << " key size " <<  Scheme.Keys->Size();
                    } else if (info->Key >= pair.Key.size()) {
                        pair.Key.resize(info->Key + 1);
                    }

                    pair.Key.at(info->Key) = raw, foundKeyCols++;
                } else if (values) {
                    pair.Ops.emplace_back(value.Tag, value.Op, raw);
                }
            }

            Y_ABORT_UNLESS(weak || foundKeyCols == Scheme.Keys->Size());

            return pair;
        }

        bool Compare(
                const TRow &row, const TState &state, const TRemap &re) const
        {
            return Cmp(row, state, re, false);
        }

        bool CmpKeys(
                const TRow &row, const TState &state, const TRemap &re) const
        {
            return Cmp(row, state, re, true);
        }

        bool Cmp(
                const TRow &row,const TState &state,
                const TRemap &remap, bool keys) const
        {
            TStackVec<bool, 64> Seen(state.Size(), false);

            for (const auto &val: *row) {
                const auto pos = remap.Has(val.Tag);
                const auto *info = ColFor(val);
                const auto &cell = state.Get(pos);

                if (keys && !info->IsKey()) {
                    continue; /* Skip non-keys columns */
                } else if (ELargeObj(val.Op) != ELargeObj(state.GetCellOp(pos))) {
                    return false; /* Missmatched value storage method */
                } else if (val.Op != ELargeObj::Inline) {
                    if (!(val.Cell.AsRef() == cell.AsRef()))
                        return false;
                } else if (val.Op == ECellOp::Empty || val.Op == ECellOp::Reset) {
                    if (ECellOp(val.Op) != state.GetCellOp(pos))
                        return false;
                } else if (CompareTypedCells(val.Cell, cell, info->TypeInfo)) {
                    return false; /* Literal comparison has been failed */
                }

                Seen.at(pos) = true;
            }

            if (keys) {
                for (auto &pin: remap.KeyPins())
                    if (!Seen.at(pin.Pos)) return false;

            } else {
                for (TPos on = 0; on < state.Size(); on++)
                    if (!Seen[on] && !state.Get(on).IsNull())
                        return false; /* Has absent in row non-null value */
            }

            return true;
        }

        TString Describe(const TRow &tagged) const
        {
            TStringStream ss;

            return Describe(ss, tagged), ss.Str();
        }

        void Describe(IOutputStream &out, const TRow &tagged) const
        {
            TVector<TCell> cells(Scheme.Cols.size());

            for (const auto &value: *tagged) {
                cells.at(ColFor(value)->Pos) = value.Cell;
            }

            out << NFmt::TCells(cells, *Scheme.RowCellDefaults, DbgRegistry());
        }

        const TColInfo* ColFor(const TRow::TUpdate &value) const
        {
            auto *info = Scheme.ColInfo(value.Tag);

            if (!info || info->Pos == Max<NTable::TPos>()) {
                ythrow
                    yexception()
                        << "Cannot find tag=" << value.Tag << " in scheme";

            } else if (value.Cell.IsNull() && value.Type == 0) {
                /* null values should be passed without type */
            } else if (info->TypeInfo.GetTypeId() != value.Type) {
                ythrow
                    yexception()
                        << "Col{" << info->Pos << ", Tag " << info->Tag
                        << ", type " << info->TypeInfo.GetTypeId() << "}"
                        << ", got type " <<  value.Type << " { "
                        << PrintValue(value.Cell, value.Type) << " }";
            }

            return info;
        }

    public:
        const TRowScheme &Scheme;
    };
}
}
}
