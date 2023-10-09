#pragma once

#include "flat_row_nulls.h"
#include "flat_row_remap.h"
#include "util_fmt_desc.h"

#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NFmt {
    using TOut = IOutputStream;

    struct TCells {
        using TReg = NScheme::TTypeRegistry;
        using TCellsRef = TArrayRef<const TCell>;
        using TTypesRef = TArrayRef<const NScheme::TTypeInfo>;

        TCells(TCellsRef cells, const NTable::TRemap &remap, const TReg *reg)
            : TCells(cells, remap.Types(), reg)
        {

        }

        TCells(TCellsRef cells, const NTable::TCellDefaults &cellDefaults, const TReg *reg)
            : TCells(cells, cellDefaults.Types, reg)
        {

        }

        TCells(TCellsRef cells, TTypesRef types, const TReg *reg)
            : Cells(cells)
            , Types(types)
            , Registry(reg)
        {
            Y_ABORT_UNLESS(cells.size() == Types.size(), "Cells and types size missmatch");
        }

        TOut& Do(TOut &out) const noexcept
        {
            TDbTupleRef tp{ Types.begin(), Cells.begin(), ui32(Cells.size()) };

            return out << DbgPrintTuple(tp, *Registry);
        }

    private:
        const TCellsRef Cells;
        const TTypesRef Types;
        const TReg *Registry = nullptr;
    };

    inline TOut& operator<<(TOut &out, const NFmt::TCells &print)
    {
        return print.Do(out);
    }

}
}
