#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NFmt {

class TPrintableTypedCells {
public:
    TPrintableTypedCells(TArrayRef<const TCell> cells, TArrayRef<const NScheme::TTypeInfo> types)
        : Cells(cells)
        , Types(types)
    {
        Y_DEBUG_ABORT_UNLESS(Cells.size() <= Types.size());
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TPrintableTypedCells& v) {
        out << '{';
        size_t pos = 0;
        for (const TCell& cell : v.Cells) {
            if (pos != 0) {
                out << ", ";
            }
            TString value;
            DbgPrintValue(value, cell, v.Types[pos++]);
            out << value;
        }
        out << '}';
        return out;
    }

private:
    const TArrayRef<const TCell> Cells;
    const TArrayRef<const NScheme::TTypeInfo> Types;
};

}
}
