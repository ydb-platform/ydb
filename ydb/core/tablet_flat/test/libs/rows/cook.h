#pragma once

#include "rows.h"
#include "tails.h"

#include <ydb/core/tablet_flat/flat_row_scheme.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TCookRow {
        TCookRow() = default;
        TCookRow(const TCookRow&) = delete;

        ~TCookRow()
        {
            Y_VERIFY(!*row, "Cooked row hasn't been grabbed to TRow");
        }

        template<typename ...TArgs>
        inline TCookRow& Do(NTable::TTag tag, TArgs&& ...args)
        {
            return row.Do(tag, std::forward<TArgs>(args)...), *this;
        }

        TRow operator *() noexcept
        {
            return std::move(row);
        }

    private:
        TRow row;
    };

    class TNatural {
    public:
        TNatural(const TRowScheme &scheme, TPos skip = 0)
            : On(skip), Scheme(scheme) { }

        inline TRow operator*() noexcept
        {
            return std::move(Row);
        }

        TNatural& To(TPos to) noexcept
        {
            if(to < On || to >= Scheme.Cols.size()) {

                Y_FAIL("TNatural row builder skip position is out of range");
            }

            return On = to, *this;
        }

        template<typename TVal, typename ...TArgs>
        inline TNatural& Col(const TVal &val, TArgs&&...args)
        {
            if (On >= Scheme.Cols.size()) {
                Y_FAIL("NO more columns left in row scheme");
            } else {
                Row.Do(Scheme.Cols[On++].Tag, val);

                Col(std::forward<TArgs>(args)...);
            }

            return *this;
        }

    private:
        inline void Col() { /* just stub for args expansion */ }

    private:
        TPos On = 0;
        TRow Row;
        const TRowScheme &Scheme;
    };

}
}
}
