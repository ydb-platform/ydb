#pragma once

#include "rows.h"
#include "tails.h"

#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TCookRow {
        TCookRow() = default;
        TCookRow(const TCookRow&) = delete;

        ~TCookRow()
        {
        }

        template<typename TVal>
        inline TCookRow& Do(NTable::TTag tag, TVal&& val)
        {
            row.Do(tag, std::move(val));
            return *this;
        }

        TRow&& operator *() noexcept
        {
            return std::move(row);
        }

    private:
        TRow row;
    };

    class TSchemedCookRow {
    public:
        TSchemedCookRow(const TRowScheme &scheme, TPos skip = 0)
            : On(skip), Scheme(scheme) { }

        inline TRow&& operator*() noexcept
        {
            return std::move(Row);
        }

        TSchemedCookRow& To(TPos to)
        {
            if (to < On || to >= Scheme.Cols.size()) {
                Y_TABLET_ERROR("TSchemedCookRow row builder skip position is out of range");
            }

            On = to;
            return *this;
        }

        template<typename TVal, typename ...TArgs>
        inline TSchemedCookRow& Col(const TVal &val, TArgs&&...args)
        {
            if (On >= Scheme.Cols.size()) {
                Y_TABLET_ERROR("NO more columns left in row scheme");
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
