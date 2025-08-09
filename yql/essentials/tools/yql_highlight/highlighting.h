#pragma once

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

namespace NSQLHighlight {

    bool IsCaseInsensitive(const THighlighting& highlighting);

    template <std::invocable<const TUnit&> Action>
    void ForEachMultiLine(const THighlighting& highlighting, Action action) {
        for (const TUnit& unit : highlighting.Units) {
            TMaybe<TRangePattern> range = unit.RangePattern;
            if (!range) {
                continue;
            }

            action(unit);
        }
    }

} // namespace NSQLHighlight
