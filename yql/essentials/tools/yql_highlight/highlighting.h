#pragma once

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

namespace NSQLHighlight {

bool IsCaseInsensitive(const THighlighting& highlighting);

template <std::invocable<const TUnit&, const TRangePattern&> Action>
void ForEachMultiLine(const THighlighting& highlighting, Action action) {
    for (const TUnit& unit : highlighting.Units) {
        for (const TRangePattern& range : unit.RangePatterns) {
            action(unit, range);
        }
    }
}

} // namespace NSQLHighlight
