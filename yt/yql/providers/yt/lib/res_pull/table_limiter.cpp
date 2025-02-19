#include "table_limiter.h"

#include <yt/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>

#include <util/generic/ylimits.h>

namespace NYql {

TTableLimiter::TTableLimiter(const TRecordsRange& range)
    : Start(range.Offset.GetOrElse(0ULL))
    , Current(0ULL)
    , TableStart(0ULL)
    , TableEnd(Max())
{
    const auto limit = range.Limit.GetOrElse(Max());
    if (limit > Max<ui64>() - Start) {
        End = Max();
    } else {
        End = Start + limit;
    }
}

bool TTableLimiter::NextTable(ui64 recordCount) {
    TableStart = 0ULL;
    TableEnd = Max();
    if (!recordCount) { // Skip empty tables
        return false;
    }
    if (Start && Current + recordCount <= Start) {
        Current += recordCount;
        return false;
    }
    if (Start && Current < Start && Current + recordCount > Start) {
        TableStart = Start - Current;
    }

    if (Current < End && Current + recordCount > End) {
        TableEnd = End - Current;
    }

    Current += recordCount;
    return true;
}

void TTableLimiter::NextDynamicTable() {
    TableStart = 0ULL;
    TableEnd = Max();
    if (Start && Current < Start) {
        TableStart = Start - Current;
    }

    if (Current < End) {
        TableEnd = End - Current;
    }
}

void TTableLimiter::Skip(ui64 recordCount) {
    Current += recordCount;
}

ui64 TTableLimiter::GetTableZEnd() const {
    return Max<ui64>() == TableEnd ? 0ULL : TableEnd;
}

} // NYql
