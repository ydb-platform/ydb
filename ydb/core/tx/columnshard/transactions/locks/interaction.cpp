#include "interaction.h"
#include <ydb/core/tx/columnshard/engines/predicate/container.h>

namespace NKikimr::NOlap::NTxInteractions {
TIntervalPoint TIntervalPoint::From(
    const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema) {
    i32 shift = container.IsInclude() ? 0 : 1;
    if (!container.GetReplaceKey()) {
        shift = -1;
    } else if (container.GetReplaceKey()->Size() < (ui32)pkSchema->num_fields()) {
        shift = 1;
    }
    return TIntervalPoint(container.GetReplaceKey(), shift);
}

TIntervalPoint TIntervalPoint::To(
    const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema) {
    i32 shift = container.IsInclude() ? 0 : -1;
    if (!container.GetReplaceKey() || container.GetReplaceKey()->Size() < (ui32)pkSchema->num_fields()) {
        shift = Max<i32>();
    }

    return TIntervalPoint(container.GetReplaceKey(), shift);
}

}   // namespace NKikimr::NOlap::NTxInteractions
