#include "interaction.h"
#include <ydb/core/tx/columnshard/engines/predicate/container.h>

namespace NKikimr::NOlap::NTxInteractions {
TIntervalPoint TIntervalPoint::From(
    const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema) {
    i32 shift = container.IsInclude() ? 0 : 1;
    if (container.IsEmpty()) {
        shift = -1;
    } else if (container.NumColumns() < (ui32)pkSchema->num_fields()) {
        shift = 1;
    }
    return TIntervalPoint(container, pkSchema, shift);
}

TIntervalPoint TIntervalPoint::To(
    const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema) {
    i32 shift = container.IsInclude() ? 0 : -1;
    if (container.NumColumns() < (ui32)pkSchema->num_fields()) {
        shift = Max<i32>();
    }

    return TIntervalPoint(container, pkSchema, shift);
}

}   // namespace NKikimr::NOlap::NTxInteractions
