#include "interaction.h"
#include <ydb/core/tx/columnshard/engines/predicate/container.h>

namespace NKikimr::NOlap::NTxInteractions {
TIntervalPoint TIntervalPoint::From(
    const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema) {
    const i32 shift = [&]() {
        if (container.IsAll()) {
            return -1;
        } else if (!container.IsInclude() || container.NumColumns() < (ui32)pkSchema->num_fields()) {
            return 1;
        } else {
            return 0;
        }
    }();
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
