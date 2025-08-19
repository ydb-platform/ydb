#include "aggr_common.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NSSA::NAggregation {

EAggregate TAggregationsHelper::GetSecondaryAggregationId(const EAggregate aggr) {
    switch (aggr) {
        case EAggregate::Unspecified:
            AFL_VERIFY(false);
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Sum:
        case EAggregate::Some:
            return aggr;
        case EAggregate::NumRows:
        case EAggregate::Count:
            return EAggregate::Sum;
    }
}

}   // namespace NKikimr::NArrow::NSSA::NAggregation
