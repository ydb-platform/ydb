#pragma once

namespace NKikimr::NArrow::NSSA::NAggregation {

enum class EAggregate {
    Unspecified = 0,
    Some = 1,
    Count = 2,
    Min = 3,
    Max = 4,
    Sum = 5,
    //Avg = 6,
    NumRows = 7,
};

class TAggregationsHelper {
public:
    static EAggregate GetSecondaryAggregationId(const EAggregate aggr) {
        switch (aggr) {
            case EAggregate::Unspecified:
                AFL_VERIFY(false);
            case EAggregate::Min:
            case EAggregate::Max:
            case EAggregate::Sum:
            case EAggregate::Some:
                return AggregationId;
            case EAggregate::NumRows:
            case EAggregate::Count:
                return EAggregate::Sum;
        }
    }
};

}   // namespace NKikimr::NArrow::NSSA::NAggregation
