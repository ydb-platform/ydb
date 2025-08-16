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
    static EAggregate GetSecondaryAggregationId(const EAggregate aggr);
};

}   // namespace NKikimr::NArrow::NSSA::NAggregation
