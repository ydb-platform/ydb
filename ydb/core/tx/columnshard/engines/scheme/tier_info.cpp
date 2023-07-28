#include "tier_info.h"

namespace NKikimr::NOlap {

std::optional<TInstant> TTierInfo::ScalarToInstant(const std::shared_ptr<arrow::Scalar>& scalar) const {
    const ui64 unitsInSeconds = TtlUnitsInSecond ? TtlUnitsInSecond : 1;
    switch (scalar->type->id()) {
        case arrow::Type::TIMESTAMP:
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::TimestampScalar>(scalar)->value);
        case arrow::Type::UINT16: // YQL Date
            return TInstant::Days(std::static_pointer_cast<arrow::UInt16Scalar>(scalar)->value);
        case arrow::Type::UINT32: // YQL Datetime or Uint32
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::UInt32Scalar>(scalar)->value / (1.0 * unitsInSeconds / 1000000));
        case arrow::Type::UINT64:
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::UInt64Scalar>(scalar)->value / (1.0 * unitsInSeconds / 1000000));
        default:
            return {};
    }
}

std::shared_ptr<NKikimr::NOlap::TTierInfo> TTiering::GetMainTierInfo() const {
    auto ttl = Ttl;
    auto tier = OrderedTiers.size() ? OrderedTiers.begin()->GetPtr() : nullptr;
    if (!ttl && !tier) {
        return nullptr;
    } else if (!tier) {
        return ttl;
    } else if (!ttl) {
        return tier;
    } else {
        const TDuration ttlDuration = ttl->GetEvictDuration();
        const TDuration tierDuration = tier->GetEvictDuration();
        if (tierDuration < ttlDuration) {
            return tier;
        } else {
            return ttl;
        }
    }
}

}
