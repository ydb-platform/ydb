#include "tier_info.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

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

TTiering::TTieringContext TTiering::GetTierToMove(const std::shared_ptr<arrow::Scalar>& max, const TInstant now) const {
    AFL_VERIFY(OrderedTiers.size());
    std::optional<TString> nextTierName;
    std::optional<TDuration> nextTierDuration;
    for (auto& tierRef : GetOrderedTiers()) {
        auto& tierInfo = tierRef.Get();
        auto mpiOpt = tierInfo.ScalarToInstant(max);
        Y_ABORT_UNLESS(mpiOpt);
        const TInstant maxTieringPortionInstant = *mpiOpt;
        const TDuration dWaitLocal = maxTieringPortionInstant - tierInfo.GetEvictInstant(now);
        if (!dWaitLocal) {
            return TTieringContext(tierInfo.GetName(), tierInfo.GetEvictInstant(now) - maxTieringPortionInstant, nextTierName, nextTierDuration);
        } else {
            nextTierName = tierInfo.GetName();
            nextTierDuration = dWaitLocal;
        }
    }
    return TTieringContext(IStoragesManager::DefaultStorageId, TDuration::Zero(), nextTierName, nextTierDuration);
}

}
