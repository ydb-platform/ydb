#include "tier_info.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/cast.h>

namespace NKikimr::NOlap {

namespace {

std::optional<TInstant> DyNumberToInstant(TStringBuf binary, ui64 unitsInSecond) {
    const auto number = NDyNumber::DyNumberToString(binary);
    if (!number) {
        return {};
    }
    if (number->StartsWith('-') || *number == "0") {
        return TInstant::Zero();
    }
    // DyNumberToString produces .<digits>[e<exponent>]. Move the decimal
    // point to microseconds without losing any of DyNumber's 38 digits.
    const TStringBuf text(*number);
    const size_t exponentPos = text.find('e');
    const TStringBuf digits = text.SubStr(1, exponentPos == TStringBuf::npos ? text.size() - 1 : exponentPos - 1);
    int integerDigits = (exponentPos == TStringBuf::npos ? 0 : FromString<int>(text.SubStr(exponentPos + 1))) + 6;
    for (; unitsInSecond > 1; unitsInSecond /= 10) {
        --integerDigits;
    }
    ui64 micros = 0;
    for (int i = 0; i < integerDigits; ++i) {
        const ui64 digit = i < static_cast<int>(digits.size()) ? digits[i] - '0' : 0;
        if (micros > (Max<ui64>() - digit) / 10) {
            return TInstant::Max();
        }
        micros = micros * 10 + digit;
    }
    // Round up: a positive fraction of a microsecond must not expire early.
    for (size_t i = Max(integerDigits, 0); i < digits.size(); ++i) {
        if (digits[i] != '0') {
            return TInstant::MicroSeconds(micros == Max<ui64>() ? micros : micros + 1);
        }
    }
    return TInstant::MicroSeconds(micros);
}

} // namespace

std::optional<TInstant> TTierInfo::ScalarToInstant(const std::shared_ptr<arrow::Scalar>& scalar, NScheme::TTypeId columnType) const {
    const ui64 unitsInSeconds = TtlUnitsInSecond ? TtlUnitsInSecond : 1;
    // Datetime64 and Timestamp64 both use Arrow Int64, so the logical type
    // is required to distinguish seconds from microseconds.
    switch (columnType) {
        case NScheme::NTypeIds::Date32:
            return TInstant::Days(Max<i32>(0, std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value));
        case NScheme::NTypeIds::Datetime64:
            return TInstant::Seconds(Max<i64>(0, std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value));
        case NScheme::NTypeIds::Timestamp64:
            return TInstant::MicroSeconds(Max<i64>(0, std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value));
        case NScheme::NTypeIds::DyNumber: {
            const auto& value = std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value;
            return DyNumberToInstant(TStringBuf(reinterpret_cast<const char*>(value->data()), value->size()), unitsInSeconds);
        }
        default:
            break;
    }
    switch (scalar->type->id()) {
        case arrow::Type::TIMESTAMP:
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::TimestampScalar>(scalar)->value);
        case arrow::Type::UINT16:   // YQL Date
            return TInstant::Days(std::static_pointer_cast<arrow::UInt16Scalar>(scalar)->value);
        case arrow::Type::UINT32:   // YQL Datetime or Uint32
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::UInt32Scalar>(scalar)->value / (1.0 * unitsInSeconds / 1000000));
        case arrow::Type::UINT64:
            return TInstant::MicroSeconds(std::static_pointer_cast<arrow::UInt64Scalar>(scalar)->value / (1.0 * unitsInSeconds / 1000000));
        default:
            return {};
    }
}

TTiering::TTieringContext TTiering::GetTierToMove(const std::shared_ptr<arrow::Scalar>& max, const TInstant now, const bool skipEviction, NScheme::TTypeId columnType) const {
    AFL_VERIFY(OrderedTiers.size());
    std::optional<TString> nextTierName;
    std::optional<TDuration> nextTierDuration;
    for (auto& tierRef : GetOrderedTiers()) {
        auto& tierInfo = tierRef.Get();
        if (skipEviction && tierInfo.GetExternalStorageId()) {
            continue;
        }

        const TString tierName =
            tierInfo.GetExternalStorageId() ? tierInfo.GetExternalStorageId()->ToString() : NTiering::NCommon::DeleteTierName;
        auto mpiOpt = tierInfo.ScalarToInstant(max, columnType);
        Y_ABORT_UNLESS(mpiOpt);
        const TInstant maxTieringPortionInstant = *mpiOpt;
        const TDuration dWaitLocal = maxTieringPortionInstant - tierInfo.GetEvictInstant(now);
        if (!dWaitLocal) {
            return TTieringContext(tierName, tierInfo.GetEvictInstant(now) - maxTieringPortionInstant, nextTierName, nextTierDuration);
        } else {
            nextTierName = tierName;
            nextTierDuration = dWaitLocal;
        }
    }
    return TTieringContext(IStoragesManager::DefaultStorageId, TDuration::Zero(), nextTierName, nextTierDuration);
}

}   // namespace NKikimr::NOlap
