#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

IChunkedArray::TLocalDataAddress TDictionaryArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    std::unique_ptr<arrow::ArrayBuilder> builderVariants = NArrow::MakeBuilder(ArrayVariants->type());
    AFL_VERIFY(SwitchType(ArrayVariants->type()->id(), [&](const auto typeVariant) {
        const auto* arrVariantsImpl = typeVariant.CastArray(ArrayVariants.get());
        auto* builder = typeVariant.CastBuilder(builderVariants.get());
        if constexpr (typeVariant.IsAppropriate) {
            AFL_VERIFY(SwitchType(ArrayRecords->type()->id(), [&](const auto type) {
                const auto* arrRecordsImpl = type.CastArray(ArrayRecords.get());
                if constexpr (type.IsIndexType()) {
                    for (ui32 i = 0; i < arrRecordsImpl->length(); ++i) {
                        if (arrRecordsImpl->IsNull(i)) {
                            TStatusValidator::Validate(builder->AppendNull());
                        } else {
                            TStatusValidator::Validate(builder->Append(typeVariant.GetValue(*arrVariantsImpl, arrRecordsImpl->Value(i))));
                        }
                    }
                    return true;
                }
                return false;
            }));
            return true;
        }
        return false;
    }));
    return TLocalDataAddress(NArrow::FinishBuilder(std::move(builderVariants)), 0, 0);
}

std::shared_ptr<IChunkedArray> TDictionaryArray::DoISlice(const ui32 offset, const ui32 count) const {
    auto arrRecordsResult = ArrayRecords->Slice(offset, count);
    std::vector<bool> mask(ArrayVariants->length(), false);
    ui32 markCount = 0;
    const auto recordsNew = ArrayRecords->Slice(offset, count);
    AFL_VERIFY(SwitchType(recordsNew->type()->id(), [&](const auto& type) {
        using TRecordsWrap = std::decay_t<decltype(type)>;
        using TRecordsArray = typename arrow::TypeTraits<typename TRecordsWrap::T>::ArrayType;
        if constexpr (arrow::has_c_type<typename TRecordsWrap::T>()) {
            const auto* arrRecordsImpl = static_cast<const TRecordsArray*>(recordsNew.get());
            for (ui32 i = 0; i < arrRecordsImpl->length() && markCount < mask.size(); ++i) {
                if (!arrRecordsImpl->IsNull(i) && !mask[arrRecordsImpl->Value(i)]) {
                    ++markCount;
                    mask[arrRecordsImpl->Value(i)] = true;
                }
            }
            return true;
        }
        return false;
    }));
    if (markCount == mask.size()) {
        return std::make_shared<TDictionaryArray>(ArrayVariants, recordsNew);
    } else {
        auto arr = TColumnFilter(std::move(mask)).Apply(std::make_shared<TTrivialArray>(ArrayVariants))->GetChunkedArray();
        AFL_VERIFY(arr && arr->num_chunks() == 1);
        return std::make_shared<TDictionaryArray>(arr->chunk(0), recordsNew);
    }
}

ui32 TDictionaryArray::GetIndexImpl(const ui32 index) const {
    std::optional<ui32> result;
    AFL_VERIFY(SwitchType(ArrayRecords->type()->id(), [&](const auto type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        if constexpr (type.IsIndexType()) {
            const auto* arr = static_cast<const TArray*>(ArrayRecords.get());
            result = arr->Value(index);
            return true;
        }
        AFL_VERIFY(false);
        return false;
    }));
    AFL_VERIFY(result);
    return *result;
}

std::shared_ptr<arrow::Scalar> TDictionaryArray::DoGetMaxScalar() const {
    std::shared_ptr<arrow::Scalar> result;
    if (!ArrayVariants->length()) {
        return result;
    }
    auto minMaxPos = NArrow::FindMinMaxPosition(ArrayVariants);
    return NArrow::TStatusValidator::GetValid(ArrayVariants->GetScalar(minMaxPos.second));
}

}   // namespace NKikimr::NArrow::NAccessor
