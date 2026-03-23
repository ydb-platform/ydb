#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <library/cpp/json/writer/json_value.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

IChunkedArray::TLocalDataAddress TDictionaryArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    std::unique_ptr<arrow::ArrayBuilder> builderDictionary = NArrow::MakeBuilder(ArrayDictionary->type());
    AFL_VERIFY(SwitchType(ArrayDictionary->type()->id(), [&](const auto typeVariant) {
        const auto* arrDictionaryImpl = typeVariant.CastArray(ArrayDictionary.get());
        auto* builder = typeVariant.CastBuilder(builderDictionary.get());
        if constexpr (typeVariant.IsAppropriate) {
            AFL_VERIFY(SwitchType(ArrayPositions->type()->id(), [&](const auto type) {
                const auto* arrPositionsImpl = type.CastArray(ArrayPositions.get());
                if constexpr (type.IsIndexType()) {
                    for (ui32 i = 0; i < arrPositionsImpl->length(); ++i) {
                        if (arrPositionsImpl->IsNull(i)) {
                            TStatusValidator::Validate(builder->AppendNull());
                        } else {
                            const ui32 dictIdx = arrPositionsImpl->Value(i);
                            if (arrDictionaryImpl->IsNull(dictIdx)) {
                                TStatusValidator::Validate(builder->AppendNull());
                            } else {
                                TStatusValidator::Validate(builder->Append(typeVariant.GetValue(*arrDictionaryImpl, dictIdx)));
                            }
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
    return TLocalDataAddress(NArrow::FinishBuilder(std::move(builderDictionary)), 0, 0);
}

std::shared_ptr<IChunkedArray> TDictionaryArray::DoISlice(const ui32 offset, const ui32 count) const {
    std::vector<bool> mask(ArrayDictionary->length(), false);
    ui32 markCount = 0;
    const auto positionsNew = ArrayPositions->Slice(offset, count);
    AFL_VERIFY(SwitchType(positionsNew->type()->id(), [&](const auto& type) {
        using TRecordsWrap = std::decay_t<decltype(type)>;
        using TRecordsArray = typename arrow::TypeTraits<typename TRecordsWrap::T>::ArrayType;
        if constexpr (arrow::has_c_type<typename TRecordsWrap::T>()) {
            const auto* arrPositionsImpl = static_cast<const TRecordsArray*>(positionsNew.get());
            for (ui32 i = 0; i < arrPositionsImpl->length() && markCount < mask.size(); ++i) {
                if (!arrPositionsImpl->IsNull(i) && !mask[arrPositionsImpl->Value(i)]) {
                    ++markCount;
                    mask[arrPositionsImpl->Value(i)] = true;
                }
            }
            return true;
        }
        return false;
    }));
    if (markCount == mask.size()) {
        return std::make_shared<TDictionaryArray>(ArrayDictionary, positionsNew);
    }
    // Build old dictionary index -> new (filtered) dictionary index.
    std::vector<ui32> oldToNew(ArrayDictionary->length(), 0);
    for (ui32 newIdx = 0, i = 0; i < mask.size(); ++i) {
        if (mask[i]) {
            oldToNew[i] = newIdx++;
        }
    }
    auto filtered = TColumnFilter(std::move(mask)).Apply(std::make_shared<TTrivialArray>(ArrayDictionary))->GetChunkedArray();
    std::shared_ptr<arrow::Array> dictArray;
    if (!filtered || filtered->num_chunks() == 0) {
        dictArray = TThreadSimpleArraysCache::GetNull(ArrayDictionary->type(), 0);
    } else if (filtered->num_chunks() == 1) {
        dictArray = filtered->chunk(0);
    } else {
        arrow::ArrayVector parts;
        for (int i = 0; i < filtered->num_chunks(); ++i) {
            parts.push_back(filtered->chunk(i));
        }
        dictArray = NArrow::TStatusValidator::GetValid(arrow::Concatenate(parts));
    }
    // Remap positions to indices into the filtered dictionary.
    std::unique_ptr<arrow::ArrayBuilder> positionsBuilder = NArrow::MakeBuilder(positionsNew->type());
    AFL_VERIFY(SwitchType(positionsNew->type()->id(), [&](const auto& type) {
        using TRecordsWrap = std::decay_t<decltype(type)>;
        using TRecordsArray = typename arrow::TypeTraits<typename TRecordsWrap::T>::ArrayType;
        if constexpr (TRecordsWrap::IsIndexType()) {
            const auto* arrPositionsImpl = static_cast<const TRecordsArray*>(positionsNew.get());
            auto* builder = type.CastBuilder(positionsBuilder.get());
            using CType = typename TRecordsWrap::ValueType;
            for (int64_t i = 0; i < arrPositionsImpl->length(); ++i) {
                if (arrPositionsImpl->IsNull(i)) {
                    TStatusValidator::Validate(builder->AppendNull());
                } else {
                    const ui32 oldIdx = arrPositionsImpl->Value(i);
                    TStatusValidator::Validate(builder->Append(static_cast<CType>(oldToNew[oldIdx])));
                }
            }
            return true;
        }
        return false;
    }));
    auto positionsRemapped = NArrow::FinishBuilder(std::move(positionsBuilder));
    return std::make_shared<TDictionaryArray>(dictArray, positionsRemapped);
}

NJson::TJsonValue TDictionaryArray::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("dictionary_count", ArrayDictionary->length());
    result.InsertValue("positions_count", GetRecordsCount());
    NJson::TJsonValue dictionaryArr = NJson::JSON_ARRAY;
    for (int64_t i = 0; i < ArrayDictionary->length(); ++i) {
        auto scalar = NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(i));
        if (scalar->is_valid) {
            dictionaryArr.AppendValue(NJson::TJsonValue(scalar->ToString()));
        } else {
            dictionaryArr.AppendValue(NJson::TJsonValue(NJson::JSON_NULL));
        }
    }
    result.InsertValue("dictionary", std::move(dictionaryArr));
    return result;
}

ui32 TDictionaryArray::GetIndexImpl(const ui32 index) const {
    std::optional<ui32> result;
    AFL_VERIFY(SwitchType(ArrayPositions->type()->id(), [&](const auto type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        if constexpr (type.IsIndexType()) {
            const auto* arr = static_cast<const TArray*>(ArrayPositions.get());
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
    if (!ArrayDictionary->length()) {
        return result;
    }
    auto minMaxPos = NArrow::FindMinMaxPosition(ArrayDictionary);
    return NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(minMaxPos.second));
}

TMinMax TDictionaryArray::DoGetMinMaxScalars() const {
    TMinMax result;
    if (!ArrayDictionary->length()) {
        return result;
    }
    auto minMaxPos = NArrow::FindMinMaxPosition(ArrayDictionary);
    result.Min = NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(minMaxPos.first));
    result.Max = NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(minMaxPos.second));
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor
