#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <library/cpp/json/writer/json_value.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/size_calcer.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

class TDictionaryArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    std::shared_ptr<arrow::Array> ArrayDictionary;
    std::shared_ptr<arrow::Array> ArrayPositions;
    const bool NeedNullsCountCalculation;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(DoGetLocalData(std::nullopt, 0).GetArray());
    }

    virtual void DoVisitDistinctValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(ArrayDictionary);
    }

    ui32 GetIndexImpl(const ui32 index) const;

protected:
    virtual std::optional<ui64> DoGetRawSize() const override {
        return NArrow::GetArrayDataSize(ArrayDictionary) + NArrow::GetArrayDataSize(ArrayPositions);
    }

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        if (ArrayPositions->IsNull(index)) {
            return arrow::MakeNullScalar(ArrayDictionary->type());
        }
        return NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(GetIndexImpl(index)));
    }
    virtual TMinMax DoGetMinMaxScalars() const override;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override;
    virtual ui32 DoGetNullsCount() const override {
        // For dictionaries created by insertion/Compaction pipeline it is not possible to have a non-null index referencing a null value.
        // But it may be possible if values we constructed, for example, by a kernel application.
        // This method is not expected to be called for them (used only for columnar statistics), but that branch is added for correctness sake.
        if (!NeedNullsCountCalculation || !ArrayDictionary->null_count()) {
            return ArrayPositions->null_count();
        }
        ui32 result = 0;
        AFL_VERIFY(SwitchType(ArrayPositions->type()->id(), [&](const auto type) {
            if constexpr (type.IsIndexType()) {
                const auto* positions = type.CastArray(ArrayPositions.get());
                for (ui32 index = 0; index < positions->length(); ++index) {
                    if (positions->IsNull(index) || ArrayDictionary->IsNull(positions->Value(index))) {
                        ++result;
                    }
                }
                return true;
            }
            return false;
        }));
        return result;
    }

    virtual ui32 DoGetValueRawBytes() const override {
        return NArrow::GetArrayDataSize(ArrayDictionary) + NArrow::GetArrayDataSize(ArrayPositions);
    }

    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const override {
        if (ArrayDictionary->length() == 1) {
            value = NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(0));
            return true;
        }
        return false;
    }

    virtual NJson::TJsonValue DoDebugJson() const override;

public:
    static EType GetTypeStatic() {
        return EType::Dictionary;
    }

    virtual void Reallocate() override {
        ArrayDictionary = NArrow::ReallocateArray(ArrayDictionary);
        ArrayPositions = NArrow::ReallocateArray(ArrayPositions);
    }

    const std::shared_ptr<arrow::Array>& GetDictionary() const {
        return ArrayDictionary;
    }

    const std::shared_ptr<arrow::Array>& GetPositions() const {
        return ArrayPositions;
    }

    TDictionaryArray(
        const std::shared_ptr<arrow::Array>& dictionary, const std::shared_ptr<arrow::Array>& positions,
        const bool needNullsCountCalculation = false)
        : TBase(TValidator::CheckNotNull(positions)->length(), EType::Dictionary, TValidator::CheckNotNull(dictionary)->type())
        , ArrayDictionary(TValidator::CheckNotNull(dictionary))
        , ArrayPositions(TValidator::CheckNotNull(positions))
        , NeedNullsCountCalculation(needNullsCountCalculation)
    {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
