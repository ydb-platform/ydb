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

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(ArrayDictionary);
    }

    ui32 GetIndexImpl(const ui32 index) const;

protected:
    virtual std::optional<ui64> DoGetRawSize() const override {
        return NArrow::GetArrayDataSize(ArrayDictionary) + NArrow::GetArrayDataSize(ArrayPositions);
    }

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return NArrow::TStatusValidator::GetValid(ArrayDictionary->GetScalar(GetIndexImpl(index)));
    }
    virtual TMinMax DoGetMinMaxScalars() const override;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override;
    virtual ui32 DoGetNullsCount() const override {
        return ArrayPositions->null_count();
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

    TDictionaryArray(const std::shared_ptr<arrow::Array>& dictionary, const std::shared_ptr<arrow::Array>& positions)
        : TBase(TValidator::CheckNotNull(positions)->length(), EType::Dictionary, dictionary->type())
        , ArrayDictionary(dictionary)
        , ArrayPositions(positions)
    {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
