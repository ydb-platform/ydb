#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/size_calcer.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

class TDictionaryArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    std::shared_ptr<arrow::Array> ArrayVariants;
    std::shared_ptr<arrow::Array> ArrayRecords;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(ArrayVariants);
    }

    ui32 GetIndexImpl(const ui32 index) const;

protected:
    virtual std::optional<ui64> DoGetRawSize() const override {
        return NArrow::GetArrayDataSize(ArrayVariants) + NArrow::GetArrayDataSize(ArrayRecords);
    }

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return NArrow::TStatusValidator::GetValid(ArrayVariants->GetScalar(GetIndexImpl(index)));
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override;
    virtual ui32 DoGetNullsCount() const override {
        return ArrayRecords->null_count();
    }
    virtual ui32 DoGetValueRawBytes() const override {
        return NArrow::GetArrayDataSize(ArrayVariants) + NArrow::GetArrayDataSize(ArrayRecords);
    }

    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const override {
        if (ArrayVariants->length() == 1) {
            value = NArrow::TStatusValidator::GetValid(ArrayVariants->GetScalar(0));
            return true;
        }
        return false;
    }

public:
    static EType GetTypeStatic() {
        return EType::Dictionary;
    }

    virtual void Reallocate() override {
        ArrayVariants = NArrow::ReallocateArray(ArrayVariants);
        ArrayRecords = NArrow::ReallocateArray(ArrayRecords);
    }

    const std::shared_ptr<arrow::Array>& GetVariants() const {
        return ArrayVariants;
    }

    const std::shared_ptr<arrow::Array>& GetRecords() const {
        return ArrayRecords;
    }

    TDictionaryArray(const std::shared_ptr<arrow::Array>& variants, const std::shared_ptr<arrow::Array>& records)
        : TBase(TValidator::CheckNotNull(records)->length(), EType::Dictionary, variants->type())
        , ArrayVariants(variants)
        , ArrayRecords(records) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
