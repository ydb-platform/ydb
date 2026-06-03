#pragma once

#include "settings.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

// Accessor for JsonDocument columns stored in the compact_kv format.
//
// Internally it simply holds the decoded plain binary-JSON arrow::BinaryArray;
// the compact_kv blob is produced/consumed only at (de)serialization time by
// NCompactKV::TConstructor. The distinct EType tag lets the engine recognise
// columns using this accessor.
class TCompactKVArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    std::shared_ptr<arrow::Array> Array;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(Array);
    }

protected:
    virtual std::optional<ui64> DoGetRawSize() const override;
    virtual TLocalDataAddress DoGetLocalData(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        return TLocalDataAddress(Array, 0, 0);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return NArrow::TStatusValidator::GetValid(Array->GetScalar(index));
    }
    virtual TMinMax DoGetMinMaxScalars() const override;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        return std::make_shared<TCompactKVArray>(Array->Slice(offset, count));
    }
    virtual ui32 DoGetNullsCount() const override {
        return Array->null_count();
    }
    virtual ui32 DoGetValueRawBytes() const override;

public:
    virtual void Reallocate() override;

    virtual std::shared_ptr<arrow::ChunkedArray> GetChunkedArrayTrivial() const override {
        return std::make_shared<arrow::ChunkedArray>(Array);
    }

    const std::shared_ptr<arrow::Array>& GetArray() const {
        return Array;
    }

    TCompactKVArray(const std::shared_ptr<arrow::Array>& data)
        : TBase(data->length(), EType::CompactKVArray, data->type())
        , Array(data) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
