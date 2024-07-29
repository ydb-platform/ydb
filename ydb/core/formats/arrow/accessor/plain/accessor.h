#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

class TTrivialArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::Array> Array;

protected:
    virtual std::optional<ui64> DoGetRawSize() const override;

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 position) const override {
        return TLocalDataAddress(Array, 0, 0);
    }
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        return std::make_shared<arrow::ChunkedArray>(Array);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return NArrow::TStatusValidator::GetValid(Array->GetScalar(index));
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        AFL_VERIFY(false);
        return TLocalChunkedArrayAddress(nullptr, TCommonChunkAddress(0, GetRecordsCount(), 0));
    }

public:
    TTrivialArray(const std::shared_ptr<arrow::Array>& data)
        : TBase(data->length(), EType::Array, data->type())
        , Array(data) {
    }
};

class TTrivialChunkedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::ChunkedArray> Array;

protected:
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        return Array;
    }
    virtual std::optional<ui64> DoGetRawSize() const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto chunk = GetChunkSlow(index);
        return NArrow::TStatusValidator::GetValid(chunk.GetArray()->GetScalar(chunk.GetAddress().GetLocalIndex(index)));
    }
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& /*saver*/, const TString& /*fullSerializedData*/, const std::vector<ui64>& /*splitSizes*/) override {
        AFL_VERIFY(false);
        return {};
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        AFL_VERIFY(false);
        return TLocalChunkedArrayAddress(nullptr, TCommonChunkAddress(0, 0, 0));
    }

public:
    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
