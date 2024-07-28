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

    virtual TCurrentChunkAddress DoGetChunk(
        const std::optional<TCurrentChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        return TCurrentChunkAddress(Array, 0, 0);
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

    virtual TCurrentArrayAddress DoGetArray(const std::optional<TCurrentArrayAddress>& /*chunkCurrent*/, const ui64 /*position*/,
        const std::shared_ptr<IChunkedArray>& selfPtr) const override {
        return TCurrentArrayAddress(selfPtr, 0, 0);
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
    virtual TCurrentChunkAddress DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        return Array;
    }
    virtual std::optional<ui64> DoGetRawSize() const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto chunk = GetChunk({}, index);
        return NArrow::TStatusValidator::GetValid(chunk.GetArray()->GetScalar(index - chunk.GetStartPosition()));
    }
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& /*saver*/, const TString& /*fullSerializedData*/, const std::vector<ui64>& /*splitSizes*/) override {
        AFL_VERIFY(false);
        return {};
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

    virtual TCurrentArrayAddress DoGetArray(const std::optional<TCurrentArrayAddress>& /*chunkCurrent*/, const ui64 /*position*/,
        const std::shared_ptr<IChunkedArray>& selfPtr) const override {
        return TCurrentArrayAddress(selfPtr, 0, 0);
    }

public:
    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
