#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NArrow::NAccessor {

class TCompositeChunkedArray: public NArrow::NAccessor::IChunkedArray {
private:
    using TBase = NArrow::NAccessor::IChunkedArray;

private:
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Chunks;

protected:
    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

    virtual std::vector<NArrow::NAccessor::TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& /*saver*/, const TString& /*fullSerializedData*/, const std::vector<ui64>& /*splitSizes*/) override {
        AFL_VERIFY(false);
        return {};
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        AFL_VERIFY(false)("problem", "cannot use method");
        return nullptr;
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return {};
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        AFL_VERIFY(false);
        return nullptr;
    }
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override;

    TCompositeChunkedArray(std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& chunks, const ui32 recordsCount,
        const std::shared_ptr<arrow::DataType>& type)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, type)
        , Chunks(std::move(chunks)) {
    }

public:
    class TBuilder {
    private:
        ui32 RecordsCount = 0;
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Chunks;
        const std::shared_ptr<arrow::DataType> Type;

    public:
        TBuilder(const std::shared_ptr<arrow::DataType>& type)
            : Type(type) {
            AFL_VERIFY(Type);
        }

        void AddChunk(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
            AFL_VERIFY(arr->GetDataType()->id() == Type->id())("incoming", arr->GetDataType()->ToString())("main", Type->ToString());
            Chunks.emplace_back(arr);
            RecordsCount += arr->GetRecordsCount();
        }

        std::shared_ptr<TCompositeChunkedArray> Finish() {
            return std::shared_ptr<TCompositeChunkedArray>(new TCompositeChunkedArray(std::move(Chunks), RecordsCount, Type));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
