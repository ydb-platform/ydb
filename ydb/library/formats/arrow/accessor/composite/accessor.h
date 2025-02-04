#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

namespace NKikimr::NArrow::NAccessor {

class TCompositeChunkedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;

private:
    std::vector<std::shared_ptr<IChunkedArray>> Chunks;

protected:
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 /*offset*/, const ui32 /*count*/) const override {
        AFL_VERIFY(false);
        return nullptr;
    }

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
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
