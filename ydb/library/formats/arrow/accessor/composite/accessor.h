#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

namespace NKikimr::NArrow::NAccessor {

class ICompositeChunkedArray: public NArrow::NAccessor::IChunkedArray {
private:
    using TBase = NArrow::NAccessor::IChunkedArray;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override final;

public:
    using TBase::TBase;
};

class TCompositeChunkedArray: public ICompositeChunkedArray {
private:
    using TBase = ICompositeChunkedArray;

private:
    std::vector<std::shared_ptr<IChunkedArray>> Chunks;

protected:
    virtual ui32 DoGetNullsCount() const override {
        AFL_VERIFY(false);
        return 0;
    }
    virtual ui32 DoGetValueRawBytes() const override {
        AFL_VERIFY(false);
        return 0;
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

public:
    TCompositeChunkedArray(std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& chunks, const ui32 recordsCount,
        const std::shared_ptr<arrow::DataType>& type)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, type)
        , Chunks(std::move(chunks)) {
    }

    class TBuilder {
    private:
        ui32 RecordsCount = 0;
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Chunks;
        const std::shared_ptr<arrow::DataType> Type;
        bool Finished = false;
    public:
        TBuilder(const std::shared_ptr<arrow::DataType>& type)
            : Type(type) {
            AFL_VERIFY(Type);
        }

        void AddChunk(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
            AFL_VERIFY(!Finished);
            AFL_VERIFY(arr->GetDataType()->id() == Type->id())("incoming", arr->GetDataType()->ToString())("main", Type->ToString());
            Chunks.emplace_back(arr);
            RecordsCount += arr->GetRecordsCount();
        }

        std::shared_ptr<TCompositeChunkedArray> Finish() {
            AFL_VERIFY(!Finished);
            Finished = true;
            return std::shared_ptr<TCompositeChunkedArray>(new TCompositeChunkedArray(std::move(Chunks), RecordsCount, Type));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
