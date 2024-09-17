#pragma once
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

namespace NKikimr::NArrow::NAccessor {

class TDeserializeChunkedArray: public NArrow::NAccessor::IChunkedArray {
private:
    using TBase = NArrow::NAccessor::IChunkedArray;

public:
    class TChunk {
    private:
        YDB_READONLY(ui32, RecordsCount, 0);
        std::shared_ptr<IChunkedArray> PredefinedArray;
        const TString Data;

    public:
        TChunk(const std::shared_ptr<IChunkedArray>& predefinedArray)
            : PredefinedArray(predefinedArray) {
            AFL_VERIFY(PredefinedArray);
            RecordsCount = PredefinedArray->GetRecordsCount();
        }

        TChunk(const ui32 recordsCount, const TString& data)
            : RecordsCount(recordsCount)
            , Data(data) {
        }

        std::shared_ptr<IChunkedArray> GetArrayVerified(const std::shared_ptr<TColumnLoader>& loader) const {
            if (PredefinedArray) {
                return PredefinedArray;
            }
            return loader->ApplyVerified(Data, RecordsCount);
        }
    };

private:
    std::shared_ptr<TColumnLoader> Loader;
    std::vector<TChunk> Chunks;

protected:
    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& /*saver*/, const TString& /*fullSerializedData*/, const std::vector<ui64>& /*splitSizes*/) override {
        AFL_VERIFY(false);
        return {};
    }

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
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        AFL_VERIFY(false);
        return nullptr;
    }

public:
    TDeserializeChunkedArray(const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, std::vector<TChunk>&& chunks)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , Chunks(std::move(chunks)) {
        AFL_VERIFY(Loader);
    }
};

}   // namespace NKikimr::NArrow::NAccessor
