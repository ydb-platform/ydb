#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/save_load/loader.h>

namespace NKikimr::NArrow::NAccessor {

class TDeserializeChunkedArray: public ICompositeChunkedArray {
private:
    using TBase = ICompositeChunkedArray;

public:
    class TChunk {
    private:
        YDB_READONLY(ui32, RecordsCount, 0);
        std::shared_ptr<IChunkedArray> PredefinedArray;
        const TString Data;
        const TStringBuf DataBuffer;

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

        TChunk(const ui32 recordsCount, const TStringBuf dataBuffer)
            : RecordsCount(recordsCount)
            , DataBuffer(dataBuffer) {
        }

        std::shared_ptr<IChunkedArray> GetArrayVerified(const std::shared_ptr<TColumnLoader>& loader) const;
    };

private:
    std::shared_ptr<TColumnLoader> Loader;
    std::vector<TChunk> Chunks;
    const bool ForLazyInitialization = false;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        for (auto&& i : Chunks) {
            i.GetArrayVerified(Loader)->VisitValues(visitor);
        }
    }

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
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

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
    virtual std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const override {
        if (!ForLazyInitialization) {
            AFL_VERIFY(false);
            return nullptr;
        } else {
            return TBase::GetChunkedArray();
        }
    }

public:
    TDeserializeChunkedArray(const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, std::vector<TChunk>&& chunks,
        const bool forLazyInitialization = false)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , Chunks(std::move(chunks))
        , ForLazyInitialization(forLazyInitialization) {
        AFL_VERIFY(Loader);
    }
};

}   // namespace NKikimr::NArrow::NAccessor
