#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/save_load/loader.h>

namespace NKikimr::NArrow::NAccessor {

class TDeserializeChunkedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<TColumnLoader> Loader;
    std::shared_ptr<IChunkedArray> PredefinedArray;
    const TString Data;
    const TStringBuf DataBuffer;
    const bool ForLazyInitialization;
    mutable TAtomicCounter Counter = 0;

protected:
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        return GetLocalChunkedArray(std::nullopt, 0).GetArray()->ISlice(offset, count);
    }
    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        return GetLocalChunkedArray(std::nullopt, 0).GetArray()->VisitValues(visitor);
    }
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
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        AFL_VERIFY(false);
        return TLocalDataAddress(nullptr, 0, 0);
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
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArrayTrivial() const override {
        if (!ForLazyInitialization) {
            AFL_VERIFY(false);
            return nullptr;
        } else {
            return TBase::GetChunkedArrayTrivial();
        }
    }

public:
    TDeserializeChunkedArray(const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, const TString& data,
        const bool forLazyInitialization = false)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , Data(data)
        , ForLazyInitialization(forLazyInitialization) {
        AFL_VERIFY(Loader);
    }

    TDeserializeChunkedArray(
        const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, const TStringBuf data, const bool forLazyInitialization = false)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , DataBuffer(data)
        , ForLazyInitialization(forLazyInitialization) {
        AFL_VERIFY(Loader);
    }

    TDeserializeChunkedArray(const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, const std::shared_ptr<IChunkedArray>& data,
        const bool forLazyInitialization = false)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , PredefinedArray(data)
        , ForLazyInitialization(forLazyInitialization) {
        AFL_VERIFY(Loader);
    }
};

}   // namespace NKikimr::NArrow::NAccessor
