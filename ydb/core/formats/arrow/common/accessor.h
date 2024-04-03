#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NAccessor {

class IChunkedArray {
public:
    enum class EType {
        Undefined,
        Array,
        ChunkedArray,
        SerializedChunkedArray
    };

    class TCurrentChunkAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_READONLY(ui64, StartPosition, 0);
        YDB_READONLY(ui64, ChunkIndex, 0);
    public:
        ui64 GetLength() const {
            return Array->length();
        }

        TCurrentChunkAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 pos, const ui32 chunkIdx)
            : Array(arr)
            , StartPosition(pos)
            , ChunkIndex(chunkIdx)
        {

        }
    };

    class TAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_READONLY(ui64, Position, 0);
    public:
        TAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 position)
            : Array(arr)
            , Position(position) {

        }

        const std::partial_ordering Compare(const TAddress& item) const;
    };
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, DataType);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(EType, Type, EType::Undefined);
    mutable std::optional<TCurrentChunkAddress> CurrentChunkAddress;
protected:
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const = 0;
    virtual TCurrentChunkAddress DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const = 0;
public:
    virtual ~IChunkedArray() = default;

    std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const {
        return DoGetChunkedArray();
    }
    static std::partial_ordering CompareColumns(const std::vector<std::shared_ptr<IChunkedArray>>& l, const ui64 lPosition, const std::vector<std::shared_ptr<IChunkedArray>>& r, const ui64 rPosition);

    TAddress GetAddress(const ui64 position) const;

    void AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize);
    std::shared_ptr<arrow::Array> CopyRecord(const ui64 recordIndex) const;
    std::shared_ptr<arrow::ChunkedArray> Slice(const ui32 offset, const ui32 count) const;
    TString DebugString(const ui32 position) const;
    IChunkedArray(const ui64 recordsCount, const EType type, const std::shared_ptr<arrow::DataType>& dataType)
        : DataType(dataType)
        , RecordsCount(recordsCount)
        , Type(type) {

    }
};

class TTrivialArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::Array> Array;
protected:
    virtual TCurrentChunkAddress DoGetChunk(const std::optional<TCurrentChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        return TCurrentChunkAddress(Array, 0, 0);
    }
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        return std::make_shared<arrow::ChunkedArray>(Array);
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

public:
    template <class TChunkAccessor>
    static TCurrentChunkAddress SelectChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position, const TChunkAccessor& accessor) {
        if (!chunkCurrent || position >= chunkCurrent->GetStartPosition() + chunkCurrent->GetLength()) {
            ui32 startIndex = 0;
            if (chunkCurrent) {
                AFL_VERIFY(chunkCurrent->GetChunkIndex() + 1 < accessor.GetChunksCount());
                startIndex = chunkCurrent->GetChunkIndex() + 1;
            }
            ui64 idx = 0;
            for (ui32 i = startIndex; i < accessor.GetChunksCount(); ++i) {
                const ui64 nextIdx = idx + accessor.GetChunkLength(i);
                if (idx <= position && position < nextIdx) {
                    return TCurrentChunkAddress(accessor.GetArray(i), idx, i);
                }
                idx = nextIdx;
            }
            AFL_VERIFY(false);
            return TCurrentChunkAddress(nullptr, 0, 0);
        } else if (position < chunkCurrent->GetStartPosition()) {
            AFL_VERIFY(chunkCurrent->GetChunkIndex() > 0);
            ui64 idx = chunkCurrent->GetStartPosition();
            for (i32 i = chunkCurrent->GetChunkIndex() - 1; i >= 0; ++i) {
                AFL_VERIFY(idx >= accessor.GetChunkLength(i))("idx", idx)("length", accessor.GetChunkLength(i));
                const ui64 nextIdx = idx - accessor.GetChunkLength(i);
                if (nextIdx <= position && position < idx) {
                    return TCurrentChunkAddress(accessor.GetArray(i), nextIdx, i);
                }
                idx = nextIdx;
            }
        }
        AFL_VERIFY(false);
        return TCurrentChunkAddress(nullptr, 0, 0);
    }


    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {

    }
};

}
