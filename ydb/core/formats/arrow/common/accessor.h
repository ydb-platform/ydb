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
        YDB_READONLY(ui64, FinishPosition, 0);
        YDB_READONLY(ui64, ChunkIndex, 0);
    public:
        TString DebugString(const ui64 position) const;

        ui64 GetLength() const {
            return Array->length();
        }

        bool Contains(const ui64 position) const {
            return position >= StartPosition && position < FinishPosition;
        }

        std::shared_ptr<arrow::Array> CopyRecord(const ui64 recordIndex) const;

        std::partial_ordering Compare(const ui64 position, const TCurrentChunkAddress& item, const ui64 itemPosition) const;

        TCurrentChunkAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 pos, const ui32 chunkIdx)
            : Array(arr)
            , StartPosition(pos)
            , ChunkIndex(chunkIdx)
        {
            AFL_VERIFY(arr);
            AFL_VERIFY(arr->length());
            FinishPosition = StartPosition + arr->length();
        }

        TString DebugString() const {
            return TStringBuilder()
                << "start=" << StartPosition << ";"
                << "chunk_index=" << ChunkIndex << ";"
                << "length=" << Array->length() << ";";
        }
    };

    class TAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_READONLY(ui64, Position, 0);
        YDB_READONLY(ui64, ChunkIdx, 0);
    public:
        bool NextPosition() {
            if (Position + 1 < (ui32)Array->length()) {
                ++Position;
                return true;
            }
            return false;
        }

        TAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 position, const ui64 chunkIdx)
            : Array(arr)
            , Position(position)
            , ChunkIdx(chunkIdx)
        {

        }

        const std::partial_ordering Compare(const TAddress& item) const;
    };
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, DataType);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(EType, Type, EType::Undefined);
    virtual std::optional<ui64> DoGetRawSize() const = 0;
protected:
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const = 0;
    virtual TCurrentChunkAddress DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const = 0;

    template <class TChunkAccessor>
    TCurrentChunkAddress SelectChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position, const TChunkAccessor& accessor) const {
        if (!chunkCurrent || position >= chunkCurrent->GetStartPosition() + chunkCurrent->GetLength()) {
            ui32 startIndex = 0;
            ui64 idx = 0;
            if (chunkCurrent) {
                AFL_VERIFY(chunkCurrent->GetChunkIndex() + 1 < accessor.GetChunksCount());
                startIndex = chunkCurrent->GetChunkIndex() + 1;
                idx = chunkCurrent->GetStartPosition() + chunkCurrent->GetLength();
            }
            for (ui32 i = startIndex; i < accessor.GetChunksCount(); ++i) {
                const ui64 nextIdx = idx + accessor.GetChunkLength(i);
                if (idx <= position && position < nextIdx) {
                    return TCurrentChunkAddress(accessor.GetArray(i), idx, i);
                }
                idx = nextIdx;
            }
        } else if (position < chunkCurrent->GetStartPosition()) {
            AFL_VERIFY(chunkCurrent->GetChunkIndex() > 0);
            ui64 idx = chunkCurrent->GetStartPosition();
            for (i32 i = chunkCurrent->GetChunkIndex() - 1; i >= 0; --i) {
                AFL_VERIFY(idx >= accessor.GetChunkLength(i))("idx", idx)("length", accessor.GetChunkLength(i));
                const ui64 nextIdx = idx - accessor.GetChunkLength(i);
                if (nextIdx <= position && position < idx) {
                    return TCurrentChunkAddress(accessor.GetArray(i), nextIdx, i);
                }
                idx = nextIdx;
            }
        }
        TStringBuilder sb;
        ui64 recordsCountChunks = 0;
        for (ui32 i = 0; i < accessor.GetChunksCount(); ++i) {
            sb << accessor.GetChunkLength(i) << ",";
            recordsCountChunks += accessor.GetChunkLength(i);
        }
        TStringBuilder chunkCurrentInfo;
        if (chunkCurrent) {
            chunkCurrentInfo << chunkCurrent->DebugString();
        }
        AFL_VERIFY(recordsCountChunks == GetRecordsCount())("pos", position)("count", GetRecordsCount())("chunks_map", sb)("chunk_current", chunkCurrentInfo);
        AFL_VERIFY(false)("pos", position)("count", GetRecordsCount())("chunks_map", sb)("chunk_current", chunkCurrentInfo);
        return TCurrentChunkAddress(nullptr, 0, 0);
    }

public:

    class TReader {
    private:
        std::shared_ptr<IChunkedArray> ChunkedArray;
        mutable std::optional<TCurrentChunkAddress> CurrentChunkAddress;
    public:
        TReader(const std::shared_ptr<IChunkedArray>& data)
            : ChunkedArray(data)
        {
            AFL_VERIFY(ChunkedArray);
        }

        ui64 GetRecordsCount() const {
            return ChunkedArray->GetRecordsCount();
        }

        TAddress GetReadChunk(const ui64 position) const;
        static std::partial_ordering CompareColumns(const std::vector<TReader>& l, const ui64 lPosition, const std::vector<TReader>& r, const ui64 rPosition);
        void AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) const;
        std::shared_ptr<arrow::Array> CopyRecord(const ui64 recordIndex) const;
        TString DebugString(const ui32 position) const;
    };

    std::optional<ui64> GetRawSize() const {
        return DoGetRawSize();
    }

    std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const {
        return DoGetChunkedArray();
    }
    virtual ~IChunkedArray() = default;

    std::shared_ptr<arrow::ChunkedArray> Slice(const ui32 offset, const ui32 count) const;

    TCurrentChunkAddress GetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const {
        return DoGetChunk(chunkCurrent, position);
    }

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
    virtual std::optional<ui64> DoGetRawSize() const override;

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
    virtual std::optional<ui64> DoGetRawSize() const override;

public:
    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {

    }
};

}
