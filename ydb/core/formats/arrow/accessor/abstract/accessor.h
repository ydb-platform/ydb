#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/validator.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow::NAccessor {

class TColumnSaver;
class IChunkedArray;

class TChunkedArraySerialized {
private:
    YDB_READONLY_DEF(std::shared_ptr<IChunkedArray>, Array);
    YDB_READONLY_DEF(TString, SerializedData);

public:
    TChunkedArraySerialized(const std::shared_ptr<IChunkedArray>& array, const TString& serializedData);
};

class IChunkedArray {
public:
    enum class EType {
        Undefined,
        Array,
        ChunkedArray,
        SerializedChunkedArray,
        SparsedArray
    };

    class TCommonChunkAddress {
    private:
        YDB_READONLY(ui64, StartPosition, 0);
        YDB_READONLY(ui64, FinishPosition, 0);
        YDB_READONLY(ui64, ChunkIndex, 0);

    public:
        TString DebugString() const {
            return TStringBuilder() << "start=" << StartPosition << ";"
                                    << "chunk_index=" << ChunkIndex << ";"
                                    << "finish=" << FinishPosition << ";"
                                    << "size=" << FinishPosition - StartPosition << ";";
        }

        ui64 GetLength() const {
            return FinishPosition - StartPosition;
        }

        bool Contains(const ui64 position) const {
            return position >= StartPosition && position < FinishPosition;
        }

        TCommonChunkAddress(const ui64 start, const ui64 finish, const ui64 index)
            : StartPosition(start)
            , FinishPosition(finish)
            , ChunkIndex(index) {
            AFL_VERIFY(FinishPosition > StartPosition);
        }
    };

    class TAddressChain {
    private:
        YDB_READONLY_DEF(std::deque<TCommonChunkAddress>, Addresses);
        YDB_READONLY(ui32, GlobalStartPosition, 0);
        YDB_READONLY(ui32, GlobalFinishPosition, 0);

    public:
        TAddressChain() = default;

        ui32 GetSize() const {
            return Addresses.size();
        }

        ui32 GetLocalIndex(const ui32 position) const {
            AFL_VERIFY(Contains(position));
            return position - GlobalStartPosition;
        }

        bool Contains(const ui32 position) const {
            return GlobalStartPosition <= position && position < GlobalFinishPosition;
        }

        const TCommonChunkAddress& GetAddress(const ui32 index) const {
            AFL_VERIFY(index < Addresses.size());
            return Addresses[index];
        }

        void Add(const TCommonChunkAddress& address) {
            if (Addresses.size()) {
                AFL_VERIFY(address.GetFinishPosition() <= Addresses.back().GetLength());
            }
            Addresses.emplace_back(address);
            GlobalStartPosition += address.GetStartPosition();
            GlobalFinishPosition = GlobalStartPosition + address.GetLength();
        }

        const TCommonChunkAddress& GetLastAddress() const {
            AFL_VERIFY(Addresses.size());
            return Addresses.back();
        }

        TString DebugString() const {
            return TStringBuilder() << "start=" << GlobalStartPosition << ";finish=" << GlobalFinishPosition
                                    << ";addresses_count=" << Addresses.size() << ";";
        }
    };

    class TFullChunkedArrayAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IChunkedArray>, Array);
        YDB_ACCESSOR_DEF(TAddressChain, Address);

    public:
        TFullChunkedArrayAddress(const std::shared_ptr<IChunkedArray>& arr, TAddressChain&& address)
            : Array(arr)
            , Address(std::move(address))
        {
            AFL_VERIFY(Address.GetSize());
            AFL_VERIFY(Array);
            AFL_VERIFY(Array->GetRecordsCount());
        }
    };

    class TLocalChunkedArrayAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IChunkedArray>, Array);
        TCommonChunkAddress Address;

    public:
        const TCommonChunkAddress& GetAddress() const {
            return Address;
        }

        TLocalChunkedArrayAddress(const std::shared_ptr<IChunkedArray>& arr, const TCommonChunkAddress& address)
            : Array(arr)
            , Address(address) {
            AFL_VERIFY(arr);
            AFL_VERIFY(address.GetLength() == (ui32)arr->GetRecordsCount());
        }

        TLocalChunkedArrayAddress(const std::shared_ptr<IChunkedArray>& arr, const ui32 start, const ui32 chunkIdx)
            : Array(arr)
            , Address(TCommonChunkAddress(start, start + TValidator::CheckNotNull(arr)->GetRecordsCount(), chunkIdx)) {
        }
    };

    class TFullDataAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_ACCESSOR_DEF(TAddressChain, Address);

    public:
        TString DebugString(const ui64 position) const;

        std::shared_ptr<arrow::Array> CopyRecord(const ui64 recordIndex) const;

        std::partial_ordering Compare(const ui64 position, const TFullDataAddress& item, const ui64 itemPosition) const;

        TFullDataAddress(const std::shared_ptr<arrow::Array>& arr, TAddressChain&& address)
            : Array(arr)
            , Address(std::move(address))
        {
            AFL_VERIFY(Array);
            AFL_VERIFY(Address.GetSize());
        }
    };

    class TLocalDataAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        TCommonChunkAddress Address;

    public:
        const TCommonChunkAddress& GetAddress() const {
            return Address;
        }

        TLocalDataAddress(const std::shared_ptr<arrow::Array>& arr, const ui32 start, const ui32 chunkIdx)
            : Array(arr)
            , Address(start, start + TValidator::CheckNotNull(arr)->length(), chunkIdx)
        {
        }

        TLocalDataAddress(const std::shared_ptr<arrow::Array>& arr, const TCommonChunkAddress& address)
            : Array(arr)
            , Address(address) {
            AFL_VERIFY(address.GetLength() == (ui32)arr->length());
        }
    };

    class TAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_READONLY(ui64, Position, 0);

    public:
        bool NextPosition() {
            if (Position + 1 < (ui32)Array->length()) {
                ++Position;
                return true;
            }
            return false;
        }

        TAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 position)
            : Array(arr)
            , Position(position)
        {
            AFL_VERIFY(!!Array);
            AFL_VERIFY(position < (ui32)Array->length());
        }

        const std::partial_ordering Compare(const TAddress& item) const;
    };

private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, DataType);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(EType, Type, EType::Undefined);
    virtual std::optional<ui64> DoGetRawSize() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const = 0;

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const = 0;
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const = 0;

protected:
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const = 0;
    TLocalChunkedArrayAddress GetLocalChunkedArray(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
        return DoGetLocalChunkedArray(chunkCurrent, position);
    }
    TLocalDataAddress GetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
        return DoGetLocalData(chunkCurrent, position);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const = 0;
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) = 0;

    template <class TCurrentPosition, class TChunkAccessor>
    void SelectChunk(const std::optional<TCurrentPosition>& chunkCurrent, const ui64 position, const TChunkAccessor& accessor) const {
        if (!chunkCurrent || chunkCurrent->GetStartPosition() <= position) {
            ui32 startIndex = 0;
            ui64 idx = 0;
            if (chunkCurrent) {
                if (position < chunkCurrent->GetFinishPosition()) {
                    return accessor.OnArray(
                        chunkCurrent->GetChunkIndex(), chunkCurrent->GetStartPosition());
                }
                AFL_VERIFY(chunkCurrent->GetChunkIndex() < accessor.GetChunksCount());
                startIndex = chunkCurrent->GetChunkIndex();
                idx = chunkCurrent->GetStartPosition();
            }
            for (ui32 i = startIndex; i < accessor.GetChunksCount(); ++i) {
                const ui64 nextIdx = idx + accessor.GetChunkLength(i);
                if (idx <= position && position < nextIdx) {
                    return accessor.OnArray(i, idx);
                }
                idx = nextIdx;
            }
        } else {
            AFL_VERIFY(chunkCurrent->GetChunkIndex() > 0);
            ui64 idx = chunkCurrent->GetStartPosition();
            for (i32 i = chunkCurrent->GetChunkIndex() - 1; i >= 0; --i) {
                AFL_VERIFY(idx >= accessor.GetChunkLength(i))("idx", idx)("length", accessor.GetChunkLength(i));
                const ui64 nextIdx = idx - accessor.GetChunkLength(i);
                if (nextIdx <= position && position < idx) {
                    return accessor.OnArray(i, nextIdx);
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
        AFL_VERIFY(recordsCountChunks == GetRecordsCount())("pos", position)("count", GetRecordsCount())("chunks_map", sb)(
            "chunk_current", chunkCurrentInfo);
        AFL_VERIFY(false)("pos", position)("count", GetRecordsCount())("chunks_map", sb)("chunk_current", chunkCurrentInfo);
    }

public:
    class TReader {
    private:
        std::shared_ptr<IChunkedArray> ChunkedArray;
        mutable std::optional<TFullDataAddress> CurrentChunkAddress;

    public:
        TReader(const std::shared_ptr<IChunkedArray>& data)
            : ChunkedArray(data) {
            AFL_VERIFY(ChunkedArray);
        }

        ui64 GetRecordsCount() const {
            return ChunkedArray->GetRecordsCount();
        }

        TAddress GetReadChunk(const ui64 position) const;
        static std::partial_ordering CompareColumns(
            const std::vector<TReader>& l, const ui64 lPosition, const std::vector<TReader>& r, const ui64 rPosition);
        void AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) const;
        std::shared_ptr<arrow::Array> CopyRecord(const ui64 recordIndex) const;
        TString DebugString(const ui32 position) const;
    };

    std::shared_ptr<arrow::Scalar> GetScalar(const ui32 index) const {
        AFL_VERIFY(index < GetRecordsCount());
        return DoGetScalar(index);
    }

    std::vector<TChunkedArraySerialized> SplitBySizes(
        const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
        return DoSplitBySizes(saver, fullSerializedData, splitSizes);
    }

    std::shared_ptr<arrow::Scalar> GetMaxScalar() const {
        AFL_VERIFY(GetRecordsCount());
        return DoGetMaxScalar();
    }

    std::optional<ui64> GetRawSize() const {
        return DoGetRawSize();
    }

    ui64 GetRawSizeVerified() const {
        auto result = GetRawSize();
        AFL_VERIFY(result);
        return *result;
    }

    std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const {
        return DoGetChunkedArray();
    }
    virtual ~IChunkedArray() = default;

    std::shared_ptr<arrow::ChunkedArray> Slice(const ui32 offset, const ui32 count) const;

    bool IsDataOwner() const {
        switch (Type) {
            case EType::SparsedArray:
            case EType::ChunkedArray:
            case EType::Array:
                return true;
            case EType::Undefined:
                AFL_VERIFY(false);
            case EType::SerializedChunkedArray:
                return false;
        };
    }

    TFullChunkedArrayAddress GetArray(
        const std::optional<TAddressChain>& chunkCurrent, const ui64 position, const std::shared_ptr<IChunkedArray>& selfPtr) const;

    TFullDataAddress GetChunk(const std::optional<TFullDataAddress>& chunkCurrent, const ui64 position) const {
        if (chunkCurrent) {
            return GetChunk(chunkCurrent->GetAddress(), position);
        } else {
            return GetChunk(std::optional<TAddressChain>(), position);
        }
    }

    TFullDataAddress GetChunkSlow(const ui64 position) const {
        return GetChunk(std::optional<TAddressChain>(), position);
    }

    TFullChunkedArrayAddress GetArray(
        const std::optional<TFullChunkedArrayAddress>& chunkCurrent, const ui64 position, const std::shared_ptr<IChunkedArray>& selfPtr) const {
        if (chunkCurrent) {
            return GetArray(chunkCurrent->GetAddress(), position, selfPtr);
        } else {
            return GetArray(std::optional<TAddressChain>(), position, selfPtr);
        }
    }

    TFullChunkedArrayAddress GetArraySlow(const ui64 position, const std::shared_ptr<IChunkedArray>& selfPtr) const {
        return GetArray(std::optional<TAddressChain>(), position, selfPtr);
    }

    TFullDataAddress GetChunk(const std::optional<TAddressChain>& chunkCurrent, const ui64 position) const;

    IChunkedArray(const ui64 recordsCount, const EType type, const std::shared_ptr<arrow::DataType>& dataType)
        : DataType(dataType)
        , RecordsCount(recordsCount)
        , Type(type) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
