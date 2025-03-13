#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/validator.h>
#include <ydb/library/formats/arrow/splitter/similar_packer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/json/writer/json_value.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow::NSerialization {
class ISerializer;
}

namespace NKikimr::NArrow {
class TColumnFilter;

}

namespace NKikimr::NArrow::NAccessor {

class TColumnLoader;
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
    enum class EType : ui8 {
        Undefined = 0,
        Array,
        ChunkedArray,
        SerializedChunkedArray,
        CompositeChunkedArray,
        SparsedArray,
        SubColumnsArray,
        SubColumnsPartialArray
    };

    using TValuesSimpleVisitor = std::function<void(std::shared_ptr<arrow::Array>)>;

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
            AFL_VERIFY(Contains(position))("pos", position)("start", GlobalStartPosition);
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
            TStringBuilder sb;
            sb << "start=" << GlobalStartPosition << ";finish=" << GlobalFinishPosition << ";addresses_count=" << Addresses.size() << ";";
            for (auto&& i : Addresses) {
                sb << "addresses=" << i.DebugString() << ";";
            }
            return sb;
        }
    };

    class TFullChunkedArrayAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IChunkedArray>, Array);
        YDB_ACCESSOR_DEF(TAddressChain, Address);

    public:
        TFullChunkedArrayAddress(const std::shared_ptr<IChunkedArray>& arr, TAddressChain&& address)
            : Array(arr)
            , Address(std::move(address)) {
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
            , Address(std::move(address)) {
            AFL_VERIFY(Array);
            AFL_VERIFY(Address.GetSize());
        }
    };

    class TLocalDataAddress {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        TCommonChunkAddress Address;

    public:
        void Reallocate();

        const TCommonChunkAddress& GetAddress() const {
            return Address;
        }

        TLocalDataAddress(const std::shared_ptr<arrow::Array>& arr, const ui32 start, const ui32 chunkIdx)
            : Array(arr)
            , Address(start, start + TValidator::CheckNotNull(arr)->length(), chunkIdx) {
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
            , Position(position) {
            AFL_VERIFY(!!Array);
            AFL_VERIFY(position < (ui32)Array->length());
        }

        const std::partial_ordering Compare(const TAddress& item) const;
    };

private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, DataType);
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY(EType, Type, EType::Undefined);
    virtual NJson::TJsonValue DoDebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("data", GetChunkedArray()->ToString());
        return result;
    }
    virtual std::optional<ui64> DoGetRawSize() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const = 0;
    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& /*value*/) const {
        return std::nullopt;
    }

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
        AFL_VERIFY(false);
        return TLocalChunkedArrayAddress(nullptr, 0, 0);
    }
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const = 0;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const = 0;
    virtual ui32 DoGetNullsCount() const = 0;
    virtual ui32 DoGetValueRawBytes() const = 0;
    virtual std::shared_ptr<IChunkedArray> DoApplyFilter(const TColumnFilter& filter) const;
    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const = 0;

protected:
    std::shared_ptr<arrow::Schema> GetArraySchema() const {
        const arrow::FieldVector fields = { std::make_shared<arrow::Field>("val", GetDataType()) };
        return std::make_shared<arrow::Schema>(fields);
    }

    TLocalChunkedArrayAddress GetLocalChunkedArray(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
        return DoGetLocalChunkedArray(chunkCurrent, position);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const = 0;

    template <class TCurrentPosition, class TChunkAccessor>
    void SelectChunk(const std::optional<TCurrentPosition>& chunkCurrent, const ui64 position, const TChunkAccessor& accessor) const {
        if (!chunkCurrent || chunkCurrent->GetStartPosition() <= position) {
            ui32 startIndex = 0;
            ui64 idx = 0;
            if (chunkCurrent) {
                if (position < chunkCurrent->GetFinishPosition()) {
                    return accessor.OnArray(chunkCurrent->GetChunkIndex(), chunkCurrent->GetStartPosition());
                } else if (position == chunkCurrent->GetFinishPosition() && chunkCurrent->GetChunkIndex() + 1 < accessor.GetChunksCount()) {
                    return accessor.OnArray(chunkCurrent->GetChunkIndex() + 1, position);
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
            if (position + 1 == chunkCurrent->GetStartPosition()) {
                const ui32 chunkIndex = chunkCurrent->GetChunkIndex() - 1;
                return accessor.OnArray(chunkIndex, chunkCurrent->GetStartPosition() - accessor.GetChunkLength(chunkIndex));
            }

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
            "chunk_current", chunkCurrentInfo)("chunks_count", recordsCountChunks);
        AFL_VERIFY(false)("pos", position)("count", GetRecordsCount())("chunks_map", sb)("chunk_current", chunkCurrentInfo);
    }

public:
    std::shared_ptr<IChunkedArray> ApplyFilter(const TColumnFilter& filter, const std::shared_ptr<IChunkedArray>& selfPtr) const;

    virtual bool HasWholeDataVolume() const {
        return true;
    }

    std::optional<bool> CheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const {
        return DoCheckOneValueAccessor(value);
    }

    virtual bool HasSubColumnData(const TString& /*subColumnName*/) const {
        return true;
    }

    virtual void Reallocate() {

    }

    void VisitValues(const TValuesSimpleVisitor& visitor) const {
        DoVisitValues(visitor);
    }

    template <class TResult, class TActor>
    static std::optional<TResult> VisitDataOwners(const std::shared_ptr<IChunkedArray>& arr, const TActor& actor) {
        AFL_VERIFY(arr);
        std::optional<IChunkedArray::TFullChunkedArrayAddress> arrCurrent;
        for (ui32 currentIndex = 0; currentIndex < arr->GetRecordsCount();) {
            arrCurrent = arr->GetArray(arrCurrent, currentIndex, arr);
            auto result = actor(arrCurrent->GetArray());
            if (!!result) {
                return result;
            }
            currentIndex = currentIndex + arrCurrent->GetArray()->GetRecordsCount();
        }
        return std::nullopt;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", ::ToString(Type));
        result.InsertValue("records_count", RecordsCount);
        result.InsertValue("internal", DoDebugJson());
        return result;
    }

    ui32 GetNullsCount() const {
        return DoGetNullsCount();
    }

    ui32 GetValueRawBytes() const {
        return DoGetValueRawBytes();
    }

    TLocalDataAddress GetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
        AFL_VERIFY(position < GetRecordsCount())("position", position)("records_count", GetRecordsCount());
        return DoGetLocalData(chunkCurrent, position);
    }

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

    template <class TSerializer>
    std::vector<TChunkedArraySerialized> SplitBySizes(
        const TSerializer& serialize, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
        const std::vector<ui32> recordsCount = NSplitter::TSimilarPacker::SizesToRecordsCount(GetRecordsCount(), fullSerializedData, splitSizes);
        std::vector<TChunkedArraySerialized> result;
        ui32 currentStartIndex = 0;
        for (auto&& i : recordsCount) {
            std::shared_ptr<IChunkedArray> slice = ISlice(currentStartIndex, i);
            result.emplace_back(slice, serialize(slice));
            currentStartIndex += i;
        }
        return result;
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

    virtual std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const;
    virtual ~IChunkedArray() = default;

    std::shared_ptr<arrow::ChunkedArray> Slice(const ui32 offset, const ui32 count) const;
    std::shared_ptr<IChunkedArray> ISlice(const ui32 offset, const ui32 count) const {
        AFL_VERIFY(offset + count <= GetRecordsCount())("offset", offset)("count", count)("records", GetRecordsCount());
        auto result = DoISlice(offset, count);
        AFL_VERIFY(result);
        AFL_VERIFY(result->GetRecordsCount() == count)("records", result->GetRecordsCount())("count", count);
        return result;
    }

    bool IsDataOwner() const {
        switch (Type) {
            case EType::SparsedArray:
            case EType::ChunkedArray:
            case EType::SubColumnsArray:
            case EType::SubColumnsPartialArray:
            case EType::Array:
                return true;
            case EType::Undefined:
                AFL_VERIFY(false);
            case EType::SerializedChunkedArray:
            case EType::CompositeChunkedArray:
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
            return GetArraySlow(position, selfPtr);
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
