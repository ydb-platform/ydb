#pragma once
#include <ydb/library/accessor/accessor.h>

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
    public:
        TCurrentChunkAddress(const std::shared_ptr<arrow::Array>& arr, const ui64 pos)
            : Array(arr)
            , StartPosition(pos) {

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
    virtual TCurrentChunkAddress DoGetChunk(const ui64 position) const = 0;
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
    virtual TCurrentChunkAddress DoGetChunk(const ui64 /*position*/) const override {
        return TCurrentChunkAddress(Array, 0);
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
    virtual TCurrentChunkAddress DoGetChunk(const ui64 position) const override;
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        return Array;
    }

public:
    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {

    }
};

}
