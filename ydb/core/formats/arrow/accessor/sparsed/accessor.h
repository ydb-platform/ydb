#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <ydb/library/accessor/accessor.h>

#include <ydb/core/formats/arrow/validation/validation.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow::NAccessor {

class TSparsedArrayChunk {
private:
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui32, StartPosition, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Records);
    std::shared_ptr<arrow::Scalar> DefaultValue;

    std::shared_ptr<arrow::Array> ColIndex;
    const ui32* RawValues = nullptr;
    ui32 NotDefaultRecordsCount = 0;
    YDB_READONLY_DEF(std::shared_ptr<arrow::UInt32Array>, UI32ColIndex);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, ColValue);

    class TInternalChunkInfo {
    private:
        YDB_READONLY(ui32, Start, 0);
        YDB_READONLY(ui32, Size, 0);
        YDB_READONLY(bool, IsDefault, false);

    public:
        TInternalChunkInfo(const ui32 start, const ui32 size, const bool defaultFlag)
            : Start(start)
            , Size(size)
            , IsDefault(defaultFlag) {
            AFL_VERIFY(Size);
        }
    };

    std::map<ui32, TInternalChunkInfo> RemapExternalToInternal;

public:
    ui32 GetFinishPosition() const {
        return StartPosition + RecordsCount;
    }

    ui32 GetNotDefaultRecordsCount() const {
        return NotDefaultRecordsCount;
    }

    ui32 GetIndexUnsafeFast(const ui32 i) const {
        return RawValues[i];
    }

    ui32 GetFirstIndexNotDefault() const;

    std::shared_ptr<arrow::Scalar> GetMaxScalar() const;

    std::shared_ptr<arrow::Scalar> GetScalar(const ui32 index) const;

    IChunkedArray::TLocalDataAddress GetChunk(
        const std::optional<IChunkedArray::TCommonChunkAddress>& chunkCurrent, const ui64 position, const ui32 chunkIdx) const;

    std::vector<std::shared_ptr<arrow::Array>> GetChunkedArray() const;

    TSparsedArrayChunk(const ui32 posStart, const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& records,
        const std::shared_ptr<arrow::Scalar>& defaultValue);

    ui64 GetRawSize() const;
};

class TSparsedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    std::shared_ptr<arrow::Scalar> DefaultValue;
    std::vector<TSparsedArrayChunk> Records;

protected:
    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        AFL_VERIFY(false);
        return TLocalChunkedArrayAddress(nullptr, 0, 0);
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

    virtual TLocalDataAddress DoGetLocalData(
        const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
        ui32 currentIdx = 0;
        for (ui32 i = 0; i < Records.size(); ++i) {
            if (currentIdx <= position && position < currentIdx + Records[i].GetRecordsCount()) {
                return Records[i].GetChunk(chunkCurrent, position - currentIdx, i);
            }
            currentIdx += Records[i].GetRecordsCount();
        }
        AFL_VERIFY(false);
        return TLocalDataAddress(nullptr, 0, 0);
    }
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        std::vector<std::shared_ptr<arrow::Array>> chunks;
        for (auto&& i : Records) {
            auto chunksLocal = i.GetChunkedArray();
            chunks.insert(chunks.end(), chunksLocal.begin(), chunksLocal.end());
        }
        return std::make_shared<arrow::ChunkedArray>(chunks, GetDataType());
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        ui64 bytes = 0;
        for (auto&& i : Records) {
            bytes += i.GetRawSize();
        }
        return bytes;
    }

    TSparsedArray(std::vector<TSparsedArrayChunk>&& data, const std::shared_ptr<arrow::Scalar>& /*defaultValue*/,
        const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
        : TBase(recordsCount, EType::SparsedArray, type)
        , Records(std::move(data)) {
    }

    static ui32 GetLastIndex(const std::shared_ptr<arrow::RecordBatch>& batch);

    static std::shared_ptr<arrow::RecordBatch> MakeRecords(std::unique_ptr<arrow::ArrayBuilder>& builderIndex, std::unique_ptr<arrow::ArrayBuilder>& builderValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
        std::shared_ptr<arrow::RecordBatch> records;
        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
            std::make_shared<arrow::Field>("value", type) };
        auto schema = std::make_shared<arrow::Schema>(fields);
        std::vector<std::shared_ptr<arrow::Array>> columns = { NArrow::TStatusValidator::GetValid(builderIndex->Finish()),
            NArrow::TStatusValidator::GetValid(builderValue->Finish()) };
        return arrow::RecordBatch::Make(schema, recordsCount, columns);
    }

    static TSparsedArrayChunk MakeDefaultChunk(const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
        auto builderIndex = NArrow::MakeBuilder(arrow::uint32());
        auto builderValue = NArrow::MakeBuilder(type);
        std::shared_ptr<arrow::RecordBatch> records = MakeRecords(builderIndex, builderValue, type, recordsCount);
        AFL_VERIFY_DEBUG(records->ValidateFull().ok());
        return TSparsedArrayChunk(0, recordsCount, records, defaultValue);
    }

public:
    TSparsedArray(const IChunkedArray& defaultArray, const std::shared_ptr<arrow::Scalar>& defaultValue);
    TSparsedArray(const std::shared_ptr<arrow::Scalar>& defaultValue,
        const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
        : TSparsedArray({MakeDefaultChunk(defaultValue, type, recordsCount)}, defaultValue, type, recordsCount)
    {
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto chunk = GetSparsedChunk(index);
        return chunk.GetScalar(index - chunk.GetStartPosition());
    }

    TSparsedArrayChunk GetSparsedChunk(const ui64 position) const {
        ui32 currentIdx = 0;
        for (ui32 i = 0; i < Records.size(); ++i) {
            if (currentIdx <= position && position < currentIdx + Records[i].GetRecordsCount()) {
                return Records[i];
            }
            currentIdx += Records[i].GetRecordsCount();
        }
        AFL_VERIFY(false);
        return Records.back();
    }

    class TBuilder {
    private:
        ui32 RecordsCount = 0;
        std::vector<TSparsedArrayChunk> Chunks;
        std::shared_ptr<arrow::Scalar> DefaultValue;
        std::shared_ptr<arrow::DataType> Type;

    public:
        TBuilder(const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type)
            : DefaultValue(defaultValue)
            , Type(type) {
        }

        void AddChunk(const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& data);

        std::shared_ptr<TSparsedArray> Finish() {
            return std::shared_ptr<TSparsedArray>(new TSparsedArray(std::move(Chunks), DefaultValue, Type, RecordsCount));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
