#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/size_calcer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow::NAccessor {

class TSparsedArrayChunk: public TMoveOnly {
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
        YDB_READONLY(ui32, StartExt, 0);
        YDB_READONLY(ui32, StartInt, 0);
        YDB_READONLY(ui32, Size, 0);
        YDB_READONLY(bool, IsDefault, false);

    public:
        TInternalChunkInfo(const ui32 startExt, const ui32 startInt, const ui32 size, const bool defaultFlag)
            : StartExt(startExt)
            , StartInt(startInt)
            , Size(size)
            , IsDefault(defaultFlag) {
            AFL_VERIFY(Size);
        }

        bool operator<(const TInternalChunkInfo& item) const {
            return StartExt < item.StartExt;
        }
    };

    std::vector<TInternalChunkInfo> RemapExternalToInternal;

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

    TSparsedArrayChunk(const ui32 posStart, const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& records,
        const std::shared_ptr<arrow::Scalar>& defaultValue);

    ui64 GetRawSize() const;

    ui32 GetNullsCount() const {
        if (!!DefaultValue) {
            return ColValue->null_count();
        } else {
            AFL_VERIFY(GetNotDefaultRecordsCount() <= GetRecordsCount());
            return GetRecordsCount() - GetNotDefaultRecordsCount();
        }
    }
    ui32 GetValueRawBytes() const {
        return NArrow::GetArrayDataSize(ColValue);
    }

    TSparsedArrayChunk Slice(const ui32 newStart, const ui32 offset, const ui32 count) const {
        AFL_VERIFY(offset + count <= RecordsCount)("offset", offset)("count", count)("records", RecordsCount);
        std::optional<ui32> startPosition = NArrow::FindUpperOrEqualPosition(*UI32ColIndex, offset);
        std::optional<ui32> finishPosition = NArrow::FindUpperOrEqualPosition(*UI32ColIndex, offset + count);
        if (!startPosition || startPosition == finishPosition) {
            return TSparsedArrayChunk(newStart, count, NArrow::MakeEmptyBatch(Records->schema(), 0), DefaultValue);
        } else {
            AFL_VERIFY(startPosition);
            auto builder = NArrow::MakeBuilder(arrow::uint32());
            for (ui32 i = *startPosition; i < finishPosition.value_or(Records->num_rows()); ++i) {
                NArrow::Append<arrow::UInt32Type>(*builder, UI32ColIndex->Value(i) - offset);
            }
            auto arrIndexes = NArrow::FinishBuilder(std::move(builder));
            auto arrValue = ColValue->Slice(*startPosition, finishPosition.value_or(Records->num_rows()) - *startPosition);
            auto sliceRecords = arrow::RecordBatch::Make(Records->schema(), arrValue->length(), { arrIndexes, arrValue });
            return TSparsedArrayChunk(newStart, count, sliceRecords, DefaultValue);
        }
    }
};

class TSparsedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    std::vector<TSparsedArrayChunk> Records;

protected:
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

    virtual ui32 DoGetNullsCount() const override {
        ui32 result = 0;
        for (auto&& i : Records) {
            result += i.GetNullsCount();
        }
        return result;
    }
    virtual ui32 DoGetValueRawBytes() const override {
        ui32 result = 0;
        for (auto&& i : Records) {
            result += i.GetValueRawBytes();
        }
        return result;
    }

    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        TBuilder builder(DefaultValue, GetDataType());
        ui32 newStart = 0;
        for (ui32 i = 0; i < Records.size(); ++i) {
            if (Records[i].GetStartPosition() + Records[i].GetRecordsCount() <= offset) {
                continue;
            }
            if (offset + count <= Records[i].GetStartPosition()) {
                continue;
            }
            const ui32 chunkStart = (offset < Records[i].GetStartPosition()) ? 0 : (offset - Records[i].GetStartPosition());
            const ui32 chunkCount = (offset + count <= Records[i].GetFinishPosition())
                                        ? (offset + count - Records[i].GetStartPosition() - chunkStart)
                                        : (Records[i].GetFinishPosition() - chunkStart);
            builder.AddChunk(Records[i].Slice(newStart, chunkStart, chunkCount));
            newStart += chunkCount;
        }
        return builder.Finish();
    }

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
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
    virtual std::optional<ui64> DoGetRawSize() const override {
        ui64 bytes = 0;
        for (auto&& i : Records) {
            bytes += i.GetRawSize();
        }
        return bytes;
    }

    TSparsedArray(std::vector<TSparsedArrayChunk>&& data, const std::shared_ptr<arrow::Scalar>& defaultValue,
        const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
        : TBase(recordsCount, EType::SparsedArray, type)
        , DefaultValue(defaultValue)
        , Records(std::move(data)) {
    }

    static ui32 GetLastIndex(const std::shared_ptr<arrow::RecordBatch>& batch);

    static std::shared_ptr<arrow::Schema> BuildSchema(const std::shared_ptr<arrow::DataType>& type) {
        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
            std::make_shared<arrow::Field>("value", type) };
        return std::make_shared<arrow::Schema>(fields);
    }

    static TSparsedArrayChunk MakeDefaultChunk(
        const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);

public:
    TSparsedArray(const IChunkedArray& defaultArray, const std::shared_ptr<arrow::Scalar>& defaultValue);

    TSparsedArray(const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
        : TBase(recordsCount, EType::SparsedArray, type)
        , DefaultValue(defaultValue) {
        Records.emplace_back(MakeDefaultChunk(defaultValue, type, recordsCount));
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto& chunk = GetSparsedChunk(index);
        return chunk.GetScalar(index - chunk.GetStartPosition());
    }

    std::shared_ptr<arrow::RecordBatch> GetRecordBatchVerified() const {
        AFL_VERIFY(Records.size() == 1)("size", Records.size());
        return Records.front().GetRecords();
    }

    const TSparsedArrayChunk& GetSparsedChunk(const ui64 position) const {
        const auto pred = [](const ui64 position, const TSparsedArrayChunk& item) {
            return position < item.GetStartPosition();
        };
        auto it = std::upper_bound(Records.begin(), Records.end(), position, pred);
        AFL_VERIFY(it != Records.begin());
        --it;
        AFL_VERIFY(position < it->GetStartPosition() + it->GetRecordsCount());
        AFL_VERIFY(it->GetStartPosition() <= position);
        return *it;
    }

    template <class TDataType>
    class TSparsedBuilder {
    private:
        std::unique_ptr<arrow::ArrayBuilder> IndexBuilder;
        std::unique_ptr<arrow::ArrayBuilder> ValueBuilder;
        ui32 RecordsCount = 0;
        const std::shared_ptr<arrow::Scalar> DefaultValue;

    public:
        TSparsedBuilder(const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 reserveItems, const ui32 reserveData)
            : DefaultValue(defaultValue) {
            IndexBuilder = NArrow::MakeBuilder(arrow::uint32(), reserveItems, 0);
            ValueBuilder = NArrow::MakeBuilder(arrow::TypeTraits<TDataType>::type_singleton(), reserveItems, reserveData);
        }

        void AddRecord(const ui32 recordIndex, const std::string_view value) {
            AFL_VERIFY(NArrow::Append<arrow::UInt32Type>(*IndexBuilder, recordIndex));
            AFL_VERIFY(NArrow::Append<TDataType>(*ValueBuilder, arrow::util::string_view(value.data(), value.size())));
            ++RecordsCount;
        }

        std::shared_ptr<IChunkedArray> Finish(const ui32 recordsCount) {
            TSparsedArray::TBuilder builder(DefaultValue, arrow::TypeTraits<TDataType>::type_singleton());
            std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
            builders.emplace_back(std::move(IndexBuilder));
            builders.emplace_back(std::move(ValueBuilder));
            builder.AddChunk(recordsCount, arrow::RecordBatch::Make(TSparsedArray::BuildSchema(arrow::TypeTraits<TDataType>::type_singleton()),
                                               RecordsCount, NArrow::Finish(std::move(builders))));
            return builder.Finish();
        }
    };

    static TSparsedBuilder<arrow::StringType> MakeBuilderUtf8(const ui32 reserveItems = 0, const ui32 reserveData = 0) {
        return TSparsedBuilder<arrow::StringType>(nullptr, reserveItems, reserveData);
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
        void AddChunk(TSparsedArrayChunk&& chunk) {
            RecordsCount += chunk.GetRecordsCount();
            Chunks.emplace_back(std::move(chunk));
        }

        std::shared_ptr<TSparsedArray> Finish() {
            return std::shared_ptr<TSparsedArray>(new TSparsedArray(std::move(Chunks), DefaultValue, Type, RecordsCount));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
