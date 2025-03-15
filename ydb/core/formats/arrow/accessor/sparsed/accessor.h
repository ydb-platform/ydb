#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/size_calcer.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow {
class TColumnFilter;
}

namespace NKikimr::NArrow::NAccessor {

class TSparsedArrayChunk: public TMoveOnly {
private:
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Records);
    std::shared_ptr<arrow::Scalar> DefaultValue;
    std::shared_ptr<arrow::DataType> DataType;

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
    std::shared_ptr<arrow::Array> DefaultsArray;

public:
    void VisitValues(const IChunkedArray::TValuesSimpleVisitor& visitor) const {
        visitor(ColValue);
        visitor(DefaultsArray);
    }

    ui32 GetFinishPosition() const {
        return RecordsCount;
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

    IChunkedArray::TLocalDataAddress GetChunk(const std::optional<IChunkedArray::TCommonChunkAddress>& chunkCurrent, const ui64 position) const;

    TSparsedArrayChunk(
        const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& records, const std::shared_ptr<arrow::Scalar>& defaultValue);

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

    TSparsedArrayChunk ApplyFilter(const TColumnFilter& filter) const;
    TSparsedArrayChunk Slice(const ui32 offset, const ui32 count) const;
};

class TSparsedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    TSparsedArrayChunk Record;
    friend class TSparsedArrayChunk;

    virtual void DoVisitValues(const IChunkedArray::TValuesSimpleVisitor& visitor) const override {
        Record.VisitValues(visitor);
    }

    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const override {
        if (Record.GetNotDefaultRecordsCount()) {
            return false;
        } else {
            value = DefaultValue;
            return true;
        }
    }

protected:
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

    virtual ui32 DoGetNullsCount() const override {
        return Record.GetNullsCount();
    }
    virtual ui32 DoGetValueRawBytes() const override {
        return Record.GetValueRawBytes();
    }

    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        TBuilder builder(DefaultValue, GetDataType());
        builder.AddChunk(Record.Slice(offset, count));
        return builder.Finish();
    }

    virtual std::shared_ptr<IChunkedArray> DoApplyFilter(const TColumnFilter& filter) const override {
        TBuilder builder(DefaultValue, GetDataType());
        builder.AddChunk(Record.ApplyFilter(filter));
        return builder.Finish();
    }

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
        return Record.GetChunk(chunkCurrent, position);
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return Record.GetRawSize();
    }

    TSparsedArray(TSparsedArrayChunk&& data, const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type)
        : TBase(data.GetRecordsCount(), EType::SparsedArray, type)
        , DefaultValue(defaultValue)
        , Record(std::move(data)) {
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
    virtual void Reallocate() override;

    static std::shared_ptr<TSparsedArray> Make(const IChunkedArray& defaultArray, const std::shared_ptr<arrow::Scalar>& defaultValue);

    TSparsedArray(const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
        : TBase(recordsCount, EType::SparsedArray, type)
        , DefaultValue(defaultValue)
        , Record(MakeDefaultChunk(defaultValue, type, recordsCount)) {
    }

    const TSparsedArrayChunk& GetSparsedChunk(const ui64 position) const {
        AFL_VERIFY(position < Record.GetRecordsCount());
        return Record;
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return Record.GetScalar(index);
    }

    std::shared_ptr<arrow::RecordBatch> GetRecordBatchVerified() const {
        return Record.GetRecords();
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
        void AddChunk(const ui32 recordsCount, const std::shared_ptr<arrow::Array>& indexes, const std::shared_ptr<arrow::Array>& values);
        void AddChunk(TSparsedArrayChunk&& chunk) {
            RecordsCount += chunk.GetRecordsCount();
            Chunks.emplace_back(std::move(chunk));
            AFL_VERIFY(Chunks.size() == 1);
        }

        std::shared_ptr<TSparsedArray> Finish() {
            AFL_VERIFY(Chunks.size() == 1);
            return std::shared_ptr<TSparsedArray>(new TSparsedArray(std::move(Chunks.front()), DefaultValue, Type));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
