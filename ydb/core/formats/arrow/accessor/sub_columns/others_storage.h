#pragma once

#include "stats.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TOthersData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    std::shared_ptr<IChunkedArray> GetPathAccessor(const std::string_view path, const ui32 recordsCount) const;

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("stats", Stats.DebugJson());
        result.InsertValue("records", Records->DebugJson(true));
        return result;
    }

    TOthersData Slice(const ui32 offset, const ui32 count, const TSettings& settings) const;
    TOthersData ApplyFilter(const TColumnFilter& filter, const TSettings& settings) const;

    static TOthersData BuildEmpty();

    ui64 GetRawSize() const {
        return Records->GetRawSizeVerified();
    }

    class TIterator {
    private:
        const ui32 RecordsCount;
        ui32 CurrentIndex = 0;

        IChunkedArray::TReader RecordIndexReader;
        std::shared_ptr<arrow::UInt32Array> RecordIndex;

        IChunkedArray::TReader KeyIndexReader;
        std::shared_ptr<arrow::UInt32Array> KeyIndex;

        IChunkedArray::TReader ValuesReader;
        std::shared_ptr<arrow::StringArray> Values;

    public:
        TIterator(const std::shared_ptr<TGeneralContainer>& records)
            : RecordsCount(records->GetRecordsCount())
            , RecordIndexReader(records->GetColumnVerified(0))
            , KeyIndexReader(records->GetColumnVerified(1))
            , ValuesReader(records->GetColumnVerified(2)) {
            if (RecordsCount) {
                auto recordIndexChunk = RecordIndexReader.GetReadChunk(0);
                AFL_VERIFY(recordIndexChunk.GetArray()->length() == RecordsCount);
                RecordIndex = std::static_pointer_cast<arrow::UInt32Array>(recordIndexChunk.GetArray());

                auto keyIndexChunk = KeyIndexReader.GetReadChunk(0);
                AFL_VERIFY(keyIndexChunk.GetArray()->length() == RecordsCount);
                KeyIndex = std::static_pointer_cast<arrow::UInt32Array>(keyIndexChunk.GetArray());

                auto valuesChunk = ValuesReader.GetReadChunk(0);
                AFL_VERIFY(valuesChunk.GetArray()->length() == RecordsCount);
                Values = std::static_pointer_cast<arrow::StringArray>(valuesChunk.GetArray());
            }

            CurrentIndex = 0;
        }

        std::optional<ui32> FindPosition(const ui32 findRecordIndex) const {
            return NArrow::FindUpperOrEqualPosition(*RecordIndex, findRecordIndex);
        }

        void MoveToPosition(const ui32 index) {
            CurrentIndex = index;
            AFL_VERIFY(IsValid());
        }

        ui32 GetRecordIndex() const {
            AFL_VERIFY(IsValid());
            return RecordIndex->Value(CurrentIndex);
        }

        ui32 GetKeyIndex() const {
            AFL_VERIFY(IsValid());
            return KeyIndex->Value(CurrentIndex);
        }

        std::string_view GetValue() const {
            AFL_VERIFY(IsValid());
            auto view = Values->GetView(CurrentIndex);
            return std::string_view(view.data(), view.size());
        }

        bool HasValue() const {
            AFL_VERIFY(IsValid());
            return true;
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            return ++CurrentIndex < RecordsCount;
        }

        bool IsValid() const {
            return CurrentIndex < RecordsCount;
        }
    };

    TIterator BuildIterator() const {
        return TIterator(Records);
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    static std::shared_ptr<arrow::Schema> GetSchema() {
        static arrow::FieldVector fields = { std::make_shared<arrow::Field>("record_idx", arrow::uint32()),
            std::make_shared<arrow::Field>("key", arrow::uint32()), std::make_shared<arrow::Field>("value", arrow::utf8()) };
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    TOthersData(const TDictStats& stats, const std::shared_ptr<TGeneralContainer>& records)
        : Stats(stats)
        , Records(records) {
        AFL_VERIFY(Records);
        AFL_VERIFY(Records->num_columns() == 3)("count", Records->num_columns());
        AFL_VERIFY(Records->GetColumnVerified(0)->GetDataType()->id() == arrow::uint32()->id());
        AFL_VERIFY(Records->GetColumnVerified(1)->GetDataType()->id() == arrow::uint32()->id());
        AFL_VERIFY(Records->GetColumnVerified(2)->GetDataType()->id() == arrow::utf8()->id());
    }

    const std::shared_ptr<IChunkedArray>& GetValuesArray() const {
        return Records->GetColumnVerified(2);
    }

    class TFinishContext {
    private:
        const TDictStats ActualStats;
        YDB_READONLY_DEF(std::optional<std::vector<ui32>>, Remap);

    public:
        const TDictStats& GetActualStats() const {
            return ActualStats;
        }

        TFinishContext(const TDictStats& actualStats, const std::vector<ui32>& remap)
            : ActualStats(actualStats)
            , Remap(remap) {
        }

        TFinishContext(const TDictStats& actualStats)
            : ActualStats(actualStats){
        }
    };

    class TBuilderWithStats: TNonCopyable {
    private:
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        arrow::UInt32Builder* RecordIndex;
        arrow::UInt32Builder* KeyIndex;
        std::vector<ui32> RTKeyIndexes;
        arrow::StringBuilder* Values;
        std::optional<ui32> LastRecordIndex;
        std::optional<ui32> LastKeyIndex;
        ui32 RecordsCount = 0;
        YDB_READONLY_DEF(std::vector<TDictStats::TRTStatsValue>, StatsByKeyIndex);

    public:
        TBuilderWithStats();

        void Add(const ui32 recordIndex, const ui32 keyIndex, const std::string_view value);

        TOthersData Finish(const TFinishContext& finishContext);
    };

    static std::shared_ptr<TBuilderWithStats> MakeMergedBuilder() {
        return std::make_shared<TBuilderWithStats>();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
