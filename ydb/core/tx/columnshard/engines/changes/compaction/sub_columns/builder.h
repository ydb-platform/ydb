#pragma once
#include "remap.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

class TMergedBuilder {
private:
    using TSparsedBuilder = NArrow::NAccessor::TSparsedArray::TSparsedBuilder<arrow::StringType>;
    using TPlainBuilder = NArrow::NAccessor::TTrivialArray::TPlainBuilder<arrow::StringType>;
    using TColumnsData = NArrow::NAccessor::NSubColumns::TColumnsData;
    using TOthersData = NArrow::NAccessor::NSubColumns::TOthersData;
    using TSettings = NArrow::NAccessor::NSubColumns::TSettings;
    using TDictStats = NArrow::NAccessor::NSubColumns::TDictStats;
    using TSubColumnsArray = NArrow::NAccessor::TSubColumnsArray;

    const TDictStats ResultColumnStats;
    const TChunkMergeContext& Context;
    std::shared_ptr<TOthersData::TBuilderWithStats> OthersBuilder;
    ui32 RecordIndex = 0;
    ui32 SumValuesSize = 0;
    const TSettings Settings;
    const TRemapColumns& Remapper;

    class TGeneralAccessorBuilder {
    private:
        std::variant<TSparsedBuilder, TPlainBuilder> Builder;
        YDB_READONLY(ui32, FilledRecordsCount, 0);
        YDB_READONLY(ui64, FilledRecordsSize, 0);
    public:
        TGeneralAccessorBuilder(TSparsedBuilder&& builder)
            : Builder(std::move(builder)) {
        }

        TGeneralAccessorBuilder(TPlainBuilder&& builder)
            : Builder(std::move(builder)) {
        }

        void AddRecord(const ui32 recordIndex, const std::string_view value) {
            struct TVisitor {
            private:
                const ui32 RecordIndex;
                const std::string_view Value;

            public:
                void operator()(TSparsedBuilder& builder) const {
                    builder.AddRecord(RecordIndex, Value);
                }
                void operator()(TPlainBuilder& builder) const {
                    builder.AddRecord(RecordIndex, Value);
                }
                TVisitor(const ui32 recordIndex, const std::string_view value)
                    : RecordIndex(recordIndex)
                    , Value(value) {
                }
            };
            std::visit(TVisitor(recordIndex, value), Builder);
            ++FilledRecordsCount;
            FilledRecordsSize += value.size();
        }
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Finish(const ui32 recordsCount) {
            struct TVisitor {
            private:
                const ui32 RecordsCount;

            public:
                std::shared_ptr<NArrow::NAccessor::IChunkedArray> operator()(TSparsedBuilder& builder) const {
                    return builder.Finish(RecordsCount);
                }
                std::shared_ptr<NArrow::NAccessor::IChunkedArray> operator()(TPlainBuilder& builder) const {
                    return builder.Finish(RecordsCount);
                }
                TVisitor(const ui32 recordsCount)
                    : RecordsCount(recordsCount) {
                }
            };
            return std::visit(TVisitor(recordsCount), Builder);
        }
    };

    std::vector<TGeneralAccessorBuilder> ColumnBuilders;
    std::vector<std::shared_ptr<TSubColumnsArray>> Result;

    void FlushData();

    void Initialize();

public:
    TMergedBuilder(const NArrow::NAccessor::NSubColumns::TDictStats& columnStats, const TChunkMergeContext& context, const TSettings& settings,
        const TRemapColumns& remapper)
        : ResultColumnStats(columnStats)
        , Context(context)
        , OthersBuilder(TOthersData::MakeMergedBuilder())
        , Settings(settings)
        , Remapper(remapper) {
        Initialize();
    }

    TColumnPortionResult Finish(const TColumnMergeContext& cmContext);

    void StartRecord() {
    }
    void FinishRecord();
    void AddColumnKV(const ui32 commonKeyIndex, const std::string_view value) {
        AFL_VERIFY(commonKeyIndex < ColumnBuilders.size());
        ColumnBuilders[commonKeyIndex].AddRecord(RecordIndex, value);
        SumValuesSize += value.size();
    }
    void AddOtherKV(const ui32 commonKeyIndex, const std::string_view value) {
        OthersBuilder->Add(RecordIndex, commonKeyIndex, value);
        SumValuesSize += value.size();
    }
};

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
