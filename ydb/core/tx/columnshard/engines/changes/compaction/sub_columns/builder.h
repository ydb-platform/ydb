#pragma once
#include "remap.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/encoding_builders.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

class TMergedBuilder {
private:
    using TEncodingSparsedBuilder = NArrow::NAccessor::NSubColumns::TEncodingSparsedBuilder;
    using TEncodingPlainBuilder = NArrow::NAccessor::NSubColumns::TEncodingPlainBuilder;
    using TColumnsData = NArrow::NAccessor::NSubColumns::TColumnsData;
    using TOthersData = NArrow::NAccessor::NSubColumns::TOthersData;
    using TSettings = NArrow::NAccessor::NSubColumns::TSettings;
    using TDictStats = NArrow::NAccessor::NSubColumns::TDictStats;
    using TSubColumnsArray = NArrow::NAccessor::TSubColumnsArray;
    using TGeneralIterator = NArrow::NAccessor::NSubColumns::TGeneralIterator;
    using EValueType = NArrow::NAccessor::NSubColumns::EValueType;

    const TDictStats ResultColumnStats;
    const TChunkMergeContext& Context;
    std::shared_ptr<TOthersData::TBuilderWithStats> OthersBuilder;
    ui32 RecordIndex = 0;
    ui32 SumValuesSize = 0;
    const TSettings Settings;
    const TRemapColumns& Remapper;

    class TGeneralAccessorBuilder {
    private:
        std::variant<TEncodingSparsedBuilder, TEncodingPlainBuilder> Builder;
        const EValueType TargetValueType;
        YDB_READONLY(ui32, FilledRecordsCount, 0);
        YDB_READONLY(ui64, FilledRecordsSize, 0);

    public:
        TGeneralAccessorBuilder(TEncodingSparsedBuilder&& builder, const EValueType targetValueType)
            : Builder(std::move(builder))
            , TargetValueType(targetValueType)
        {
        }

        TGeneralAccessorBuilder(TEncodingPlainBuilder&& builder, const EValueType targetValueType)
            : Builder(std::move(builder))
            , TargetValueType(targetValueType)
        {
        }

        // Returns the number of value bytes actually stored, since a re-encoded value's stored size differs from its
        // size in the source. Both builders share the same interface, so one generic visitor covers both.
        ui32 AddRecord(const ui32 recordIndex, const TGeneralIterator& it) {
            const bool passthrough = it.GetValueType() == TargetValueType;
            const ui32 storedSize = std::visit([&](auto& builder) -> ui32 {
                if (passthrough) {
                    builder.AddArrayElement(recordIndex, it.GetArray(), it.GetLocalIndex());
                    return it.GetValueSize();
                }
                const auto bj = it.GetValueAsBinaryJson();
                builder.AddFromBinaryJson(recordIndex, bj);
                return bj.size();
            }, Builder);
            ++FilledRecordsCount;
            FilledRecordsSize += storedSize;
            return storedSize;
        }

        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Finish(const ui32 recordsCount) {
            return std::visit([recordsCount](auto& builder) {
                return builder.Finish(recordsCount);
            }, Builder);
        }
    };

    std::vector<TGeneralAccessorBuilder> ColumnBuilders;
    std::vector<std::shared_ptr<TSubColumnsArray>> Result;

    void FlushData();

    void Initialize();

    std::shared_ptr<NArrow::NAccessor::IChunkedArray> MaybeDictionaryEncode(
        const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& accessor, const ui32 filledRecordsCount, const EValueType valueType) const;

public:
    TMergedBuilder(const NArrow::NAccessor::NSubColumns::TDictStats& columnStats, const TChunkMergeContext& context, const TSettings& settings,
        const TRemapColumns& remapper)
        : ResultColumnStats(columnStats)
        , Context(context)
        , OthersBuilder(TOthersData::MakeMergedBuilder())
        , Settings(settings)
        , Remapper(remapper)
    {
        Initialize();
    }

    TColumnPortionResult Finish(const TColumnMergeContext& cmContext);

    void StartRecord() {
    }

    void FinishRecord();

    ui32 AddColumnKV(const ui32 commonKeyIndex, const TGeneralIterator& iter) {
        AFL_VERIFY(commonKeyIndex < ColumnBuilders.size());
        const ui32 storedSize = ColumnBuilders[commonKeyIndex].AddRecord(RecordIndex, iter);
        SumValuesSize += storedSize;
        return storedSize;
    }

    ui32 AddOtherKV(const ui32 commonKeyIndex, const std::string_view value) {
        OthersBuilder->Add(RecordIndex, commonKeyIndex, value);
        SumValuesSize += value.size();
        return value.size();
    }
};

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
