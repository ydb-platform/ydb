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
    ui32 TotalRecordsCount = 0;
    ui32 RecordIndex = 0;
    ui32 SumValuesSize = 0;
    const TSettings Settings;
    const TRemapColumns& Remapper;

    class TGeneralAccessorBuilder {
    private:
        std::variant<TSparsedBuilder, TPlainBuilder> Builder;

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
    std::vector<std::vector<std::shared_ptr<TSubColumnsArray>>> Results;

    void FlushData() {
        AFL_VERIFY(RecordIndex);
        auto portionOthersData = OthersBuilder->Finish(Remapper.BuildRemapInfo(OthersBuilder->GetStatsByKeyIndex(), Settings, RecordIndex));
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrays;
        for (auto&& i : ColumnBuilders) {
            arrays.emplace_back(i.Finish(RecordIndex));
        }
        TColumnsData cData(
            ResultColumnStats, std::make_shared<NArrow::TGeneralContainer>(ResultColumnStats.BuildColumnsSchema()->fields(), std::move(arrays)));
        Results.back().emplace_back(
            std::make_shared<TSubColumnsArray>(std::move(cData), std::move(portionOthersData), arrow::binary(), RecordIndex, Settings));
        Initialize();
    }

    void Initialize() {
        ColumnBuilders.clear();
        for (ui32 i = 0; i < ResultColumnStats.GetColumnsCount(); ++i) {
            switch (ResultColumnStats.GetAccessorType(i)) {
                case NArrow::NAccessor::IChunkedArray::EType::Array:
                    ColumnBuilders.emplace_back(TPlainBuilder(0, 0));
                    break;
                case NArrow::NAccessor::IChunkedArray::EType::SparsedArray:
                    ColumnBuilders.emplace_back(TSparsedBuilder(nullptr, 0, 0));
                    break;
                case NArrow::NAccessor::IChunkedArray::EType::Undefined:
                case NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray:
                case NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray:
                case NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray:
                case NArrow::NAccessor::IChunkedArray::EType::ChunkedArray:
                    AFL_VERIFY(false);
            }
        }
        OthersBuilder = TOthersData::MakeMergedBuilder();
        RecordIndex = 0;
        SumValuesSize = 0;
    }

public:
    TMergedBuilder(const NArrow::NAccessor::NSubColumns::TDictStats& columnStats, const TChunkMergeContext& context, const TSettings& settings,
        const TRemapColumns& remapper)
        : ResultColumnStats(columnStats)
        , Context(context)
        , OthersBuilder(TOthersData::MakeMergedBuilder())
        , Settings(settings)
        , Remapper(remapper) {
        Results.emplace_back();
        Initialize();
    }

    class TPortionColumn: public TColumnPortionResult {
    public:
        void AddChunk(const std::shared_ptr<TSubColumnsArray>& cArray, const TColumnMergeContext& cmContext) {
            AFL_VERIFY(cArray);
            AFL_VERIFY(cArray->GetRecordsCount());
            auto accContext = cmContext.GetLoader()->BuildAccessorContext(cArray->GetRecordsCount());
            Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(cArray->SerializeToString(accContext), cArray,
                TChunkAddress(cmContext.GetColumnId(), Chunks.size()),
                cmContext.GetIndexInfo().GetColumnFeaturesVerified(cmContext.GetColumnId())));
        }
    };

    std::vector<TColumnPortionResult> Finish(const TColumnMergeContext& cmContext) {
        if (RecordIndex) {
            FlushData();
        }
        std::vector<TColumnPortionResult> portions;
        for (auto&& i : Results) {
            TPortionColumn pColumn(cmContext.GetColumnId());
            AFL_VERIFY(i.size());
            for (auto&& p : i) {
                pColumn.AddChunk(p, cmContext);
            }
            portions.emplace_back(std::move(pColumn));
        }
        return std::move(portions);
    }

    void StartRecord() {
    }
    void FinishRecord() {
        Y_UNUSED(Context);
        ++TotalRecordsCount;
        ++RecordIndex;
        if (TotalRecordsCount == Context.GetPortionRowsCountLimit()) {
            TotalRecordsCount = 0;
            FlushData();
            Results.emplace_back();
        } else if (SumValuesSize >= Settings.GetChunkMemoryLimit()) {
            FlushData();
        }
    }
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
