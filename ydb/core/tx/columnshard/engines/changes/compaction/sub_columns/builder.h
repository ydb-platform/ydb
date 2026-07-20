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
    using TSparsedBuilder = NArrow::NAccessor::TSparsedArray::TSparsedBuilder<arrow::BinaryType>;
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

    // A plain array builder whose element type is chosen at construction type.
    // Commonality with TPlainBuilder to be refactored.
    class TPlainRuntimeBuilder {
    private:
        std::unique_ptr<arrow::ArrayBuilder> Builder;
        std::optional<ui32> LastRecordIndex;

        void AppendGapNulls(const ui32 recordIndex) {
            if (LastRecordIndex) {
                AFL_VERIFY(*LastRecordIndex < recordIndex)("last", LastRecordIndex)("index", recordIndex);
                NArrow::TStatusValidator::Validate(Builder->AppendNulls(recordIndex - *LastRecordIndex - 1));
            } else {
                NArrow::TStatusValidator::Validate(Builder->AppendNulls(recordIndex));
            }
            LastRecordIndex = recordIndex;
        }

    public:
        TPlainRuntimeBuilder(const std::shared_ptr<arrow::DataType>& type)
            : Builder(NArrow::MakeBuilder(type))
        {
        }

        void AddNativeValue(const ui32 recordIndex, const arrow::Array& array, const i64 position) {
            AFL_VERIFY(Builder->type()->id() == array.type_id())("builder", Builder->type()->ToString())("array", array.type()->ToString());
            AppendGapNulls(recordIndex);
            AFL_VERIFY(NArrow::Append(*Builder, array, position));
        }

        void AddBinaryValue(const ui32 recordIndex, const TStringBuf value) {
            AFL_VERIFY(Builder->type()->id() == arrow::Type::BINARY)("type", Builder->type()->ToString());
            AppendGapNulls(recordIndex);
            AFL_VERIFY(NArrow::Append<arrow::BinaryType>(*Builder, arrow::util::string_view(value.data(), value.size())));
        }

        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Finish(const ui32 recordsCount) {
            if (LastRecordIndex) {
                AFL_VERIFY(*LastRecordIndex < recordsCount)("last", LastRecordIndex)("count", recordsCount);
                NArrow::TStatusValidator::Validate(Builder->AppendNulls(recordsCount - *LastRecordIndex - 1));
            } else {
                NArrow::TStatusValidator::Validate(Builder->AppendNulls(recordsCount));
            }
            return std::make_shared<NArrow::NAccessor::TTrivialArray>(NArrow::FinishBuilder(std::move(Builder)));
        }
    };

    class TGeneralAccessorBuilder {
    private:
        std::variant<TSparsedBuilder, TPlainRuntimeBuilder> Builder;
        const EValueType TargetValueType;
        YDB_READONLY(ui32, FilledRecordsCount, 0);
        YDB_READONLY(ui64, FilledRecordsSize, 0);

    public:
        TGeneralAccessorBuilder(TSparsedBuilder&& builder, const EValueType targetValueType)
            : Builder(std::move(builder))
            , TargetValueType(targetValueType)
        {
        }

        TGeneralAccessorBuilder(TPlainRuntimeBuilder&& builder, const EValueType targetValueType)
            : Builder(std::move(builder))
            , TargetValueType(targetValueType)
        {
        }

        // Returns the number of value bytes actually stored, since a re-encoded value's stored size differs from its size in the source.
        ui32 AddRecord(const ui32 recordIndex, const TGeneralIterator& it) {
            const bool passthrough = it.GetValueType() == TargetValueType;

            struct TVisitor {
                const ui32 RecordIndex;
                const TGeneralIterator& It;
                const bool Passthrough;

                ui32 operator()(TSparsedBuilder& builder) const {
                    // Sparsed columns are always binary-backed, so the passthrough value is its storage bytes.
                    if (Passthrough) {
                        builder.AddRecord(RecordIndex, It.GetStorageView());
                        return It.GetValueSize();
                    } else {
                        const auto bj = It.GetValueAsBinaryJson();
                        builder.AddRecord(RecordIndex, TStringBuf(bj.data(), bj.size()));
                        return bj.size();
                    }
                }

                ui32 operator()(TPlainRuntimeBuilder& builder) const {
                    if (Passthrough) {
                        builder.AddNativeValue(RecordIndex, It.GetArray(), It.GetLocalIndex());
                        return It.GetValueSize();
                    } else {
                        const auto bj = It.GetValueAsBinaryJson();
                        builder.AddBinaryValue(RecordIndex, TStringBuf(bj.data(), bj.size()));
                        return bj.size();
                    }
                }
            };

            const ui32 storedSize = std::visit(TVisitor{ recordIndex, it, passthrough }, Builder);
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
