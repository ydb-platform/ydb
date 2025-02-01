#pragma once
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NOlap::NCompaction {

class TSubColumnsMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TSubColumnsMerger>(NArrow::NAccessor::TGlobalConst::SubColumnsDataAccessorName);
    using TBase = IColumnMerger;
    std::vector<std::shared_ptr<TSubColumnsArray>> Sources;
    std::vector<TReadIteratorOrderedKeys> OrderedIterators;
    ui32 OutputRecordsCount = 0;
    ui32 InputRecordsCount = 0;
    std::optional<TDictStats> ResultColumnStats;
    std::optional<TDictStats> ResultOtherStats;
    class TRemapColumns {
    private:
        class TSourceAddress {
        private:
            const ui32 SourceIndex;
            const ui32 SourceKeyIndex;
            const bool IsColumnKey;

        public:
            TSourceAddress(const ui32 sourceIndex, const ui32 sourceKeyIndex, const bool isColumnKey)
                : SourceIndex(sourceIndex)
                , SourceKeyIndex(sourceKeyIndex)
                , IsColumnKey(isColumnKey) {
            }

            bool operator<(const TSourceAddress& item) const {
                return std::tie(SourceIndex, SourceKeyIndex, IsColumnKey) < std::tie(item.SourceIndex, item.SourceKeyIndex, item.IsColumnKey);
            }
        };

        std::map<TSourceAddress, TRemapInfo> RemapInfo;
        std::vector<std::vector<std::vector<TRemapInfo>>> RemapInfo;

    public:
        void AddRemap(const ui32 sourceIdx, const TDictStats& sourceColumnStats, const TDictStats& sourceOtherStats,
            const TDictStats& resultColumnStats, const TDictStats& resultOtherStats) {
            for (ui32 i = 0; i < sourceColumnStats.GetColumnsCount(); ++i) {
                if (auto commonKeyIndex = resultColumnStats.GetKeyIndexOptional(sourceColumnStats.GetColumnName(i))) {
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, true)).second);
                } else {
                    const ui32 commonKeyIndex = resultOtherStats.GetKeyIndexVerified(sourceColumnStats.GetColumnName(i));
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, false)).second);
                }
            }
            for (ui32 i = 0; i < sourceOtherStats.GetColumnsCount(); ++i) {
                if (auto commonKeyIndex = resultColumnStats.GetKeyIndexOptional(sourceOtherStats.GetColumnName(i))) {
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, true)).second);
                } else {
                    const ui32 commonKeyIndex = resultOtherStats.GetKeyIndexVerified(sourceOtherStats.GetColumnName(i));
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, false)).second);
                }
            }
        }

        class TRemapInfo {
        private:
            YDB_READONLY(ui32, CommonKeyIndex, 0);
            YDB_READONLY(bool, IsColumnKey, false);

        public:
            TRemapInfo(const ui32 keyIndex, const bool isColumnKey)
                : CommonKeyIndex(keyIndex)
                , IsColumnKey(isColumnKey) {
            }
        };

        TRemapInfo RemapIndex(const ui32 sourceIdx, const ui32 sourceKeyIndex, const bool isColumnKey) const {
            auto it = RemapInfo.find(TSourceAddress(sourceIdx, sourceKeyIndex, isColumnKey));
            AFL_VERIFY(it != RemapInfo.end());
            return it->second;
        }
    };

    TRemapColumns RemapKeyIndex;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override {
        for (auto&& i : mergeContext.GetChunks()) {
            OutputRecordsCount += i.GetRecordsCount();
        }
        for (auto&& i : input) {
            if (i->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                Sources.emplace_back(std::static_pointer_cast<TSubColumnsArray>(i));
            } else {
                auto subColumnsAccessor = mergeContext.GetAccessorConstructor().Construct(i);
                Sources.emplace_back(std::static_pointer_cast<TSubColumnsArray>(subColumnsAccessor));
            }
        }
        std::vector<const TDictStats*> stats;
        for (auto&& i : Sources) {
            stats.emplace_back(&i->GetColumnsData().GetStats());
            stats.emplace_back(&i->GetOthersData().GetStats());
            InputRecordsCount += i->GetRecordsCount();
        }
        auto commonStats = TDictStats::Merge(stats);
        auto splitted = commonStats.SplitByVolume(1024);
        ResultColumnStats = splitted.ExtractColumns();
        ResultOtherStats = splitted.ExtractOthers();

        ui32 sourceIdx = 0;
        for (auto&& i : Sources) {
            RemapKeyIndex.AddRemap(sourceIdx++, i, ResultColumnStats, ResultOtherStats);
        }
    }

    class TMergedBuilder {
    private:
        const ui32 OutputRecordsCount = 0;
        const ui32 InputRecordsCount = 0;
        const TDictStats ResultColumnStats;
        const TDictStats ResultOtherStats;
        const TChunkMergeContext& Context;
        TOthersData::TBuilderWithStats OthersBuilder;

        class TSparsedBuilder {
        private:
            std::shared_ptr<arrow::Schema> Schema;
            std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
            arrow::UInt32Builder* IndexBuilder;
            arrow::StringBuilder* ValueBuilder;
            ui32 RecordsCount = 0;
        public:
            TSparsedBuilder() {
                arrow::FieldVector fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
                    std::make_shared<arrow::Field>("value", arrow::utf8()) };
                Schema = std::make_shared<arrow::Schema>(fields);
                Builders = NArrow::MakeBuilders(Schema);
                IndexBuilder = static_cast<arrow::UInt32Builder*>(Builders[0].get());
                ValueBuilder = static_cast<arrow::StringBuilder*>(Builders[1].get());
            }

            void AddRecord(const ui32 recordIndex, const std::string_view value) {
                IndexBuilder->Append(recordIndex);
                ValueBuilder->Append(value.data(), value.size());
                ++RecordsCount;
            }

            std::shared_ptr<IChunkedArray> Finish(const ui32 recordsCount) {
                TSparsedArray::TBuilder builder(nullptr, arrow::utf8());
                builder.AddChunk(recordsCount, arrow::RecordBatch::Make(Schema, RecordsCount, NArrow::Finish(std::move(Builders))));
                return builder.Finish();
            }
        };

        class TPlainBuilder {
        private:
            std::shared_ptr<arrow::Schema> Schema;
            std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
            arrow::StringBuilder* ValueBuilder;
            std::optional<ui32> LastRecordIndex;
        public:
            TPlainBuilder() {
                arrow::FieldVector fields = { std::make_shared<arrow::Field>("value", arrow::utf8()) };
                Schema = std::make_shared<arrow::Schema>(fields);
                Builders = NArrow::MakeBuilders(Schema);
                ValueBuilder = static_cast<arrow::StringBuilder*>(Builders[0].get());
            }

            void AddRecord(const ui32 recordIndex, const std::string_view value) {
                ValueBuilder->AppendNulls(recordIndex - LastRecordIndex.value_or(0));
                LastRecordIndex = recordIndex;
                ValueBuilder->Append(value.data(), value.size());
            }

            std::shared_ptr<IChunkedArray> Finish(const ui32 recordsCount) {
                if (LastRecordIndex) {
                    ValueBuilder->AppendNulls(recordsCount - *LastRecordIndex - 1);
                } else {
                    ValueBuilder->AppendNulls(recordsCount);
                }
                return std::make_shared<TTrivialArray>(arrow::RecordBatch::Make(NArrow::Finish(std::move(Builders)).front()));
            }
        };

        class TGeneralAccessorBuilder {
        private:
            std::variant<TSparsedBuilder, TPlainBuilder> Builder;
        public:
            void AddRecord(const ui32 recordIndex, const std::string_view value) {
                struct TVisitor {
                    void operator(TSparsedBuilder& builder) const {
                        builder.AddRecord(RecordIndex, Value);
                    }
                    void operator(TPlainBuilder& builder) const {
                        builder.AddRecord(RecordIndex, Value);
                    }
                };
                Builder.visit(TVisitor(recordIndex, value));
            }
            std::shared_ptr<IChunkedArray> Finish(const ui32 recordIndex, const std::string_view value) {
                struct TVisitor {
                    void operator(TSparsedBuilder& builder) const {
                        return builder.Finish();
                    }
                    void operator(TPlainBuilder& builder) const {
                        return builder.Finish();
                    }
                };
                return Builder.visit(TVisitor());
            }
        };

        std::vector<TGeneralAccessorBuilder> ColumnBuilders;
        std::vector<std::shared_ptr<TSubColumnsArray>> Results;

        void FlushData() {
            AFL_VERIFY(RecordIndex);
            auto portionOthersData = OthersBuilder.Finish(ResultOtherStats);
            std::vector<std::shared_ptr<IChunkedArray>> arrays;
            for (auto&& i : ColumnBuilders) {
                arrays.emplace_back(i.Finish(RecordIndex));
            }
            TColumnsData cData(ResultColumnStats, std::make_shared<TGeneralContainer>(ResultColumnStats.BuildSchema(), arrays));
            Results.emplace_back(std::make_shared<TSubColumnsArray>(std::move(cData), std::move(portionOthersData)));
            Initialize();
        }

        void Initialize() {
            ColumnBuilders.clear();
            for (ui32 i = 0; i < ResultColumnStats.GetColumnsCount(); ++i) {
                if (ResultColumnStats.GetColumnRecordsCount(i) * 10 < OutputRecordsCount) {
                    ColumnBuilders.emplace_back(TGeneralAccessorBuilder::MakeSparsedBuilder());
                } else {
                    ColumnBuilders.emplace_back(TGeneralAccessorBuilder::MakePlainBuilder());
                }
            }
            OthersBuilder = TOthersData::MakeMergedBuilder();
            RecordIndex = 0;
        }
    public:
        TMergedBuilder(const TDictStats& columnStats, const TDictStats& otherStats, const ui32 inputRecordsCount, const ui32 outputRecordsCount,
            const TChunkMergeContext& context)
            : OutputRecordsCount(outputRecordsCount)
            , InputRecordsCount(inputRecordsCount)
            , ResultColumnStats(columnStats)
            , ResultOtherStats(otherStats)
            , Context(context)
            , OthersBuilder(TOthersData::MakeMergedBuilder()) {
            Initialize();
        }

        class TPortionColumn: public TColumnPortionResult {
        public:
            void AddChunk(const std::shared_ptr<TSubColumnsArray>& cArray, const TColumnMergeContext& cmContext) {
                Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(cArray->SerializeToString(), cArray,
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
                portions.emplace_back(TPortionColumn(cmContext.GetColumnId()));
                portions.back().AddChunk(i, cmContext);
            }
            return std::move(portions);
        }

        void StartRecord() {
            ++RecordIndex;
        }
        void FinishRecord() {
            if (RecordIndex == Context.GetPortionRowsCountLimit()) {
                FlushData();
            }
        }
        void AddColumnKV(const ui32 commonKeyIndex, const std::string_view value) {
            AFL_VERIFY(commonKeyIndex < ColumnBuilders.size());
            ColumnBuilders[commonKeyIndex].AddRecord(RecordIndex, value)
        }
        void AddOtherKV(const ui32 commonKeyIndex, const std::string_view value) {
            OthersBuilder.Add(RecordIndex, commonKeyIndex, value);
        }
    };

    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override {
        AFL_VERIFY(ResultColumnStats && ResultOtherStats);
        auto& mergeChunkContext = mergeContext.GetChunk(context.GetBatchIdx());
        TMergedBuilder builder(*ResultColumnStats, *ResultOtherStats);
        for (ui32 i = 0; i < context.GetRecordsCount(); ++i) {
            const ui32 sourceIdx = mergeChunkContext.GetIdxArray()->Value(i);
            const ui32 recordIdx = mergeChunkContext.GetRecordIdxArray()->Value(i);
            const auto startRecord = [](const ui32 /*sourceRecordIndex*/) {
                builder.StartRecord();
            };
            const auto addKV = [sourceIdx](const ui32 sourceKeyIndex, const std::string_view value, const bool isColumnKey) {
                auto commonKeyInfo = RemapKeyIndex.RemapIndex(sourceIdx, sourceKeyIndex, isColumnKey);
                if (commonKeyInfo.IsColumnKey()) {
                    builder.AddColumnKV(commonKeyInfo.GetCommonKeyIndex(), value);
                } else {
                    builder.AddOtherKV(commonKeyInfo.GetCommonKeyIndex(), value);
                }
            };
            const auto finishRecord = []() {
                builder.FinishRecord();
            };
            OrderedIterators[sourceIdx].ReadRecords(1, startRecord, addKV, finishRecord);
        }
        return builder.Finish();
    }

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
