#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NOlap::NCompaction {

class TSubColumnsMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TSubColumnsMerger>(NArrow::NAccessor::TGlobalConst::SubColumnsDataAccessorName);
    using TBase = IColumnMerger;
    using TDictStats = NArrow::NAccessor::NSubColumns::TDictStats;
    using TOthersData = NArrow::NAccessor::NSubColumns::TOthersData;
    using TColumnsData = NArrow::NAccessor::NSubColumns::TColumnsData;
    using TSparsedBuilder = NArrow::NAccessor::TSparsedArray::TSparsedBuilder;
    using TPlainBuilder = NArrow::NAccessor::TTrivialArray::TPlainBuilder;
    using TSubColumnsArray = NArrow::NAccessor::TSubColumnsArray;
    using TReadIteratorOrderedKeys = NArrow::NAccessor::NSubColumns::TReadIteratorOrderedKeys;
    std::vector<std::shared_ptr<TSubColumnsArray>> Sources;
    std::vector<TReadIteratorOrderedKeys> OrderedIterators;
    ui32 OutputRecordsCount = 0;
    ui32 InputRecordsCount = 0;
    std::optional<TDictStats> ResultColumnStats;
    std::optional<TDictStats> ResultOtherStats;
    class TRemapColumns {
    private:
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

    public:
        void AddRemap(const ui32 sourceIdx, const TDictStats& sourceColumnStats, const TDictStats& sourceOtherStats,
            const TDictStats& resultColumnStats, const TDictStats& resultOtherStats) {
            for (ui32 i = 0; i < sourceColumnStats.GetColumnsCount(); ++i) {
                if (auto commonKeyIndex = resultColumnStats.GetKeyIndexOptional(sourceColumnStats.GetColumnName(i))) {
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, true)).second);
                } else {
                    commonKeyIndex = resultOtherStats.GetKeyIndexVerified(sourceColumnStats.GetColumnName(i));
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, false)).second);
                }
            }
            for (ui32 i = 0; i < sourceOtherStats.GetColumnsCount(); ++i) {
                if (auto commonKeyIndex = resultColumnStats.GetKeyIndexOptional(sourceOtherStats.GetColumnName(i))) {
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, true)).second);
                } else {
                    commonKeyIndex = resultOtherStats.GetKeyIndexVerified(sourceOtherStats.GetColumnName(i));
                    AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, false)).second);
                }
            }
        }

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
                auto subColumnsAccessor = Context.GetLoader()
                                              ->GetAccessorConstructor()
                                              ->Construct(i, Context.GetLoader()->BuildAccessorContext(i->GetRecordsCount()))
                                              .DetachResult();
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
        auto splitted = commonStats.SplitByVolume(NArrow::NAccessor::NSubColumns::TSettings::ColumnAccessorsCountLimit);
        ResultColumnStats = splitted.ExtractColumns();
        ResultOtherStats = splitted.ExtractOthers();

        for (ui32 sourceIdx = 0; sourceIdx < Sources.size(); ++sourceIdx) {
            const auto& source = Sources[sourceIdx];
            RemapKeyIndex.AddRemap(
                sourceIdx, source->GetColumnsData().GetStats(), source->GetOthersData().GetStats(), *ResultColumnStats, *ResultOtherStats);
            OrderedIterators.emplace_back(source->BuildOrderedIterator());
        }
    }

    class TMergedBuilder {
    private:
        const ui32 OutputRecordsCount = 0;
        const ui32 InputRecordsCount = 0;
        const TDictStats ResultColumnStats;
        const TDictStats ResultOtherStats;
        const TChunkMergeContext& Context;
        std::shared_ptr<TOthersData::TBuilderWithStats> OthersBuilder;
        ui32 RecordIndex = 0;

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
        std::vector<std::shared_ptr<TSubColumnsArray>> Results;

        void FlushData() {
            AFL_VERIFY(RecordIndex);
            auto portionOthersData = OthersBuilder->Finish(ResultOtherStats);
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrays;
            for (auto&& i : ColumnBuilders) {
                arrays.emplace_back(i.Finish(RecordIndex));
            }
            TColumnsData cData(
                ResultColumnStats, std::make_shared<NArrow::TGeneralContainer>(ResultColumnStats.BuildColumnsSchema()->fields(), std::move(arrays)));
            Results.emplace_back(
                std::make_shared<TSubColumnsArray>(std::move(cData), std::move(portionOthersData), arrow::binary(), RecordIndex));
            Initialize();
        }

        void Initialize() {
            ColumnBuilders.clear();
            for (ui32 i = 0; i < ResultColumnStats.GetColumnsCount(); ++i) {
                if (ResultColumnStats.IsSparsed(i, OutputRecordsCount)) {
                    ColumnBuilders.emplace_back(TSparsedBuilder(0, 0));
                } else {
                    ColumnBuilders.emplace_back(TPlainBuilder(0, 0));
                }
            }
            OthersBuilder = TOthersData::MakeMergedBuilder();
            RecordIndex = 0;
        }

    public:
        TMergedBuilder(const NArrow::NAccessor::NSubColumns::TDictStats& columnStats,
            const NArrow::NAccessor::NSubColumns::TDictStats& otherStats, const ui32 inputRecordsCount, const ui32 outputRecordsCount,
            const TChunkMergeContext& context)
            : OutputRecordsCount(outputRecordsCount)
            , InputRecordsCount(inputRecordsCount)
            , ResultColumnStats(columnStats)
            , ResultOtherStats(otherStats)
            , Context(context)
            , OthersBuilder(TOthersData::MakeMergedBuilder()) {
            Y_UNUSED(InputRecordsCount);
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
                pColumn.AddChunk(i, cmContext);
                portions.emplace_back(std::move(pColumn));
            }
            return std::move(portions);
        }

        void StartRecord() {
        }
        void FinishRecord() {
            if (++RecordIndex == Context.GetPortionRowsCountLimit()) {
                FlushData();
            }
        }
        void AddColumnKV(const ui32 commonKeyIndex, const std::string_view value) {
            AFL_VERIFY(commonKeyIndex < ColumnBuilders.size());
            ColumnBuilders[commonKeyIndex].AddRecord(RecordIndex, value);
        }
        void AddOtherKV(const ui32 commonKeyIndex, const std::string_view value) {
            OthersBuilder->Add(RecordIndex, commonKeyIndex, value);
        }
    };

    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override {
        AFL_VERIFY(ResultColumnStats && ResultOtherStats);
        auto& mergeChunkContext = mergeContext.GetChunk(context.GetBatchIdx());
        TMergedBuilder builder(*ResultColumnStats, *ResultOtherStats, InputRecordsCount, OutputRecordsCount, context);
        for (ui32 i = 0; i < context.GetRecordsCount(); ++i) {
            const ui32 sourceIdx = mergeChunkContext.GetIdxArray().Value(i);
            const ui32 recordIdx = mergeChunkContext.GetRecordIdxArray().Value(i);
            const auto startRecord = [&](const ui32 /*sourceRecordIndex*/) {
                builder.StartRecord();
            };
            const auto addKV = [&](const ui32 sourceKeyIndex, const std::string_view value, const bool isColumnKey) {
                auto commonKeyInfo = RemapKeyIndex.RemapIndex(sourceIdx, sourceKeyIndex, isColumnKey);
                if (commonKeyInfo.GetIsColumnKey()) {
                    builder.AddColumnKV(commonKeyInfo.GetCommonKeyIndex(), value);
                } else {
                    builder.AddOtherKV(commonKeyInfo.GetCommonKeyIndex(), value);
                }
            };
            const auto finishRecord = [&]() {
                builder.FinishRecord();
            };
            OrderedIterators[sourceIdx].ReadRecord(recordIdx, startRecord, addKV, finishRecord);
        }
        return builder.Finish(Context);
    }

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
