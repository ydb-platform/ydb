#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NReader::NSysView {

template <class TSysViewSchema>
class TStatScannerConstructor: public IScannerConstructor {
private:
    using TBase = IScannerConstructor;

    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const = 0;

    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override {
        THashSet<ui32> readColumnIds(read.ColumnIds.begin(), read.ColumnIds.end());
        for (auto& [id, name] : read.GetProgram().GetSourceColumns()) {
            readColumnIds.insert(id);
        }

        for (ui32 colId : readColumnIds) {
            if (!NAbstract::TStatsIterator<TSysViewSchema>::StatsSchema.Columns.contains(colId)) {
                return TConclusionStatus::Fail(Sprintf("Columnd id %" PRIu32 " not found", colId));
            }
        }

        auto out = BuildMetadata(self, read);

        out->SetPKRangesFilter(read.PKRangesFilter);
        out->ReadColumnIds.assign(readColumnIds.begin(), readColumnIds.end());
        out->ResultColumnIds = read.ColumnIds;

        const TColumnEngineForLogs* logsIndex = dynamic_cast<const TColumnEngineForLogs*>(self->GetIndexOptional());
        if (!logsIndex) {
            return dynamic_pointer_cast<TReadMetadataBase>(out);
        }
        THashSet<ui64> pathIds;
        for (auto&& filter : read.PKRangesFilter) {
            const ui64 fromPathId = *filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1);
            const ui64 toPathId = *filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>());
            if (read.TableName.EndsWith(IIndexInfo::TABLE_INDEX_STATS_TABLE) 
                || read.TableName.EndsWith(IIndexInfo::TABLE_INDEX_PORTION_STATS_TABLE)
                || read.TableName.EndsWith(IIndexInfo::TABLE_INDEX_GRANULE_STATS_TABLE)
                ) {
                if (fromPathId <= read.PathId && read.PathId <= toPathId) {
                    auto pathInfo = logsIndex->GetGranuleOptional(read.PathId);
                    if (!pathInfo) {
                        continue;
                    }
                    if (pathIds.emplace(pathInfo->GetPathId()).second) {
                        out->IndexGranules.emplace_back(NAbstract::TGranuleMetaView(*pathInfo, out->IsDescSorted()));
                    }
                }
            } else if (read.TableName.EndsWith(IIndexInfo::STORE_INDEX_STATS_TABLE) 
                || read.TableName.EndsWith(IIndexInfo::STORE_INDEX_PORTION_STATS_TABLE)
                || read.TableName.EndsWith(IIndexInfo::STORE_INDEX_GRANULE_STATS_TABLE)
                ) {
                auto pathInfos = logsIndex->GetTables(fromPathId, toPathId);
                for (auto&& pathInfo : pathInfos) {
                    if (pathIds.emplace(pathInfo->GetPathId()).second) {
                        out->IndexGranules.emplace_back(NAbstract::TGranuleMetaView(*pathInfo, out->IsDescSorted()));
                    }
                }
            }
        }
        std::sort(out->IndexGranules.begin(), out->IndexGranules.end());
        if (out->IsDescSorted()) {
            std::reverse(out->IndexGranules.begin(), out->IndexGranules.end());
        }
        return dynamic_pointer_cast<TReadMetadataBase>(out);
    }
public:
    using TBase::TBase;
    virtual TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const override {
        typename NAbstract::TStatsIterator<TSysViewSchema>::TStatsColumnResolver columnResolver;
        return TBase::ParseProgram(vIndex, proto.GetOlapProgramType(), proto.GetOlapProgram(), read, columnResolver);
    }
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* /*self*/) const override {
        return GetColumns(NAbstract::TStatsIterator<TSysViewSchema>::StatsSchema, NAbstract::TStatsIterator<TSysViewSchema>::StatsSchema.KeyColumns);
    }
};

}