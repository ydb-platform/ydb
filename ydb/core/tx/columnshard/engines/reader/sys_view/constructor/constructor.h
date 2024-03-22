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
        THashMap<ui64, THashSet<ui64>> portionsInUse;
        const auto predStatSchema = [](const std::shared_ptr<TPortionInfo>& l, const std::shared_ptr<TPortionInfo>& r) {
            return std::tuple(l->GetPathId(), l->GetPortionId()) < std::tuple(r->GetPathId(), r->GetPortionId());
        };
        for (auto&& filter : read.PKRangesFilter) {
            const ui64 fromPathId = *filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1);
            const ui64 toPathId = *filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>());
            if (read.TableName.EndsWith(IIndexInfo::TABLE_INDEX_STATS_TABLE) || read.TableName.EndsWith(IIndexInfo::TABLE_INDEX_PORTION_STATS_TABLE)) {
                if (fromPathId <= read.PathId && toPathId >= read.PathId) {
                    auto pathInfo = logsIndex->GetGranuleOptional(read.PathId);
                    if (!pathInfo) {
                        continue;
                    }
                    for (auto&& p : pathInfo->GetPortions()) {
                        if (portionsInUse[read.PathId].emplace(p.first).second) {
                            out->IndexPortions.emplace_back(p.second);
                        }
                    }
                }
                std::sort(out->IndexPortions.begin(), out->IndexPortions.end(), predStatSchema);
            } else if (read.TableName.EndsWith(IIndexInfo::STORE_INDEX_STATS_TABLE) || read.TableName.EndsWith(IIndexInfo::STORE_INDEX_PORTION_STATS_TABLE)) {
                auto pathInfos = logsIndex->GetTables(fromPathId, toPathId);
                for (auto&& pathInfo : pathInfos) {
                    for (auto&& p : pathInfo->GetPortions()) {
                        if (portionsInUse[p.second->GetPathId()].emplace(p.first).second) {
                            out->IndexPortions.emplace_back(p.second);
                        }
                    }
                }
                std::sort(out->IndexPortions.begin(), out->IndexPortions.end(), predStatSchema);
            }
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