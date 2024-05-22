#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>
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

        auto policy = NAbstract::ISysViewPolicy::BuildByPath(read.TableName);
        if (!policy) {
            return TConclusionStatus::Fail("undefined table name: " + TFsPath(read.TableName).GetName());
        }
        auto filler = policy->CreateMetadataFiller();

        auto fillConclusion = filler->FillMetadata(self, out, read);
        if (fillConclusion.IsFail()) {
            return fillConclusion;
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