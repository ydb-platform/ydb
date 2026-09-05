#pragma once
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/abstract/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NOptimizer {

class TSourceData: public NAbstract::TTabletSourceData {
private:
    using TBase = NAbstract::TTabletSourceData;
    std::shared_ptr<const TGranuleMeta> Granule;
    std::vector<NStorageOptimizer::TTaskDescription> OptimizerTasks;
    NColumnShard::TSchemeShardLocalPathId ExternalPathId;

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const override;

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = 0;
    }

public:
    TSourceData(const ui32 sourceIdx, const ui64 tabletId, const std::shared_ptr<const TGranuleMeta>& granule,
        std::vector<NStorageOptimizer::TTaskDescription>&& tasks, const NColumnShard::TSchemeShardLocalPathId& externalPathId,
        NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : TBase(sourceIdx, tabletId, std::move(start), std::move(finish), tasks.size(), TSnapshot::Zero(), TSnapshot::Zero(), context)
        , Granule(granule)
        , OptimizerTasks(std::move(tasks))
        , ExternalPathId(externalPathId)
    {
        // TaskId order differs from the sys view PK; the sort is only consumed by the flag-on limit-pushdown
        // path. A flag-off sorted scan re-sorts in KQP, so skip the work there.
        if (context->GetReadMetadata()->IsSorted() && HasAppData() && AppDataVerified().FeatureFlags.GetEnableSysViewOrderByLimitPushdown()) {
            std::sort(OptimizerTasks.begin(), OptimizerTasks.end());
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NOptimizer
