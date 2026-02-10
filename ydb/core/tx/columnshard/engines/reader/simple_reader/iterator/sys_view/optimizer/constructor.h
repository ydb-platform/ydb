#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {
class TDataSourceConstructor: public NAbstract::TDataSourceConstructor {
private:
    using TBase = NAbstract::TDataSourceConstructor;
    std::shared_ptr<const TGranuleMeta> Granule;
    NColumnShard::TSchemeShardLocalPathId ExternalPathId;
    ui32 PortionsCount;

public:
    TDataSourceConstructor(
        const NColumnShard::TSchemeShardLocalPathId& externalPathId, const ui64 tabletId, const std::shared_ptr<const TGranuleMeta>& granule)
        : TBase(tabletId, TSchemaAdapter::GetPKSimpleRow(externalPathId, tabletId, 0),
              TSchemaAdapter::GetPKSimpleRow(externalPathId, tabletId, Max<ui64>()))
        , Granule(std::move(granule))
        , ExternalPathId(externalPathId)
        , PortionsCount(Granule->GetPortions().size())
    {
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        auto tasks = Granule->GetOptimizerPlanner().GetTasksDescription();
        return std::make_shared<TSourceData>(GetSourceIdx(), GetTabletId(), Granule, std::move(tasks), ExternalPathId,
            ExtractStart().ExtractValue(), ExtractFinish().ExtractValue(), context);
    }

    virtual bool QueryAgnosticLess(const NCommon::TDataSourceConstructor& rhs) const override {
        auto* rhsLocal = VerifyDynamicCast<const TDataSourceConstructor*>(&rhs);
        return std::make_tuple(GetTabletId(), ExternalPathId) < std::make_tuple(rhsLocal->GetTabletId(), rhsLocal->ExternalPathId);
    }
};

class TConstructor: public NAbstract::TConstructor<TDataSourceConstructor> {
private:
    using TBase = NAbstract::TConstructor<TDataSourceConstructor>;

public:
    TConstructor(const IPathIdTranslator& translator, const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
        const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const ERequestSorting sorting);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
