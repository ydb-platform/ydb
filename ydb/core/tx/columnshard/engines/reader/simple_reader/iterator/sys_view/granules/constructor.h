#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {
class TDataSourceConstructor: public NAbstract::TDataSourceConstructor {
private:
    using TBase = NAbstract::TDataSourceConstructor;
    std::shared_ptr<const TGranuleMeta> Granule;
    NColumnShard::TSchemeShardLocalPathId ExternalPathId;
    ui32 PortionsCount;

public:
    TDataSourceConstructor(const IPathIdTranslator& translator, const ui64 tabletId, const std::shared_ptr<const TGranuleMeta>& granule)
        : TBase(tabletId, granule->GetPathId().GetRawValue(), TSchemaAdapter::GetPKSimpleRow(ExternalPathId, tabletId),
              TSchemaAdapter::GetPKSimpleRow(ExternalPathId, tabletId))
        , Granule(std::move(granule))
        , ExternalPathId(translator.ResolveSchemeShardLocalPathIdVerified(Granule->GetPathId()))
        , PortionsCount(Granule->GetPortions().size()) {
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        std::vector<std::shared_ptr<const TGranuleMeta>> g = { Granule };
        std::vector<NColumnShard::TSchemeShardLocalPathId> p = { ExternalPathId };
        std::vector<ui32> c = { PortionsCount };
        return std::make_shared<TSourceData>(
            GetSourceId(), GetSourceIdx(), GetTabletId(), std::move(g), std::move(p), std::move(c), ExtractStart(), ExtractFinish(), context);
    }
};

class TConstructor: public NAbstract::TConstructor<TDataSourceConstructor> {
private:
    using TBase = NAbstract::TConstructor<TDataSourceConstructor>;
public:
    TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
        const std::optional<NOlap::TInternalPathId> internalPathId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const ERequestSorting sorting);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
