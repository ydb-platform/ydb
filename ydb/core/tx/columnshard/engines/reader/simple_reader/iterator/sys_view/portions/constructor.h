#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {
class TDataSourceConstructor: public NAbstract::TDataSourceConstructor {
private:
    using TBase = NAbstract::TDataSourceConstructor;
    NColumnShard::TUnifiedPathId PathId;
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);

public:
    TDataSourceConstructor(const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId, const std::vector<TPortionInfo::TConstPtr>& portions)
        : TBase(tabletId, TSchemaAdapter::GetPKSimpleRow(pathId, tabletId, portions.front()->GetPortionId()),
              TSchemaAdapter::GetPKSimpleRow(pathId, tabletId, portions.back()->GetPortionId()))
        , PathId(pathId)
        , Portions(portions)
    {
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        return std::make_shared<TSourceData>(
            GetSourceIdx(), PathId, GetTabletId(), std::move(Portions), ExtractStart().ExtractValue(), ExtractFinish().ExtractValue(), context);
    }

    virtual bool QueryAgnosticLess(const NCommon::TDataSourceConstructor& rhs) const override {
        auto* rhsLocal = VerifyDynamicCast<const TDataSourceConstructor*>(&rhs);
        AFL_VERIFY(!Portions.empty());
        AFL_VERIFY(!rhsLocal->Portions.empty());
        return std::make_tuple(GetTabletId(), Portions.front()->GetPortionId()) <
               std::make_tuple(rhsLocal->GetTabletId(), rhsLocal->Portions.front()->GetPortionId());
    }
};

class TConstructor: public NAbstract::TConstructor<TDataSourceConstructor> {
private:
    using TBase = NAbstract::TConstructor<TDataSourceConstructor>;

public:
    TConstructor(const IPathIdTranslator& translator, const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
        const TSnapshot reqSnapshot, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const ERequestSorting sorting);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
