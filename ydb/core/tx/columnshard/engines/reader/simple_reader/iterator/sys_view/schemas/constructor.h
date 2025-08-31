#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {
class TDataSourceConstructor: public NAbstract::TDataSourceConstructor {
private:
    using TBase = NAbstract::TDataSourceConstructor;
    std::vector<ISnapshotSchema::TPtr> Schemas;

public:
    TDataSourceConstructor(const ui64 tabletId, std::vector<ISnapshotSchema::TPtr>&& schemas)
        : TBase(tabletId, schemas.front()->GetIndexInfo().GetPresetId() + 1,
              TSchemaAdapter::GetPKSimpleRow(tabletId, schemas.front()->GetIndexInfo().GetPresetId(), schemas.front()->GetVersion()),
              TSchemaAdapter::GetPKSimpleRow(tabletId, schemas.back()->GetIndexInfo().GetPresetId(), schemas.back()->GetVersion()))
        , Schemas(std::move(schemas)) {
        if (Schemas.size() > 1) {
            AFL_VERIFY(GetStart().GetView(*TSchemaAdapter::GetPKSchema()) < GetFinish().GetView(*TSchemaAdapter::GetPKSchema()))("start", GetStart().GetView(*TSchemaAdapter::GetPKSchema()).DebugString())(
                    "finish", GetFinish().GetView(*TSchemaAdapter::GetPKSchema()).DebugString());
        }
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        return std::make_shared<TSourceData>(
            GetSourceId(), GetSourceIdx(), GetTabletId(), std::move(Schemas), ExtractStart(), ExtractFinish(), context);
    }
};

class TConstructor: public NAbstract::TConstructor<TDataSourceConstructor> {
private:
    using TBase = NAbstract::TConstructor<TDataSourceConstructor>;

public:
    TConstructor(const IColumnEngine& engine, const ui64 tabletId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const ERequestSorting sorting);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
