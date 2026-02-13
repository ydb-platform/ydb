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
        : TBase(tabletId, TSchemaAdapter::GetPKSimpleRow(tabletId, schemas.front()->GetIndexInfo().GetPresetId(), schemas.front()->GetVersion()),
              TSchemaAdapter::GetPKSimpleRow(tabletId, schemas.back()->GetIndexInfo().GetPresetId(), schemas.back()->GetVersion()))
        , Schemas(std::move(schemas))
    {
        if (Schemas.size() > 1) {
            AFL_VERIFY(GetStart() < GetFinish())("start", GetStart().DebugString())("finish", GetFinish().DebugString());
        }
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        return std::make_shared<TSourceData>(
            GetSourceIdx(), GetTabletId(), std::move(Schemas), ExtractStart().ExtractValue(), ExtractFinish().ExtractValue(), context);
    }

    virtual bool QueryAgnosticLess(const NCommon::TDataSourceConstructor& rhs) const override {
        auto* rhsLocal = VerifyDynamicCast<const TDataSourceConstructor*>(&rhs);
        AFL_VERIFY(!Schemas.empty());
        AFL_VERIFY(!rhsLocal->Schemas.empty());
        return std::make_tuple(GetTabletId(), Schemas.front()->GetVersion()) <
               std::make_tuple(rhsLocal->GetTabletId(), rhsLocal->Schemas.front()->GetVersion());
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
