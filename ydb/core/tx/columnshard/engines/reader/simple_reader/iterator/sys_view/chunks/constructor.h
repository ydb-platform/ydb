#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessors_ordering.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {
class TPortionDataConstructor: public NAbstract::TDataSourceConstructor {
private:
    using TBase = NAbstract::TDataSourceConstructor;
    NColumnShard::TUnifiedPathId PathId;
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);
    ISnapshotSchema::TPtr Schema;

public:
    TPortionDataConstructor(const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId, const TPortionInfo::TConstPtr& portion,
        const ISnapshotSchema::TPtr& schema)
        : TBase(tabletId, TSchemaAdapter::GetPKSimpleRow(pathId, tabletId, portion->GetPortionId(), 0, 0),
              TSchemaAdapter::GetPKSimpleRow(pathId, tabletId, portion->GetPortionId(), Max<ui32>(), Max<ui32>()))
        , PathId(pathId)
        , Portion(portion)
        , Schema(schema)
    {
    }

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor);
    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NCommon::TSpecialReadContext>& context);

    virtual bool QueryAgnosticLess(const NCommon::TDataSourceConstructor& rhs) const override {
        auto* rhsLocal = VerifyDynamicCast<const TPortionDataConstructor*>(&rhs);
        return std::make_tuple(GetTabletId(), GetPortion()->GetPortionId()) <
               std::make_tuple(rhsLocal->GetTabletId(), rhsLocal->GetPortion()->GetPortionId());
    }
};

class TConstructor: public NCommon::TSourcesConstructorWithAccessors<TPortionDataConstructor> {
private:
    using TBase = NCommon::TSourcesConstructorWithAccessors<TPortionDataConstructor>;

    virtual std::shared_ptr<NReader::NCommon::IDataSource> DoExtractNextImpl(
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) override;
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override {
        while (TBase::GetConstructorsCount()) {
            bool usage = false;
            if (!cursor->CheckEntityIsBorder(TBase::MutableNextConstructor(), usage)) {
                TBase::DropNextConstructor();
                continue;
            }
            AFL_VERIFY(!usage);
            TBase::DropNextConstructor();
            break;
        }
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

public:
    TConstructor(const IPathIdTranslator& translator, const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
        const TSnapshot reqSnapshot, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const ERequestSorting sorting);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
