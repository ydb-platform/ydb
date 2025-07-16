#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

class TSourceData: public NAbstract::TTabletSourceData {
private:
    using TBase = NAbstract::TTabletSourceData;
    std::vector<ISnapshotSchema::TPtr> Schemas;

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const override;

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = 0;
    }

public:
    TSourceData(const ui32 sourceId, const ui32 sourceIdx, const ui64 tabletId, std::vector<ISnapshotSchema::TPtr>&& schemas,
        NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context)
        : TBase(
              sourceId, sourceIdx, tabletId, std::move(start), std::move(finish), schemas.size(), TSnapshot::Zero(), TSnapshot::Zero(), context)
        , Schemas(std::move(schemas)) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
