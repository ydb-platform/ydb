#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

class TSourceData: public NAbstract::TPathSourceData {
private:
    using TBase = NAbstract::TPathSourceData;
    std::vector<TPortionInfo::TConstPtr> Portions;

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const override;

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = 0;
    }

public:
    TSourceData(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        std::vector<TPortionInfo::TConstPtr>&& portions, NArrow::TSimpleRowContent&& start, NArrow::TSimpleRowContent&& finish,
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, pathId, tabletId, std::move(start), std::move(finish), portions.size(),
              portions.front()->RecordSnapshotMin(), portions.back()->RecordSnapshotMin(), context)
        , Portions(std::move(portions)) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
