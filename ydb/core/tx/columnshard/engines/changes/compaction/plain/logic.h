#pragma once
#include "column_cursor.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

namespace NKikimr::NOlap::NCompaction {
class TPlainMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TPlainMerger>(NArrow::NAccessor::TGlobalConst::PlainDataAccessorName);
    using TBase = IColumnMerger;
    std::vector<NCompaction::TPortionColumnCursor> Cursors;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;

    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
