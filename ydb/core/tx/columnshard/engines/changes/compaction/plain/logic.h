#pragma once
#include "column_cursor.h"

#include <ydb/core/formats/arrow/common/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

namespace NKikimr::NOlap::NCompaction {
class TPlainMerger: public IColumnMerger {
private:
    std::vector<NCompaction::TPortionColumnCursor> Cursors;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) override;

    virtual std::vector<TColumnPortionResult> DoExecute(
        const NCompaction::TColumnMergeContext& context, const std::shared_ptr<arrow::RecordBatch>& remap) override;

public:
};

}   // namespace NKikimr::NOlap::NCompaction
