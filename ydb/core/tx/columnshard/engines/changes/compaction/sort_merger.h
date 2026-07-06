#pragma once
#include <ydb/core/formats/arrow/container/container.h>
#include <ydb/core/formats/arrow/reader/position.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <memory>
#include <vector>

namespace NKikimr::NOlap::NCompaction {

// Alternative implementation of the compaction row-merge that produces the same "remapper" batches
// as NArrow::NMerger::TMergePartialStream::DrainAllParts, but via arrow `sort_indices` + `take`
// (using arrow_next / Arrow 20, the only build where the multi-key sort is reliable) instead of the
// streaming heap merge.
//
// Each input container must already contain the synthetic IColumnMerger::PortionId / PortionRecordIndex
// columns (added by TMerger::Execute), the snapshot columns and, when present, the delete-flag column.
//
// The returned vector matches DrainAllParts: the merged+deduplicated stream split at the checkpoint PK
// boundaries, each batch projected to exactly `indexFields`. Empty batches are dropped.
class TSortIndicesMerger {
public:
    static std::vector<std::shared_ptr<arrow::RecordBatch>> BuildRemapper(const std::vector<std::shared_ptr<NArrow::TGeneralContainer>>& batches,
        const std::vector<std::shared_ptr<NArrow::TColumnFilter>>& filters, const std::shared_ptr<arrow::Schema>& replaceKey,
        const std::vector<std::string>& snapshotColumnNames, const std::vector<std::shared_ptr<arrow::Field>>& indexFields,
        const NArrow::NMerger::TIntervalPositions& checkPoints);
};

}   // namespace NKikimr::NOlap::NCompaction
