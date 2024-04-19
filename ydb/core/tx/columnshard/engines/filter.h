#pragma once

#include "defs.h"
#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot);
NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::Table>& batch, const TSnapshot& snapshot);

struct TReadMetadata;
NArrow::TColumnFilter FilterPortion(const std::shared_ptr<arrow::Table>& batch, const TReadMetadata& readMetadata, const bool useSnapshotFilter);
NArrow::TColumnFilter FilterNotIndexed(const std::shared_ptr<arrow::Table>& batch, const TReadMetadata& readMetadata);

} // namespace NKikimr::NOlap
