#pragma once

#include "defs.h"
#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/formats/arrow/replace_key.h>

namespace NKikimr::NOlap {

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot);
NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::Table>& batch, const TSnapshot& snapshot);

struct TReadMetadata;
NArrow::TColumnFilter FilterPortion(const std::shared_ptr<arrow::Table>& batch, const TReadMetadata& readMetadata, const bool useSnapshotFilter);
NArrow::TColumnFilter FilterNotIndexed(const std::shared_ptr<arrow::Table>& batch, const TReadMetadata& readMetadata);
NArrow::TColumnFilter EarlyFilter(const std::shared_ptr<arrow::Table>& batch, std::shared_ptr<NSsa::TProgram> ssa);
NArrow::TColumnFilter EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa);

} // namespace NKikimr::NOlap
