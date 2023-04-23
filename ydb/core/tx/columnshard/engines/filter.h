#pragma once

#include <ydb/core/formats/program.h>
#include <ydb/core/formats/replace_key.h>

namespace NKikimr::NOlap {

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::shared_ptr<arrow::Schema>& snapSchema,
                                     ui64 planStep, ui64 txId);

NArrow::TColumnFilter MakeReplaceFilter(const std::shared_ptr<arrow::RecordBatch>& batch, THashSet<NArrow::TReplaceKey>& keys);
NArrow::TColumnFilter MakeReplaceFilterLastWins(const std::shared_ptr<arrow::RecordBatch>& batch, THashSet<NArrow::TReplaceKey>& keys);

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins = false);

struct TReadMetadata;
NArrow::TColumnFilter FilterPortion(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
NArrow::TColumnFilter FilterNotIndexed(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
NArrow::TColumnFilter EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa);

} // namespace NKikimr::NOlap
