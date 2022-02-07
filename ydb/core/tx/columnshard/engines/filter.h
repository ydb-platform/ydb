#pragma once
#include <ydb/core/formats/replace_key.h>

namespace NKikimr::NOlap {

std::vector<bool> MakeSnapshotFilter(std::shared_ptr<arrow::Table> table,
                                     std::shared_ptr<arrow::Schema> snapSchema,
                                     ui64 planStep, ui64 txId);

std::vector<bool> MakeReplaceFilter(std::shared_ptr<arrow::RecordBatch> batch, THashSet<NArrow::TReplaceKey>& keys);
std::vector<bool> MakeReplaceFilterLastWins(std::shared_ptr<arrow::RecordBatch> batch, THashSet<NArrow::TReplaceKey>& keys);
#if 0
std::vector<bool> MakeReplaceFilter(std::shared_ptr<arrow::RecordBatch> batch,
                                    const THashSet<NArrow::TReplaceKey>& staticKeys,
                                    THashSet<NArrow::TReplaceKey>& keys);
#endif

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins = false);

struct TReadMetadata;
std::shared_ptr<arrow::RecordBatch> FilterPortion(std::shared_ptr<arrow::Table> table,
                                                  const TReadMetadata& readMetadata);

}
