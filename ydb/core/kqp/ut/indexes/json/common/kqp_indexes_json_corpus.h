#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/json_index/json_corpus.h>

namespace NKikimr::NKqp {

using NJsonIndex::EJsonShape;
using NJsonIndex::kJsonCorpusNumShapes;
using NJsonIndex::TCorpusOptions;
using NJsonIndex::TGeneratedRow;
using NJsonIndex::TJsonCorpus;

// Insert Rows_[offset .. offset+count) into the given table.
void UpsertJsonCorpusRange(const TJsonCorpus& corpus, NYdb::NQuery::TQueryClient& db,
    std::string_view tableName, std::string_view jsonType, size_t offset, size_t count);

} // namespace NKikimr::NKqp
