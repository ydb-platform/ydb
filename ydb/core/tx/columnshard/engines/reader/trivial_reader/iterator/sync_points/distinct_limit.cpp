#include "distinct_limit.h"

#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/collections/abstract.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NTrivial {

ISyncPoint::ESourceAction TSyncPointDistinctLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/)
{
    if (Seen.size() >= Limit) {
        return ESourceAction::Finish;
    }

    AFL_VERIFY(source->HasStageResult());
    const auto& sr = source->GetStageResult();

    if (sr.IsEmpty()) {
        // No rows to deduplicate; forward to RESULT (terminal Finish for empty is handled there).
        return ESourceAction::ProvideNext;
    }

    const auto& resolver = *source->GetContext()->GetCommonContext()->GetResolver();
    // Must match TAccessorsCollection::ToGeneralContainer (formats/arrow/program/collection.cpp, strictResolver=false):
    // storage columns use resolver names; SSA / projection columns fall back to ascii column id as field name.
    TString columnName = resolver.GetColumnName(KeyColumnId, false);
    if (!columnName) {
        columnName = TStringBuilder() << KeyColumnId;
    }
    const auto batch = sr.GetBatch();
    if (!batch) {
        return ESourceAction::ProvideNext;
    }

    const auto keyAccessor = batch->GetAccessorByNameOptional(std::string(columnName.data(), columnName.size()));
    AFL_VERIFY(keyAccessor)("column", columnName)("key_column_id", KeyColumnId);

    const ui32 recordsCount = keyAccessor->GetRecordsCount();
    if (!recordsCount) {
        return ESourceAction::ProvideNext;
    }

    const auto existing = source->GetStageResult().GetNotAppliedFilter();
    const bool hasRowFilter = existing && !existing->IsTotalAllowFilter();
    // The key is either the fetched column itself or derived from it (JSON_VALUE over a sub-column). The SSA optimizer
    // enables dictionary-only fetching only when the whole request needs exactly one data column and the DISTINCT key
    // is computed from it, so any dictionary-only fetch means the key values are dictionary entries, not rows.
    const bool isDictionaryOnlyFetch = sr.IsDictionaryOnlyFetch(KeyColumnId) || !sr.GetDictionaryOnlyFetchColumns().empty();
    bool applyRowFilter = false;
    std::optional<NArrow::TColumnFilter::TIterator> filterIterator;
    if (hasRowFilter) {
        // Dictionary-only accessors are indexed by dictionary entries: portion-row deny filters (PK range, duplicates,
        // deletions) are excluded by the fetch guards, so a filter here was produced by the program over the same
        // entries (e.g. the projection cut to the requested limit) and its length must match the accessor.
        AFL_VERIFY(existing->GetRecordsCountVerified() == recordsCount)("filter", existing->GetRecordsCountVerified())("records", recordsCount)(
            "dictionary_only", isDictionaryOnlyFetch);
        applyRowFilter = true;
        filterIterator.emplace(existing->GetBegin(false, recordsCount));
    }

    NArrow::TColumnFilter distinctFilter = NArrow::TColumnFilter::BuildAllowFilter();

    auto chunked = keyAccessor->GetChunkedArray();
    for (const auto& chunk : chunked->chunks()) {
        if (!chunk || chunk->length() == 0) {
            continue;
        }

        for (int64_t i = 0; i < chunk->length(); ++i) {
            const bool rowAllowed = !applyRowFilter || filterIterator->GetCurrentAcceptance();
            if (applyRowFilter) {
                // Last row may return false (iterator exhausted).
                filterIterator->Next(1);
            }
            if (!rowAllowed) {
                distinctFilter.Add(false);
                continue;
            }

            bool isNew = false;
            if (Seen.size() < Limit) {
                auto scalarRes = chunk->GetScalar(i);
                if (!scalarRes.ok()) {
                    // Fail-open: do not drop the row if Arrow failed to materialize a scalar (unexpected path).
                    distinctFilter.Add(true);
                    continue;
                }
                auto scalarPtr = std::move(scalarRes).ValueOrDie();
                isNew = Seen.emplace(std::move(scalarPtr)).second;
            }
            distinctFilter.Add(isNew);
        }
    }

    AFL_VERIFY(distinctFilter.GetRecordsCountVerified() == recordsCount);

    if (existing && applyRowFilter) {
        distinctFilter = existing->And(distinctFilter);
    }
    source->MutableStageResult().SetNotAppliedFilter(std::make_shared<NArrow::TColumnFilter>(std::move(distinctFilter)));
    source->GetContext()->GetCommonContext()->GetCounters().OnDistinctLimitSyncPointInvocation();

    if (Seen.size() >= Limit) {
        if (Collection) {
            Collection->Clear();
        }
    }

    return ESourceAction::ProvideNext;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
