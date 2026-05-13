#include "distinct_limit.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/abstract.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NSimple {

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
    if (!keyAccessor) {
        return ESourceAction::Finish;
    }

    const ui32 recordsCount = keyAccessor->GetRecordsCount();
    if (!recordsCount) {
        return ESourceAction::ProvideNext;
    }

    NArrow::TColumnFilter distinctFilter = NArrow::TColumnFilter::BuildAllowFilter();

    auto chunked = keyAccessor->GetChunkedArray();
    for (const auto& chunk : chunked->chunks()) {
        if (!chunk || chunk->length() == 0) {
            continue;
        }

        for (int64_t i = 0; i < chunk->length(); ++i) {
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

    if (auto existing = source->GetStageResult().GetNotAppliedFilter()) {
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

}   // namespace NKikimr::NOlap::NReader::NSimple
