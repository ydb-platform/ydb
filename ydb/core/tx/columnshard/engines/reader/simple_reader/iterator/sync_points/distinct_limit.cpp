#include "distinct_limit.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/abstract.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NSimple {

namespace {

static TString MakeKey(const bool isNull, const std::string_view value) {
    // Prefix avoids collision with real values; only up to Limit unique keys are kept.
    TString out;
    out.reserve(2 + value.size());
    out.push_back(isNull ? '0' : '1');
    out.push_back(':');
    out.append(value.data(), value.size());
    return out;
}

static TString MakeNullKey() {
    return MakeKey(true, {});
}

}   // namespace

ISyncPoint::ESourceAction TSyncPointDistinctLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/)
{
    if (Seen.size() >= Limit) {
        return ESourceAction::Finish;
    }

    AFL_VERIFY(source->HasStageResult());
    const auto& sr = source->GetStageResult();

    if (sr.IsEmpty()) {
        return ESourceAction::Finish;
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
        return ESourceAction::Finish;
    }

    const auto keyAccessor = batch->GetAccessorByNameOptional(std::string(columnName.data(), columnName.size()));
    if (!keyAccessor) {
        return ESourceAction::Finish;
    }

    const ui32 recordsCount = keyAccessor->GetRecordsCount();
    if (!recordsCount) {
        return ESourceAction::Finish;
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
                if (scalarRes.ok()) {
                    const auto& scalar = *scalarRes.ValueUnsafe();
                    if (!scalar.is_valid) {
                        isNew = Seen.emplace(MakeNullKey()).second;
                    } else {
                        const auto str = scalar.ToString();
                        isNew = Seen.emplace(MakeKey(false, std::string_view(str))).second;
                    }
                }
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
