#include "distinct_limit.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISyncPoint::ESourceAction TSyncPointDistinctLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/)
{
    if (FetchedDistinct >= Limit) {
        return ESourceAction::Finish;
    }

    AFL_VERIFY(source->HasStageResult());
    const auto& sr = source->GetStageResult();

    if (sr.IsEmpty()) {
        return ESourceAction::Finish;
    }

    const ui32 sourceIdx = source->GetSourceIdx();
    ui32 rows = 0;
    if (sr.HasResultChunk()) {
        if (!SourcesWithFullBatchDistinctCount.contains(sourceIdx)) {
            rows = sr.GetResultChunkRowsCount();
        }
    } else {
        rows = source->GetFilteredRowsCount();
        SourcesWithFullBatchDistinctCount.insert(sourceIdx);
    }

    FetchedDistinct += rows;

    if (FetchedDistinct >= Limit) {
        // Stop producing new sources; allow current source to be sent further (it may overshoot limit).
        if (Collection) {
            Collection->Clear();
        }
    }

    return ESourceAction::ProvideNext;
}

} // namespace NKikimr::NOlap::NReader::NSimple

