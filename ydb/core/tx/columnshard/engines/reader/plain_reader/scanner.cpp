#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NOlap::NPlainReader {

void TScanHead::OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 intervalIdx) {
    Y_VERIFY(FetchingIntervals.size());
    Y_VERIFY(FetchingIntervals.front().GetIntervalIdx() == intervalIdx);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "interval_result")("interval", FetchingIntervals.front().GetIntervalIdx())("count", batch ? batch->num_rows() : 0);
    FetchingIntervals.pop_front();
    Reader.OnIntervalResult(batch);
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, TPlainReadData& reader)
    : Reader(reader)
    , Sources(std::move(sources))
{
    auto resultSchema = reader.GetReadMetadata()->GetLoadSchema(reader.GetReadMetadata()->GetSnapshot());
    for (auto&& f : reader.GetReadMetadata()->GetAllColumns()) {
        ResultFields.emplace_back(resultSchema->GetFieldByColumnIdVerified(f));
    }
    Merger = std::make_shared<NIndexedReader::TMergePartialStream>(reader.GetReadMetadata()->GetReplaceKey(), std::make_shared<arrow::Schema>(ResultFields), reader.GetReadMetadata()->IsDescSorted());
    DrainSources();
}

bool TScanHead::BuildNextInterval() {
    while (BorderPoints.size()) {
        Y_VERIFY(FrontEnds.size());
        auto position = BorderPoints.begin()->first;
        auto firstBorderPointInfo = std::move(BorderPoints.begin()->second);
        const bool isIncludeStart = CurrentSegments.empty();

        for (auto&& i : firstBorderPointInfo.GetStartSources()) {
            CurrentSegments.emplace(i->GetSourceIdx(), i);
        }

        if (firstBorderPointInfo.GetStartSources().size() && firstBorderPointInfo.GetFinishSources().size()) {
            FetchingIntervals.emplace_back(
                BorderPoints.begin()->first, BorderPoints.begin()->first, SegmentIdxCounter++, CurrentSegments,
                *this, std::make_shared<NIndexedReader::TRecordBatchBuilder>(ResultFields), true, true);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "new_interval")("interval", FetchingIntervals.back().DebugJson());
        }

        for (auto&& i : firstBorderPointInfo.GetFinishSources()) {
            Y_VERIFY(CurrentSegments.erase(i->GetSourceIdx()));
        }

        const bool isFirstFinished = (position == *FrontEnds.begin());
        if (firstBorderPointInfo.GetFinishSources().size()) {
            Y_VERIFY(isFirstFinished);
            Y_VERIFY(FrontEnds.erase(position));
        } else {
            Y_VERIFY(!FrontEnds.erase(position));
        }

        if (isFirstFinished) {
            DrainSources();
        }
        CurrentStart = BorderPoints.begin()->first;
        BorderPoints.erase(BorderPoints.begin());
        if (CurrentSegments.size()) {
            Y_VERIFY(BorderPoints.size());
            const bool includeFinish = BorderPoints.begin()->second.GetFinishSources().size() == CurrentSegments.size() && !BorderPoints.begin()->second.GetStartSources().size();
            FetchingIntervals.emplace_back(
                *CurrentStart, BorderPoints.begin()->first, SegmentIdxCounter++, CurrentSegments,
                *this, std::make_shared<NIndexedReader::TRecordBatchBuilder>(ResultFields), includeFinish, isIncludeStart);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "new_interval")("interval", FetchingIntervals.back().DebugJson());
            return true;
        }

    }
    return false;
}

void TScanHead::DrainResults() {
    while (FetchingIntervals.size()) {
        if (!FetchingIntervals.front().HasMerger()) {
            FetchingIntervals.front().StartMerge(Merger);
        } else {
            break;
        }
    }
}

void TScanHead::DrainSources() {
    if (Sources.empty()) {
        return;
    }
    while (Sources.size() && (FrontEnds.empty() || Sources.front()->GetStart().Compare(*FrontEnds.begin()) != std::partial_ordering::greater)) {
        auto source = Sources.front();
        FrontEnds.emplace(source->GetFinish());
        BorderPoints[source->GetStart()].AddStart(source);
        BorderPoints[source->GetFinish()].AddFinish(source);
        Sources.pop_front();
    }
}

NKikimr::NOlap::TReadContext& TScanHead::GetContext() {
    return Reader.GetContext();
}

bool TScanHead::IsReverse() const {
    return Reader.GetReadMetadata()->IsDescSorted();
}

NKikimr::NOlap::NPlainReader::TFetchingPlan TScanHead::GetColumnsFetchingPlan(const bool exclusiveSource) const {
    return Reader.GetColumnsFetchingPlan(exclusiveSource);
}

}
