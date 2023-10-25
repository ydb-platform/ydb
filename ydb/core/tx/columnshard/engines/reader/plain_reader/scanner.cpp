#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NOlap::NPlainReader {

void TScanHead::OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 intervalIdx) {
    AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, batch).second);
    Y_ABORT_UNLESS(FetchingIntervals.size());
    while (FetchingIntervals.size()) {
        auto it = ReadyIntervals.find(FetchingIntervals.front().GetIntervalIdx());
        if (it == ReadyIntervals.end()) {
            break;
        }
        const std::shared_ptr<arrow::RecordBatch>& batch = it->second;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "interval_result")("interval", FetchingIntervals.front().GetIntervalIdx())("count", batch ? batch->num_rows() : 0);
        FetchingIntervals.pop_front();
        Reader.OnIntervalResult(batch);
        ReadyIntervals.erase(it);
    }
    if (FetchingIntervals.empty()) {
        AFL_VERIFY(ReadyIntervals.empty());
    }
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, TPlainReadData& reader)
    : Reader(reader)
    , Sources(std::move(sources))
{
    auto resultSchema = reader.GetReadMetadata()->GetLoadSchema(reader.GetReadMetadata()->GetSnapshot());
    for (auto&& f : reader.GetReadMetadata()->GetAllColumns()) {
        ResultFields.emplace_back(resultSchema->GetFieldByColumnIdVerified(f));
        ResultFieldNames.emplace_back(ResultFields.back()->name());
    }
    ResultSchema = std::make_shared<arrow::Schema>(ResultFields);
    DrainSources();
}

bool TScanHead::BuildNextInterval() {
    while (BorderPoints.size()) {
        auto firstBorderPointInfo = std::move(BorderPoints.begin()->second);
        bool includeStart = firstBorderPointInfo.GetStartSources().size();

        for (auto&& i : firstBorderPointInfo.GetStartSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("add_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSegments.emplace(i->GetSourceIdx(), i).second)("idx", i->GetSourceIdx());
        }

        if (firstBorderPointInfo.GetStartSources().size() && firstBorderPointInfo.GetFinishSources().size()) {
            includeStart = false;
            FetchingIntervals.emplace_back(
                BorderPoints.begin()->first, BorderPoints.begin()->first, SegmentIdxCounter++, CurrentSegments,
                *this, std::make_shared<NIndexedReader::TRecordBatchBuilder>(ResultFields), true, true);
            IntervalStats.emplace_back(CurrentSegments.size(), true);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "new_interval")("interval", FetchingIntervals.back().DebugJson());
        }

        for (auto&& i : firstBorderPointInfo.GetFinishSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("remove_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSegments.erase(i->GetSourceIdx()))("idx", i->GetSourceIdx());
        }

        CurrentStart = BorderPoints.begin()->first;
        BorderPoints.erase(BorderPoints.begin());
        if (CurrentSegments.size()) {
            Y_ABORT_UNLESS(BorderPoints.size());
            const bool includeFinish = BorderPoints.begin()->second.GetStartSources().empty();
            FetchingIntervals.emplace_back(
                *CurrentStart, BorderPoints.begin()->first, SegmentIdxCounter++, CurrentSegments,
                *this, std::make_shared<NIndexedReader::TRecordBatchBuilder>(ResultFields), includeFinish, includeStart);
            IntervalStats.emplace_back(CurrentSegments.size(), false);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "new_interval")("interval", FetchingIntervals.back().DebugJson());
            return true;
        } else {
            IntervalStats.emplace_back(CurrentSegments.size(), false);
        }

    }
    return false;
}

void TScanHead::DrainSources() {
    while (Sources.size()) {
        auto source = Sources.front();
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

std::shared_ptr<NKikimr::NOlap::NIndexedReader::TMergePartialStream> TScanHead::BuildMerger() const {
    return std::make_shared<NIndexedReader::TMergePartialStream>(Reader.GetReadMetadata()->GetReplaceKey(), ResultSchema, Reader.GetReadMetadata()->IsDescSorted());
}

}
