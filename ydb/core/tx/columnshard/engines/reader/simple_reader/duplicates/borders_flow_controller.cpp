#include "borders_flow_controller.h"
#include "executor.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TBordersFlowController::TBordersFlowController(const std::shared_ptr<TMergeContext>& mergeContext, const std::deque<std::shared_ptr<TPortionInfo>>& portions, const TReadMetadataBase::TConstPtr& readMetadata, const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters)
    : MergeContext(mergeContext)
    , Counters(counters)
    , ReadMetadata(readMetadata) {
    for (const auto& portion : portions) {
        Borders[NCommon::TReplaceKeyAdapter::BuildStart(*portion, *ReadMetadata)].Start.push_back(portion->GetPortionId());
        Borders[NCommon::TReplaceKeyAdapter::BuildFinish(*portion, *ReadMetadata)].Finish.push_back(portion->GetPortionId());
    }
    BuildExclusivePortions();
    Counters->OnLeftBorders(Borders.size());
}

void TBordersFlowController::BuildExclusivePortions() {
    size_t openIntervals = 0;

    for (auto it = Borders.begin(); it != Borders.end();) {
        auto currentIt = it;
        ++it;

        if (openIntervals == 0 && ((currentIt->second.Start.size() == 1 && currentIt->second.Finish.size() == 1) || (it == Borders.end() || (it->second.Finish.size() == 1 && it->second.Start.empty())))) {
            if (currentIt->second.Start.size() == 1) {
                ExclusivePortions.insert(currentIt->second.Start.front());
            }
        }

        openIntervals += currentIt->second.Start.size();
        openIntervals -= currentIt->second.Finish.size();
    }

    for (auto it = Borders.begin(); it != Borders.end();) {
        if (it->second.Start.size() == 1 && it->second.Finish.size() == 0 && ExclusivePortions.count(it->second.Start.front())) {
            it = Borders.erase(it);
            continue;
        }

        if (it->second.Start.size() == 0 && it->second.Finish.size() == 1 && ExclusivePortions.count(it->second.Finish.front())) {
            it = Borders.erase(it);
            continue;
        }

        ++it;
    }
}

bool TBordersFlowController::IsExclusiveInterval(const ui64 portionId) const {
    return ExclusivePortions.find(portionId) != ExclusivePortions.end();
}

TBordersIterator TBordersFlowController::Next(const std::shared_ptr<const TPortionInfo>& portion) {
    auto border = NCommon::TReplaceKeyAdapter::BuildFinish(*portion, *ReadMetadata);
    TBordersIteratorBuilder builder;
    ui32 oldWaitingBordersSize = WaitingBorders.size();
    for (auto it = Borders.begin(); it != Borders.end() && it->first <= border; it = Borders.erase(it)) {
        WaitingBorders.insert(it->first);
        builder.AppendBorder(TBorder{std::make_shared<NArrow::NMerger::TSortableBatchPosition>(it->first.GetValue().BuildSortablePosition()), it->second.Start});
    }
    Counters->OnWaitingBorders(WaitingBorders.size() - oldWaitingBordersSize);
    return builder.Build();
}

TString TBordersFlowController::DebugString() {
    TStringBuilder sb;
    sb << "{";
    sb << "Borders=" << Borders.size() << ";";
    sb << "WaitingBorders=" << WaitingBorders.size() << ";";
    sb << "ReadyBorders=" << ReadyBorders.size() << ";";
    sb << "BordersQueue=" << BordersQueue.size() << ";";
    sb << "Reverse=" << IsReversed() << ";";
    sb << "InFlight=" << IsInflight << ";";
    sb << "}";
    return sb;
}

void TBordersFlowController::AddBatch(const TBordersBatch& batch) {
    for (const auto& border : batch.GetBorders()) {
        auto borderKey = NCommon::TReplaceKeyAdapter(NArrow::TSimpleRow{border.GetKey()->MakeRecordBatch(), 0}, IsReversed());
        AFL_VERIFY(WaitingBorders.erase(borderKey) == 1);
        AFL_VERIFY(ReadyBorders.insert(borderKey).second);
    }
    Counters->OnWaitingBorders(-1 * static_cast<i64>(batch.GetBorders().size()));
    Counters->OnReadyBorders(static_cast<i64>(batch.GetBorders().size()));
}

std::optional<NArrow::TSimpleRow> TBordersFlowController::NextReadyBorder() {
    if (ReadyBorders.empty()) {
        return std::nullopt;
    }

    if (auto it = ReadyBorders.begin(); WaitingBorders.empty() || *it < *WaitingBorders.begin()) {
        auto result = *it;
        ReadyBorders.erase(it);
        Counters->OnReadyBorders(-1);
        return result.GetValue();
    }

    return std::nullopt;
}

TBordersFlowController::~TBordersFlowController() {
    Counters->OnLeftBorders(-1 * static_cast<i64>(Borders.size()));
    Counters->OnWaitingBorders(-1 * static_cast<i64>(WaitingBorders.size()));
    Counters->OnReadyBorders(-1 * static_cast<i64>(ReadyBorders.size()));
}

bool TBordersFlowController::IsReversed() const {
    return ReadMetadata->IsDescSorted();
}

void TBordersFlowController::DrainQueue() {
    if (IsInflight || BordersQueue.empty()) {
        return;
    }
    auto ev = BordersQueue.front();
    BordersQueue.pop_front();
    AddBatch(ev->Get()->Context.GetBatch());
    std::vector<NArrow::TSimpleRow> readyBorders;
    while (auto readyBorder = NextReadyBorder()) {
        readyBorders.push_back(*readyBorder);
    }
    const std::shared_ptr<TMergeBorders> task = std::make_shared<TMergeBorders>(ev.Get()->Recipient, MergeContext, ev, readyBorders);
    NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
    IsInflight = true;
}

void TBordersFlowController::OnReadyMergeBorders() {
    IsInflight = false;
    DrainQueue();
}

void TBordersFlowController::Enqueue(const TEvBordersConstructionResult::TPtr& event) {
    BordersQueue.push_back(event);
    DrainQueue();
}

}
