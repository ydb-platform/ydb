#include "context.h"
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

std::shared_ptr<NKikimr::NOlap::NIndexedReader::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_shared<NIndexedReader::TMergePartialStream>(ReadMetadata->GetReplaceKey(), ResultSchema, CommonContext->IsReverse());
}

ui64 TSpecialReadContext::GetMemoryForSources(const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive) {
    auto fetchingPlan = GetColumnsFetchingPlan(isExclusive);
    ui64 result = 0;
    for (auto&& i : sources) {
        AFL_VERIFY(i.second->GetIntervalsCount());
        result += i.second->GetRawBytes(fetchingPlan.GetFilterStage()->GetColumnIds()) / i.second->GetIntervalsCount();
        result += i.second->GetRawBytes(fetchingPlan.GetFetchingStage()->GetColumnIds()) / i.second->GetIntervalsCount();
    }
    return result;
}

}
