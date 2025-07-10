#include "constructors.h"

#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TSortedPortionsSources::DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) {
    while (HeapSources.size()) {
        bool usage = false;
        if (!cursor->CheckEntityIsBorder(HeapSources.front(), usage)) {
            std::pop_heap(HeapSources.begin(), HeapSources.end());
            HeapSources.pop_back();
            continue;
        }
        if (usage) {
            HeapSources.front().SetIsStartedByCursor();
        } else {
            std::pop_heap(HeapSources.begin(), HeapSources.end());
            HeapSources.pop_back();
        }
        break;
    }
}

std::vector<TInsertWriteId> TSortedPortionsSources::GetUncommittedWriteIds() const {
    std::vector<TInsertWriteId> result;
    for (auto&& i : HeapSources) {
        if (!i.GetPortion()->IsCommitted()) {
            AFL_VERIFY(i.GetPortion()->GetPortionType() == EPortionType::Written);
            auto* written = static_cast<const TWrittenPortionInfo*>(i.GetPortion().get());
            result.emplace_back(written->GetInsertWriteId());
        }
    }
    return result;
}

std::shared_ptr<TPortionDataSource> TSourceConstructor::Construct(
    const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context) const {
    auto result = std::make_shared<TPortionDataSource>(sourceIdx, Portion, context);
    if (IsStartedByCursorFlag) {
        result->SetIsStartedByCursor();
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, result->AddEvent("s"));
    return result;
}

std::vector<TInsertWriteId> TNotSortedPortionsSources::GetUncommittedWriteIds() const {
    std::vector<TInsertWriteId> result;
    for (auto&& i : Sources) {
        if (!i.GetPortion()->IsCommitted()) {
            AFL_VERIFY(i.GetPortion()->GetPortionType() == EPortionType::Written);
            auto* written = static_cast<const TWrittenPortionInfo*>(i.GetPortion().get());
            result.emplace_back(written->GetInsertWriteId());
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
