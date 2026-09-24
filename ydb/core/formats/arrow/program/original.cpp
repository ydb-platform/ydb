#include "execution.h"
#include "original.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<TExecutionResult> TOriginalColumnDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto& source = context.GetDataSource();
    THashSet<uint32_t> uniqueEntityIds;
    std::vector<std::shared_ptr<IFetchLogic>> logic;
    for (auto&& [_, i] : DataAddresses) {
        auto acc = context.GetResources().GetAccessorOptional(i.GetColumnId());
        THashSet<TString> subColumnsToFetch;
        for (auto&& sc : i.GetSubColumnNames(true)) {
            if (!acc || !acc->HasSubColumnData(sc)) {
                if (!sc && acc) {
                    context.MutableResources().Remove(i.GetColumnId());
                }
                subColumnsToFetch.emplace(sc);
            }
        }
        if (subColumnsToFetch.empty()) {
            continue;
        }
        auto conclusion = source.StartFetchData(context, i.SelectSubColumns(subColumnsToFetch));
        if (conclusion.IsFail()) {
            return conclusion;
        } else if (!!conclusion.GetResult()) {
            auto entityId = conclusion.GetResult()->GetEntityId();
            auto [_, emplaced] = uniqueEntityIds.emplace(entityId);
            if (!emplaced) {
                return TConclusionStatus::Fail(TStringBuilder{} << "Try to process the same entity id (data) " << entityId << " twice");
            }
            logic.emplace_back(conclusion.DetachResult());
        } else {
            continue;
        }
    }

    for (auto&& [_, i] : IndexContext) {
        auto conclusion = source.StartFetchIndex(context, i);
        if (conclusion.IsFail()) {
            return conclusion;
        } else {
            for (const auto& index: conclusion.GetResult()) {
                auto entityId = index->GetEntityId();
                auto [_, emplaced] = uniqueEntityIds.emplace(entityId);
                if (!emplaced) {
                    return TConclusionStatus::Fail(TStringBuilder{} << "Try to process the same entity id (index) " << entityId << " twice");
                }
            }
            logic.insert(logic.end(), conclusion.GetResult().begin(), conclusion.GetResult().end());
        }
    }
    for (auto&& [_, i] : HeaderContext) {
        if (context.GetResources().GetAccessorOptional(i.GetColumnId())) {
            continue;
        }
        auto conclusion = source.StartFetchHeader(context, i);
        if (conclusion.IsFail()) {
            return conclusion;
        } else if (!!conclusion.GetResult()) {
            auto entityId = conclusion.GetResult()->GetEntityId();
            auto [_, emplaced] = uniqueEntityIds.emplace(entityId);
            if (!emplaced) {
                return TConclusionStatus::Fail(TStringBuilder{} << "Try to process the same entity id (header) " << entityId << " twice");
            }
            logic.emplace_back(conclusion.DetachResult());
        } else {
            continue;
        }
    }

    return source.StartFetch(context, logic);
}

TConclusion<TExecutionResult> TOriginalColumnAccessorProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    const auto acc = context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce());
    for (auto&& sc : DataAddress.GetSubColumnNames(true)) {
        if (!acc || !acc->HasSubColumnData(sc)) {
            auto& source = context.GetDataSource();
            auto conclusion = source.AssembleAccessor(context, GetOutputColumnIdOnce(), sc);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
    }
    return TExecutionResult::Done();
}

}   // namespace NKikimr::NArrow::NSSA
