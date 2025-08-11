#include "execution.h"
#include "original.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TOriginalColumnDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto source = context.GetDataSource().lock();
    if (!source) {
        return TConclusionStatus::Fail("source was destroyed before (original fetch start)");
    }
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
        auto conclusion = source->StartFetchData(context, i.SelectSubColumns(subColumnsToFetch));
        if (conclusion.IsFail()) {
            return conclusion;
        } else if (!!conclusion.GetResult()) {
            logic.emplace_back(conclusion.DetachResult());
        } else {
            continue;
        }
    }

    for (auto&& [_, i] : IndexContext) {
        auto conclusion = source->StartFetchIndex(context, i);
        if (conclusion.IsFail()) {
            return conclusion;
        } else {
            logic.insert(logic.end(), conclusion.GetResult().begin(), conclusion.GetResult().end());
        }
    }
    for (auto&& [_, i] : HeaderContext) {
        if (context.GetResources().GetAccessorOptional(i.GetColumnId())) {
            continue;
        }
        auto conclusion = source->StartFetchHeader(context, i);
        if (conclusion.IsFail()) {
            return conclusion;
        } else if (!!conclusion.GetResult()) {
            logic.emplace_back(conclusion.DetachResult());
        } else {
            continue;
        }
    }

    auto conclusion = source->StartFetch(context, logic);
    if (conclusion.IsFail()) {
        return conclusion;
    } else if (!*conclusion) {
        return IResourceProcessor::EExecutionResult::Success;
    } else {
        return EExecutionResult::InBackground;
    }
}

TConclusion<IResourceProcessor::EExecutionResult> TOriginalColumnAccessorProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    const auto acc = context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce());
    for (auto&& sc : DataAddress.GetSubColumnNames(true)) {
        if (!acc || !acc->HasSubColumnData(sc)) {
            auto source = context.GetDataSource().lock();
            if (!source) {
                return TConclusionStatus::Fail("source was destroyed before (original assemble start)");
            }
            source->AssembleAccessor(context, GetOutputColumnIdOnce(), sc);
        }
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
