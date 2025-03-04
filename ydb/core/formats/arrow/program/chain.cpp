#include "chain.h"
#include "collection.h"
#include "graph.h"

namespace NKikimr::NArrow::NSSA {

namespace {
class TColumnUsage {
private:
    YDB_READONLY_DEF(std::optional<ui32>, FirstUsage);
    YDB_READONLY_DEF(std::optional<ui32>, LastUsage);
    YDB_READONLY_DEF(std::optional<ui32>, Construction);
    YDB_READONLY_DEF(std::shared_ptr<IResourceProcessor>, Processor);

    TColumnUsage(const std::shared_ptr<IResourceProcessor>& processor)
        : Processor(processor) {
    }

public:
    static TColumnUsage Construct(const ui32 stepIdx, const std::shared_ptr<IResourceProcessor>& processor) {
        TColumnUsage result(processor);
        result.Construction = stepIdx;
        return result;
    }

    static TColumnUsage Fetch(const ui32 stepIdx, const std::shared_ptr<IResourceProcessor>& processor) {
        TColumnUsage result(processor);
        result.FirstUsage = stepIdx;
        result.LastUsage = stepIdx;
        return result;
    }

    void SetLastUsage(const ui32 stepIdx) {
        AFL_VERIFY(!LastUsage || *LastUsage <= stepIdx)("last", LastUsage)("current", stepIdx);
        if (!FirstUsage) {
            FirstUsage = stepIdx;
        }
        LastUsage = stepIdx;
    }
};
}   // namespace

TConclusion<TProgramChain> TProgramChain::Build(std::vector<std::shared_ptr<IResourceProcessor>>&& processorsExt, const IColumnResolver& resolver) {
    NOptimization::TGraph graph(std::move(processorsExt), resolver);
    auto conclusion = graph.Collapse();
    if (conclusion.IsFail()) {
        return conclusion;
    }
    auto processorsConclusion = graph.BuildChain();
    if (processorsConclusion.IsFail()) {
        return processorsConclusion;
    }
    auto processors = processorsConclusion.DetachResult();
    THashMap<ui32, TColumnUsage> contextUsage;
    ui32 stepIdx = 0;
    THashSet<ui32> sourceColumns;
    std::optional<ui32> lastFilter;
    std::optional<ui32> firstAggregation;
    std::vector<std::vector<TColumnChainInfo>> originalsToUse;
    originalsToUse.resize(processors.size());
    for (auto&& i : processors) {
        if (!firstAggregation && i->IsAggregation()) {
            firstAggregation = stepIdx;
        }
        if (!firstAggregation && i->GetProcessorType() == EProcessorType::Filter) {
            lastFilter = stepIdx;
        }
        for (auto&& c : i->GetOutput()) {
            auto it = contextUsage.find(c.GetColumnId());
            if (it != contextUsage.end()) {
                AFL_VERIFY(false);
            } else {
                contextUsage.emplace(c.GetColumnId(), TColumnUsage::Construct(stepIdx, i));
            }
        }
        for (auto&& c : i->GetInput()) {
            auto it = contextUsage.find(c.GetColumnId());
            const bool isOriginalColumn = resolver.HasColumn(c.GetColumnId());
            if (isOriginalColumn) {
                originalsToUse[stepIdx].emplace_back(c);
            }
            if (it == contextUsage.end()) {
                if (!isOriginalColumn) {
                    return TConclusionStatus::Fail("incorrect input column: " + c.DebugString());
                }
                it = contextUsage.emplace(c.GetColumnId(), TColumnUsage::Fetch(stepIdx, i)).first;
                sourceColumns.emplace(c.GetColumnId());
            } else {
                it->second.SetLastUsage(stepIdx);
            }
        }
        ++stepIdx;
    }

    std::vector<std::vector<TColumnChainInfo>> columnsToFetch;
    columnsToFetch.resize(processors.size());
    std::vector<std::vector<TColumnChainInfo>> columnsToDrop;
    columnsToDrop.resize(processors.size());
    for (auto&& ctx : contextUsage) {
        if (!ctx.second.GetLastUsage() && ctx.second.GetProcessor()->GetProcessorType() != EProcessorType::Const) {
            return TConclusionStatus::Fail(
                "not used column in program: " + ::ToString(ctx.first) + ", original_name=" + resolver.GetColumnName(ctx.first, false));
        }
        if (!ctx.second.GetConstruction()) {
            columnsToFetch[ctx.second.GetFirstUsage().value_or(0)].emplace_back(ctx.first);
        }
        if (ctx.second.GetLastUsage().value_or(0) + 1 < processors.size()) {
            columnsToDrop[ctx.second.GetLastUsage().value_or(0)].emplace_back(ctx.first);
        }
    }
    TProgramChain result;
    for (ui32 i = 0; i < processors.size(); ++i) {
        result.Processors.emplace_back(
            std::move(columnsToFetch[i]), std::move(originalsToUse[i]), std::move(processors[i]), std::move(columnsToDrop[i]));
    }
    auto initStatus = result.Initialize();
    result.LastOriginalDataFilter = lastFilter;
    result.FirstAggregation = firstAggregation;
    if (initStatus.IsFail()) {
        return initStatus;
    }
    return result;
}

NJson::TJsonValue TProgramChain::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonArr = result.InsertValue("processors", NJson::JSON_ARRAY);
    for (auto&& i : Processors) {
        jsonArr.AppendValue(i.DebugJson());
    }
    return result;
}

TConclusionStatus TProgramChain::Initialize() {
    for (auto&& i : Processors) {
        for (auto&& cInput : i->GetInput()) {
            auto itSources = SourcesByColumnId.find(cInput.GetColumnId());
            if (itSources == SourcesByColumnId.end()) {
                itSources = SourcesByColumnId.emplace(cInput.GetColumnId(), THashSet<ui32>({ cInput.GetColumnId() })).first;
                SourceColumns.emplace(cInput.GetColumnId());
            }
            if (i->GetProcessorType() == EProcessorType::Filter) {
                FilterColumns.insert(itSources->second.begin(), itSources->second.end());
            }
        }
        for (auto&& cOut : i->GetOutput()) {
            auto [itOut, inserted] = SourcesByColumnId.emplace(cOut.GetColumnId(), THashSet<ui32>());
            if (!inserted) {
                return TConclusionStatus::Fail("output column duplication: " + ::ToString(cOut.GetColumnId()));
            }
            for (auto&& cInput : i->GetInput()) {
                auto itSources = SourcesByColumnId.find(cInput.GetColumnId());
                AFL_VERIFY(itSources != SourcesByColumnId.end());
                itOut->second.insert(itSources->second.begin(), itSources->second.end());
            }
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramChain::Apply(const std::shared_ptr<TAccessorsCollection>& resources) const {
    for (auto&& i : Processors) {
        auto status = i->Execute(resources, i);
        if (status.IsFail()) {
            return status;
        }
        resources->Remove(i.GetColumnsToDrop(), true);
        if (resources->IsEmptyFiltered()) {
            resources->Clear();
            break;
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NSSA
