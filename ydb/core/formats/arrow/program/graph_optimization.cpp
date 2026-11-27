#include "distinct_marker.h"
#include "assign_const.h"
#include "assign_internal.h"
#include "aggr_keys.h"
#include "filter.h"
#include "graph_optimization.h"
#include "header.h"
#include "index.h"
#include "original.h"
#include "reserve.h"
#include "short_circuit_logic.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>
#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <library/cpp/string_utils/quote/quote.h>
#include <memory>
#include <util/string/builder.h>
#include <util/string/escape.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA::NGraph::NOptimization {

void TGraphNode::AddDependencyTo(TGraphNode& to, const TDependency dependency) {
    YDB_LOG_VERIFY(this != &to);
    YDB_LOG_VERIFY(OutputDependencies.emplace(TAddress(to.GetIdentifier(), dependency), &to).second);
    YDB_LOG_VERIFY(to.InputDependencies.emplace(TAddress(GetIdentifier(), dependency), this).second);
}

void TGraphNode::RemoveDependencyTo(TGraphNode& to, const TDependency dependency) {
    YDB_LOG_VERIFY(OutputDependencies.erase(TAddress(to.GetIdentifier(), dependency)));
    YDB_LOG_VERIFY(to.InputDependencies.erase(TAddress(GetIdentifier(), dependency)));
}

void TGraph::AddDependency(TGraphNode& from, TGraphNode& to, const TDependency dependency) {
    from.AddDependencyTo(to, dependency);
}

void TGraph::RemoveDependency(TGraphNode& from, TGraphNode& to, const TDependency dependency) {
    from.RemoveDependencyTo(to, dependency);
}

void TGraph::RemoveNode(const ui32 idenitifier) {
    auto it = Nodes.find(idenitifier);
    YDB_LOG_VERIFY(it != Nodes.end());
    auto* node = it->second.get();
    while (!node->GetInputDependencies().empty()) {
        const auto& [address, from] = *node->GetInputDependencies().begin();
        RemoveDependency(*from, *node, address.GetDependency());
    }
    while (!node->GetOutputDependencies().empty()) {
        const auto& [address, to] = *node->GetOutputDependencies().begin();
        RemoveDependency(*node, *to, address.GetDependency());
    }
    Nodes.erase(it);
}

TGraph::TGraph(std::vector<std::shared_ptr<IProgramOperation>>&& processors, const IColumnResolver& resolver, const bool reserveIndexMemory)
    : ReserveIndexMemory(reserveIndexMemory)
    , Resolver(resolver) {
    NextResourceId = 0;
    for (auto&& i : processors) {
        for (auto&& input : i->GetInput()) {
            NextResourceId = std::max<TResourceId>(NextResourceId, input.GetColumnId());
        }
        for (auto&& input : i->GetOutput()) {
            NextResourceId = std::max<TResourceId>(NextResourceId, input.GetColumnId());
        }
    }
    ++NextResourceId;
    for (auto&& i : processors) {
        auto node = AddNode(i);
        for (auto&& output : i->GetOutput()) {
            YDB_LOG_VERIFY(Producers.emplace(output.GetColumnId(), node.get()).second);
        }
        for (auto&& input : i->GetInput()) {
            if (Producers.find(input.GetColumnId()) != Producers.end()) {
                continue;
            }
            const TString name = Resolver.GetColumnName(input.GetColumnId(), true);

            const IDataSource::TDataAddress dataAddr(
                input.GetColumnId(), Resolver.GetColumnName(input.GetColumnId()), NAccessor::NSubColumns::TCanonicalSubColumnName{});
            auto inputFetcher = AddNode(std::make_shared<TFetchOriginalDataOperation>(input.GetColumnId(), dataAddr));
            //            YDB_LOG_VERIFY(Producers.emplace(input.GetColumnId(), inputFetcher.get()).second);

            auto nodeInputAssembler =
                AddNode(std::make_shared<TAssembleOriginalDataOperation>(input.GetColumnId(), input.GetColumnId(), dataAddr));
            YDB_LOG_VERIFY(Producers.emplace(input.GetColumnId(), nodeInputAssembler.get()).second);
            AddDependency(*inputFetcher, *nodeInputAssembler, TDependency::Data(input.GetColumnId()));
        }
    }
    for (auto&& [_, i] : Nodes) {
        for (auto&& p : i->GetProcessor()->GetInput()) {
            if (i->GetProcessor()->GetProcessorType() == EProcessorType::AssembleOriginalData ||
                i->GetProcessor()->GetProcessorType() == EProcessorType::FetchOriginalData) {
                continue;
            }
            auto node = GetProducerVerified(p.GetColumnId());
            AddDependency(*node, *i, TDependency::Data(p.GetColumnId()));
        }
    }
}

TConclusion<bool> TGraph::FlattenNestedLogicalOperations(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::ShortCircuitLogic) {
        return false;
    }
    if (condNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    auto* nodeOwner = condNode->GetOutputDataDependencies().begin()->second;
    if (nodeOwner->GetProcessor()->GetProcessorType() != EProcessorType::ShortCircuitLogic) {
        return false;
    }
    auto childLogic = condNode->GetProcessorAs<TShortCircuitLogicOperation>();
    auto parentLogic = nodeOwner->GetProcessorAs<TShortCircuitLogicOperation>();
    if (childLogic->GetOperation() != parentLogic->GetOperation()) {
        return false;
    }
    for (auto&& [connectInfo, inputNode] : condNode->GetInputDataDependencies()) {
        AddDependency(*inputNode, *nodeOwner, connectInfo.GetDependency());
        nodeOwner->GetProcessor()->AddInput(connectInfo.GetDependency().GetResourceId());
    }
    nodeOwner->GetProcessor()->RemoveInput(condNode->GetOutputDataDependencies().begin()->first.GetDependency().GetResourceId());
    RemoveNode(condNode->GetIdentifier());
    return true;
}

TConclusion<bool> TGraph::OptimizeForFetchSubColumns(TGraphNode* condNode) {
    if (!condNode->Is(EProcessorType::AssembleOriginalData)) {
        return false;
    }
    if (condNode->GetProcessorAs<TAssembleOriginalDataOperation>()->GetDataAddress().HasSubColumns()) {
        return false;
    }
    auto originalAssemble = condNode->GetProcessorAs<TAssembleOriginalDataOperation>();
    std::vector<TGraphNode*> removeTo;
    std::vector<TResourceId> removeResourceId;
    THashMap<TResourceAddress, TGraphNode*> resourceProducers;
    for (auto&& [_, i] : condNode->GetOutputDataDependencies()) {
        if (!i->Is(EProcessorType::Calculation)) {
            continue;
        }
        auto addr = GetOriginalAddress(i);
        if (!addr) {
            continue;
        }
        if (!addr->GetSubColumnName()) {
            continue;
        }
        YDB_LOG_VERIFY(addr->GetColumnId() == originalAssemble->GetDataAddress().GetColumnId());
        auto it = resourceProducers.find(*addr);
        if (it == resourceProducers.end()) {
            const IDataSource::TDataAddress dataAddr(addr->GetColumnId(), Resolver.GetColumnName(addr->GetColumnId()), addr->GetSubColumnName());
            auto inputFetcher = AddNode(std::make_shared<TFetchOriginalDataOperation>(addr->GetColumnId(), dataAddr));
            auto nodeInputAssembler =
                AddNode(std::make_shared<TAssembleOriginalDataOperation>(addr->GetColumnId(), addr->GetColumnId(), dataAddr));
            AddDependency(*inputFetcher, *nodeInputAssembler, TDependency::Data(addr->GetColumnId()));
            it = resourceProducers.emplace(*addr, nodeInputAssembler.get()).first;
        }
        AddDependency(*it->second, *i, TDependency::Data(addr->GetColumnId()));
        removeTo.emplace_back(i);
        removeResourceId.emplace_back(addr->GetColumnId());
    }
    ui32 idx = 0;
    for (auto&& i : removeTo) {
        RemoveDependency(*condNode, *i, TDependency::Data(removeResourceId[idx++]));
    }
    if (condNode->GetOutputDependencies().empty()) {
        RemoveBranch(condNode, false);
    }
    return (bool)removeTo.size();
}

TConclusion<bool> TGraph::OptimizeMergeFetching(TGraphNode* baseNode) {
    if (!baseNode->GetOutputDependencies().empty()) {
        return false;
    }
    if (FetchersMerged.contains(baseNode->GetIdentifier())) {
        return false;
    }
    auto nodes = GetBranch(baseNode, true);
    std::vector<TGraphNode*> dataAddresses;
    std::vector<TGraphNode*> indexes;
    std::vector<TGraphNode*> headers;
    for (auto&& i : nodes) {
        if (!i.second->Is(EProcessorType::FetchOriginalData)) {
            continue;
        }
        if (!i.second->AddOptimizerMarker(EOptimizerMarkers::FetchMerged)) {
            continue;
        }
        if (i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetDataAddresses().size() +
                i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetIndexContext().size() +
                i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetHeaderContext().size() >
            1) {
            continue;
        }
        if (i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetDataAddresses().size()) {
            dataAddresses.emplace_back(i.second);
        }
        if (i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetIndexContext().size()) {
            indexes.emplace_back(i.second);
        }
        if (i.second->GetProcessorAs<TFetchOriginalDataOperation>()->GetHeaderContext().size()) {
            headers.emplace_back(i.second);
        }
    }
    bool changed = false;
    TGraphNode* nodeFetch = nullptr;
    if (dataAddresses.size() > 1) {
        THashSet<ui32> columnIds;
        for (auto&& i : dataAddresses) {
            columnIds.emplace(i->GetProcessorAs<TFetchOriginalDataOperation>()->GetOutputColumnIdOnce());
        }
        auto proc = std::make_shared<TFetchOriginalDataOperation>(std::vector<ui32>(columnIds.begin(), columnIds.end()));
        for (auto&& i : dataAddresses) {
            for (auto&& addr : i->GetProcessorAs<TFetchOriginalDataOperation>()->GetDataAddresses()) {
                proc->Add(addr.second);
            }
        }
        nodeFetch = AddNode(proc).get();
        FetchersMerged.emplace(nodeFetch->GetIdentifier());
        for (auto&& i : dataAddresses) {
            for (auto&& to : i->GetOutputDataDependencies()) {
                AddDependency(*nodeFetch, *to.second, to.first.GetDependency());
            }
            RemoveNode(i->GetIdentifier());
        }
        changed = true;
    } else if (dataAddresses.size() == 1) {
        nodeFetch = dataAddresses.front();
    }
    const auto attachReserveMemory = [&](TGraphNode* fetchNode) {
        std::shared_ptr<IMemoryCalculationPolicy> policy;
        if (baseNode->Is(EProcessorType::Filter) || baseNode->Is(EProcessorType::DistinctMarker)) {
            policy = std::make_shared<TFilterCalculationPolicy>();
        } else if (baseNode->Is(EProcessorType::Projection)) {
            policy = std::make_shared<TFetchingCalculationPolicy>();
        }
        YDB_LOG_VERIFY(policy);
        auto reserveMemory = std::make_shared<TReserveMemoryOperation>(*fetchNode->GetProcessorAs<TFetchOriginalDataOperation>(), policy);
        auto nodeReserve = AddNode(reserveMemory);
        AddDependency(*nodeReserve, *fetchNode, TDependency::Execution());
        changed = true;
    };
    if (nodeFetch) {
        attachReserveMemory(nodeFetch);
    }

    if (indexes.size() + headers.size() > 1) {
        THashSet<ui32> columnIds;
        for (auto&& i : indexes) {
            columnIds.emplace(i->GetProcessorAs<TFetchOriginalDataOperation>()->GetOutputColumnIdOnce());
        }
        for (auto&& i : headers) {
            columnIds.emplace(i->GetProcessorAs<TFetchOriginalDataOperation>()->GetOutputColumnIdOnce());
        }
        auto proc = std::make_shared<TFetchOriginalDataOperation>(std::vector<ui32>(columnIds.begin(), columnIds.end()));
        for (auto&& i : indexes) {
            for (auto&& addr : i->GetProcessorAs<TFetchOriginalDataOperation>()->GetIndexContext()) {
                proc->Add(addr.second);
            }
        }
        for (auto&& i : headers) {
            for (auto&& addr : i->GetProcessorAs<TFetchOriginalDataOperation>()->GetHeaderContext()) {
                proc->Add(addr.second);
            }
        }
        auto nodeFetch = AddNode(proc);
        for (auto&& i : indexes) {
            for (auto&& to : i->GetOutputDataDependencies()) {
                AddDependency(*nodeFetch, *to.second, to.first.GetDependency());
            }
            RemoveNode(i->GetIdentifier());
        }
        for (auto&& i : headers) {
            for (auto&& to : i->GetOutputDataDependencies()) {
                AddDependency(*nodeFetch, *to.second, to.first.GetDependency());
            }
            RemoveNode(i->GetIdentifier());
        }
        if (ReserveIndexMemory && !indexes.empty()) {
            attachReserveMemory(nodeFetch.get());
        }
        changed = true;
    } else if (ReserveIndexMemory && indexes.size() == 1 && headers.empty()) {
        attachReserveMemory(indexes.front());
    }
    return changed;
}

namespace {

TGraphNode* GetSingleInputProducer(const TGraphNode* node, const TResourceId resourceId) {
    TGraphNode* result = nullptr;
    for (const auto& [addr, inputNode] : node->GetInputDataDependencies()) {
        if (addr.GetDependency().GetResourceId() != resourceId) {
            continue;
        }
        if (result) {
            return nullptr;
        }
        result = inputNode;
    }
    return result;
}

}   // namespace

TGraphNode* TGraph::FindSingleSubColumnJsonKeyFetch(TGraphNode* markerNode, const ui32 dataColumnId) const {
    const auto marker = markerNode->GetProcessorAs<TDistinctMarkerOperation>();
    TGraphNode* calcNode = GetSingleInputProducer(markerNode, marker->GetKeyColumnId());
    if (!calcNode || !calcNode->Is(EProcessorType::Calculation)) {
        return nullptr;
    }
    const auto calc = calcNode->GetProcessorAs<TCalculationOperation>();
    if (!calc->GetKernelLogic() || calc->GetKernelLogic()->GetClassName() != TGetJsonPath::GetClassNameStatic()) {
        return nullptr;
    }
    if (calc->GetInput().size() != 2 || calc->GetOutput().size() != 1) {
        return nullptr;
    }
    TGraphNode* pathNode = GetSingleInputProducer(calcNode, calc->GetInput()[1].GetColumnId());
    if (!pathNode || !pathNode->Is(EProcessorType::Const)) {
        return nullptr;
    }
    // The data input must be the dedicated single sub-column assembler produced by OptimizeForFetchSubColumns.
    TGraphNode* assembleNode = GetSingleInputProducer(calcNode, calc->GetInput()[0].GetColumnId());
    if (!assembleNode || !assembleNode->Is(EProcessorType::AssembleOriginalData)) {
        return nullptr;
    }
    const auto& assembleAddr = assembleNode->GetProcessorAs<TAssembleOriginalDataOperation>()->GetDataAddress();
    if (assembleAddr.GetColumnId() != dataColumnId || assembleAddr.GetSubColumnNames(false).size() != 1) {
        return nullptr;
    }
    TGraphNode* fetchNode = GetSingleInputProducer(assembleNode, dataColumnId);
    if (!fetchNode || !fetchNode->Is(EProcessorType::FetchOriginalData)) {
        return nullptr;
    }
    const auto fetchProc = fetchNode->GetProcessorAs<TFetchOriginalDataOperation>();
    if (!fetchProc) {
        return nullptr;
    }
    const auto& fetchAddrs = fetchProc->GetDataAddresses();
    auto itAddr = fetchAddrs.find(dataColumnId);
    if (itAddr == fetchAddrs.end() || itAddr->second.GetSubColumnNames(false) != assembleAddr.GetSubColumnNames(false)) {
        return nullptr;
    }
    // Any other fetch of the same column (another JSON path, the whole column) would be merged with this one later
    // and inherit the dictionary-only flag: refuse unless this is the only fetch of the column.
    for (auto&& [_, n] : Nodes) {
        if (n.get() == fetchNode || !n->Is(EProcessorType::FetchOriginalData)) {
            continue;
        }
        const auto otherProc = n->GetProcessorAs<TFetchOriginalDataOperation>();
        if (!otherProc) {
            return nullptr;
        }
        if (otherProc->GetDataAddresses().contains(dataColumnId)) {
            return nullptr;
        }
    }
    return fetchNode;
}

bool TGraph::SetDictionaryOnlyOnFetch(const ui32 columnId) {
    TGraphNode* producer = GetProducerVerified(columnId);
    for (const auto& [addr, inputNode] : producer->GetInputDataDependencies()) {
        if (addr.GetDependency().GetResourceId() != columnId) {
            continue;
        }
        if (!inputNode->Is(EProcessorType::FetchOriginalData)) {
            break;
        }
        auto proc = inputNode->GetProcessorAs<TFetchOriginalDataOperation>();
        if (proc) {
            const auto& addrs = proc->GetDataAddresses();
            auto it = addrs.find(columnId);
            if (it != addrs.end() && !it->second.GetUseDictionaryOnly()) {
                proc->SetDictionaryOnlyForColumn(columnId);
                return true;
            }
        }
        break;
    }
    return false;
}

// Strict: only allow dictionary-only when the entire request needs exactly one data column, and either:
// - aggregation: that column is a group key and every aggregate is SOME(columnId), or
// - distinct marker: a stateless DISTINCT marker on that single column, or on JsonValue(column, path) computed
//   over the dedicated single sub-column fetch of that column (the sub-column dictionary values are the DISTINCT domain).
TConclusion<bool> TGraph::OptimizeForFetchDictionaryOnly(TGraphNode* node, const THashSet<ui32>& requiredDataColumnIds) {
    if (requiredDataColumnIds.size() != 1) {
        return false;
    }
    for (const auto& [_, graphNode] : Nodes) {
        if (graphNode->Is(EProcessorType::Filter)) {
            return false;
        }
    }

    const ui32 columnId = *requiredDataColumnIds.begin();

    if (node->Is(EProcessorType::DistinctMarker)) {
        const auto distinctMarker = node->GetProcessorAs<TDistinctMarkerOperation>();
        if (!distinctMarker) {
            return false;
        }
        if (distinctMarker->GetKeyColumnId() != columnId) {
            TGraphNode* fetchNode = FindSingleSubColumnJsonKeyFetch(node, columnId);
            if (!fetchNode) {
                return false;
            }
            auto proc = fetchNode->GetProcessorAs<TFetchOriginalDataOperation>();
            if (!proc) {
                return false;
            }
            const auto& addrs = proc->GetDataAddresses();
            auto it = addrs.find(columnId);
            if (it == addrs.end() || it->second.GetUseDictionaryOnly()) {
                // Already dictionary-only: report no graph change so the optimizer loop terminates.
                return false;
            }
            proc->SetDictionaryOnlyForColumn(columnId);
            return true;
        }
    } else if (node->Is(EProcessorType::Aggregation)) {
        const auto aggrProc = node->GetProcessorAs<NAggregation::TGroupedAggregationOperation>();
        if (!aggrProc) {
            return false;
        }

        // True only if *all* aggregations are exactly SOME(columnId)
        auto isKeyColumnOnlyUsedInSome = [aggrProc](const ui32 columnId) -> bool {
            bool usedInAggr = false;
            for (const auto& aggr : aggrProc->GetAggregations()) {
                // Every aggregation must be SOME with exactly one input
                if (aggr.GetAggregationId() != NAggregation::EAggregate::Some) {
                    return false;
                }
                const auto& inputs = aggr.GetInputs();
                if (inputs.size() != 1) {
                    return false;
                }
                if (inputs[0].GetColumnId() != columnId) {
                    return false;
                }
                usedInAggr = true;
            }
            return usedInAggr;
        };

        if (!isKeyColumnOnlyUsedInSome(columnId)) {
            return false;
        }

        THashSet<ui32> keyIds;
        for (const auto& k : aggrProc->GetAggregationKeys()) {
            keyIds.insert(k.GetColumnId());
        }
        if (!keyIds.contains(columnId)) {
            return false;
        }
    } else {
        return false;
    }

    return SetDictionaryOnlyOnFetch(columnId);
}

TConclusion<bool> TGraph::OptimizeIndexesToApply(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::CheckIndexData) {
        return false;
    }
    if (condNode->GetProcessorAs<TCheckIndexOperation>()->GetApplyToFilter()) {
        return false;
    }
    if (condNode->GetProcessor()->GetOutput().size() != 1) {
        return false;
    }
    if (condNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    const auto* dest = condNode->GetOutputDataDependencies().begin()->second;
    if (dest->GetProcessor()->GetProcessorType() != EProcessorType::ShortCircuitLogic) {
        return false;
    }
    if (dest->GetProcessorAs<TShortCircuitLogicOperation>()->GetOperation() != NKernels::EOperation::And) {
        return false;
    }
    if (dest->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    const auto* destDest = dest->GetOutputDataDependencies().begin()->second;
    if (destDest->GetProcessor()->GetProcessorType() == EProcessorType::Filter) {
        condNode->GetProcessorAs<TCheckIndexOperation>()->SetApplyToFilter();
        return true;
    }
    return false;
}

std::optional<TResourceAddress> TGraph::GetOriginalAddress(TGraphNode* condNode) const {
    if (condNode->GetProcessor()->GetProcessorType() == EProcessorType::AssembleOriginalData) {
        const auto proc = condNode->GetProcessorAs<TAssembleOriginalDataOperation>();
        if (proc->GetDataAddress().GetSubColumnNames(true).size() > 1) {
            return std::nullopt;
        }
        return TResourceAddress(proc->GetDataAddress().GetColumnId(), *proc->GetDataAddress().GetSubColumnNames(true).begin());
    } else if (condNode->GetProcessor()->GetProcessorType() == EProcessorType::Calculation) {
        const auto proc = condNode->GetProcessorAs<TCalculationOperation>();
        if (!proc->GetKernelLogic()) {
            return std::nullopt;
        }
        if (const auto inputIdx = proc->GetKernelLogic()->GetOriginalAddressFromInput()) {
            if (proc->GetInput().size() <= *inputIdx) {
                return std::nullopt;
            }
            return GetOriginalAddress(GetProducerVerified(proc->GetInput()[*inputIdx].GetColumnId()));
        }
        if (proc->GetKernelLogic()->GetClassName() == TGetJsonPath::GetClassNameStatic()) {
        } else if (proc->GetKernelLogic()->GetClassName() == TExistsJsonPath::GetClassNameStatic()) {
        } else {
            return std::nullopt;
        }
        if (proc->GetInput().size() != 2) {
            return std::nullopt;
        }
        auto nodeData = GetProducerVerified(proc->GetInput()[0].GetColumnId());
        auto nodePath = GetProducerVerified(proc->GetInput()[1].GetColumnId());
        if (nodeData->GetProcessor()->GetOutput().size() != 1) {
            return std::nullopt;
        }
        if (!nodeData->Is(EProcessorType::AssembleOriginalData)) {
            return std::nullopt;
        }
        if (!nodePath->Is(EProcessorType::Const)) {
            return std::nullopt;
        }
        auto constProc = nodePath->GetProcessorAs<TConstantOperation>();
        if (constProc->GetScalarConstant()->type->id() == arrow::utf8()->id() ||
            constProc->GetScalarConstant()->type->id() == arrow::binary()->id()) {
            return TResourceAddress(nodeData->GetProcessor()->GetOutput()[0].GetColumnId(),
                NAccessor::NSubColumns::TCanonicalSubColumnName::Parse(constProc->GetScalarConstant()->ToString()));
        } else {
            return std::nullopt;
        }
    } else {
        return std::nullopt;
    }
}

// ANDs one index check per hint into the kernel result. A check yields a constant column: all false
// when the index denies the probe, all true otherwise. A deny means no row of the portion contains
// the probe, and the kernel is false on every such row, so the AND changes no value. The rewrite
// moves the kernel's only output edge, hence the single-consumer requirement.
TConclusion<bool> TGraph::OptimizeConditionsForPatternHints(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    auto calc = condNode->GetProcessorAs<TCalculationOperation>();
    if (!calc->GetKernelLogic()) {
        return false;
    }
    const auto& hints = calc->GetKernelLogic()->GetIndexHints();
    if (hints.empty() || condNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    if (IndexesConstructed.contains(condNode->GetIdentifier())) {
        return false;
    }

    struct TResolved {
        TResourceAddress Addr;
        TIndexCheckOperation Operation;
        TString Value;
    };
    std::vector<TResolved> resolved;
    for (const auto& hint : hints) {
        if (hint.InputIndex >= calc->GetInput().size()) {
            continue;
        }
        auto* dataNode = GetProducerVerified(calc->GetInput()[hint.InputIndex].GetColumnId());
        if (auto addr = GetOriginalAddress(dataNode)) {
            resolved.push_back(TResolved{*addr, hint.Operation, hint.Value});
        }
    }
    if (resolved.empty()) {
        return false;
    }
    IndexesConstructed.emplace(condNode->GetIdentifier());

    auto* dest = condNode->GetOutputDataDependencies().begin()->second;
    const TResourceId destResourceId = condNode->GetOutputDataDependencies().begin()->first.GetDependency().GetResourceId();
    RemoveDependency(*condNode, *dest, TDependency::Data(destResourceId));

    std::vector<TResourceId> andInputs;
    std::vector<TGraphNode*> andSources;
    const TResourceId calcResourceId = BuildNextResourceId();
    RegisterProducer(calcResourceId, condNode);
    calc->SetOutputResourceIdOnce(calcResourceId);
    andInputs.push_back(calcResourceId);
    andSources.push_back(condNode);

    for (const auto& item : resolved) {
        const TResourceId constId = BuildNextResourceId();
        auto scalar = std::make_shared<arrow::BinaryScalar>(
            arrow::Buffer::FromString(std::string(item.Value.data(), item.Value.size())), arrow::binary());
        auto constNode = AddNode(std::make_shared<TConstantOperation>(scalar, constId));
        RegisterProducer(constId, constNode.get());

        const TResourceId fetchId = BuildNextResourceId();
        IDataSource::TFetchIndexContext fetchContext(item.Addr.GetColumnId(),
            IDataSource::TFetchIndexContext::TOperationsBySubColumn().Add(item.Addr.GetSubColumnName(), item.Operation));
        auto fetchNode = AddNode(std::make_shared<TFetchOriginalDataOperation>(fetchId, fetchContext));
        RegisterProducer(fetchId, fetchNode.get());

        const TResourceId checkId = BuildNextResourceId();
        const auto columnName = Resolver.GetColumnName(item.Addr.GetColumnId(), false);
        IDataSource::TCheckIndexContext checkContext(item.Addr.GetColumnId(), item.Addr.GetSubColumnName(), item.Operation,
            columnName.empty() ? "COLUMN_NAME_NOT_RESOLVED" : columnName);
        auto checkNode = AddNode(std::make_shared<TCheckIndexOperation>(fetchId, constId, checkContext, checkId));
        RegisterProducer(checkId, checkNode.get());
        AddDependency(*fetchNode, *checkNode, TDependency::Data(fetchId));
        AddDependency(*constNode, *checkNode, TDependency::Data(constId));
        andInputs.push_back(checkId);
        andSources.push_back(checkNode.get());
    }

    auto andNode = AddNode(std::make_shared<TShortCircuitLogicOperation>(
        TColumnChainInfo::BuildVector(andInputs), TColumnChainInfo(destResourceId), NKernels::EOperation::And));
    for (size_t i = 0; i < andInputs.size(); ++i) {
        AddDependency(*andSources[i], *andNode, TDependency::Data(andInputs[i]));
    }
    AddDependency(*andNode, *dest, TDependency::Data(destResourceId));
    ResetProducer(destResourceId, andNode.get());
    return true;
}

TConclusion<bool> TGraph::OptimizeConditionsForIndexes(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    auto calc = condNode->GetProcessorAs<TCalculationOperation>();
    if (!calc->GetKernelLogic()) {
        return false;
    }
    if (condNode->GetProcessor()->GetInput().size() != 2) {
        return false;
    }
    if (condNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    auto dataNode = GetProducerVerified(calc->GetInput().front().GetColumnId());
    auto constNode = GetProducerVerified(calc->GetInput().back().GetColumnId());
    if (constNode->GetProcessor()->GetProcessorType() != EProcessorType::Const) {
        return false;
    }
    if (!calc->GetKernelLogic()->IsBoolInResult()) {
        return false;
    }
    std::optional<TResourceAddress> dataAddr = GetOriginalAddress(dataNode);
    if (!dataAddr) {
        return false;
    }
    auto* dest = condNode->GetOutputDataDependencies().begin()->second;
    const TResourceId destResourceId = condNode->GetOutputDataDependencies().begin()->first.GetDependency().GetResourceId();
    auto indexChecker = calc->GetKernelLogic()->GetIndexCheckerOperation();
    if (!indexChecker) {
        return false;
    }
    if (!IndexesConstructed.emplace(condNode->GetIdentifier()).second) {
        return false;
    }
    RemoveDependency(*condNode, *dest, TDependency::Data(destResourceId));

    const TResourceId resourceIdxFetch = BuildNextResourceId();
    IDataSource::TFetchIndexContext indexContext(
        dataAddr->GetColumnId(), IDataSource::TFetchIndexContext::TOperationsBySubColumn().Add(dataAddr->GetSubColumnName(), *indexChecker));
    auto indexFetchProc = std::make_shared<TFetchOriginalDataOperation>(resourceIdxFetch, indexContext);
    auto indexFetchNode = AddNode(indexFetchProc);
    RegisterProducer(resourceIdxFetch, indexFetchNode.get());

    const TResourceId resourceIdIndexToAnd = BuildNextResourceId();
    auto resolvedColumnName = Resolver.GetColumnName(dataAddr->GetColumnId(), false);
    IDataSource::TCheckIndexContext checkIndexContext(dataAddr->GetColumnId(), dataAddr->GetSubColumnName(), *indexChecker,
        resolvedColumnName.empty() ? "COLUMN_NAME_NOT_RESOLVED" : resolvedColumnName);
    auto indexCheckProc = std::make_shared<TCheckIndexOperation>(
        resourceIdxFetch, constNode->GetProcessor()->GetOutputColumnIdOnce(), checkIndexContext, resourceIdIndexToAnd);
    auto indexProcNode = AddNode(indexCheckProc);
    RegisterProducer(resourceIdIndexToAnd, indexProcNode.get());
    AddDependency(*indexFetchNode, *indexProcNode, TDependency::Data(resourceIdxFetch));
    AddDependency(*constNode, *indexProcNode, TDependency::Data(constNode->GetProcessor()->GetOutputColumnIdOnce()));

    const TResourceId resourceIdEqToAnd = BuildNextResourceId();
    RegisterProducer(resourceIdEqToAnd, condNode);
    calc->SetOutputResourceIdOnce(resourceIdEqToAnd);

    auto andProcessor = std::make_shared<TShortCircuitLogicOperation>(
        TColumnChainInfo::BuildVector({ resourceIdEqToAnd, resourceIdIndexToAnd }), TColumnChainInfo(destResourceId), NKernels::EOperation::And);
    auto andNode = AddNode(andProcessor);
    AddDependency(*andNode, *dest, TDependency::Data(destResourceId));

    AddDependency(*indexProcNode, *andNode, TDependency::Data(resourceIdIndexToAnd));
    AddDependency(*condNode, *andNode, TDependency::Data(resourceIdEqToAnd));
    ResetProducer(destResourceId, andNode.get());
    return true;
}

TConclusion<bool> TGraph::OptimizeConditionsForHeadersCheck(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    if (condNode->GetProcessor()->GetInput().empty()) {
        return false;
    }
    auto calc = condNode->GetProcessorAs<TCalculationOperation>();
    if (condNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    auto* dest = condNode->GetOutputDataDependencies().begin()->second;
    const TResourceId destResourceId = condNode->GetOutputDataDependencies().begin()->first.GetDependency().GetResourceId();
    if (!calc->GetKernelLogic() || !calc->GetKernelLogic()->IsBoolInResult()) {
        return false;
    }
    auto* node = GetProducerVerified(condNode->GetProcessor()->GetInput()[0].GetColumnId());
    std::optional<TResourceAddress> dataAddr = GetOriginalAddress(node);
    if (!dataAddr) {
        return false;
    }
    if (!dataAddr->GetSubColumnName()) {
        return false;
    }

    if (!HeaderCheckConstructed.emplace(condNode->GetIdentifier()).second) {
        return false;
    }
    RemoveDependency(*condNode, *dest, TDependency::Data(destResourceId));

    const TResourceId resourceIdxFetch = BuildNextResourceId();
    IDataSource::TFetchHeaderContext headerContext(dataAddr->GetColumnId(), { dataAddr->GetSubColumnName() });
    auto indexFetchProc = std::make_shared<TFetchOriginalDataOperation>(resourceIdxFetch, headerContext);
    auto indexFetchNode = AddNode(indexFetchProc);
    RegisterProducer(resourceIdxFetch, indexFetchNode.get());

    const TResourceId resourceIdIndexToAnd = BuildNextResourceId();
    IDataSource::TCheckHeaderContext checkHeaderContext(dataAddr->GetColumnId(), dataAddr->GetSubColumnName());
    auto indexCheckProc = std::make_shared<TCheckHeaderOperation>(resourceIdxFetch, checkHeaderContext, resourceIdIndexToAnd);
    auto indexProcNode = AddNode(indexCheckProc);
    RegisterProducer(resourceIdIndexToAnd, indexProcNode.get());
    AddDependency(*indexFetchNode, *indexProcNode, TDependency::Data(resourceIdxFetch));

    const TResourceId resourceIdEqToAnd = BuildNextResourceId();
    RegisterProducer(resourceIdEqToAnd, condNode);
    calc->SetOutputResourceIdOnce(resourceIdEqToAnd);

    auto andProcessor = std::make_shared<TShortCircuitLogicOperation>(
        TColumnChainInfo::BuildVector({ resourceIdEqToAnd, resourceIdIndexToAnd }), TColumnChainInfo(destResourceId), NKernels::EOperation::And);
    auto andNode = AddNode(andProcessor);
    AddDependency(*andNode, *dest, TDependency::Data(destResourceId));

    AddDependency(*indexProcNode, *andNode, TDependency::Data(resourceIdIndexToAnd));
    AddDependency(*condNode, *andNode, TDependency::Data(resourceIdEqToAnd));
    ResetProducer(destResourceId, andNode.get());
    return true;
}

TConclusion<bool> TGraph::OptimizeFilterWithCoalesce(TGraphNode* cNode) {
    if (cNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    const auto calc = cNode->GetProcessorAs<TCalculationOperation>();
    if (!calc->GetKernelLogic()->GetYqlOperationId()) {
        return false;
    }
    if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetKernelLogic()->GetYqlOperationId() !=
        NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
        return false;
    }
    if (cNode->GetOutputDataDependenciesCount() != 1) {
        return false;
    }
    if (calc->GetInput().size() != 2) {
        return TConclusionStatus::Fail("incorrect coalesce incoming columns (!= 2) : " + ::ToString(calc->GetInput().size()));
    }
    TGraphNode* dataNode = GetProducerVerified(calc->GetInput()[0].GetColumnId());
    TGraphNode* argNode = GetProducerVerified(calc->GetInput()[1].GetColumnId());
    if (argNode->GetProcessor()->GetProcessorType() != EProcessorType::Const) {
        return false;
    }
    auto scalar = argNode->GetProcessorAs<TConstantOperation>()->GetScalarConstant();
    if (!scalar) {
        return TConclusionStatus::Fail("coalesce with null arg is impossible");
    }

    auto* nextNode = cNode->GetOutputDataDependencies().begin()->second;
    if (nextNode->GetProcessor()->GetProcessorType() != EProcessorType::Filter) {
        if (nextNode->GetProcessor()->GetProcessorType() != EProcessorType::ShortCircuitLogic) {
            return false;
        }
        const auto outputCalc = nextNode->GetProcessorAs<TShortCircuitLogicOperation>();
        if (outputCalc->GetOperation() != NKernels::EOperation::And) {
            return false;
        }
        if (nextNode->GetOutputDataDependenciesCount() != 1) {
            return false;
        }
    }
    if (scalar) {
        bool doOptimize = false;
        NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TScalar = typename arrow::TypeTraits<T>::ScalarType;
            auto& typedScalar = static_cast<const TScalar&>(*scalar);
            if constexpr (arrow::has_c_type<T>()) {
                doOptimize = (typedScalar.value == 0);
            }
            return true;
        });
        if (!doOptimize) {
            return false;
        }
    }
    nextNode->GetProcessor()->ExchangeInput(cNode->GetProcessor()->GetOutputColumnIdOnce(), dataNode->GetProcessor()->GetOutputColumnIdOnce());
    AddDependency(*dataNode, *nextNode, TDependency::Data(dataNode->GetProcessor()->GetOutputColumnIdOnce()));
    RemoveNode(cNode->GetIdentifier());
    if (argNode->IsDisconnected()) {
        RemoveNode(argNode->GetIdentifier());
    }
    return true;
}

TConclusionStatus TGraph::Collapse() {
    bool hasChanges = true;
    //    Cerr << DebugJson() << Endl;

    std::vector<TGraphNode*> filters;
    for (auto&& [_, n] : Nodes) {
        if (n->Is(EProcessorType::Filter)) {
            filters.emplace_back(n.get());
        }
    }
    if (filters.size() > 1) {
        const TResourceId finalResourceId = BuildNextResourceId();
        auto filterNode = AddNode(std::make_shared<TFilterOperation>(finalResourceId));
        std::vector<TResourceId> inputs;
        std::vector<TGraphNode*> inputNodes;
        for (auto&& i : filters) {
            inputs.emplace_back(i->GetProcessor()->GetInputColumnIdOnce());
            inputNodes.emplace_back(GetProducerVerified(i->GetProcessor()->GetInputColumnIdOnce()));
            RemoveNode(i->GetIdentifier());
        }
        auto mergeNode = AddNode(std::make_shared<TShortCircuitLogicOperation>(
            TColumnChainInfo::BuildVector(inputs), TColumnChainInfo(finalResourceId), NKernels::EOperation::And));
        ui32 idx = 0;
        for (auto&& i : inputNodes) {
            AddDependency(*i, *mergeNode, TDependency::Data(inputs[idx]));
            ++idx;
        }
        AddDependency(*mergeNode, *filterNode, TDependency::Data(finalResourceId));
        RegisterProducer(finalResourceId, mergeNode.get());
    }

    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            {
                auto conclusion = OptimizeFilterWithCoalesce(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }

            {
                auto conclusion = OptimizeConditionsForIndexes(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }

            {
                auto conclusion = OptimizeConditionsForPatternHints(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }

            //            {
            //                auto conclusion = OptimizeConditionsForHeadersCheck(n.get());
            //                if (conclusion.IsFail()) {
            //                    return conclusion;
            //                }
            //                if (*conclusion) {
            //                    hasChanges = true;
            //                    break;
            //                }
            //            }

            {
                auto conclusion = FlattenNestedLogicalOperations(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }
        }
    }

    hasChanges = true;
    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            auto conclusion = OptimizeForFetchSubColumns(n.get());
            if (conclusion.IsFail()) {
                return conclusion;
            }
            if (*conclusion) {
                hasChanges = true;
                break;
            }
        }
    }

    THashSet<ui32> requiredDataColumnIds = CollectRequiredDataColumnIdsFromGraph();
    hasChanges = true;
    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            auto conclusion = OptimizeForFetchDictionaryOnly(n.get(), requiredDataColumnIds);
            if (conclusion.IsFail()) {
                return conclusion;
            }
            if (*conclusion) {
                hasChanges = true;
                break;
            }
        }
    }

    {
        std::vector<TGraphNode*> nodesToOptimize;
        for (auto&& [_, n] : Nodes) {
            if (n->Is(EProcessorType::Filter) || n->Is(EProcessorType::DistinctMarker)) {
                nodesToOptimize.emplace_back(n.get());
            }
        }
        for (auto&& [_, n] : Nodes) {
            if (n->Is(EProcessorType::Projection)) {
                nodesToOptimize.emplace_back(n.get());
            }
        }
        for (auto&& i : nodesToOptimize) {
            auto conclusion = OptimizeMergeFetching(i);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
    }

    return TConclusionStatus::Success();
}

class TFilterChain {
private:
    YDB_READONLY_DEF(std::vector<const TGraphNode*>, Nodes);
    ui64 Weight = 0;

public:
    TFilterChain(const std::vector<const TGraphNode*>& nodes)
        : Nodes(nodes) {
        for (auto&& i : nodes) {
            Weight += i->GetProcessor()->GetWeight();
        }
    }

    bool operator<(const TFilterChain& item) const {
        return Weight < item.Weight;
    }
};

std::shared_ptr<NExecution::TCompiledGraph> TGraph::Compile() {
    return std::make_shared<NExecution::TCompiledGraph>(*this, Resolver);
}

THashMap<ui32, TGraphNode*> TGraph::GetBranch(TGraphNode* from, const bool backOnly) const {
    THashMap<ui32, TGraphNode*> nodeIdsResult;
    THashMap<ui32, TGraphNode*> current;
    current.emplace(from->GetIdentifier(), from);
    nodeIdsResult.emplace(from->GetIdentifier(), from);
    while (current.size()) {
        THashMap<ui32, TGraphNode*> next;
        for (auto&& [_, i] : current) {
            for (auto&& [_, e] : i->GetInputDependencies()) {
                if (nodeIdsResult.emplace(e->GetIdentifier(), e).second) {
                    next.emplace(e->GetIdentifier(), e);
                }
            }
            if (!backOnly) {
                for (auto&& [_, e] : i->GetOutputDependencies()) {
                    if (nodeIdsResult.emplace(e->GetIdentifier(), e).second) {
                        next.emplace(e->GetIdentifier(), e);
                    }
                }
            }
        }
        current = next;
    }
    return nodeIdsResult;
}

void TGraph::RemoveBranch(TGraphNode* from, const bool backOnly) {
    const auto nodes = GetBranch(from, backOnly);
    for (auto&& i : nodes) {
        RemoveNode(i.first);
    }
}

THashSet<ui32> TGraph::CollectRequiredDataColumnIdsFromGraph() const {
    THashSet<ui32> columnIds;
    for (auto&& [_, node] : Nodes) {
        if (!node->Is(EProcessorType::FetchOriginalData)) {
            continue;
        }
        const auto proc = node->GetProcessorAs<TFetchOriginalDataOperation>();
        for (const auto& [colId, _] : proc->GetDataAddresses()) {
            columnIds.insert(colId);
        }
    }
    return columnIds;
}

TString TResourceAddress::DebugString() const {
    if (SubColumnName) {
        return TStringBuilder() << "[" << ColumnId << "," << SubColumnName << "]";
    } else {
        return TStringBuilder() << "[" << ColumnId << "]";
    }
}

}   // namespace NKikimr::NArrow::NSSA::NGraph::NOptimization
