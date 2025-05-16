#include "cbo_interesting_orderings.h"

#include <library/cpp/disjoint_sets/disjoint_sets.h>

#include <util/string/builder.h>
#include <util/string/join.h>

#include <bit>
#include <set>
#include <unordered_set>
#include <functional>

namespace NYql::NDq {

bool TOrdering::operator==(const TOrdering& other) const {
    return std::tie(this->Type, this->Items) == std::tie(other.Type, other.Items);
}

TString TOrdering::ToString() const {
    return "{" + JoinSeq(", ", Items) + "}";
}

bool TFunctionalDependency::IsEquivalence() const {
    return Type == EType::EEquivalence;
}

bool TFunctionalDependency::MatchesAntecedentItems(const TOrdering& ordering) const {
    if (ordering.Items.size() < AntecedentItems.size()) {
        return false;
    }

    for (std::size_t i = 0; i < AntecedentItems.size(); ++i) {
        if (ordering.Items[i] != AntecedentItems[i]) {
            return false;
        }
    }

    return true;
}

TString TFunctionalDependency::ToString() const {
    TStringBuilder ss;
    ss << "{" + JoinSeq(", ", AntecedentItems) + "}";

    if (Type == EEquivalence) {
        ss << " = ";
    } else {
        ss << " -> ";
    }
    ss << ConsequentItem;

    return ss;
}

void TTableAliasMap::AddMapping(const TString& table, const TString& alias) {
    TableByAlias[alias] = table;
}

void TTableAliasMap::AddRename(const TString& from, const TString& to) {
    if (auto pointIdx = from.find('.'); pointIdx != TString::npos) {
        TString alias = from.substr(0, pointIdx);
        TString baseTable = GetBaseTableByAlias(alias);
        TString columnName = from.substr(pointIdx + 1);

        if (auto it = BaseColumnByRename.find(columnName); it != BaseColumnByRename.end()) {
            auto baseColumn = it->second;
            BaseColumnByRename[to] = BaseColumnByRename[from] = it->second;
            return;
        }

        auto fromColumn = TBaseColumn(alias, columnName);
        auto baseColumn = TBaseColumn(baseTable, columnName);

        BaseColumnByRename[to] = BaseColumnByRename[from] = baseColumn;
        return;
    }

    if (BaseColumnByRename.contains(from)) {
        BaseColumnByRename[to] = BaseColumnByRename[from];
    }
}

TTableAliasMap::TBaseColumn TTableAliasMap::GetBaseColumnByRename(const TString& renamedColumn) {
    if (BaseColumnByRename.contains(renamedColumn)) {
        return BaseColumnByRename[renamedColumn];
    }

    if (auto pointIdx = renamedColumn.find('.'); pointIdx != TString::npos) {
        TString alias = renamedColumn.substr(0, pointIdx);
        TString column = renamedColumn.substr(pointIdx + 1);
        if (auto baseTable = GetBaseTableByAlias(alias)) {
            return TBaseColumn(std::move(baseTable), std::move(column));
        }
        return TBaseColumn(std::move(alias), std::move(column));
    }

    return TBaseColumn("", renamedColumn);
}

TTableAliasMap::TBaseColumn TTableAliasMap::GetBaseColumnByRename(const NDq::TJoinColumn& renamedColumn) {
    return GetBaseColumnByRename(renamedColumn.RelName + "." + renamedColumn.AttributeName);
}

TString TTableAliasMap::ToString() const {
    TString result;

    if (!BaseColumnByRename.empty()) {
        result += "Renames: ";
        for (const auto& [from, to] : BaseColumnByRename) {
            result += from + "->" + to.Relation + "." + to.Column + " ";
        }
        result.pop_back();
        result += ", ";
    }

    result += "TableAliases: ";
    for (const auto& [alias, table] : TableByAlias) {
        result += alias + "->" + table + ", ";
    }
    result.pop_back();

    return result;
}

void TTableAliasMap::Merge(const TTableAliasMap& other) {
    for (const auto& [alias, table] : other.TableByAlias) {
        TableByAlias[alias] = table;
    }
    for (const auto& [from, to] : other.BaseColumnByRename) {
        BaseColumnByRename[from] = TBaseColumn(to.Relation, to.Column);
    }
}

TString TTableAliasMap::GetBaseTableByAlias(const TString& alias) {
    return TableByAlias[alias];
}

i64 TFDStorage::FindFDIdx(
    const TJoinColumn& antecedentColumn,
    const TJoinColumn& consequentColumn,
    TFunctionalDependency::EType type,
    TTableAliasMap* tableAliases
) {
    auto convertedAntecedent = ConvertColumnIntoIndexes({antecedentColumn}, false, tableAliases);
    auto convertedConsequent = ConvertColumnIntoIndexes({consequentColumn}, false, tableAliases).at(0);

    for (std::size_t i = 0; i < FDs.size(); ++i) {
        auto& fd = FDs[i];
        if (
            fd.AntecedentItems == convertedAntecedent &&
            fd.ConsequentItem == convertedConsequent &&
            fd.Type == type
        ) {
            return i;
        }
    }

    return -1;
}

std::size_t TFDStorage::AddFD(
    const TJoinColumn& antecedentColumn,
    const TJoinColumn& consequentColumn,
    TFunctionalDependency::EType type,
    bool alwaysActive,
    TTableAliasMap* tableAliases
) {
    auto fd = TFunctionalDependency{
        .AntecedentItems = {GetIdxByColumn(antecedentColumn, true, tableAliases)},
        .ConsequentItem = GetIdxByColumn(consequentColumn, true, tableAliases),
        .Type = type,
        .AlwaysActive = alwaysActive
    };

    FDs.push_back(std::move(fd));
    return FDs.size() - 1;
}

i64 TFDStorage::FindInterestingOrderingIdx(
    const std::vector<TJoinColumn>& interestingOrdering,
    TOrdering::EType type,
    TTableAliasMap* tableAliases
) {
    const auto& [_, orderingIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, type, false, tableAliases);
    return orderingIdx;
}

std::size_t TFDStorage::AddInterestingOrdering(
    const std::vector<TJoinColumn>& interestingOrdering,
    TOrdering::EType type,
    TTableAliasMap* tableAliases
) {
    if (interestingOrdering.empty()) {
        return std::numeric_limits<std::size_t>::max();
    }

    auto [items, foundIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, type, true, tableAliases);

    if (foundIdx >= 0) {
        return static_cast<std::size_t>(foundIdx);
    }

    InterestingOrderings.emplace_back(std::move(items), type);
    return InterestingOrderings.size() - 1;
}

TVector<TJoinColumn> TFDStorage::GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const {
    Y_ASSERT(interestingOrderingIdx < InterestingOrderings.size());

    TVector<TJoinColumn> columns;
    columns.reserve(InterestingOrderings[interestingOrderingIdx].Items.size());
    for (std::size_t columnIdx: InterestingOrderings[interestingOrderingIdx].Items) {
        columns.push_back(ColumnByIdx[columnIdx]);
    }

    return columns;
}

TString TFDStorage::ToString() const {
    auto toVectorString = [](auto seq) {
        TVector<TString> strVector;
        strVector.reserve(seq.size());
        for (const auto& item: seq) {
            strVector.push_back(item.ToString());
        }
        return strVector;
    };

    TStringBuilder ss;

    ss << "Columns mapping: ";
    TVector<TString> columnsMapping(IdCounter);
    for (const auto& [column, idx]: IdxByColumn) {
        TStringBuilder columnSs;
        columnSs << "{" << idx << ": " << column << "}";
        columnsMapping[idx] = columnSs;
    }
    ss << JoinSeq(", ", columnsMapping) << "\n";
    ss << "FDs: " << JoinSeq(", ", toVectorString(FDs)) << "\n";
    ss << "Interesting Orderings: " << JoinSeq(", ", toVectorString(InterestingOrderings)) << "\n";

    return ss;
}

std::pair<std::vector<std::size_t>, i64> TFDStorage::ConvertColumnsAndFindExistingOrdering(
    const std::vector<TJoinColumn>& interestingOrdering,
    TOrdering::EType type,
    bool createIfNotExists,
    TTableAliasMap* tableAliases
) {
    std::vector<std::size_t> items = ConvertColumnIntoIndexes(interestingOrdering, createIfNotExists, tableAliases);

    for (std::size_t i = 0; i < InterestingOrderings.size(); ++i) {
        if (items == InterestingOrderings[i].Items && type == InterestingOrderings[i].Type) {
            return {items, static_cast<i64>(i)};
        }
    }

    return {items, -1};
}

std::vector<std::size_t> TFDStorage::ConvertColumnIntoIndexes(
    const std::vector<TJoinColumn>& ordering,
    bool createIfNotExists,
    TTableAliasMap* tableAliases
) {
    std::vector<std::size_t> items;
    items.reserve(ordering.size());

    for (const auto& column: ordering) {
        items.push_back(GetIdxByColumn(column, createIfNotExists, tableAliases));
    }

    return items;
}

std::size_t TFDStorage::GetIdxByColumn(
    const TJoinColumn& column,
    bool createIfNotExists,
    TTableAliasMap* tableAliases
) {
    TJoinColumn baseColumn("", "");
    if (tableAliases) {
        baseColumn = tableAliases->GetBaseColumnByRename(column).ToJoinColumn();
    } else {
        baseColumn = column;
    }

    const TString fullPath = baseColumn.RelName + "." + baseColumn.AttributeName;

    if (IdxByColumn.contains(fullPath)) {
        return IdxByColumn[fullPath];
    }

    Y_ENSURE(!baseColumn.AttributeName.empty());
    Y_ENSURE(createIfNotExists, "There's no such column: " + fullPath);

    ColumnByIdx.push_back(baseColumn);
    IdxByColumn[fullPath] = IdCounter++;
    return IdxByColumn[fullPath];
}

bool TOrderingsStateMachine::TLogicalOrderings::ContainsShuffle(std::size_t orderingIdx) {
    return DFSM->ContainsMatrix[State][orderingIdx];
}

void TOrderingsStateMachine::TLogicalOrderings::InduceNewOrderings(const TFDSet& fds) {
    AppliedFDs |= fds;

    if (State == -1) {
        return;
    }

    for (;;) {
        auto availableTransitions = DFSM->Nodes[State].OutgoingFDs & AppliedFDs;
        if (availableTransitions.none()) {
            return;
        }

        std::size_t fdIdx = std::countr_zero(availableTransitions.to_ullong()); // take any edge
        State = DFSM->TransitionMatrix[State][fdIdx];
    }
}

void TOrderingsStateMachine::TLogicalOrderings::RemoveState() {
    *this = TLogicalOrderings(DFSM);
}

void TOrderingsStateMachine::TLogicalOrderings::SetOrdering(i64 orderingIdx) {
    Y_ASSERT(0 <= orderingIdx && orderingIdx < static_cast<i64>(DFSM->InitStateByOrderingIdx.size()));

    auto state = DFSM->InitStateByOrderingIdx[orderingIdx];
    State = state.StateIdx;
    ShuffleHashFuncArgsCount = state.ShuffleHashFuncArgsCount;
}

i64 TOrderingsStateMachine::TLogicalOrderings::GetShuffleHashFuncArgsCount() {
    return ShuffleHashFuncArgsCount;
}

void TOrderingsStateMachine::TLogicalOrderings::SetShuffleHashFuncArgsCount(std::size_t value) {
    ShuffleHashFuncArgsCount = value;
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::TLogicalOrderings::GetFDs() {
    return AppliedFDs;
}

bool TOrderingsStateMachine::TLogicalOrderings::HasState() {
    return State != -1;
}

bool TOrderingsStateMachine::TLogicalOrderings::HasState() const {
    return State != -1;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsSubsetOf(const TLogicalOrderings& logicalOrderings) {
    Y_ASSERT(DFSM == logicalOrderings.DFSM);
    return HasState() && logicalOrderings.HasState() && IsSubset(DFSM->Nodes[State].NFSMNodesBitset, logicalOrderings.DFSM->Nodes[logicalOrderings.State].NFSMNodesBitset);
}

i64 TOrderingsStateMachine::TLogicalOrderings::GetState() const {
    return State;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs) {
    return (lhs & rhs) == lhs;
}

TOrderingsStateMachine::TLogicalOrderings TOrderingsStateMachine::CreateState() {
    return TLogicalOrderings(DFSM.Get());
}

TOrderingsStateMachine::TLogicalOrderings TOrderingsStateMachine::CreateState(i64 orderingIdx) {
    auto state = TLogicalOrderings(DFSM.Get());
    state.SetOrdering(orderingIdx);
    return state;
}

bool TOrderingsStateMachine::IsBuilt() const {
    return Built;
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::GetFDSet(i64 fdIdx) {
    if (fdIdx < 0) { return TFDSet(); }
    return GetFDSet(std::vector<std::size_t> {static_cast<std::size_t>(fdIdx)});
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::GetFDSet(const std::vector<std::size_t>& fdIdxes) {
    TFDSet fdSet;

    for (std::size_t fdIdx: fdIdxes) {
        if (FdMapping[fdIdx] != -1) {
            fdSet[FdMapping[fdIdx]] = 1;
        }
    }

    return fdSet;
}

TString TOrderingsStateMachine::ToString() const {
    TStringBuilder ss;
    ss << "TOrderingsStateMachine:\n";
    ss << "Built: " << (Built ? "true" : "false") << "\n";
    ss << "FdMapping: [" << JoinSeq(", ", FdMapping) << "]\n";
    ss << "FDStorage:\n" << FDStorage.ToString() << "\n";
    ss << NFSM.ToString();
    ss << DFSM->ToString(NFSM);
    return ss;
}

void TOrderingsStateMachine::Build(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    std::vector<TFunctionalDependency> processedFDs = PruneFDs(fds, interestingOrderings);
    NFSM.Build(processedFDs, interestingOrderings);
    DFSM->Build(NFSM, processedFDs, interestingOrderings);
    Built = true;
}

TString TOrderingsStateMachine::TNFSM::TNode::ToString() const {
    TStringBuilder ss;
    ss << "Node{Type=" << (Type == EArtificial ? "Artificial" : "Interesting")
       << ", Ordering=" << Ordering.ToString()
       << ", InterestingOrderingIdx=" << InterestingOrderingIdx
       << ", OutgoingEdges=[" << JoinSeq(", ", OutgoingEdges) << "]}";
    return ss;
}

TString TOrderingsStateMachine::TNFSM::TEdge::ToString() const {
    TStringBuilder ss;
    ss << "Edge{src=" << srcNodeIdx
       << ", dst=" << dstNodeIdx
       << ", fdIdx=" << (fdIdx == EPSILON ? "EPSILON" : std::to_string(fdIdx))
       << "}";
    return ss;
}

std::size_t TOrderingsStateMachine::TNFSM::Size() {
    return Nodes.size();
}

TString TOrderingsStateMachine::TNFSM::ToString() const {
    TStringBuilder ss;
    ss << "NFSM:\n";
    ss << "Nodes (" << Nodes.size() << "):\n";
    for (std::size_t i = 0; i < Nodes.size(); ++i) {
        ss << "  " << i << ": " << Nodes[i].ToString() << "\n";
    }
    ss << "Edges (" << Edges.size() << "):\n";
    for (std::size_t i = 0; i < Edges.size(); ++i) {
        ss << "  " << i << ": " << Edges[i].ToString() << "\n";
    }
    return ss;
}

void TOrderingsStateMachine::TNFSM::Build(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interesting
) {
    for (std::size_t i = 0; i < interesting.size(); ++i) {
        AddNode(interesting[i], TNFSM::TNode::EInteresting, i);
    }

    ApplyFDs(fds);
    PrefixClosure();

    for (std::size_t idx = 0; idx < Edges.size(); ++idx) {
        Nodes[Edges[idx].srcNodeIdx].OutgoingEdges.push_back(idx);
    }
}

std::size_t TOrderingsStateMachine::TNFSM::AddNode(const TOrdering& ordering, TNode::EType type, i64 interestingOrderingIdx) {
    for (std::size_t i = 0; i < Nodes.size(); ++i) {
        if (Nodes[i].Ordering == ordering) {
            return i;
        }
    }

    Nodes.emplace_back(ordering, type, interestingOrderingIdx);
    return Nodes.size() - 1;
}

void TOrderingsStateMachine::TNFSM::AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx) {
    Edges.emplace_back(srcNodeIdx, dstNodeIdx, fdIdx);
}

void TOrderingsStateMachine::TNFSM::PrefixClosure() {
    for (std::size_t i = 0; i < Nodes.size(); ++i) {
        const auto& iItems = Nodes[i].Ordering.Items;

        for (std::size_t j = 0; j < Nodes.size(); ++j) {
            const auto& jItems = Nodes[j].Ordering.Items;
            if (i == j || iItems.size() >= jItems.size()) {
                continue;
            }

            std::size_t k = 0;
            for (; k < iItems.size() && (iItems[k] == jItems[k]); ++k) {
            }

            if (k == iItems.size()) {
                AddEdge(i, j, TNFSM::TEdge::EPSILON);
            }
        }
    }
}

void TOrderingsStateMachine::TNFSM::ApplyFDs(const std::vector<TFunctionalDependency>& fds) {
    for (std::size_t nodeIdx = 0; nodeIdx < Nodes.size() && Nodes.size() < EMaxNFSMStates; ++nodeIdx) {
        for (std::size_t fdIdx = 0; fdIdx < fds.size() && Nodes.size() < EMaxNFSMStates; ++fdIdx) {
            const auto& fd = fds[fdIdx];

            if (!fd.MatchesAntecedentItems(Nodes[nodeIdx].Ordering)) {
                continue;
            }

            if (fd.IsEquivalence()) {
                std::size_t replacedElement = fd.AntecedentItems[0];
                std::vector<std::size_t> newOrdering = Nodes[nodeIdx].Ordering.Items;
                *std::find(newOrdering.begin(), newOrdering.end(), replacedElement) = fd.ConsequentItem;

                std::size_t dstIdx = AddNode(TOrdering(newOrdering, Nodes[nodeIdx].Ordering.Type), TNode::EArtificial);
                AddEdge(nodeIdx, dstIdx, fdIdx);
                AddEdge(dstIdx, nodeIdx, fdIdx);
                continue;
            }
        }
    }
}

TString TOrderingsStateMachine::TDFSM::TNode::ToString() const {
    TStringBuilder ss;
    ss << "Node{NFSMNodes=[" << JoinSeq(", ", NFSMNodes) << "], "
       << "OutgoingFDs=" << OutgoingFDs.to_string() << "}";
    return ss;
}

TString TOrderingsStateMachine::TDFSM::TEdge::ToString() const {
    TStringBuilder ss;
    ss << "Edge{src=" << srcNodeIdx
       << ", dst=" << dstNodeIdx
       << ", fdIdx=" << fdIdx
       << "}";
    return ss;
}

std::size_t TOrderingsStateMachine::TDFSM::Size() {
    return Nodes.size();
}

TString TOrderingsStateMachine::TDFSM::ToString(const TNFSM& nfsm) const {
    TStringBuilder ss;
    ss << "DFSM:\n";
    ss << "Nodes (" << Nodes.size() << "):\n";
    for (std::size_t i = 0; i < Nodes.size(); ++i) {
        ss << "  " << i << ": " << Nodes[i].ToString() << "\n";
        ss << "    NFSMNodes: [";
        for (std::size_t j = 0; j < Nodes[i].NFSMNodes.size(); ++j) {
            std::size_t nfsmNodeIdx = Nodes[i].NFSMNodes[j];
            if (j > 0) {
                ss << ", ";
            }
            ss << nfsmNodeIdx;
            if (nfsmNodeIdx < nfsm.Nodes.size()) {
                ss << "(" << nfsm.Nodes[nfsmNodeIdx].Ordering.ToString() << ")";
            }
        }
        ss << "]\n";
    }
    ss << "Edges (" << Edges.size() << "):\n";
    for (std::size_t i = 0; i < Edges.size(); ++i) {
        ss << "  " << i << ": " << Edges[i].ToString() << "\n";
    }
    ss << "InitStateByOrderingIdx (" << InitStateByOrderingIdx.size() << "):\n";
    for (std::size_t i = 0; i < InitStateByOrderingIdx.size(); ++i) {
        ss << "  " << i << ": StateIdx=" << InitStateByOrderingIdx[i].StateIdx
           << ", ShuffleHashFuncArgsCount=" << InitStateByOrderingIdx[i].ShuffleHashFuncArgsCount << "\n";
    }
    return ss;
}

void TOrderingsStateMachine::TDFSM::Build(
    const TNFSM& nfsm,
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    InitStateByOrderingIdx.resize(interestingOrderings.size());

    for (std::size_t i = 0; i < interestingOrderings.size(); ++i) {
        for (std::size_t nfsmNodeIdx = 0; nfsmNodeIdx < nfsm.Nodes.size(); ++nfsmNodeIdx) {
            if (nfsm.Nodes[nfsmNodeIdx].Ordering == interestingOrderings[i]) {
                auto nfsmNodes = CollectNodesWithEpsOrFdEdge(nfsm, {i});
                InitStateByOrderingIdx[i] = TInitState{AddNode(std::move(nfsmNodes)), interestingOrderings[i].Items.size()};
            }
        }
    }

    for (std::size_t nodeIdx = 0; nodeIdx < Nodes.size() && Nodes.size() < EMaxDFSMStates; ++nodeIdx) {
        std::unordered_set<i64> outgoingDFSMNodeFDs;
        for (std::size_t nfsmNodeIdx: Nodes[nodeIdx].NFSMNodes) {
            for (std::size_t nfsmEdgeIdx: nfsm.Nodes[nfsmNodeIdx].OutgoingEdges) {
                outgoingDFSMNodeFDs.insert(nfsm.Edges[nfsmEdgeIdx].fdIdx);
            }
        }

        for (i64 fdIdx: outgoingDFSMNodeFDs) {
            if (fdIdx == TNFSM::TEdge::EPSILON) {
                continue;
            }

            std::size_t dstNodeIdx = AddNode(CollectNodesWithEpsOrFdEdge(nfsm, Nodes[nodeIdx].NFSMNodes, fdIdx));
            if (nodeIdx == dstNodeIdx) {
                continue;
            }
            AddEdge(nodeIdx, dstNodeIdx, fdIdx);

            Nodes[nodeIdx].OutgoingFDs[fdIdx] = 1;
        }
    }

    Precompute(nfsm, fds, interestingOrderings);
}

std::size_t TOrderingsStateMachine::TDFSM::AddNode(const std::vector<std::size_t>& nfsmNodes) {
    for (std::size_t i = 0; i < Nodes.size(); ++i) {
        if (Nodes[i].NFSMNodes == nfsmNodes) {
            return i;
        }
    }

    Nodes.emplace_back(nfsmNodes);
    return Nodes.size() - 1;
}

void TOrderingsStateMachine::TDFSM::AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx) {
    Edges.emplace_back(srcNodeIdx, dstNodeIdx, fdIdx);
}

std::vector<std::size_t> TOrderingsStateMachine::TDFSM::CollectNodesWithEpsOrFdEdge(
    const TNFSM& nfsm,
    const std::vector<std::size_t>& startNFSMNodes,
    i64 fdIdx
) {
    std::set<std::size_t> visited;

    std::function<void(std::size_t)> DFS;
    DFS = [&DFS, &visited, fdIdx, &nfsm](std::size_t nodeIdx){
        if (visited.contains(nodeIdx)) {
            return;
        }

        visited.insert(nodeIdx);

        for (std::size_t edgeIdx: nfsm.Nodes[nodeIdx].OutgoingEdges) {
            const TNFSM::TEdge& edge = nfsm.Edges[edgeIdx];
            if (edge.fdIdx == fdIdx || edge.fdIdx == TNFSM::TEdge::EPSILON) {
                DFS(edge.dstNodeIdx);
            }
        }
    };

    for (std::size_t nodeIdx : startNFSMNodes) {
        DFS(nodeIdx);
    }

    return std::vector<std::size_t>(visited.begin(), visited.end());
}

void TOrderingsStateMachine::TDFSM::Precompute(
    const TNFSM& nfsm,
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    TransitionMatrix = std::vector<std::vector<i64>>(Nodes.size(), std::vector<i64>(fds.size(), -1));
    for (const auto& edge: Edges) {
        TransitionMatrix[edge.srcNodeIdx][edge.fdIdx] = edge.dstNodeIdx;
    }

    ContainsMatrix = std::vector<std::vector<bool>>(Nodes.size(), std::vector<bool>(interestingOrderings.size(), false));
    for (std::size_t dfsmNodeIdx = 0; dfsmNodeIdx < Nodes.size(); ++dfsmNodeIdx) {
        for (std::size_t nfsmNodeIdx : Nodes[dfsmNodeIdx].NFSMNodes) {
            auto interestingOrderIdx = nfsm.Nodes[nfsmNodeIdx].InterestingOrderingIdx;
            if (interestingOrderIdx == -1) { continue; }

            ContainsMatrix[dfsmNodeIdx][interestingOrderIdx] = true;
        }
    }

    for (auto& node: Nodes) {
        for (auto& nfsmNodeIdx: node.NFSMNodes) {
            node.NFSMNodesBitset[nfsmNodeIdx] = 1;
        }
    }
}

std::vector<TFunctionalDependency> TOrderingsStateMachine::PruneFDs(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    std::size_t newIdxCounter = 0;
    std::unordered_map<std::size_t, std::size_t> idxByItem;

    for (const auto& ordering: interestingOrderings) {
        for (const auto& item: ordering.Items) {
            if (!idxByItem.contains(item)) {
                idxByItem[item] = newIdxCounter++;
            }
        }
    }

    for (const auto& fd: fds) {
        if (fd.IsEquivalence()) {
            if (!idxByItem.contains(fd.ConsequentItem)) {
                idxByItem[fd.ConsequentItem] = newIdxCounter++;
            }

            if (!idxByItem.contains(fd.AntecedentItems[0])) {
                idxByItem[fd.AntecedentItems[0]] = newIdxCounter++;
            }
        }
    }

    TDisjointSets equivalenceSets(idxByItem.size());
    for (const auto& fd: fds) {
        if (fd.IsEquivalence()) {
            equivalenceSets.UnionSets(idxByItem[fd.ConsequentItem], idxByItem[fd.AntecedentItems[0]]);
        }
    }

    std::vector<TFunctionalDependency> filteredFds;
    filteredFds.reserve(fds.size());
    FdMapping.resize(fds.size());
    for (std::size_t i = 0; i < fds.size(); ++i) {
        if (fds[i].IsEquivalence()) {
            bool canLeadToInteresting = false;

            for (const auto& ordering: interestingOrderings) {
                for (const auto& item: ordering.Items) {
                    if (
                        equivalenceSets.CanonicSetElement(idxByItem[item]) == equivalenceSets.CanonicSetElement(idxByItem[fds[i].ConsequentItem]) ||
                        equivalenceSets.CanonicSetElement(idxByItem[item]) == equivalenceSets.CanonicSetElement(idxByItem[fds[i].AntecedentItems[0]])
                    ) {
                        canLeadToInteresting = true;
                        break;
                    }
                }

            }

            if (canLeadToInteresting && filteredFds.size() < EMaxFDCount) {
                filteredFds.push_back(std::move(fds[i]));
                FdMapping[i] = filteredFds.size() - 1;
            } else {
                FdMapping[i] = -1;
            }
        }
    }

    return filteredFds;
}

TTableAliasMap::TBaseColumn& TTableAliasMap::TBaseColumn::operator=(const TBaseColumn& other) {
    if (this != &other) {
        Relation = other.Relation;
        Column = other.Column;
    }
    return *this;
}

NDq::TJoinColumn TTableAliasMap::TBaseColumn::ToJoinColumn() {
    return NDq::TJoinColumn(Relation, Column);
}

TTableAliasMap::TBaseColumn::operator bool() {
    return !(Relation.empty() && Column.empty());
}

} // namespace NYql::NDq
