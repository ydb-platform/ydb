#include "cbo_interesting_orderings.h"

#include <library/cpp/disjoint_sets/disjoint_sets.h>

#include <yql/essentials/utils/log/log.h>

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

bool TFunctionalDependency::IsImplication() const {
    return Type == EType::EImplication;
}

bool TFunctionalDependency::IsConstant() const {
    return AntecedentItems.empty();
}

TMaybe<std::size_t> TFunctionalDependency::MatchesAntecedentItems(const TOrdering& ordering) const {
    auto it = std::search(
        ordering.Items.begin(), ordering.Items.end(),
        AntecedentItems.begin(), AntecedentItems.end()
    );

    if (it == ordering.Items.end()) {
        return Nothing();
    }

    return static_cast<i64>(std::distance(ordering.Items.begin(), it));
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

    if (AlwaysActive) {
        ss << "(AA)";
    }

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

        if (alias.empty() && BaseColumnByRename.contains(column)) {
            return BaseColumnByRename[column];
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
            result += from + " -> " + to.Relation + "." + to.Column + " ";
        }
        result.pop_back();
        result += ", ";
    }

    result += "TableAliases: ";
    for (const auto& [alias, table] : TableByAlias) {
        result += alias + " -> " + table + ", ";
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
    if (!TableByAlias.contains(alias)) {
        return alias;
    }
    return TableByAlias[alias];
}

i64 TFDStorage::FindFDIdx(
    const TJoinColumn& antecedentColumn,
    const TJoinColumn& consequentColumn,
    TFunctionalDependency::EType type,
    TTableAliasMap* tableAliases
) {
    auto convertedAntecedent = ConvertColumnIntoIndexes({antecedentColumn}, false, tableAliases);
    auto convertedConsequents = ConvertColumnIntoIndexes({consequentColumn}, false, tableAliases);

    if (convertedAntecedent.empty() || convertedConsequents.empty()) {
        return -1;
    }

    auto convertedConsequent = convertedConsequents[0];
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

bool operator==(const TFunctionalDependency& lhs, const TFunctionalDependency& rhs) {
    if (lhs.IsConstant() && rhs.IsConstant()) {
        return lhs.ConsequentItem == rhs.ConsequentItem;
    }

    if (lhs.IsImplication() && rhs.IsImplication()) {
        return std::tie(lhs.AntecedentItems, lhs.ConsequentItem) == std::tie(rhs.AntecedentItems, rhs.ConsequentItem);
    }

    if (lhs.IsEquivalence() && rhs.IsEquivalence()) {
        return
            rhs.AntecedentItems.size() == 1 && rhs.AntecedentItems[0] == lhs.ConsequentItem ||
            lhs.AntecedentItems.size() == 1 && lhs.AntecedentItems[0] == rhs.ConsequentItem;
    }

    return false;
}


std::size_t TFDStorage::AddFDImpl(TFunctionalDependency fd) {
    for (std::size_t i = 0; i < FDs.size(); ++i) {
        if (FDs[i] == fd) {
            return i;
        }
    }

    FDs.push_back(std::move(fd));
    return FDs.size() - 1;
}

std::size_t TFDStorage::AddFD(
    const TJoinColumn& antecedentColumn,
    const TJoinColumn& consequentColumn,
    TFunctionalDependency::EType type,
    bool alwaysActive,
    TTableAliasMap* tableAliases
) {
    auto fd = TFunctionalDependency{
        .AntecedentItems = {static_cast<std::size_t>(GetIdxByColumn(antecedentColumn, true, tableAliases))},
        .ConsequentItem = static_cast<std::size_t>(GetIdxByColumn(consequentColumn, true, tableAliases)),
        .Type = type,
        .AlwaysActive = alwaysActive
    };

    return AddFDImpl(std::move(fd));
}

std::size_t TFDStorage::AddConstant(
    const TJoinColumn& constantColumn,
    bool alwaysActive,
    TTableAliasMap* tableAliases
) {
    auto fd = TFunctionalDependency{
        .AntecedentItems = {},
        .ConsequentItem = GetIdxByColumn(constantColumn, true, tableAliases),
        .Type = TFunctionalDependency::EImplication,
        .AlwaysActive = alwaysActive
    };

    return AddFDImpl(std::move(fd));
}

std::size_t TFDStorage::AddImplication(
    const TVector<TJoinColumn>& antecedentColumns,
    const TJoinColumn& consequentColumn,
    bool alwaysActive,
    TTableAliasMap* tableAliases
) {
    auto fd = TFunctionalDependency{
        .AntecedentItems = ConvertColumnIntoIndexes(antecedentColumns, true, tableAliases),
        .ConsequentItem = GetIdxByColumn(consequentColumn, true, tableAliases),
        .Type = TFunctionalDependency::EImplication,
        .AlwaysActive = alwaysActive
    };

    return AddFDImpl(std::move(fd));
}

std::size_t TFDStorage::AddEquivalence(
    const TJoinColumn& lhs,
    const TJoinColumn& rhs,
    bool alwaysActive,
    TTableAliasMap* tableAliases
) {
    auto fd = TFunctionalDependency{
        .AntecedentItems = {GetIdxByColumn(lhs, true, tableAliases)},
        .ConsequentItem = GetIdxByColumn(rhs, true, tableAliases),
        .Type = TFunctionalDependency::EEquivalence,
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
    const TVector<TString>& interestingOrdering,
    TOrdering::EType type,
    TTableAliasMap* tableAliases
) {
    std::vector<TJoinColumn> columns;
    columns.reserve(interestingOrdering.size());
    for (const auto& column: interestingOrdering) {
        columns.push_back(TJoinColumn("", column));
    }
    return AddInterestingOrdering(columns, type, tableAliases);
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
    if (items.empty()) {
        return {{}, -1};
    }

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
        if (auto idx = GetIdxByColumn(column, createIfNotExists, tableAliases); idx != Max<size_t>()) {
            items.push_back(idx);
        } else {
            return {};
        }
    }

    return items;
}

i64 TFDStorage::GetIdxByColumn(
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
    if (!createIfNotExists) {
        return Max<size_t>();
    }

    ColumnByIdx.push_back(baseColumn);
    IdxByColumn[fullPath] = IdCounter++;
    return IdxByColumn[fullPath];
}

bool TOrderingsStateMachine::TLogicalOrderings::ContainsShuffle(i64 orderingIdx) {
    return IsInitialized() && HasState() && (orderingIdx >= 0) && DFSM->Nodes[State].InterestingOrderings[orderingIdx];
}

bool TOrderingsStateMachine::TLogicalOrderings::ContainsSorting(i64 orderingIdx) {
    return IsInitialized() && HasState() && (orderingIdx >= 0) && DFSM->Nodes[State].InterestingOrderings[orderingIdx];
}

void TOrderingsStateMachine::TLogicalOrderings::InduceNewOrderings(const TFDSet& fds) {
    AppliedFDs |= fds;

    if (!(HasState() && IsInitialized())) {
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
    if (orderingIdx < 0 || orderingIdx >= static_cast<i64>(DFSM->InitStateByOrderingIdx.size())) {
        RemoveState();
        return;
    }

    if (!IsInitialized()) {
        return;
    }

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

bool TOrderingsStateMachine::TLogicalOrderings::IsInitialized() {
    return DFSM != nullptr;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsInitialized() const {
    return DFSM != nullptr;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsSubsetOf(const TLogicalOrderings& logicalOrderings) {
    return
        HasState() && logicalOrderings.HasState() &&
        IsInitialized() && logicalOrderings.IsInitialized() &&
        DFSM == logicalOrderings.DFSM &&
        IsSubset(DFSM->Nodes[State].NFSMNodesBitset, logicalOrderings.DFSM->Nodes[logicalOrderings.State].NFSMNodesBitset);
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
    DFSM = MakeSimpleShared<TDFSM>();
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

    ApplyFDs(fds, interesting);
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

bool TOrderingsStateMachine::TNFSM::TEdge::operator==(const TEdge& other) const {
    return std::tie(srcNodeIdx, dstNodeIdx, fdIdx) == std::tie(other.srcNodeIdx, other.dstNodeIdx, other.fdIdx);
}

void TOrderingsStateMachine::TNFSM::AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx) {
    auto newEdge = TNFSM::TEdge(srcNodeIdx, dstNodeIdx, fdIdx);
    for (std::size_t i = 0; i < Edges.size(); ++i) {
        if (Edges[i] == newEdge) {
            return;
        }
    }
    Edges.emplace_back(newEdge);
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
                if (Nodes[i].Ordering.Type == TOrdering::EShuffle) {
                    AddEdge(i, j, TNFSM::TEdge::EPSILON);
                }

                if (Nodes[i].Ordering.Type == TOrdering::ESorting) {
                    AddEdge(j, i, TNFSM::TEdge::EPSILON);
                }
            }
        }
    }
}

void TOrderingsStateMachine::TNFSM::ApplyFDs(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    std::size_t maxInterestingOrderingSize = 0;
    if (!interestingOrderings.empty()) {
        maxInterestingOrderingSize =
            std::max_element(
                interestingOrderings.begin(),
                interestingOrderings.end(),
                [](const TOrdering& a, const TOrdering& b) { return a.Items.size() < b.Items.size(); }
            )->Items.size();
    }


    for (std::size_t nodeIdx = 0; nodeIdx < Nodes.size() && Nodes.size() < EMaxNFSMStates; ++nodeIdx) {
        for (std::size_t fdIdx = 0; fdIdx < fds.size() && Nodes.size() < EMaxNFSMStates; ++fdIdx) {
            TFunctionalDependency fd = fds[fdIdx];

            auto applyFD = [this, nodeIdx, maxInterestingOrderingSize](const TFunctionalDependency& fd, std::size_t fdIdx) {
                if (Nodes.size() >= EMaxNFSMStates) {
                    return;
                }

                // if (fd.ConsequentItem == 14 && (Nodes[nodeIdx].Ordering.Items[0] == 12 || Nodes[nodeIdx].Ordering.Items[0] == 17)) {
                //     Cout << "huet" << Endl;
                // }

                if (fd.IsConstant() && Nodes[nodeIdx].Ordering.Items.size() > 1) {
                    std::vector<std::size_t> newOrdering = Nodes[nodeIdx].Ordering.Items;
                    auto it = std::find(newOrdering.begin(), newOrdering.end(), fd.ConsequentItem);
                    if (it == newOrdering.end()) {
                        return;
                    }
                    bool isNewOrderingPrefixOfOld = (it == (newOrdering.end() - 1));
                    newOrdering.erase(it);

                    std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes[nodeIdx].Ordering.Type), TNode::EArtificial);

                    if (!isNewOrderingPrefixOfOld || Nodes[nodeIdx].Ordering.Type == TOrdering::EShuffle) {
                        AddEdge(nodeIdx, dstIdx, fdIdx);
                    }

                    if (!isNewOrderingPrefixOfOld || Nodes[nodeIdx].Ordering.Type == TOrdering::ESorting) {
                        AddEdge(dstIdx, nodeIdx, fdIdx);
                    }
                }

                if (fd.IsConstant()) {
                    return;
                }

                auto maybeAntecedentItemIdx = fd.MatchesAntecedentItems(Nodes[nodeIdx].Ordering);
                if (!maybeAntecedentItemIdx) {
                    return;
                }

                std::size_t antecedentItemIdx = maybeAntecedentItemIdx.GetRef();
                if (
                    auto it = std::find(Nodes[nodeIdx].Ordering.Items.begin(), Nodes[nodeIdx].Ordering.Items.end(), fd.ConsequentItem);
                    it != Nodes[nodeIdx].Ordering.Items.end()
                ) {
                    if (fd.IsEquivalence()) { // (a, b) -> (b, a)
                        std::size_t consequentItemIdx = std::distance(Nodes[nodeIdx].Ordering.Items.begin(), it);
                        auto newOrdering = Nodes[nodeIdx].Ordering.Items;
                        std::swap(newOrdering[antecedentItemIdx], newOrdering[consequentItemIdx]);
                        std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes[nodeIdx].Ordering.Type), TNode::EArtificial);
                        AddEdge(nodeIdx, dstIdx, fdIdx);
                        AddEdge(dstIdx, nodeIdx, fdIdx);
                    }

                    return;
                }

                Y_ENSURE(antecedentItemIdx < Nodes[nodeIdx].Ordering.Items.size());
                if (fd.IsEquivalence()) {
                    Y_ENSURE(fd.AntecedentItems.size() == 1);
                    std::vector<std::size_t> newOrdering = Nodes[nodeIdx].Ordering.Items;
                    newOrdering[antecedentItemIdx] = fd.ConsequentItem;

                    std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes[nodeIdx].Ordering.Type), TNode::EArtificial);
                    AddEdge(nodeIdx, dstIdx, fdIdx);
                    AddEdge(dstIdx, nodeIdx, fdIdx);
                }

                if (
                    Nodes[nodeIdx].Ordering.Type == TOrdering::EShuffle ||
                    Nodes[nodeIdx].Ordering.Items.size() == maxInterestingOrderingSize
                ) {
                    return;
                }

                if (fd.IsImplication() || fd.IsEquivalence()) {
                    for (std::size_t i = antecedentItemIdx + fd.AntecedentItems.size(); i <= Nodes[nodeIdx].Ordering.Items.size() && Nodes.size() < EMaxNFSMStates; ++i) {
                        std::vector<std::size_t> newOrdering = Nodes[nodeIdx].Ordering.Items;
                        newOrdering.insert(newOrdering.begin() + i, fd.ConsequentItem);

                        std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes[nodeIdx].Ordering.Type), TNode::EArtificial);
                        AddEdge(nodeIdx, dstIdx, fdIdx); // Epsilon edge will be added during PrefixClosure
                    }
                }
            };

            applyFD(fd, fdIdx);
            if (fd.IsEquivalence()) {
                Y_ENSURE(fd.AntecedentItems.size() == 1);
                TFunctionalDependency reversedEquiv = fd;
                std::swap(reversedEquiv.ConsequentItem, reversedEquiv.AntecedentItems[0]);
                applyFD(reversedEquiv, fdIdx);
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
                auto nfsmNodes = CollectNodesWithEpsOrFdEdge(nfsm, {i}, fds);
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

            std::size_t dstNodeIdx = AddNode(CollectNodesWithEpsOrFdEdge(nfsm, Nodes[nodeIdx].NFSMNodes, fds, fdIdx));
            if (nodeIdx == dstNodeIdx) {
                continue;
            }
            AddEdge(nodeIdx, dstNodeIdx, fdIdx);

            Nodes[nodeIdx].OutgoingFDs[fdIdx] = 1;
        }
    }

    Precompute(nfsm, fds);
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
    const std::vector<TFunctionalDependency>& fds,
    i64 fdIdx
) {
    std::set<std::size_t> visited;

    std::function<void(std::size_t)> DFS;
    DFS = [&DFS, &visited, fdIdx, &nfsm, &fds](std::size_t nodeIdx){
        if (visited.contains(nodeIdx)) {
            return;
        }

        visited.insert(nodeIdx);

        for (std::size_t edgeIdx: nfsm.Nodes[nodeIdx].OutgoingEdges) {
            const TNFSM::TEdge& edge = nfsm.Edges[edgeIdx];
            if (edge.fdIdx == fdIdx || edge.fdIdx == TNFSM::TEdge::EPSILON || fds[edge.fdIdx].AlwaysActive) {
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
    const std::vector<TFunctionalDependency>& fds
) {
    TransitionMatrix = std::vector<std::vector<i64>>(Nodes.size(), std::vector<i64>(fds.size(), -1));
    for (const auto& edge: Edges) {
        TransitionMatrix[edge.srcNodeIdx][edge.fdIdx] = edge.dstNodeIdx;
    }

    for (std::size_t dfsmNodeIdx = 0; dfsmNodeIdx < Nodes.size(); ++dfsmNodeIdx) {
        for (std::size_t nfsmNodeIdx : Nodes[dfsmNodeIdx].NFSMNodes) {
            auto interestingOrderIdx = nfsm.Nodes[nfsmNodeIdx].InterestingOrderingIdx;
            if (interestingOrderIdx == -1) { continue; }

            Nodes[dfsmNodeIdx].InterestingOrderings[interestingOrderIdx] = 1;
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
    std::vector<TFunctionalDependency> filteredFds;
    filteredFds.reserve(fds.size());
    FdMapping.resize(fds.size());
    for (std::size_t i = 0; i < fds.size(); ++i) {
        bool canLeadToInteresting = false;

        for (const auto& ordering: interestingOrderings) {
            if (std::find(ordering.Items.begin(), ordering.Items.end(), fds[i].ConsequentItem) != ordering.Items.end()) {
                canLeadToInteresting = true;
                break;
            }

            if (
                fds[i].IsEquivalence() &&
                std::find(ordering.Items.begin(), ordering.Items.end(), fds[i].AntecedentItems[0]) != ordering.Items.end()
            ) {
                canLeadToInteresting = true;
                break;
            }
        }

        if (canLeadToInteresting && filteredFds.size() < EMaxFDCount) {
            filteredFds.push_back(std::move(fds[i]));
            FdMapping[i] = filteredFds.size() - 1;
        } else {
            FdMapping[i] = -1;
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
