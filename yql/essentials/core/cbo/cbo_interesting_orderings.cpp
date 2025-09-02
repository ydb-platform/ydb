#include "cbo_interesting_orderings.h"

#include <library/cpp/disjoint_sets/disjoint_sets.h>
#include <library/cpp/iterator/zip.h>

#include <yql/essentials/utils/log/log.h>

#include <util/string/builder.h>
#include <util/string/join.h>

#include <bit>
#include <set>
#include <unordered_set>
#include <functional>

namespace NYql::NDq {

bool TOrdering::operator==(const TOrdering& other) const {
    return
        std::tie(this->Type, this->Items, this->Directions) ==
        std::tie(other.Type, other.Items, other.Directions);
}

TString TOrdering::ToString() const {
    TVector<TString> itemStrings;
    itemStrings.reserve(Items.size());

    for (size_t i = 0; i < Items.size(); ++i) {
        TString itemStr = ::ToString(Items[i]);

        if (i < Directions.size()) {
            switch (Directions[i]) {
                case TItem::EDirection::EAscending:
                    itemStr += "^";
                    break;
                case TItem::EDirection::EDescending:
                    itemStr += "v";
                    break;
                default:
                    break;
            }
        }

        itemStrings.push_back(std::move(itemStr));
    }

    return "{" + JoinSeq(", ", itemStrings) + "}";
}

bool TOrdering::HasItem(std::size_t item) const {
        return std::find(Items.begin(), Items.end(), item) != Items.end();
}

bool TFunctionalDependency::IsEquivalence() const {
    return Type == EType::EEquivalence;
}

bool TFunctionalDependency::IsImplication() const {
    return Type == EType::EImplication && !IsConstant();
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
    TableByAlias_[alias] = table;
}

void TTableAliasMap::AddRename(TString from, TString to) {
    if (auto pointIdx = from.find('.'); pointIdx != TString::npos) {
        TString alias = from.substr(0, pointIdx);
        TString baseTable = GetBaseTableByAlias(alias);
        TString columnName = from.substr(pointIdx + 1);

        if (pointIdx == 0) {
            from = from.substr(1);
        }
        if (auto pointIdx = to.find('.'); pointIdx == 0) {
            to = to.substr(1);
        }

        auto baseColumn = TBaseColumn(baseTable, columnName);
        BaseColumnByRename_[to] = BaseColumnByRename_[from] = baseColumn;
        return;
    }

    if (BaseColumnByRename_.contains(from)) {
        BaseColumnByRename_[to] = BaseColumnByRename_[from];
    }
}

TTableAliasMap::TBaseColumn TTableAliasMap::GetBaseColumnByRename(const TString& renamedColumn) {
    if (BaseColumnByRename_.contains(renamedColumn)) {
        return BaseColumnByRename_[renamedColumn];
    }

    if (auto pointIdx = renamedColumn.find('.'); pointIdx != TString::npos) {
        TString alias = renamedColumn.substr(0, pointIdx);
        TString column = renamedColumn.substr(pointIdx + 1);
        if (auto baseTable = GetBaseTableByAlias(alias)) {
            return TBaseColumn(std::move(baseTable), std::move(column));
        }

        if (alias.empty() && BaseColumnByRename_.contains(column)) {
            return BaseColumnByRename_[column];
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

    if (!BaseColumnByRename_.empty()) {
        result += "Renames: ";
        for (const auto& [from, to] : BaseColumnByRename_) {
            result += from + " -> " + to.Relation + "." + to.Column + " ";
        }
        result.pop_back();
        result += ", ";
    }

    result += "TableAliases: ";
    for (const auto& [alias, table] : TableByAlias_) {
        result += alias + " -> " + table + ", ";
    }
    result.pop_back();

    return result;
}

void TTableAliasMap::Merge(const TTableAliasMap& other) {
    for (const auto& [alias, table] : other.TableByAlias_) {
        TableByAlias_[alias] = table;
    }
    for (const auto& [from, to] : other.BaseColumnByRename_) {
        if (BaseColumnByRename_.contains(from)) {
            continue;
        }
        BaseColumnByRename_[from] = TBaseColumn(to.Relation, to.Column);
    }
}

TString TTableAliasMap::GetBaseTableByAlias(const TString& alias) {
    if (!TableByAlias_.contains(alias)) {
        return alias;
    }
    return TableByAlias_[alias];
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
        .AntecedentItems = {GetIdxByColumn(antecedentColumn, true, tableAliases)},
        .ConsequentItem = GetIdxByColumn(consequentColumn, true, tableAliases),
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
    const auto& [_, orderingIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, {}, type, false, false, tableAliases);
    return orderingIdx;
}

// returns was ordering reordered or not
bool ReorderByNatural(
    std::vector<std::size_t>& ordering,
    const std::vector<std::size_t> natural
) {
    std::unordered_set<std::size_t> itemsLeft{ordering.begin(), ordering.end()};
    std::vector<std::size_t> reordered;
    reordered.reserve(ordering.size());
    for (std::size_t item: natural) {
        if (itemsLeft.contains(item)) {
            reordered.push_back(item);
            itemsLeft.erase(item);
        }
    }

    if (reordered.empty()) {
        return false;
    }

    reordered.insert(reordered.end(), itemsLeft.begin(), itemsLeft.end());
    ordering = std::move(reordered);
    return true;
}

void TFDStorage::ApplyNaturalOrderings() {
    std::vector<TOrdering> naturalOrderings;
    naturalOrderings.reserve(InterestingOrderings.size());

    for (const auto& ordering: InterestingOrderings) {
        if (ordering.IsNatural) {
            naturalOrderings.push_back(ordering);
        }
    }

    for (TOrdering& ordering: InterestingOrderings) {
        if (!ordering.IsNatural) {
            for (const auto& natural: naturalOrderings) {
                if (/* wasReordered = */ ReorderByNatural(ordering.Items, natural.Items)) {
                    break;
                }
            }
        }
    }

    std::sort(
        InterestingOrderings.begin(),
        InterestingOrderings.end(),
        [](const TOrdering& lhs, const TOrdering& rhs){ return lhs.Items < rhs.Items; }
    );

    InterestingOrderings.erase(
        std::unique(
            InterestingOrderings.begin(),
            InterestingOrderings.end()
        ),
        InterestingOrderings.end()
    );
}

std::size_t TFDStorage::FindSorting(
    const TSorting& sorting,
    TTableAliasMap* tableAliases
) {
    const auto& [_, orderingIdx] = ConvertColumnsAndFindExistingOrdering(sorting.Ordering, sorting.Directions, TOrdering::ESorting, false, true, tableAliases);
    return orderingIdx;
}

std::size_t TFDStorage::FindShuffling(
    const TShuffling& shuffling,
    TTableAliasMap* tableAliases
) {
    const auto& [_, orderingIdx] = ConvertColumnsAndFindExistingOrdering(shuffling.Ordering, {}, TOrdering::EShuffle, false, shuffling.IsNatural, tableAliases);
    return orderingIdx;
}


std::size_t TFDStorage::AddSorting(
    const TSorting& sorting,
    TTableAliasMap* tableAliases
) {
    return AddInterestingOrdering(sorting.Ordering, TOrdering::ESorting, sorting.Directions, true, tableAliases);
}

std::size_t TFDStorage::AddShuffling(
    const TShuffling& shuffling,
    TTableAliasMap* tableAliases
) {
    return AddInterestingOrdering(shuffling.Ordering, TOrdering::EShuffle, std::vector<TOrdering::TItem::EDirection>{}, shuffling.IsNatural, tableAliases);
}

std::size_t TFDStorage::AddInterestingOrdering(
    const std::vector<TJoinColumn>& interestingOrdering,
    TOrdering::EType type,
    const std::vector<TOrdering::TItem::EDirection>& directions,
    bool isNatural,
    TTableAliasMap* tableAliases
) {
    if (interestingOrdering.empty()) {
        return std::numeric_limits<std::size_t>::max();
    }

    auto [items, foundIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, directions, type, true, isNatural, tableAliases);
    if (items.Items.empty()) {
        return std::numeric_limits<std::size_t>::max();
    }

    if (foundIdx >= 0) {
        return static_cast<std::size_t>(foundIdx);
    }

    InterestingOrderings.push_back(std::move(items));
    return InterestingOrderings.size() - 1;
}

std::size_t TFDStorage::AddInterestingOrdering(
    const std::vector<TJoinColumn>& interestingOrdering,
    TOrdering::EType type,
    TTableAliasMap* tableAliases
) {
    if (interestingOrdering.empty()) {
        return std::numeric_limits<std::size_t>::max();
    }

    auto [items, foundIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, {}, type, true, false, tableAliases);

    if (foundIdx >= 0) {
        return static_cast<std::size_t>(foundIdx);
    }

    InterestingOrderings.emplace_back(std::move(items));
    return InterestingOrderings.size() - 1;
}

TVector<TJoinColumn> TFDStorage::GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const {
    Y_ENSURE(interestingOrderingIdx < InterestingOrderings.size());

    TVector<TJoinColumn> columns;
    columns.reserve(InterestingOrderings[interestingOrderingIdx].Items.size());
    for (std::size_t columnIdx: InterestingOrderings[interestingOrderingIdx].Items) {
        columns.push_back(ColumnByIdx_[columnIdx]);
    }

    return columns;
}

TSorting TFDStorage::GetInterestingSortingByOrderingIdx(std::size_t interestingOrderingIdx) const {
    Y_ENSURE(interestingOrderingIdx < InterestingOrderings.size());

    TVector<TJoinColumn> columns;
    columns.reserve(InterestingOrderings[interestingOrderingIdx].Items.size());
    for (std::size_t columnIdx: InterestingOrderings[interestingOrderingIdx].Items) {
        columns.push_back(ColumnByIdx_[columnIdx]);
    }

    return {columns, InterestingOrderings[interestingOrderingIdx].Directions};
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
    TVector<TString> columnsMapping(IdCounter_);
    for (const auto& [column, idx]: IdxByColumn_) {
        TStringBuilder columnSs;
        columnSs << "{" << idx << ": " << column << "}";
        columnsMapping[idx] = columnSs;
    }
    ss << JoinSeq(", ", columnsMapping) << "\n";
    ss << "FDs: " << JoinSeq(", ", toVectorString(FDs)) << "\n";
    ss << "Interesting Orderings: " << JoinSeq(", ", toVectorString(InterestingOrderings)) << "\n";

    return ss;
}

std::function<bool(const std::vector<std::size_t>&)> GetItemsComparator(std::vector<std::size_t> items, bool isNatural) {
    if (isNatural) {
        return
            [items](const std::vector<std::size_t>& interestingOrdering) -> bool
            {
                return items == interestingOrdering;
            };
    }

    std::sort(items.begin(), items.end());

    return
        [items](std::vector<std::size_t> interestingOrdering) -> bool
        {
            std::sort(interestingOrdering.begin(), interestingOrdering.end());
            return items == interestingOrdering;
        };
}

std::pair<TOrdering, i64> TFDStorage::ConvertColumnsAndFindExistingOrdering(
    const std::vector<TJoinColumn>& interestingOrdering,
    const std::vector<TOrdering::TItem::EDirection>& directions,
    TOrdering::EType type,
    bool createIfNotExists,
    bool isNatural,
    TTableAliasMap* tableAliases
) {
    if(!(
        directions.empty() && type == TOrdering::EShuffle ||
        directions.size() == interestingOrdering.size() && type == TOrdering::ESorting
    )) {
        YQL_CLOG(TRACE, CoreDq)
            << "Ordering and directions sizes mismatch : " << directions.size() << " vs " << interestingOrdering.size();
        return {TOrdering(), -1};
    }

    std::vector<std::size_t> items = ConvertColumnIntoIndexes(interestingOrdering, createIfNotExists, tableAliases);
    if (items.empty()) {
        return {TOrdering(), -1};
    }

    auto comp = GetItemsComparator(items, isNatural);

    for (std::size_t i = 0; i < InterestingOrderings.size(); ++i) {
        if (
            comp(InterestingOrderings[i].Items) &&
            type == InterestingOrderings[i].Type &&
            directions == InterestingOrderings[i].Directions
        ) {
            return {InterestingOrderings[i], static_cast<i64>(i)};
        }
    }

    return {TOrdering(std::move(items), directions, type, isNatural), -1};
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

    if (IdxByColumn_.contains(fullPath)) {
        return IdxByColumn_[fullPath];
    }

    Y_ENSURE(!baseColumn.AttributeName.empty());
    if (!createIfNotExists) {
        return Max<size_t>();
    }

    ColumnByIdx_.push_back(baseColumn);
    IdxByColumn_[fullPath] = IdCounter_++;
    return IdxByColumn_[fullPath];
}

bool TOrderingsStateMachine::TLogicalOrderings::ContainsShuffle(i64 orderingIdx) {
    return IsInitialized() && HasState() && (orderingIdx >= 0) && Dfsm_->Nodes_[State_].InterestingOrderings[orderingIdx];
}

bool TOrderingsStateMachine::TLogicalOrderings::ContainsSorting(i64 orderingIdx) {
    return IsInitialized() && HasState() && (orderingIdx >= 0) && Dfsm_->Nodes_[State_].InterestingOrderings[orderingIdx];
}

void TOrderingsStateMachine::TLogicalOrderings::InduceNewOrderings(const TFDSet& fds) {
    AppliedFDs_ |= fds;

    if (!(HasState() && IsInitialized())) {
        return;
    }

    for (;;) {
        auto availableTransitions = Dfsm_->Nodes_[State_].OutgoingFDs & AppliedFDs_;
        if (availableTransitions.none()) {
            return;
        }

        std::size_t fdIdx = std::countr_zero(availableTransitions.to_ullong()); // take any edge
        State_ = Dfsm_->TransitionMatrix_[State_][fdIdx];
    }
}

void TOrderingsStateMachine::TLogicalOrderings::RemoveState() {
    *this = TLogicalOrderings(Dfsm_);
}

i64 TOrderingsStateMachine::TLogicalOrderings::GetInitOrderingIdx() const {
    return InitOrderingIdx_;
}

void TOrderingsStateMachine::TLogicalOrderings::SetOrdering(i64 orderingIdx) {
    if (!IsInitialized() || orderingIdx < 0 || orderingIdx >= static_cast<i64>(Dfsm_->InitStateByOrderingIdx_.size())) {
        RemoveState();
        return;
    }

    if (!IsInitialized()) {
        return;
    }

    InitOrderingIdx_ = orderingIdx;
    auto state = Dfsm_->InitStateByOrderingIdx_[orderingIdx];
    State_ = state.StateIdx;
    ShuffleHashFuncArgsCount_ = state.ShuffleHashFuncArgsCount;
}

i64 TOrderingsStateMachine::TLogicalOrderings::GetShuffleHashFuncArgsCount() {
    return ShuffleHashFuncArgsCount_;
}

void TOrderingsStateMachine::TLogicalOrderings::SetShuffleHashFuncArgsCount(std::size_t value) {
    ShuffleHashFuncArgsCount_ = value;
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::TLogicalOrderings::GetFDs() {
    return AppliedFDs_;
}

bool TOrderingsStateMachine::TLogicalOrderings::HasState() {
    return State_ != -1;
}

bool TOrderingsStateMachine::TLogicalOrderings::HasState() const {
    return State_ != -1;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsInitialized() {
    return Dfsm_ != nullptr;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsInitialized() const {
    return Dfsm_ != nullptr;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsSubsetOf(const TLogicalOrderings& logicalOrderings) {
    return
        HasState() && logicalOrderings.HasState() &&
        IsInitialized() && logicalOrderings.IsInitialized() &&
        Dfsm_ == logicalOrderings.Dfsm_ &&
        IsSubset(Dfsm_->Nodes_[State_].NFSMNodesBitset, logicalOrderings.Dfsm_->Nodes_[logicalOrderings.State_].NFSMNodesBitset);
}

i64 TOrderingsStateMachine::TLogicalOrderings::GetState() const {
    return State_;
}

bool TOrderingsStateMachine::TLogicalOrderings::IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs) {
    return (lhs & rhs) == lhs;
}

TOrderingsStateMachine::TLogicalOrderings TOrderingsStateMachine::CreateState() const {
    return TLogicalOrderings(Dfsm_.Get());
}

TOrderingsStateMachine::TLogicalOrderings TOrderingsStateMachine::CreateState(i64 orderingIdx) const {
    auto state = TLogicalOrderings(Dfsm_.Get());
    state.SetOrdering(orderingIdx);
    return state;
}

bool TOrderingsStateMachine::IsBuilt() const {
    return Built_;
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::GetFDSet(i64 fdIdx) {
    if (fdIdx < 0) { return TFDSet(); }
    return GetFDSet(std::vector<std::size_t> {static_cast<std::size_t>(fdIdx)});
}

TOrderingsStateMachine::TFDSet TOrderingsStateMachine::GetFDSet(const std::vector<std::size_t>& fdIdxes) {
    TFDSet fdSet;

    for (std::size_t fdIdx: fdIdxes) {
        if (FdMapping_[fdIdx] != -1) {
            fdSet[FdMapping_[fdIdx]] = 1;
        }
    }

    return fdSet;
}

TString TOrderingsStateMachine::ToString() const {
    TStringBuilder ss;
    ss << "TOrderingsStateMachine:\n";
    ss << "Built: " << (Built_ ? "true" : "false") << "\n";
    ss << "FdMapping: [" << JoinSeq(", ", FdMapping_) << "]\n";
    ss << "FDStorage:\n" << FDStorage.ToString() << "\n";
    ss << Nfsm_.ToString();
    ss << Dfsm_->ToString(Nfsm_);
    return ss;
}

void TOrderingsStateMachine::CollectItemInfo(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    std::size_t maxItem = 0;

    for (const auto& ordering: interestingOrderings) {
        if (ordering.Items.empty()) {
            continue;
        }

        std::size_t orderingMaxItem = *std::max_element(ordering.Items.begin(), ordering.Items.end());
        maxItem = std::max(maxItem, orderingMaxItem);
    }

    for (const auto& fd: fds) {
        std::size_t maxAntecedentItems = 0;
        if (!fd.AntecedentItems.empty()) {
            maxAntecedentItems = *std::max_element(fd.AntecedentItems.begin(), fd.AntecedentItems.end());
        }

        maxItem = std::max({maxItem, fd.ConsequentItem, maxAntecedentItems});
    }

    ItemInfo_.resize(maxItem + 1);

    for (const auto& ordering: interestingOrderings) {
        Y_ENSURE(ordering.Items.size() == ordering.Directions.size() || ordering.Directions.empty());

        for (const auto& [item, direction]: Zip(ordering.Items, ordering.Directions)) {
            switch (direction) {
                case TOrdering::TItem::EDirection::EAscending: {
                    ItemInfo_[item].UsedInAscOrdering = true;
                    break;
                }
                case TOrdering::TItem::EDirection::EDescending: {
                    ItemInfo_[item].UsedInDescOrdering = true;
                    break;
                }
                default: {}
            }
        }
    }

}

void TOrderingsStateMachine::Build(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    CollectItemInfo(fds, interestingOrderings);
    std::vector<TFunctionalDependency> processedFDs = PruneFDs(fds, interestingOrderings);
    Nfsm_.Build(processedFDs, interestingOrderings, ItemInfo_);
    Dfsm_ = MakeSimpleShared<TDFSM>();
    Dfsm_->Build(Nfsm_, processedFDs, interestingOrderings);
    Built_ = true;
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
    ss << "Edge{src=" << SrcNodeIdx
       << ", dst=" << DstNodeIdx
       << ", fdIdx=" << (FdIdx == EPSILON ? "EPSILON" : std::to_string(FdIdx))
       << "}";
    return ss;
}

std::size_t TOrderingsStateMachine::TNFSM::Size() {
    return Nodes_.size();
}

TString TOrderingsStateMachine::TNFSM::ToString() const {
    TStringBuilder ss;
    ss << "NFSM:\n";
    ss << "Nodes (" << Nodes_.size() << "):\n";
    for (std::size_t i = 0; i < Nodes_.size(); ++i) {
        ss << "  " << i << ": " << Nodes_[i].ToString() << "\n";
    }
    ss << "Edges (" << Edges_.size() << "):\n";
    for (std::size_t i = 0; i < Edges_.size(); ++i) {
        ss << "  " << i << ": " << Edges_[i].ToString() << "\n";
    }
    return ss;
}

void TOrderingsStateMachine::TNFSM::Build(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interesting,
    const std::vector<TItemInfo>& itemInfo
) {
    for (std::size_t i = 0; i < interesting.size(); ++i) {
        AddNode(interesting[i], TNFSM::TNode::EInteresting, i);
    }

    ApplyFDs(fds, interesting, itemInfo);
    PrefixClosure();

    for (std::size_t idx = 0; idx < Edges_.size(); ++idx) {
        Nodes_[Edges_[idx].SrcNodeIdx].OutgoingEdges.push_back(idx);
    }
}

std::size_t TOrderingsStateMachine::TNFSM::AddNode(const TOrdering& ordering, TNode::EType type, i64 interestingOrderingIdx) {
    for (std::size_t i = 0; i < Nodes_.size(); ++i) {
        if (Nodes_[i].Ordering == ordering) {
            return i;
        }
    }

    Nodes_.emplace_back(ordering, type, interestingOrderingIdx);
    return Nodes_.size() - 1;
}

bool TOrderingsStateMachine::TNFSM::TEdge::operator==(const TEdge& other) const {
    return std::tie(SrcNodeIdx, DstNodeIdx, FdIdx) == std::tie(other.SrcNodeIdx, other.DstNodeIdx, other.FdIdx);
}

void TOrderingsStateMachine::TNFSM::AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx) {
    auto newEdge = TNFSM::TEdge(srcNodeIdx, dstNodeIdx, fdIdx);
    for (std::size_t i = 0; i < Edges_.size(); ++i) {
        if (Edges_[i] == newEdge) {
            return;
        }
    }
    Edges_.emplace_back(newEdge);
}

void TOrderingsStateMachine::TNFSM::PrefixClosure() {
    for (std::size_t i = 0; i < Nodes_.size(); ++i) {
        const auto& iItems = Nodes_[i].Ordering.Items;

        for (std::size_t j = 0; j < Nodes_.size(); ++j) {
            const auto& jItems = Nodes_[j].Ordering.Items;
            if (i == j || iItems.size() >= jItems.size()) {
                continue;
            }

            std::size_t k = 0;
            for (; k < iItems.size() && (iItems[k] == jItems[k]); ++k) {
            }

            if (k == iItems.size()) {
                if (Nodes_[i].Ordering.Type == TOrdering::EShuffle) {
                    AddEdge(i, j, TNFSM::TEdge::EPSILON);
                }

                Y_ENSURE(Nodes_[i].Ordering.Directions.size() <= Nodes_[j].Ordering.Directions.size());
                bool areDirsCompatable = std::equal(
                    Nodes_[i].Ordering.Directions.begin(),
                    Nodes_[i].Ordering.Directions.end(),
                    Nodes_[j].Ordering.Directions.begin()
                );
                if (Nodes_[i].Ordering.Type == TOrdering::ESorting && areDirsCompatable) {
                    AddEdge(j, i, TNFSM::TEdge::EPSILON);
                }
            }
        }
    }
}

void TOrderingsStateMachine::TNFSM::ApplyFDs(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings,
    const std::vector<TItemInfo>& itemInfo
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

    for (std::size_t nodeIdx = 0; nodeIdx < Nodes_.size() && Nodes_.size() < EMaxNFSMStates; ++nodeIdx) {
        for (std::size_t fdIdx = 0; fdIdx < fds.size() && Nodes_.size() < EMaxNFSMStates; ++fdIdx) {
            if (Nodes_[nodeIdx].Ordering.Items.empty()) {
                continue;
            }

            TFunctionalDependency fd = fds[fdIdx];

            auto applyFD = [this, &itemInfo, nodeIdx, maxInterestingOrderingSize](const TFunctionalDependency& fd, std::size_t fdIdx) {
                if (Nodes_.size() >= EMaxNFSMStates) {
                    return;
                }

                if (fd.IsConstant() && Nodes_[nodeIdx].Ordering.Items.size() > 1) {
                    std::vector<std::size_t> newOrdering = Nodes_[nodeIdx].Ordering.Items;
                    auto it = std::find(newOrdering.begin(), newOrdering.end(), fd.ConsequentItem);
                    if (it == newOrdering.end()) {
                        return;
                    }
                    bool isNewOrderingPrefixOfOld = (it == (newOrdering.end() - 1));

                    auto newDirections = Nodes_[nodeIdx].Ordering.Directions;
                    if (!newDirections.empty()) {
                        newDirections.erase(newDirections.begin() + std::distance(newOrdering.begin(), it));
                        newOrdering.erase(it);
                    }

                    std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), std::move(newDirections), Nodes_[nodeIdx].Ordering.Type), TNode::EArtificial);

                    if (!isNewOrderingPrefixOfOld || Nodes_[nodeIdx].Ordering.Type == TOrdering::EShuffle) {
                        AddEdge(nodeIdx, dstIdx, fdIdx);
                    }

                    if (!isNewOrderingPrefixOfOld || Nodes_[nodeIdx].Ordering.Type == TOrdering::ESorting) {
                        AddEdge(dstIdx, nodeIdx, fdIdx);
                    }
                }

                if (fd.IsConstant()) {
                    return;
                }

                auto maybeAntecedentItemIdx = fd.MatchesAntecedentItems(Nodes_[nodeIdx].Ordering);
                if (!maybeAntecedentItemIdx) {
                    return;
                }

                std::size_t antecedentItemIdx = maybeAntecedentItemIdx.GetRef();
                if (
                    auto it = std::find(Nodes_[nodeIdx].Ordering.Items.begin(), Nodes_[nodeIdx].Ordering.Items.end(), fd.ConsequentItem);
                    it != Nodes_[nodeIdx].Ordering.Items.end()
                ) {
                    if (fd.IsEquivalence()) { // swap (a, b) -> (b, a)
                        std::size_t consequentItemIdx = std::distance(Nodes_[nodeIdx].Ordering.Items.begin(), it);
                        auto newOrdering = Nodes_[nodeIdx].Ordering.Items;
                        std::swap(newOrdering[antecedentItemIdx], newOrdering[consequentItemIdx]);
                        std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes_[nodeIdx].Ordering.Directions, Nodes_[nodeIdx].Ordering.Type), TNode::EArtificial);
                        AddEdge(nodeIdx, dstIdx, fdIdx);
                        AddEdge(dstIdx, nodeIdx, fdIdx);
                    }

                    return;
                }

                Y_ENSURE(antecedentItemIdx < Nodes_[nodeIdx].Ordering.Items.size());
                if (fd.IsEquivalence()) {
                    Y_ENSURE(fd.AntecedentItems.size() == 1);
                    std::vector<std::size_t> newOrdering = Nodes_[nodeIdx].Ordering.Items;
                    newOrdering[antecedentItemIdx] = fd.ConsequentItem;

                    std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), Nodes_[nodeIdx].Ordering.Directions, Nodes_[nodeIdx].Ordering.Type), TNode::EArtificial);
                    AddEdge(nodeIdx, dstIdx, fdIdx);
                    AddEdge(dstIdx, nodeIdx, fdIdx);
                }

                if (
                    Nodes_[nodeIdx].Ordering.Type == TOrdering::EShuffle ||
                    Nodes_[nodeIdx].Ordering.Items.size() == maxInterestingOrderingSize
                ) {
                    return;
                }

                if (fd.IsImplication() || fd.IsEquivalence()) {
                    for (std::size_t i = antecedentItemIdx + fd.AntecedentItems.size(); i <= Nodes_[nodeIdx].Ordering.Items.size() && Nodes_.size() < EMaxNFSMStates; ++i) {
                        std::vector<std::size_t> newOrdering = Nodes_[nodeIdx].Ordering.Items;
                        newOrdering.insert(newOrdering.begin() + i, fd.ConsequentItem);

                        auto newDirections = Nodes_[nodeIdx].Ordering.Directions;
                        if (newDirections.empty()) { // smthing went wrong during ordering adding stage
                            return;
                        }
                        newDirections.insert(newDirections.begin() + i, TOrdering::TItem::EDirection::ENone);

                        Y_ENSURE(fd.ConsequentItem < itemInfo.size());

                        if (itemInfo[fd.ConsequentItem].UsedInAscOrdering) {
                            newDirections[i] = TOrdering::TItem::EDirection::EAscending;
                            std::size_t dstIdx = AddNode(TOrdering(newOrdering, newDirections, Nodes_[nodeIdx].Ordering.Type), TNode::EArtificial);
                            AddEdge(nodeIdx, dstIdx, fdIdx); // Epsilon edge will be added during PrefixClosure
                        }

                        if (itemInfo[fd.ConsequentItem].UsedInDescOrdering) {
                            newDirections[i] = TOrdering::TItem::EDirection::EDescending;
                            std::size_t dstIdx = AddNode(TOrdering(std::move(newOrdering), std::move(newDirections), Nodes_[nodeIdx].Ordering.Type), TNode::EArtificial);
                            AddEdge(nodeIdx, dstIdx, fdIdx); // Epsilon edge will be added during PrefixClosure
                        }
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
    ss << "Edge{src=" << SrcNodeIdx
       << ", dst=" << DstNodeIdx
       << ", fdIdx=" << FdIdx
       << "}";
    return ss;
}

std::size_t TOrderingsStateMachine::TDFSM::Size() {
    return Nodes_.size();
}

TString TOrderingsStateMachine::TDFSM::ToString(const TNFSM& nfsm) const {
    TStringBuilder ss;
    ss << "DFSM:\n";
    ss << "Nodes (" << Nodes_.size() << "):\n";
    for (std::size_t i = 0; i < Nodes_.size(); ++i) {
        ss << "  " << i << ": " << Nodes_[i].ToString() << "\n";
        ss << "    NFSMNodes: [";
        for (std::size_t j = 0; j < Nodes_[i].NFSMNodes.size(); ++j) {
            std::size_t nfsmNodeIdx = Nodes_[i].NFSMNodes[j];
            if (j > 0) {
                ss << ", ";
            }
            ss << nfsmNodeIdx;
            if (nfsmNodeIdx < nfsm.Nodes_.size()) {
                ss << "(" << nfsm.Nodes_[nfsmNodeIdx].Ordering.ToString() << ")";
            }
        }
        ss << "]\n";
    }
    ss << "Edges (" << Edges_.size() << "):\n";
    for (std::size_t i = 0; i < Edges_.size(); ++i) {
        ss << "  " << i << ": " << Edges_[i].ToString() << "\n";
    }
    ss << "InitStateByOrderingIdx (" << InitStateByOrderingIdx_.size() << "):\n";
    for (std::size_t i = 0; i < InitStateByOrderingIdx_.size(); ++i) {
        ss << "  " << i << ": StateIdx=" << InitStateByOrderingIdx_[i].StateIdx
           << ", ShuffleHashFuncArgsCount=" << InitStateByOrderingIdx_[i].ShuffleHashFuncArgsCount << "\n";
    }
    return ss;
}

void TOrderingsStateMachine::TDFSM::Build(
    const TNFSM& nfsm,
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings
) {
    InitStateByOrderingIdx_.resize(interestingOrderings.size());

    for (std::size_t i = 0; i < interestingOrderings.size(); ++i) {
        for (std::size_t nfsmNodeIdx = 0; nfsmNodeIdx < nfsm.Nodes_.size(); ++nfsmNodeIdx) {
            if (nfsm.Nodes_[nfsmNodeIdx].Ordering == interestingOrderings[i]) {
                auto nfsmNodes = CollectNodesWithEpsOrFdEdge(nfsm, {i}, fds);
                InitStateByOrderingIdx_[i] = TInitState{AddNode(std::move(nfsmNodes)), interestingOrderings[i].Items.size()};
            }
        }
    }

    for (std::size_t nodeIdx = 0; nodeIdx < Nodes_.size() && Nodes_.size() < EMaxDFSMStates; ++nodeIdx) {
        std::unordered_set<i64> outgoingDFSMNodeFDs;
        for (std::size_t nfsmNodeIdx: Nodes_[nodeIdx].NFSMNodes) {
            for (std::size_t nfsmEdgeIdx: nfsm.Nodes_[nfsmNodeIdx].OutgoingEdges) {
                outgoingDFSMNodeFDs.insert(nfsm.Edges_[nfsmEdgeIdx].FdIdx);
            }
        }

        for (i64 fdIdx: outgoingDFSMNodeFDs) {
            if (fdIdx == TNFSM::TEdge::EPSILON) {
                continue;
            }

            std::size_t dstNodeIdx = AddNode(CollectNodesWithEpsOrFdEdge(nfsm, Nodes_[nodeIdx].NFSMNodes, fds, fdIdx));
            if (nodeIdx == dstNodeIdx) {
                continue;
            }
            AddEdge(nodeIdx, dstNodeIdx, fdIdx);

            Nodes_[nodeIdx].OutgoingFDs[fdIdx] = 1;
        }
    }

    Precompute(nfsm, fds);
}

std::size_t TOrderingsStateMachine::TDFSM::AddNode(const std::vector<std::size_t>& nfsmNodes) {
    for (std::size_t i = 0; i < Nodes_.size(); ++i) {
        if (Nodes_[i].NFSMNodes == nfsmNodes) {
            return i;
        }
    }

    Nodes_.emplace_back(nfsmNodes);
    return Nodes_.size() - 1;
}

void TOrderingsStateMachine::TDFSM::AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx) {
    Edges_.emplace_back(srcNodeIdx, dstNodeIdx, fdIdx);
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

        for (std::size_t edgeIdx: nfsm.Nodes_[nodeIdx].OutgoingEdges) {
            const TNFSM::TEdge& edge = nfsm.Edges_[edgeIdx];
            if (edge.FdIdx == fdIdx || edge.FdIdx == TNFSM::TEdge::EPSILON || fds[edge.FdIdx].AlwaysActive) {
                DFS(edge.DstNodeIdx);
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
    TransitionMatrix_ = std::vector<std::vector<i64>>(Nodes_.size(), std::vector<i64>(fds.size(), -1));
    for (const auto& edge: Edges_) {
        TransitionMatrix_[edge.SrcNodeIdx][edge.FdIdx] = edge.DstNodeIdx;
    }

    for (std::size_t dfsmNodeIdx = 0; dfsmNodeIdx < Nodes_.size(); ++dfsmNodeIdx) {
        for (std::size_t nfsmNodeIdx : Nodes_[dfsmNodeIdx].NFSMNodes) {
            auto interestingOrderIdx = nfsm.Nodes_[nfsmNodeIdx].InterestingOrderingIdx;
            if (interestingOrderIdx == -1) { continue; }

            Nodes_[dfsmNodeIdx].InterestingOrderings[interestingOrderIdx] = 1;
        }
    }

    for (auto& node: Nodes_) {
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
    FdMapping_.resize(fds.size());
    for (std::size_t i = 0; i < fds.size(); ++i) {
        bool canLeadToInteresting = false;

        for (const auto& ordering: interestingOrderings) {
            if (ordering.HasItem(fds[i].ConsequentItem)) {
                canLeadToInteresting = true;
                break;
            }

            if (
                fds[i].IsEquivalence() &&
                ordering.HasItem(fds[i].AntecedentItems[0])
            ) {
                canLeadToInteresting = true;
                break;
            }
        }

        if (canLeadToInteresting && filteredFds.size() < EMaxFDCount) {
            filteredFds.push_back(std::move(fds[i]));
            FdMapping_[i] = filteredFds.size() - 1;
        } else {
            FdMapping_[i] = -1;
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
