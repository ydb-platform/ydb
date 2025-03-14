#pragma once

#include <yql/essentials/core/yql_cost_function.h>

#include <util/generic/hash.h>

#include <bitset>
#include <stdint.h>
#include <vector>

/*
 * This header contains all essentials to work with Interesting Orderings.
 * Interesting ordering is an ordering which is produced by a query or tested in the query
 * At this moment, we process only shuffles, but in future there will be groupings and sortings
 * For details of the algorithms and examples look at the white papers -
 * - "An efficient framework for order optimization" / "A Combined Framework for Grouping and Order Optimization"
 */

namespace NYql::NDq {

template<typename Sep, typename Container>
TString JoinSubsequence(const Sep& sep, const Container& container) {
    std::ostringstream ss;
    auto it = container.begin();
    if (it != container.end()) {
        ss << *it;
        ++it;
    }
    for (; it != container.end(); ++it) {
        ss << sep << *it;
    }
    return ss.str();
}

struct TOrdering {
    enum EType : uint32_t {
        EShuffle
    };

    bool operator==(const TOrdering& other) const;
    TString ToString() const;

    TOrdering(
        std::vector<std::size_t> items,
        EType type,
        bool isNatural = false
    )
        : Items(std::move(items))
        , Type(type)
        , IsNatural(isNatural)
    {}

    std::string ToString() const {
        return "{" + JoinSubsequence(", ", Items) + "}";
    }

    TOrdering(
        std::vector<std::size_t> items,
        EType type,
        bool isNatural = false
    )
        : Items(std::move(items))
        , Type(type)
        , IsNatural(isNatural)
    {}

    std::vector<std::size_t> Items;
    EType Type;
    /* Definition was taken from 'Complex Ordering Requirements' section. Not natural orderings are complex join predicates or grouping. */
    bool IsNatural = false;
};

/*
 * A relation 'R' satisfies a functional dependency (or FD): A -> B if and only if
 * forall t1, t2 from the 'R' : t1.A = t2.A -> t1.B = t2.B
 * Examples:
 *      b = cos(a) : a -> b
 *      a = b : a -> b, b -> a
 *      a = const : {} -> a
 */
struct TFunctionalDependency {
    enum EType : uint32_t {
        EEquivalence
    };

    bool IsEquivalence() const;
    bool MatchesAntecedentItems(const TOrdering& ordering) const;
    TString ToString() const;

    std::string ToString() const {
        std::ostringstream ss;
        ss << "{" + JoinSubsequence(", ", AntecedentItems) + "}";

        if (Type == EEquivalence) {
            ss << " = ";
        } else {
            ss << " -> ";
        }
        ss << ConsequentItem;

        return ss.str();
    }


    std::vector<std::size_t> AntecedentItems;
    std::size_t ConsequentItem;

    EType Type;
    bool AlwaysActive;
};

// Map of table aliases to their original table names
struct TTableAliasMap : public TSimpleRefCount<TTableAliasMap> {
public:
    struct TBaseColumn {
        TBaseColumn() = default;

        TBaseColumn(
            TString relation,
            TString column
        )
            : Relation(std::move(relation))
            , Column(std::move(column))
        {}

        TBaseColumn(const TBaseColumn& other)
            : Relation(other.Relation)
            , Column(other.Column)
        {}

<<<<<<< HEAD
        TBaseColumn& operator=(const TBaseColumn& other);

        NDq::TJoinColumn ToJoinColumn();
        operator bool();
=======
        TBaseColumn& operator=(const TBaseColumn& other) {
            if (this != &other) {
                Relation = other.Relation;
                Column = other.Column;
            }
            return *this;
        }

        NDq::TJoinColumn ToJoinColumn() {
            return NDq::TJoinColumn(Relation, Column);
        }

        operator bool() {
            return !(Relation.empty() && Column.empty());
        }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

        TString Relation;
        TString Column;
    };

    TTableAliasMap() = default;

<<<<<<< HEAD
    void AddMapping(const TString& table, const TString& alias);
    void AddRename(const TString& from, const TString& to);
    TBaseColumn GetBaseColumnByRename(const TString& renamedColumn);
    TBaseColumn GetBaseColumnByRename(const NDq::TJoinColumn& renamedColumn);
    TString ToString() const;
    void Merge(const TTableAliasMap& other);

private:
    TString GetBaseTableByAlias(const TString& alias);

private:
    THashMap<TString, TString> TableByAlias;
=======
    void AddMapping(const TString& table, const TString& alias) {
        TableByAlias[alias] = table;
    }

    void AddRename(const TString& from, const TString& to) {
        Cout << "FROM: " << from << " TO: " << to << Endl;
        Cout << ToString() << Endl;

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

    TBaseColumn GetBaseColumnByRename(const TString& renamedColumn) {
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

    TBaseColumn GetBaseColumnByRename(const NDq::TJoinColumn& renamedColumn) {
        return GetBaseColumnByRename(renamedColumn.RelName + "." + renamedColumn.AttributeName);
    }

    TString ToString() const {
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

    void Merge(const TTableAliasMap& other) {
        for (const auto& [alias, table] : other.TableByAlias) {
            TableByAlias[alias] = table;
        }
        for (const auto& [from, to] : other.BaseColumnByRename) {
            BaseColumnByRename[from] = TBaseColumn(to.Relation, to.Column);
        }
    }

private:
    TString GetBaseTableByAlias(const TString& alias) {
        return TableByAlias[alias];
    }

private:
    THashMap<TString, TString> TableByAlias;

>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
    THashMap<TString, TBaseColumn> BaseColumnByRename;
};

/*
 * This class contains internal representation of the columns (mapping [column -> int]), FDs and interesting orderings
 */
class TFDStorage {
public:
<<<<<<< HEAD
    i64 FindFDIdx(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        TTableAliasMap* tableAliases = nullptr
    );
=======
    std::int64_t FindFDIdx(
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
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

    std::size_t AddFD(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        bool alwaysActive,
<<<<<<< HEAD
        TTableAliasMap* tableAliases = nullptr
    );
=======
        TTableAliasMap* tableAliases
    ) {
        auto fd = TFunctionalDependency{
            .AntecedentItems = {GetIdxByColumn(antecedentColumn, true, tableAliases)},
            .ConsequentItem = GetIdxByColumn(consequentColumn, true, tableAliases),
            .Type = type,
            .AlwaysActive = alwaysActive
        };
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

    i64 FindInterestingOrderingIdx(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases = nullptr
    );

    std::int64_t FindInterestingOrderingIdx(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases
    ) {
        const auto& [_, orderingIdx] = ConvertColumnsAndFindExistingOrdering(interestingOrdering, type, false, tableAliases);
        return orderingIdx;
    }

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
<<<<<<< HEAD
        TTableAliasMap* tableAliases = nullptr
    );

    TVector<TJoinColumn> GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const;
    TString ToString() const;
=======
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

    TVector<TJoinColumn> GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const {
        Y_ASSERT(interestingOrderingIdx < InterestingOrderings.size());

        TVector<TJoinColumn> columns;
        columns.reserve(InterestingOrderings[interestingOrderingIdx].Items.size());
        for (std::size_t columnIdx: InterestingOrderings[interestingOrderingIdx].Items) {
            columns.push_back(ColumnByIdx[columnIdx]);
        }

        return columns;
    }

    std::string ToString() const {
        auto toVectorString = [](auto seq) {
            TVector<TString> strVector;
            strVector.reserve(seq.size());
            for (const auto& item: seq) {
                strVector.push_back(item.ToString());
            }
            return strVector;
        };

        std::stringstream ss;

        ss << "Columns mapping: ";
        TVector<TString> columnsMapping(IdCounter);
        for (const auto& [column, idx]: IdxByColumn) {
            std::stringstream columnSs;
            columnSs << "{" << idx << ": " << column << "}";
            columnsMapping[idx] = columnSs.str();
        }
        ss << JoinSubsequence(", ", columnsMapping) << "\n";
        ss << "FDs: " << JoinSubsequence(", ", toVectorString(FDs)) << "\n";
        ss << "Interesting Orderings: " << JoinSubsequence(", ", toVectorString(InterestingOrderings)) << "\n";

        return ss.str();
    }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)


public:
    std::vector<TFunctionalDependency> FDs;
    std::vector<TOrdering> InterestingOrderings;

private:
<<<<<<< HEAD
    std::pair<std::vector<std::size_t>, i64> ConvertColumnsAndFindExistingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        bool createIfNotExists,
        TTableAliasMap* tableAliases = nullptr
    );

    std::vector<std::size_t> ConvertColumnIntoIndexes(
        const std::vector<TJoinColumn>& ordering,
        bool createIfNotExists,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t GetIdxByColumn(
        const TJoinColumn& column,
        bool createIfNotExists,
        TTableAliasMap* tableAliases = nullptr
    );
=======
    /*
     * Returns converted columns (interestingOrdering -> inner representation),
     * and index > 0 of the ordering if it has been already added, -1 otherwise
     */
    std::pair<std::vector<std::size_t>, std::int64_t> ConvertColumnsAndFindExistingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        bool createIfNotExists,
        TTableAliasMap* tableAliases
    ) {
        std::vector<std::size_t> items = ConvertColumnIntoIndexes(interestingOrdering, createIfNotExists, tableAliases);

        for (std::size_t i = 0; i < InterestingOrderings.size(); ++i) {
            if (items == InterestingOrderings[i].Items && type == InterestingOrderings[i].Type) {
                return {items, static_cast<std::int64_t>(i)};
            }
        }

        return {items, -1};
    }

    std::vector<std::size_t> ConvertColumnIntoIndexes(
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

    std::size_t GetIdxByColumn(
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

        const std::string fullPath = baseColumn.RelName + "." + baseColumn.AttributeName;

        if (IdxByColumn.contains(fullPath)) {
            return IdxByColumn[fullPath];
        }

        Y_ENSURE(!baseColumn.AttributeName.empty());
        Y_ENSURE(createIfNotExists, "There's no such column: " + fullPath);

        ColumnByIdx.push_back(baseColumn);
        IdxByColumn[fullPath] = IdCounter++;
        return IdxByColumn[fullPath];
    }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

private:
    THashMap<TString, std::size_t> IdxByColumn;
    std::vector<TJoinColumn> ColumnByIdx;
    std::size_t IdCounter = 0;
};

/*
 * This class represents Finite-State Machine (FSM). The state in this machine is all available logical orderings.
 * The steps of building the FSM:
 *      1) Construct nodes of the NFSM (Non-Determenistic FSM)
 *      2) Prune functional dependencies which won't lead us to the interestng orderings
 *      3) Construct edges of the NFSM
 *      4) It is inconvenient to work with NFSM, because it contains many states at the moment, so
 *         we will convert NFSM to DFSM (Determenistic FSM) and precompute values for it for O(1) switch state operations.
 */
class TOrderingsStateMachine {
private:
    class TDFSM;
    enum _ : std::uint32_t {
        EMaxFDCount = 64,
        EMaxNFSMStates = 512,
        EMaxDFSMStates = 32768,
    };

public:
    using TFDSet = std::bitset<EMaxFDCount>;

    /*
     * This class represents a state of the FSM (node idx in the DFSM and some metadata).
     */
    class TLogicalOrderings {
    public:
        TLogicalOrderings() = default;
        TLogicalOrderings(const TLogicalOrderings&) = default;
        TLogicalOrderings& operator= (const TLogicalOrderings&) = default;

        TLogicalOrderings(TDFSM* dfsm)
            : DFSM(dfsm)
        {}

    public: // API
<<<<<<< HEAD
        bool ContainsShuffle(std::size_t orderingIdx);
        void InduceNewOrderings(const TFDSet& fds);
        void RemoveState();
        void SetOrdering(i64 orderingIdx);
        i64 GetShuffleHashFuncArgsCount();
        void SetShuffleHashFuncArgsCount(std::size_t value);
        TFDSet GetFDs();
        bool HasState();
        bool HasState() const;
        bool IsSubsetOf(const TLogicalOrderings& logicalOrderings);
        i64 GetState() const;
=======
        bool ContainsShuffle(std::size_t orderingIdx) {
            return DFSM->ContainsMatrix[State][orderingIdx];
        }

        void InduceNewOrderings(const TFDSet& fds) {
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

        void RemoveState() {
            *this = TLogicalOrderings(DFSM);
        }

        void SetOrdering(std::int64_t orderingIdx) {
            Y_ASSERT(0 <= orderingIdx && orderingIdx < static_cast<std::int64_t>(DFSM->InitStateByOrderingIdx.size()));

            auto state = DFSM->InitStateByOrderingIdx[orderingIdx];
            State = state.StateIdx;
            ShuffleHashFuncArgsCount = state.ShuffleHashFuncArgsCount;
        }

        std::int64_t GetShuffleHashFuncArgsCount() {
            return ShuffleHashFuncArgsCount;
        }

        void SetShuffleHashFuncArgsCount(std::size_t value) {
            ShuffleHashFuncArgsCount = value;
        }

        TFDSet GetFDs() {
            return AppliedFDs;
        }

        bool HasState() {
            return State != -1;
        }

        bool HasState() const {
            return State != -1;
        }

        inline bool IsSubsetOf(const TLogicalOrderings& logicalOrderings) {
            Y_ASSERT(DFSM == logicalOrderings.DFSM);
            return HasState() && logicalOrderings.HasState() && IsSubset(DFSM->Nodes[State].NFSMNodesBitset, logicalOrderings.DFSM->Nodes[logicalOrderings.State].NFSMNodesBitset);
        }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

        std::int64_t GetState() const {
            return State;
        }

    private:
        bool IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs);

    private:
        TDFSM* DFSM = nullptr;
        /* we can have different args in hash shuffle function, so shuffles can be incompitable in this case */
        i64 ShuffleHashFuncArgsCount = -1;
        i64 State = -1;
        TFDSet AppliedFDs{};
    };

<<<<<<< HEAD
    TLogicalOrderings CreateState();
    TLogicalOrderings CreateState(i64 orderingIdx);
=======
    TLogicalOrderings CreateState() {
        return TLogicalOrderings(DFSM.Get());
    }

    TLogicalOrderings CreateState(std::int64_t orderingIdx) {
        auto state = TLogicalOrderings(DFSM.Get());
        state.SetOrdering(orderingIdx);
        return state;
    }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

public:
    TOrderingsStateMachine() = default;

    TOrderingsStateMachine(TFDStorage fdStorage)
        : FDStorage(std::move(fdStorage))
        , DFSM(MakeSimpleShared<TDFSM>())
    {
        Build(FDStorage.FDs, FDStorage.InterestingOrderings);
    }

<<<<<<< HEAD
    TOrderingsStateMachine(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    ) {
        DFSM = MakeSimpleShared<TDFSM>();
        Build(fds, interestingOrderings);
=======
public:
    TFDStorage FDStorage;

    bool IsBuilt() const {
        return Built;
    }

    TFDSet GetFDSet(std::int64_t fdIdx) {
        if (fdIdx < 0) { return TFDSet(); }
        return GetFDSet(std::vector<std::size_t> {static_cast<std::size_t>(fdIdx)});
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
    }

public:
    TFDStorage FDStorage;
    bool IsBuilt() const;
    TFDSet GetFDSet(i64 fdIdx);
    TFDSet GetFDSet(const std::vector<std::size_t>& fdIdxes);
    TString ToString() const;

<<<<<<< HEAD
=======
    TFDSet GetFDSet(const std::vector<std::size_t>& fdIdxes) {
        TFDSet fdSet;

        for (std::size_t fdIdx: fdIdxes) {
            if (FdMapping[fdIdx] != -1) {
                fdSet[FdMapping[fdIdx]] = 1;
            }
        }

        return fdSet;
    }

    std::string ToString() const {
        std::ostringstream ss;
        ss << "TOrderingsStateMachine:\n";
        ss << "Built: " << (Built ? "true" : "false") << "\n";
        ss << "FdMapping: [" << JoinSubsequence(", ", FdMapping) << "]\n";
        ss << "FDStorage:\n" << FDStorage.ToString() << "\n";
        ss << NFSM.ToString();
        ss << DFSM->ToString(NFSM);
        return ss.str();
    }

>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
private:
    void Build(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
<<<<<<< HEAD
    );
=======
    ) {
        std::vector<TFunctionalDependency> processedFDs = PruneFDs(fds, interestingOrderings);
        NFSM.Build(processedFDs, interestingOrderings);
        DFSM->Build(NFSM, processedFDs, interestingOrderings);
        Built = true;
    }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)

private:
    class TNFSM {
    public:
        friend class TDFSM;

        struct TNode {
            enum EType : uint32_t {
                EArtificial,
                EInteresting
            };

            TNode(TOrdering ordering, EType type, i64 interestingOrderingIdx = -1)
                : Type(type)
                , Ordering(ordering)
                , InterestingOrderingIdx(interestingOrderingIdx)
            {}

            EType Type;
            std::vector<std::size_t> OutgoingEdges;

            TOrdering Ordering;
<<<<<<< HEAD
            i64 InterestingOrderingIdx; // -1 if node isn't interesting

            TString ToString() const;
=======
            std::int64_t InterestingOrderingIdx; // -1 if node isn't interesting

            std::string ToString() const {
                std::ostringstream ss;
                ss << "Node{Type=" << (Type == EArtificial ? "Artificial" : "Interesting")
                   << ", Ordering=" << Ordering.ToString()
                   << ", InterestingOrderingIdx=" << InterestingOrderingIdx
                   << ", OutgoingEdges=[" << JoinSubsequence(", ", OutgoingEdges) << "]}";
                return ss.str();
            }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
            i64 fdIdx;

            enum _ : i64 {
                EPSILON = -1 // eps edges with give us nodes without applying any FDs.
            };

<<<<<<< HEAD
            TString ToString() const;
        };

        std::size_t Size();
        TString ToString() const;
=======
            std::string ToString() const {
                std::ostringstream ss;
                ss << "Edge{src=" << srcNodeIdx
                   << ", dst=" << dstNodeIdx
                   << ", fdIdx=" << (fdIdx == EPSILON ? "EPSILON" : std::to_string(fdIdx))
                   << "}";
                return ss.str();
            }
        };

        std::size_t Size() {
            return Nodes.size();
        }

        std::string ToString() const {
            std::ostringstream ss;
            ss << "NFSM:\n";
            ss << "Nodes (" << Nodes.size() << "):\n";
            for (std::size_t i = 0; i < Nodes.size(); ++i) {
                ss << "  " << i << ": " << Nodes[i].ToString() << "\n";
            }
            ss << "Edges (" << Edges.size() << "):\n";
            for (std::size_t i = 0; i < Edges.size(); ++i) {
                ss << "  " << i << ": " << Edges[i].ToString() << "\n";
            }
            return ss.str();
        }

    public:
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
        void Build(
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interesting
        );

    private:
        std::size_t AddNode(const TOrdering& ordering, TNode::EType type, i64 interestingOrderingIdx = -1);
        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx);
        void PrefixClosure();
        void ApplyFDs(const std::vector<TFunctionalDependency>& fds);

    private:
        std::vector<TNode> Nodes;
        std::vector<TEdge> Edges;
    };

    class TDFSM {
    public:
        friend class TLogicalOrderings;
        friend class TStateMachineBuilder;

        struct TNode {
            std::vector<std::size_t> NFSMNodes;
            std::bitset<EMaxFDCount> OutgoingFDs;
            std::bitset<EMaxNFSMStates> NFSMNodesBitset;

<<<<<<< HEAD
            TString ToString() const;
=======
            std::string ToString() const {
                std::ostringstream ss;
                ss << "Node{NFSMNodes=[" << JoinSubsequence(", ", NFSMNodes) << "], "
                   << "OutgoingFDs=" << OutgoingFDs.to_string() << "}";
                return ss.str();
            }
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
<<<<<<< HEAD
            i64 fdIdx;

            TString ToString() const;
        };

        std::size_t Size();
        TString ToString(const TNFSM& nfsm) const;
=======
            std::int64_t fdIdx;

            std::string ToString() const {
                std::ostringstream ss;
                ss << "Edge{src=" << srcNodeIdx
                   << ", dst=" << dstNodeIdx
                   << ", fdIdx=" << fdIdx
                   << "}";
                return ss.str();
            }
        };

        std::size_t Size() {
            return Nodes.size();
        }

        std::string ToString(const TNFSM& nfsm) const {
            std::ostringstream ss;
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
            return ss.str();
        }

    public:
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
        void Build(
            const TNFSM& nfsm,
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interestingOrderings
        );

    private:
        std::size_t AddNode(const std::vector<std::size_t>& nfsmNodes);
        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx);
        std::vector<std::size_t> CollectNodesWithEpsOrFdEdge(
            const TNFSM& nfsm,
            const std::vector<std::size_t>& startNFSMNodes,
            i64 fdIdx = TNFSM::TEdge::EPSILON
        );
        void Precompute(
            const TNFSM& nfsm,
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interestingOrderings
        );

    private:
        std::vector<TNode> Nodes;
        std::vector<TEdge> Edges;

        std::vector<std::vector<i64>> TransitionMatrix;
        std::vector<std::vector<bool>> ContainsMatrix;

        struct TInitState {
            std::size_t StateIdx;
            std::size_t ShuffleHashFuncArgsCount;
        };
        std::vector<TInitState> InitStateByOrderingIdx;
    };

    /*
     * For equivalences we build equivalence classes and if item belongs to the class, which has no interesting
     * orderings, so we can easily prune it.
     */
    std::vector<TFunctionalDependency> PruneFDs(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    );

private:
    TNFSM NFSM;
    TSimpleSharedPtr<TDFSM> DFSM; // it is important to have sharedptr here, otherwise all logicalorderings will invalidate after copying of FSM

<<<<<<< HEAD
    std::vector<i64> FdMapping; // We to remap FD idxes after the pruning
=======
    std::vector<std::int64_t> FdMapping; // We to remap FD idxes after the pruning
>>>>>>> 359e517b6d3 ([KQP / RBO] Build and propogate Orderings FSM through the all query)
    bool Built = false;
};

}
