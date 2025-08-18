#pragma once

#include <yql/essentials/core/yql_cost_function.h>

#include <util/generic/hash.h>
#include <util/generic/algorithm.h>

#include <bitset>
#include <stdint.h>
#include <vector>

/*
 * This header contains all essentials to work with Interesting Orderings.
 * Interesting ordering is an ordering which is produced by a query or tested in the query
 * At this moment, we process only shuffles, but in future there will be groupings and sortings
 * For details of the algorithms and examples look at the white papers -
 * - "An efficient framework for order optimization" / "A Combined Framework for Grouping and Order Optimization" by T. Neumann, G. Moerkotte
 */

namespace NYql::NDq {

struct TOrdering {
    enum EType : uint32_t {
        EShuffle = 0,
        ESorting = 1
    };

    bool operator==(const TOrdering& other) const;
    TString ToString() const;

    struct TItem {
        enum EDirection : uint32_t {
            ENone = 0,
            EAscending = 1,
            EDescending = 2
        };
    };

    TOrdering(
        std::vector<std::size_t> items,
        std::vector<TItem::EDirection> directions,
        EType type,
        bool isNatural = false
    )
        : Items(std::move(items))
        , Directions(std::move(directions))
        , Type(type)
        , IsNatural(isNatural)
    {
        Y_ENSURE(
            Directions.empty() && type == TOrdering::EShuffle ||
            Directions.size() == Items.size() && type == TOrdering::ESorting
        );
    }

    TOrdering(
        std::vector<std::size_t> items,
        EType type
    )
        : TOrdering(
            std::move(items),
            std::vector<TItem::EDirection>{},
            type,
            false
        )
    {}

    TOrdering() = default;

    bool HasItem(std::size_t item) const;

    std::vector<std::size_t> Items;
    std::vector<TItem::EDirection> Directions;

    EType Type;
    /*
     * Definition was taken from 'Complex Ordering Requirements' section. Not natural orderings are complex join predicates or grouping.
     * There can occure a problem when we have a natural ordering - shuffling (a, b) of the table and we must aggregate by (b, a, c) - non natural ordering
     * So for this case (b, a, c) suits for us as well and we must reorder (b, a, c) to (a, b, c). In the section from the white papper this is
     * described more detailed.
     */
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
        /* default fd: a -> b */
        EImplication = 0,
        /* equivalence: a = b */
        EEquivalence = 1
    };

    bool IsEquivalence() const;
    bool IsImplication() const;
    bool IsConstant() const;

    // Returns index of the first matching antecedent item in the ordering, if it matches
    TMaybe<std::size_t> MatchesAntecedentItems(const TOrdering& ordering) const;
    TString ToString() const;

    std::vector<std::size_t> AntecedentItems;
    std::size_t ConsequentItem;

    EType Type;
    bool AlwaysActive;
};

bool operator==(const TFunctionalDependency& lhs, const TFunctionalDependency& rhs);

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

        TBaseColumn& operator=(const TBaseColumn& other);

        NDq::TJoinColumn ToJoinColumn();
        operator bool();

        TString Relation;
        TString Column;
    };

    TTableAliasMap() = default;

    void AddMapping(const TString& table, const TString& alias);
    void AddRename(TString from, TString to);
    TBaseColumn GetBaseColumnByRename(const TString& renamedColumn);
    TBaseColumn GetBaseColumnByRename(const NDq::TJoinColumn& renamedColumn);
    TString ToString() const;
    void Merge(const TTableAliasMap& other);
    bool Empty() const { return TableByAlias_.empty() && BaseColumnByRename_.empty(); }

private:
    TString GetBaseTableByAlias(const TString& alias);

private:
    THashMap<TString, TString> TableByAlias_;
    THashMap<TString, TBaseColumn> BaseColumnByRename_;
};

struct TSorting {
    TSorting(
        std::vector<TJoinColumn> ordering,
        std::vector<TOrdering::TItem::EDirection> directions
    )
        : Ordering(std::move(ordering))
        , Directions(std::move(directions))
    {}

    std::vector<TJoinColumn> Ordering;
    std::vector<TOrdering::TItem::EDirection> Directions;
};

struct TShuffling {
    explicit TShuffling(
        std::vector<TJoinColumn> ordering
    )
        : Ordering(std::move(ordering))
    {}

    TShuffling& SetNatural() { IsNatural = true; return *this; }

    std::vector<TJoinColumn> Ordering;
    bool IsNatural = false; // look at the IsNatural field at the Ordering struct
};

/*
 * This class contains internal representation of the columns (mapping [column -> int]), FDs and interesting orderings
 */
class TFDStorage {
public:
    i64 FindFDIdx(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        TTableAliasMap* tableAliases = nullptr
    );

public: // deprecated section, use the section below instead of this
    std::size_t AddFD(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        bool alwaysActive = false,
        TTableAliasMap* tableAliases = nullptr
    );

public:
    std::size_t AddConstant(
        const TJoinColumn& constantColumn,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t AddImplication(
        const TVector<TJoinColumn>& antecedentColumns,
        const TJoinColumn& consequentColumn,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t AddEquivalence(
        const TJoinColumn& lhs,
        const TJoinColumn& rhs,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr
    );

private:
    std::size_t AddFDImpl(TFunctionalDependency fd);

public: // deprecated section, use the section below instead of this
    i64 FindInterestingOrderingIdx(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases = nullptr
    );

public:
    std::size_t FindSorting(
        const TSorting&,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t AddSorting(
        const TSorting&,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t FindShuffling(
        const TShuffling&,
        TTableAliasMap* tableAliases = nullptr
    );

    std::size_t AddShuffling(
        const TShuffling&,
        TTableAliasMap* tableAliases = nullptr
    );

public:
    TVector<TJoinColumn> GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const;

    TSorting GetInterestingSortingByOrderingIdx(std::size_t interestingOrderingIdx) const;
    TString ToString() const;

    // look at the IsNatural field at the Ordering struct
    void ApplyNaturalOrderings();

public:
    std::vector<TFunctionalDependency> FDs;
    std::vector<TOrdering> InterestingOrderings;

private:
    std::pair<TOrdering, i64> ConvertColumnsAndFindExistingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        const std::vector<TOrdering::TItem::EDirection>& directions,
        TOrdering::EType type,
        bool createIfNotExists,
        bool isNatural,
        TTableAliasMap* tableAliases
    );

    std::vector<std::size_t> ConvertColumnIntoIndexes(
        const std::vector<TJoinColumn>& ordering,
        bool createIfNotExists,
        TTableAliasMap* tableAliases
    );

    std::size_t GetIdxByColumn(
        const TJoinColumn& column,
        bool createIfNotExists,
        TTableAliasMap* tableAliases
    );

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        const std::vector<TOrdering::TItem::EDirection>& directions,
        bool isNatural,
        TTableAliasMap* tableAliases
    );

private:
    THashMap<TString, std::size_t> IdxByColumn_;
    std::vector<TJoinColumn> ColumnByIdx_;
    std::size_t IdCounter_ = 0;
};

/*
 * This class represents Finite-State Machine (FSM) for tracking ordering transformations.
 * Each state represents a set of available logical orderings that can be derived from functional dependencies.
 *
 * The FSM construction follows these steps:
 *      1) Build NFSM (Non-Deterministic FSM): Create nodes for each interesting ordering and apply
 *         functional dependencies to generate all possible ordering transformations. This creates
 *         a graph where nodes are orderings and edges represent FD applications.
 *
 *      2) Prune FDs: Remove functional dependencies that cannot lead to any interesting orderings
 *         to reduce the state space and improve performance.
 *
 *      3) Add NFSM edges: Connect orderings through epsilon transitions (prefix relationships)
 *         and FD transitions (functional dependency applications).
 *
 *      4) Convert to DFSM (Deterministic FSM): Since NFSM can have exponential states and is
 *         hard to work with, we convert it to DFSM using subset construction. Each DFSM state
 *         represents a set of NFSM states reachable through epsilon transitions and always-active FDs.
 *         This allows O(1) state transitions and efficient ordering containment checks.
 */
class TOrderingsStateMachine {
private:
    class TDFSM;
    enum _ : std::uint32_t {
        EMaxFDCount = 64,
        EMaxNFSMStates = 256,
        EMaxDFSMStates = 512,
    };

    struct TItemInfo {
        bool UsedInAscOrdering  = false;
        bool UsedInDescOrdering = false;
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
            : Dfsm_(dfsm)
        {}

    public: // API
        bool ContainsShuffle(i64 orderingIdx);
        bool ContainsSorting(i64 orderingIdx);
        void InduceNewOrderings(const TFDSet& fds);
        void RemoveState();
        void SetOrdering(i64 orderingIdx);
        i64 GetShuffleHashFuncArgsCount();
        void SetShuffleHashFuncArgsCount(std::size_t value);
        TFDSet GetFDs();
        bool IsSubsetOf(const TLogicalOrderings& logicalOrderings);
        i64 GetState() const;
        i64 GetInitOrderingIdx() const;

    public:
        bool HasState();
        bool HasState() const;
        bool IsInitialized();
        bool IsInitialized() const;

    private:
        bool IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs);

    private:
        TDFSM* Dfsm_ = nullptr;
        /* we can have different args in hash shuffle function, so shuffles can be incompitable in this case */
        i64 ShuffleHashFuncArgsCount_ = -1;

        i64 State_ = -1;

        /* Index of the state which was set in SetOrdering */
        i64 InitOrderingIdx_ = -1;
        TFDSet AppliedFDs_{};
    };

    TLogicalOrderings CreateState() const;
    TLogicalOrderings CreateState(i64 orderingIdx) const;

public:
    TOrderingsStateMachine() = default;

    TOrderingsStateMachine(
        TFDStorage fdStorage,
        TOrdering::EType machineType = TOrdering::EShuffle
    )
        : FDStorage(std::move(fdStorage))
    {
        EraseIf(FDStorage.InterestingOrderings, [machineType](const TOrdering& ordering){ return ordering.Type != machineType; });
        FDStorage.ApplyNaturalOrderings();
        Build(FDStorage.FDs, FDStorage.InterestingOrderings);
    }

    TOrderingsStateMachine(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    ) {
        Build(fds, interestingOrderings);
    }

public:
    TFDStorage FDStorage;
    bool IsBuilt() const;
    TFDSet GetFDSet(i64 fdIdx);
    TFDSet GetFDSet(const std::vector<std::size_t>& fdIdxes);
    TString ToString() const;

private:
    void Build(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    );

private:
    /*
     * Non-Deterministic Finite State Machine (NFSM) for ordering transformations.
     *
     * The NFSM represents all possible ordering transformations that can be achieved
     * through functional dependencies. Each node represents a specific ordering (either
     * interesting or artificially generated), and edges represent transformations via FDs.
     */
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
                , Ordering(std::move(ordering))
                , InterestingOrderingIdx(interestingOrderingIdx)
            {}

            EType Type;
            std::vector<std::size_t> OutgoingEdges;

            TOrdering Ordering;
            i64 InterestingOrderingIdx; // -1 if node isn't interesting

            TString ToString() const;
        };

        struct TEdge {
            std::size_t SrcNodeIdx;
            std::size_t DstNodeIdx;
            i64 FdIdx;

            enum _ : i64 {
                EPSILON = -1 // eps edges with give us nodes without applying any FDs.
            };

            bool operator==(const TEdge& other) const;

            TString ToString() const;
        };

        std::size_t Size();
        TString ToString() const;
        void Build(
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interesting,
            const std::vector<TItemInfo>& itemInfo
        );

    private:
        std::size_t AddNode(const TOrdering& ordering, TNode::EType type, i64 interestingOrderingIdx = -1);
        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx);
        void PrefixClosure();
        void ApplyFDs(
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interesting,
            const std::vector<TItemInfo>& itemInfo
        );

    private:
        std::vector<TNode> Nodes_;
        std::vector<TEdge> Edges_;
    };

    /*
     * Deterministic Finite State Machine (DFSM) for efficient ordering operations.
     *
     * The DFSM is constructed from NFSM using subset construction algorithm. Each DFSM state
     * represents a set of NFSM states that are reachable through epsilon transitions and
     * always-active functional dependencies.
     *
     * Key benefits over NFSM:
     * - Deterministic: Exactly one transition per FD from each state
     * - Efficient: O(1) state transitions using precomputed transition matrix
     * - Compact: Significantly fewer states than NFSM through state merging
     * - Fast containment checks: Bitset operations for ordering membership tests
     *
     * The DFSM enables efficient runtime queries like "does current state contain ordering X?"
     * and "what orderings become available after applying FD set Y?".
     */
    class TDFSM {
    public:
        friend class TLogicalOrderings;
        friend class TStateMachineBuilder;

        struct TNode {
            std::vector<std::size_t> NFSMNodes;
            std::bitset<EMaxFDCount> OutgoingFDs;
            std::bitset<EMaxNFSMStates> NFSMNodesBitset;
            std::bitset<EMaxNFSMStates> InterestingOrderings;

            TString ToString() const;
        };

        struct TEdge {
            std::size_t SrcNodeIdx;
            std::size_t DstNodeIdx;
            i64 FdIdx;

            TString ToString() const;
        };

        std::size_t Size();
        TString ToString(const TNFSM& nfsm) const;
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
            const std::vector<TFunctionalDependency>& fds,
            i64 fdIdx = TNFSM::TEdge::EPSILON
        );
        void Precompute(
            const TNFSM& nfsm,
            const std::vector<TFunctionalDependency>& fds
        );

    private:
        std::vector<TNode> Nodes_;
        std::vector<TEdge> Edges_;

        std::vector<std::vector<i64>> TransitionMatrix_;
        std::vector<std::vector<bool>> ContainsMatrix_;

        struct TInitState {
            std::size_t StateIdx;
            std::size_t ShuffleHashFuncArgsCount;
        };
        std::vector<TInitState> InitStateByOrderingIdx_;
    };

    /*
     * For equivalences we build equivalence classes and if item belongs to the class, which has no interesting
     * orderings, so we can easily prune it.
     */
    std::vector<TFunctionalDependency> PruneFDs(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    );

    void CollectItemInfo(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    );

private:
    TNFSM Nfsm_;
    TSimpleSharedPtr<TDFSM> Dfsm_; // it is important to have sharedptr here, otherwise all logicalorderings will invalidate after copying of FSM

    std::vector<i64> FdMapping_; // We to remap FD idxes after the pruning
    std::vector<TItemInfo> ItemInfo_;
    bool Built_ = false;
};

}
