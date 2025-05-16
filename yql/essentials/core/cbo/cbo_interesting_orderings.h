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

        TBaseColumn& operator=(const TBaseColumn& other);

        NDq::TJoinColumn ToJoinColumn();
        operator bool();

        TString Relation;
        TString Column;
    };

    TTableAliasMap() = default;

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
    THashMap<TString, TBaseColumn> BaseColumnByRename;
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

    std::size_t AddFD(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr
    );

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

    TVector<TJoinColumn> GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const;
    TString ToString() const;

public:
    std::vector<TFunctionalDependency> FDs;
    std::vector<TOrdering> InterestingOrderings;

private:
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

    private:
        bool IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs);

    private:
        TDFSM* DFSM = nullptr;
        /* we can have different args in hash shuffle function, so shuffles can be incompitable in this case */
        i64 ShuffleHashFuncArgsCount = -1;
        i64 State = -1;
        TFDSet AppliedFDs{};
    };

    TLogicalOrderings CreateState();
    TLogicalOrderings CreateState(i64 orderingIdx);

public:
    TOrderingsStateMachine() = default;

    TOrderingsStateMachine(TFDStorage fdStorage)
        : FDStorage(std::move(fdStorage))
        , DFSM(MakeSimpleShared<TDFSM>())
    {
        Build(FDStorage.FDs, FDStorage.InterestingOrderings);
    }

    TOrderingsStateMachine(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    ) {
        DFSM = MakeSimpleShared<TDFSM>();
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
            i64 InterestingOrderingIdx; // -1 if node isn't interesting

            TString ToString() const;
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
            i64 fdIdx;

            enum _ : i64 {
                EPSILON = -1 // eps edges with give us nodes without applying any FDs.
            };

            TString ToString() const;
        };

        std::size_t Size();
        TString ToString() const;
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

            TString ToString() const;
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
            i64 fdIdx;

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

    std::vector<i64> FdMapping; // We to remap FD idxes after the pruning
    bool Built = false;
};

}
