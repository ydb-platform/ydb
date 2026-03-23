#pragma once

// KQP-owned independent copy of yql/essentials/core/cbo/cbo_interesting_orderings.h
// All types live in NKikimr::NKqp — no aliases to NYql::NDq.
// The YQL original is unchanged; edits here do NOT affect YQL/YT.

#include <util/generic/hash.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <bitset>
#include <stdint.h>
#include <vector>
#include <optional>

namespace NKikimr::NKqp {

// -------------------------------------------------------------------------
// TJoinColumn — forked from yql/essentials/core/yql_cost_function.h (NYql::NDq)
// -------------------------------------------------------------------------
struct TJoinColumn {
    TString RelName{};
    TString AttributeName{};
    TString AttributeNameWithAliases{};
    std::optional<TString> OriginalRelName{};
    std::optional<ui32> EquivalenceClass{};
    bool IsConstant = false;

    TJoinColumn() = default;

    TJoinColumn(TString relName, TString attributeName)
        : RelName(std::move(relName))
        , AttributeName(attributeName)
        , AttributeNameWithAliases(attributeName)
    {}

    bool operator==(const TJoinColumn& other) const;

    static TJoinColumn FromString(const TString& column) {
        if (column.find('.') != TString::npos) {
            return TJoinColumn(column.substr(0, column.find('.')), column.substr(column.find('.') + 1));
        }
        return TJoinColumn("", column);
    }

    struct THashFunction {
        size_t operator()(const TJoinColumn& c) const;
    };
};

bool operator<(const TJoinColumn& c1, const TJoinColumn& c2);

// -------------------------------------------------------------------------
// Ordering types — forked from yql/essentials/core/cbo/cbo_interesting_orderings.h
// -------------------------------------------------------------------------

struct TOrdering {
    enum EType: uint32_t {
        EShuffle = 0,
        ESorting = 1
    };

    bool operator==(const TOrdering& other) const;
    TString ToString() const;

    struct TItem {
        enum EDirection: uint32_t {
            ENone = 0,
            EAscending = 1,
            EDescending = 2
        };
    };

    TOrdering(
        std::vector<std::size_t> items,
        std::vector<TItem::EDirection> directions,
        EType type,
        bool isNatural = false)
        : Items(std::move(items))
        , Directions(std::move(directions))
        , Type(type)
        , IsNatural(isNatural)
    {
        Y_ENSURE(
            Directions.empty() && type == TOrdering::EShuffle ||
            Directions.size() == Items.size() && type == TOrdering::ESorting);
    }

    TOrdering(
        std::vector<std::size_t> items,
        EType type)
        : TOrdering(
              std::move(items),
              std::vector<TItem::EDirection>{},
              type,
              false)
    {
    }

    TOrdering() = default;

    bool HasItem(std::size_t item) const;

    std::vector<std::size_t> Items;
    std::vector<TItem::EDirection> Directions;

    EType Type;
    bool IsNatural = false;
};

struct TFunctionalDependency {
    enum EType: uint32_t {
        EImplication = 0,
        EEquivalence = 1
    };

    bool IsEquivalence() const;
    bool IsImplication() const;
    bool IsConstant() const;

    TMaybe<std::size_t> MatchesAntecedentItems(const TOrdering& ordering) const;
    TString ToString() const;

    std::vector<std::size_t> AntecedentItems;
    std::size_t ConsequentItem;

    EType Type;
    bool AlwaysActive;
};

bool operator==(const TFunctionalDependency& lhs, const TFunctionalDependency& rhs);

struct TTableAliasMap: public TSimpleRefCount<TTableAliasMap> {
public:
    struct TBaseColumn {
        TBaseColumn() = default;

        TBaseColumn(
            TString relation,
            TString column)
            : Relation(std::move(relation))
            , Column(std::move(column))
        {
        }

        TBaseColumn(const TBaseColumn& other)
            : Relation(other.Relation)
            , Column(other.Column)
        {
        }

        TBaseColumn& operator=(const TBaseColumn& other);

        TJoinColumn ToJoinColumn();
        explicit operator bool();

        TString Relation;
        TString Column;
    };

    TTableAliasMap() = default;

    void AddMapping(const TString& table, const TString& alias);
    void AddRename(TString from, TString to);
    TBaseColumn GetBaseColumnByRename(const TString& renamedColumn);
    TBaseColumn GetBaseColumnByRename(const TJoinColumn& renamedColumn);
    TString ToString() const;
    void Merge(const TTableAliasMap& other);
    bool Empty() const {
        return TableByAlias_.empty() && BaseColumnByRename_.empty();
    }

private:
    TString GetBaseTableByAlias(const TString& alias);

private:
    THashMap<TString, TString> TableByAlias_;
    THashMap<TString, TBaseColumn> BaseColumnByRename_;
};

struct TSorting {
    TSorting(
        std::vector<TJoinColumn> ordering,
        std::vector<TOrdering::TItem::EDirection> directions)
        : Ordering(std::move(ordering))
        , Directions(std::move(directions))
    {
    }

    std::vector<TJoinColumn> Ordering;
    std::vector<TOrdering::TItem::EDirection> Directions;
};

struct TShuffling {
    explicit TShuffling(
        std::vector<TJoinColumn> ordering)
        : Ordering(std::move(ordering))
    {
    }

    TShuffling& SetNatural() {
        IsNatural = true;
        return *this;
    }

    std::vector<TJoinColumn> Ordering;
    bool IsNatural = false;
};

class TFDStorage {
public:
    i64 FindFDIdx(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        TTableAliasMap* tableAliases = nullptr);

public:
    std::size_t AddFD(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        bool alwaysActive = false,
        TTableAliasMap* tableAliases = nullptr);

public:
    std::size_t AddConstant(
        const TJoinColumn& constantColumn,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t AddImplication(
        const TVector<TJoinColumn>& antecedentColumns,
        const TJoinColumn& consequentColumn,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t AddEquivalence(
        const TJoinColumn& lhs,
        const TJoinColumn& rhs,
        bool alwaysActive,
        TTableAliasMap* tableAliases = nullptr);

private:
    std::size_t AddFDImpl(TFunctionalDependency fd);

public:
    i64 FindInterestingOrderingIdx(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        TTableAliasMap* tableAliases = nullptr);

public:
    std::size_t FindSorting(
        const TSorting&,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t AddSorting(
        const TSorting&,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t FindShuffling(
        const TShuffling&,
        TTableAliasMap* tableAliases = nullptr);

    std::size_t AddShuffling(
        const TShuffling&,
        TTableAliasMap* tableAliases = nullptr);

public:
    TVector<TJoinColumn> GetInterestingOrderingsColumnNamesByIdx(std::size_t interestingOrderingIdx) const;

    TSorting GetInterestingSortingByOrderingIdx(std::size_t interestingOrderingIdx) const;
    TString ToString() const;

    void ApplyNaturalOrderings();

    const std::vector<TJoinColumn>& GetColumns() const { return ColumnByIdx_; }

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
        TTableAliasMap* tableAliases);

    std::vector<std::size_t> ConvertColumnIntoIndexes(
        const std::vector<TJoinColumn>& ordering,
        bool createIfNotExists,
        TTableAliasMap* tableAliases);

    std::size_t GetIdxByColumn(
        const TJoinColumn& column,
        bool createIfNotExists,
        TTableAliasMap* tableAliases);

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type,
        const std::vector<TOrdering::TItem::EDirection>& directions,
        bool isNatural,
        TTableAliasMap* tableAliases);

private:
    THashMap<TString, std::size_t> IdxByColumn_;
    std::vector<TJoinColumn> ColumnByIdx_;
    std::size_t IdCounter_ = 0;
};

class TOrderingsStateMachine {
private:
    class TDFSM;

    static constexpr std::uint32_t MaxFDCount = 64;
    static constexpr std::uint32_t MaxNFSMStates = 256;
    static constexpr std::uint32_t MaxDFSMStates = 512;

    struct TItemInfo {
        bool UsedInAscOrdering = false;
        bool UsedInDescOrdering = false;
    };

public:
    using TFDSet = std::bitset<MaxFDCount>;

    class TLogicalOrderings {
    public:
        TLogicalOrderings() = default;
        TLogicalOrderings(const TLogicalOrderings&) = default;
        TLogicalOrderings& operator=(const TLogicalOrderings&) = default;

        explicit TLogicalOrderings(TDFSM* dfsm)
            : Dfsm_(dfsm)
        {
        }

    public:
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
        bool IsSubset(const std::bitset<MaxNFSMStates>& lhs, const std::bitset<MaxNFSMStates>& rhs);

    private:
        TDFSM* Dfsm_ = nullptr;
        i64 ShuffleHashFuncArgsCount_ = -1;
        i64 State_ = -1;
        i64 InitOrderingIdx_ = -1;
        TFDSet AppliedFDs_{};
    };

    TLogicalOrderings CreateState() const;
    TLogicalOrderings CreateState(i64 orderingIdx) const;

public:
    TOrderingsStateMachine() = default;

    explicit TOrderingsStateMachine(
        TFDStorage fdStorage,
        TOrdering::EType machineType = TOrdering::EShuffle)
        : FDStorage(std::move(fdStorage))
    {
        EraseIf(FDStorage.InterestingOrderings, [machineType](const TOrdering& ordering) { return ordering.Type != machineType; });
        FDStorage.ApplyNaturalOrderings();
        Build(FDStorage.FDs, FDStorage.InterestingOrderings);
    }

    TOrderingsStateMachine(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings) {
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
        const std::vector<TOrdering>& interestingOrderings);

private:
    class TNFSM {
    public:
        friend class TDFSM;

        struct TNode {
            enum EType: uint32_t {
                EArtificial,
                EInteresting
            };

            TNode(TOrdering ordering, EType type, i64 interestingOrderingIdx = -1)
                : Type(type)
                , Ordering(std::move(ordering))
                , InterestingOrderingIdx(interestingOrderingIdx)
            {
            }

            EType Type;
            std::vector<std::size_t> OutgoingEdges;

            TOrdering Ordering;
            i64 InterestingOrderingIdx;

            TString ToString() const;
        };

        struct TEdge {
            std::size_t SrcNodeIdx;
            std::size_t DstNodeIdx;
            i64 FdIdx;

            static constexpr i64 Epsilon = -1;

            bool operator==(const TEdge& other) const;

            TString ToString() const;
        };

        std::size_t Size();
        TString ToString() const;
        void Build(
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interesting,
            const std::vector<TItemInfo>& itemInfo);

    private:
        std::size_t AddNode(const TOrdering& ordering, TNode::EType type, i64 interestingOrderingIdx = -1);
        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx);
        void PrefixClosure();
        void ApplyFDs(
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interesting,
            const std::vector<TItemInfo>& itemInfo);

    private:
        std::vector<TNode> Nodes_;
        std::vector<TEdge> Edges_;
    };

    class TDFSM {
    public:
        friend class TLogicalOrderings;
        friend class TStateMachineBuilder;

        struct TNode {
            std::vector<std::size_t> NFSMNodes;
            std::bitset<MaxFDCount> OutgoingFDs;
            std::bitset<MaxNFSMStates> NFSMNodesBitset;
            std::bitset<MaxNFSMStates> InterestingOrderings;

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
            const std::vector<TOrdering>& interestingOrderings);

    private:
        std::size_t AddNode(const std::vector<std::size_t>& nfsmNodes);
        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, i64 fdIdx);
        std::vector<std::size_t> CollectNodesWithEpsOrFdEdge(
            const TNFSM& nfsm,
            const std::vector<std::size_t>& startNFSMNodes,
            const std::vector<TFunctionalDependency>& fds,
            i64 fdIdx = TNFSM::TEdge::Epsilon);
        void Precompute(
            const TNFSM& nfsm,
            const std::vector<TFunctionalDependency>& fds);

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

    std::vector<TFunctionalDependency> PruneFDs(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings);

    void CollectItemInfo(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings);

private:
    TNFSM Nfsm_;
    TSimpleSharedPtr<TDFSM> Dfsm_;

    std::vector<i64> FdMapping_;
    std::vector<TItemInfo> ItemInfo_;
    bool Built_ = false;
};

} // namespace NKikimr::NKqp
