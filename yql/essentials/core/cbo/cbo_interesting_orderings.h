#pragma once

#include <yql/essentials/core/yql_cost_function.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>
#include <util/generic/hash.h>

#include <sstream>
#include <bit>
#include <bitset>
#include <stdint.h>
#include <vector>
#include <set>
#include <unordered_set>
#include <functional>

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

    bool operator==(const TOrdering& other) const {
        return std::tie(this->Type, this->Items) == std::tie(other.Type, other.Items);
    }

    std::vector<std::size_t> Items;
    EType Type;
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

    bool IsEquivalence() const {
        return Type == EType::EEquivalence;
    }

    bool MatchesAntecedentItems(const TOrdering& ordering) const {
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

    std::vector<std::size_t> AntecedentItems;
    std::size_t ConsequentItem;

    EType Type;
    bool AlwaysActive;
};

/*
 * This class contains internal representation of the columns (mapping [column -> int]), FDs and interesting orderings
 */
class TFDStorage {
public:
    std::size_t AddFD(
        const TJoinColumn& antecedentColumn,
        const TJoinColumn& consequentColumn,
        TFunctionalDependency::EType type,
        bool alwaysActive
    ) {
        auto fd = TFunctionalDependency{
            .AntecedentItems = {GetIdxByColumn(antecedentColumn)},
            .ConsequentItem = GetIdxByColumn(consequentColumn),
            .Type = type,
            .AlwaysActive = alwaysActive
        };

        FDs.push_back(std::move(fd));
        return FDs.size() - 1;
    }

    std::size_t AddInterestingOrdering(
        const std::vector<TJoinColumn>& interestingOrdering,
        TOrdering::EType type
    ) {
        std::vector<std::size_t> items;
        items.reserve(interestingOrdering.size());

        for (const auto& column: interestingOrdering) {
            items.push_back(GetIdxByColumn(column));
        }

        for (std::size_t i = 0; i < InterestingOrderings.size(); ++i) {
            if (items == InterestingOrderings[i].Items) {
                return i;
            }
        }

        InterestingOrderings.push_back(TOrdering{.Items = std::move(items), .Type = type });
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

    std::string ToString() {
        std::stringstream ss;

        for (const auto& [column, idx]: IdxByColumn) {
            ss << idx << " " << column << '\n';
        }

        return ss.str();
    }

public:
    std::vector<TFunctionalDependency> FDs;
    std::vector<TOrdering> InterestingOrderings;

private:
    std::size_t GetIdxByColumn(const TJoinColumn& column) {
        const std::string fullPath = column.RelName + "." + column.AttributeName;
        if (IdxByColumn.contains(fullPath)) {
            return IdxByColumn[fullPath];
        }

        ColumnByIdx.push_back(column);
        IdxByColumn[fullPath] = IdCounter++;
        return IdxByColumn[fullPath];
    }

private:
    THashMap<std::string, std::size_t> IdxByColumn;
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

        void SetOrdering(std::size_t orderingIdx) {
            Y_ASSERT(orderingIdx < DFSM->InitStateByOrderingIdx.size());

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

    private:
        inline bool IsSubset(const std::bitset<EMaxNFSMStates>& lhs, const std::bitset<EMaxNFSMStates>& rhs) {
            return (lhs & rhs) == lhs;
        }

    private:
        TDFSM* DFSM = nullptr;
        /* we can have different args in hash shuffle function, so shuffles can be incompitable in this case */
        std::int64_t ShuffleHashFuncArgsCount = -1;
        std::int64_t State = -1;
        TFDSet AppliedFDs{};
    };

    TLogicalOrderings CreateState() { // TODO : THINK ABOUT IT createstate + setordering in one func
        return TLogicalOrderings(&DFSM);
    }

public:
    TOrderingsStateMachine(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    ) {
        Build(fds, interestingOrderings);
    }

    TFDSet GetFDSet(std::size_t fdIdx) {
        return GetFDSet(std::vector<std::size_t> {fdIdx});
    }

    TFDSet GetFDSet(const std::vector<std::size_t>& fdIdxes) {
        TFDSet fdSet;

        for (std::size_t fdIdx: fdIdxes) {
            if (FdMapping[fdIdx] != -1) {
                fdSet[FdMapping[fdIdx]] = 1;
            }
        }

        return fdSet;
    }

    std::array<std::size_t, 64> ShuffleCountByColumnIdx;

private:
    class TNFSM {
    public:
        friend class TDFSM;

        struct TNode {
            enum EType : uint32_t {
                EArtificial,
                EInteresting
            };

            TNode(TOrdering ordering, EType type, std::int64_t interestingOrderingIdx = -1)
                : Type(type)
                , Ordering(ordering)
                , InterestingOrderingIdx(interestingOrderingIdx)
            {}

            EType Type;
            std::vector<std::size_t> OutgoingEdges;

            TOrdering Ordering;
            std::int64_t InterestingOrderingIdx; // -1 if node isn't interesting
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
            std::int64_t fdIdx;

            enum _ : std::int64_t {
                EPSILON = -1 // eps edges with give us nodes without applying any FDs.
            };
        };

        std::size_t Size() {
            return Nodes.size();
        }

    public:
        void Build(
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

    private:
        std::size_t AddNode(const TOrdering& ordering, TNode::EType type, std::int64_t interestingOrderingIdx = -1) {
            for (std::size_t i = 0; i < Nodes.size(); ++i) {
                if (Nodes[i].Ordering == ordering) {
                    return i;
                }
            }

            Nodes.emplace_back(ordering, type, interestingOrderingIdx);
            return Nodes.size() - 1;
        }

        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, std::int64_t fdIdx) {
            Edges.emplace_back(srcNodeIdx, dstNodeIdx, fdIdx);
        }

        /*
         * Create nodes with prefixies of the current node and connect them with edges
         * For shuffles we have an interesting property : (a_{0}, ... a_{n})      -> (a_{0}, ..., a_{n + 1})
         * For sortings we have an inverse property :     (a_{0}, ..., a_{n + 1}) -> (a_{0}, ... a_{n})
         */
        void PrefixClosure() {
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

        void ApplyFDs(const std::vector<TFunctionalDependency>& fds) {
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

            if (Nodes.size() >= EMaxNFSMStates) {
                YQL_CLOG(TRACE, CoreDq) << "NFSM states exceeded the limit";
            }
        }

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
        };

        struct TEdge {
            std::size_t srcNodeIdx;
            std::size_t dstNodeIdx;
            std::int64_t fdIdx;
        };

        std::size_t Size() {
            return Nodes.size();
        }

    public:
        void Build(
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
                std::unordered_set<std::int64_t> outgoingDFSMNodeFDs;
                for (std::size_t nfsmNodeIdx: Nodes[nodeIdx].NFSMNodes) {
                    for (std::size_t nfsmEdgeIdx: nfsm.Nodes[nfsmNodeIdx].OutgoingEdges) {
                        outgoingDFSMNodeFDs.insert(nfsm.Edges[nfsmEdgeIdx].fdIdx);
                    }
                }

                for (std::int64_t fdIdx: outgoingDFSMNodeFDs) {
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

            if (Nodes.size() >= EMaxDFSMStates) {
                YQL_CLOG(TRACE, CoreDq) << "DFSM states exceeded the limit";
            }

            Precompute(nfsm, fds, interestingOrderings);
        }

    private:
        std::size_t AddNode(const std::vector<std::size_t>& nfsmNodes) {
            for (std::size_t i = 0; i < Nodes.size(); ++i) {
                if (Nodes[i].NFSMNodes == nfsmNodes) {
                    return i;
                }
            }

            Nodes.emplace_back(nfsmNodes);
            return Nodes.size() - 1;
        }

        void AddEdge(std::size_t srcNodeIdx, std::size_t dstNodeIdx, std::int64_t fdIdx) {
            Edges.emplace_back(srcNodeIdx, dstNodeIdx, fdIdx);
        }

        std::vector<std::size_t> CollectNodesWithEpsOrFdEdge(
            const TNFSM& nfsm,
            const std::vector<std::size_t>& startNFSMNodes,
            std::int64_t fdIdx = TNFSM::TEdge::EPSILON
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

            // it is important to return sorted output cause we have one order for NFSMnodes for DFSMnode
            return std::vector<std::size_t>(visited.begin(), visited.end());
        }

        void Precompute(
            const TNFSM& nfsm,
            const std::vector<TFunctionalDependency>& fds,
            const std::vector<TOrdering>& interestingOrderings
        ) {
            TransitionMatrix = std::vector<std::vector<std::int64_t>>(Nodes.size(), std::vector<std::int64_t>(fds.size(), -1));
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

    private:
        std::vector<TNode> Nodes;
        std::vector<TEdge> Edges;

        std::vector<std::vector<std::int64_t>> TransitionMatrix;
        std::vector<std::vector<bool>> ContainsMatrix;

        struct TInitState {
            std::size_t StateIdx;
            std::size_t ShuffleHashFuncArgsCount;
        };
        std::vector<TInitState> InitStateByOrderingIdx;
    };

    void Build(
        const std::vector<TFunctionalDependency>& fds,
        const std::vector<TOrdering>& interestingOrderings
    ) {
        YQL_CLOG(TRACE, CoreDq) << "Building Orderings State Machine, Interesting Orderings amount:" << interestingOrderings.size();
        std::vector<TFunctionalDependency> processedFDs = PruneFDs(fds, interestingOrderings);
        NFSM.Build(processedFDs, interestingOrderings);
        DFSM.Build(NFSM, processedFDs, interestingOrderings);
        YQL_CLOG(TRACE, CoreDq) << "Orderings FSM has been built, DFSM state count: " << DFSM.Size() << ", NFSM state count: " << NFSM.Size();
        // for (const auto& ordering: interestingOrderings) {
        //     for (const auto& item: ordering.Items) {
        //         Cerr << item << '\n';
        //         ++ShuffleCountByColumnIdx[item];
        //     }
        // }
    }

    /*
     * For equivalences we build equivalence classes and if item belongs to the class, which has no interesting
     * orderings, so we can easily prune it.
     */
    std::vector<TFunctionalDependency> PruneFDs(
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

private:
    TNFSM NFSM;
    TDFSM DFSM;

    std::vector<std::int64_t> FdMapping; // We to remap FD idxes after the pruning
};

}
