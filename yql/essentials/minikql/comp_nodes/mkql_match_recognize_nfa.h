#pragma once

#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_save_load.h"
#include "../computation/mkql_computation_node_holders.h"
#include "util/generic/overloaded.h"
#include <yql/essentials/core/sql_types/match_recognize.h>
#include <util/generic/hash_table.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

using namespace NYql::NMatchRecognize;

struct TVoidTransition {
    friend constexpr bool operator==(const TVoidTransition&, const TVoidTransition&) = default;
};
struct TEpsilonTransitions {
    std::vector<size_t, TMKQLAllocator<size_t>> To;
    friend constexpr bool operator==(const TEpsilonTransitions&, const TEpsilonTransitions&) = default;
};
struct TMatchedVarTransition {
    ui32 VarIndex;
    bool SaveState;
    size_t To;
    friend constexpr bool operator==(const TMatchedVarTransition&, const TMatchedVarTransition&) = default;
};
struct TQuantityEnterTransition {
    size_t To;
    friend constexpr bool operator==(const TQuantityEnterTransition&, const TQuantityEnterTransition&) = default;
};
struct TQuantityExitTransition {
    ui64 QuantityMin;
    ui64 QuantityMax;
    size_t ToFindMore;
    size_t ToMatched;
    friend constexpr bool operator==(const TQuantityExitTransition&, const TQuantityExitTransition&) = default;
};

template <typename... Ts>
struct TVariantHelper {
    using TVariant =  std::variant<Ts...>;
    using TTuple =  std::tuple<Ts...>;

    static std::variant<Ts...> GetVariantByIndex(size_t i) {
        MKQL_ENSURE(i < sizeof...(Ts), "Wrong variant index");
        static std::variant<Ts...> table[] = { Ts{ }... };
        return table[i];
    }
};

using TNfaTransitionHelper = TVariantHelper<
    TVoidTransition,
    TMatchedVarTransition,
    TEpsilonTransitions,
    TQuantityEnterTransition,
    TQuantityExitTransition
>;

using TNfaTransition = TNfaTransitionHelper::TVariant;

struct TNfaTransitionDestinationVisitor {
    std::function<size_t(size_t)> Callback;

    template<typename Callback>
    explicit TNfaTransitionDestinationVisitor(Callback callback)
        : Callback(std::move(callback)) {}

    TNfaTransition operator()(TVoidTransition tr) const {
        return tr;
    }

    TNfaTransition operator()(TMatchedVarTransition tr) const {
        tr.To = Callback(tr.To);
        return tr;
    }

    TNfaTransition operator()(TEpsilonTransitions tr) const {
        for (size_t& toNode: tr.To) {
            toNode = Callback(toNode);
        }
        return tr;
    }

    TNfaTransition operator()(TQuantityEnterTransition tr) const {
        tr.To = Callback(tr.To);
        return tr;
    }

    TNfaTransition operator()(TQuantityExitTransition tr) const {
        tr.ToFindMore = Callback(tr.ToFindMore);
        tr.ToMatched = Callback(tr.ToMatched);
        return tr;
    }
};

struct TNfaTransitionGraph {
    using TTransitions = std::vector<TNfaTransition, TMKQLAllocator<TNfaTransition>>;

    TTransitions Transitions;
    size_t Input;
    size_t Output;

    using TPtr = std::shared_ptr<TNfaTransitionGraph>;

    template<class>
    inline constexpr static bool always_false_v = false;

    void Save(TMrOutputSerializer& serializer) const {
        serializer(Transitions.size());
        for (ui64 i = 0; i < Transitions.size(); ++i) {
            serializer.Write(Transitions[i].index());
            std::visit(TOverloaded{
                [&](const TVoidTransition&) {},
                [&](const TEpsilonTransitions& tr) {
                    serializer(tr.To);
                },
                [&](const TMatchedVarTransition& tr) {
                    serializer(tr.VarIndex, tr.SaveState, tr.To);
                },
                [&](const TQuantityEnterTransition& tr) {
                    serializer(tr.To);
                },
                [&](const TQuantityExitTransition& tr) {
                    serializer(tr.QuantityMin, tr.QuantityMax, tr.ToFindMore, tr.ToMatched);
                },
            }, Transitions[i]);
        }
        serializer(Input, Output);
    }

    void Load(TMrInputSerializer& serializer) {
        ui64 transitionSize = serializer.Read<TTransitions::size_type>();
        Transitions.resize(transitionSize);
        for (ui64 i = 0; i < transitionSize; ++i) {
            size_t index = serializer.Read<std::size_t>();
            Transitions[i] = TNfaTransitionHelper::GetVariantByIndex(index);
            std::visit(TOverloaded{
                [&](TVoidTransition&) {},
                [&](TEpsilonTransitions& tr) {
                    serializer(tr.To);
                },
                [&](TMatchedVarTransition& tr) {
                    serializer(tr.VarIndex, tr.SaveState, tr.To);
                },
                [&](TQuantityEnterTransition& tr) {
                    serializer(tr.To);
                },
                [&](TQuantityExitTransition& tr) {
                    serializer(tr.QuantityMin, tr.QuantityMax, tr.ToFindMore, tr.ToMatched);
                },
            }, Transitions[i]);
        }
        serializer(Input, Output);
    }

    bool operator==(const TNfaTransitionGraph& other) {
        return Transitions == other.Transitions
            && Input == other.Input
            && Output == other.Output;
    }
};

class TNfaTransitionGraphOptimizer {
public:
    TNfaTransitionGraphOptimizer(TNfaTransitionGraph::TPtr graph)
        : Graph(graph) {}

    void DoOptimizations() {
        EliminateEpsilonChains();
        EliminateSingleEpsilons();
        CollectGarbage();
    }

private:
    void EliminateEpsilonChains() {
        for (size_t node = 0; node != Graph->Transitions.size(); node++) {
            if (auto* ts = std::get_if<TEpsilonTransitions>(&Graph->Transitions[node])) {
                // new vector of eps transitions,
                // contains refs to all nodes which are reachable from oldNode via eps transitions
                TEpsilonTransitions optimizedTs;
                auto dfsStack = ts->To;
                while (!dfsStack.empty()) {
                    auto curNode = dfsStack.back();
                    dfsStack.pop_back();
                    if (auto* curTs = std::get_if<TEpsilonTransitions>(&Graph->Transitions[curNode])) {
                        std::copy(curTs->To.begin(), curTs->To.end(), std::back_inserter(dfsStack));
                    } else {
                        optimizedTs.To.push_back(curNode);
                    }
                }
                *ts = optimizedTs;
            }
        }
    }
    void EliminateSingleEpsilons() {
        for (size_t node = 0; node != Graph->Transitions.size(); node++) {
            if (std::holds_alternative<TEpsilonTransitions>(Graph->Transitions[node])) {
                continue;
            }
            Graph->Transitions[node] = std::visit(TNfaTransitionDestinationVisitor([&](size_t toNode) -> size_t {
                if (auto *tr = std::get_if<TEpsilonTransitions>(&Graph->Transitions[toNode])) {
                    if (tr->To.size() == 1) {
                        return tr->To[0];
                    }
                }
                return toNode;
            }), Graph->Transitions[node]);
        }
    }
    void CollectGarbage() {
        auto oldInput = Graph->Input;
        auto oldOutput = Graph->Output;
        decltype(Graph->Transitions) oldTransitions;
        Graph->Transitions.swap(oldTransitions);
        // Scan for reachable nodes and map old node ids to new node ids
        std::vector<std::optional<size_t>> mapping(oldTransitions.size(), std::nullopt);
        std::vector<size_t> dfsStack = {oldInput};
        mapping[oldInput] = 0;
        Graph->Transitions.emplace_back();
        while (!dfsStack.empty()) {
            auto oldNode = dfsStack.back();
            dfsStack.pop_back();
            std::visit(TNfaTransitionDestinationVisitor([&](size_t oldToNode) {
                if (!mapping[oldToNode]) {
                    mapping[oldToNode] = Graph->Transitions.size();
                    Graph->Transitions.emplace_back();
                    dfsStack.push_back(oldToNode);
                }
                return 0;
            }), oldTransitions[oldNode]);
        }
        // Rebuild transition vector
        for (size_t oldNode = 0; oldNode != oldTransitions.size(); oldNode++) {
            if (!mapping[oldNode]) {
                continue;
            }
            auto node = mapping[oldNode].value();
            if (oldNode == oldInput) {
                Graph->Input = node;
            }
            if (oldNode == oldOutput) {
                Graph->Output = node;
            }
            Graph->Transitions[node] = oldTransitions[oldNode];
            Graph->Transitions[node] = std::visit(TNfaTransitionDestinationVisitor([&](size_t oldToNode) {
                return mapping[oldToNode].value();
            }), Graph->Transitions[node]);
        }
    }

    TNfaTransitionGraph::TPtr Graph;
};

class TNfaTransitionGraphBuilder {
private:
    struct TNfaItem {
        size_t Input;
        size_t Output;
    };

    TNfaTransitionGraphBuilder(TNfaTransitionGraph::TPtr graph)
        : Graph(graph) {}

    size_t AddNode() {
        Graph->Transitions.emplace_back();
        return Graph->Transitions.size() - 1;
    }

    TNfaItem BuildTerms(const TVector<TRowPatternTerm>& terms, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        TEpsilonTransitions fromInput;
        for (const auto& t: terms) {
            auto a = BuildTerm(t, varNameToIndex);
            fromInput.To.push_back(a.Input);
            Graph->Transitions[a.Output] = TEpsilonTransitions({output});
        }
        Graph->Transitions[input] = std::move(fromInput);
        return {input, output};
    }
    TNfaItem BuildTerm(const TRowPatternTerm& term, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        std::vector<TNfaItem, TMKQLAllocator<TNfaItem>> automata;
        for (const auto& f: term) {
            automata.push_back(BuildFactor(f, varNameToIndex));
        }
        for (size_t i = 0; i != automata.size() - 1; ++i) {
            Graph->Transitions[automata[i].Output] = TEpsilonTransitions({automata[i + 1].Input});
        }
        Graph->Transitions[input] = TEpsilonTransitions({automata.front().Input});
        Graph->Transitions[automata.back().Output] = TEpsilonTransitions({output});
        return {input, output};
    }
    TNfaItem BuildFactor(const TRowPatternFactor& factor, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        auto item = factor.Primary.index() == 0 ?
                    BuildVar(varNameToIndex.at(std::get<0>(factor.Primary)), !factor.Unused) :
                    BuildTerms(std::get<1>(factor.Primary), varNameToIndex);
        if (1 == factor.QuantityMin && 1 == factor.QuantityMax) { //simple linear case
            Graph->Transitions[input] = TEpsilonTransitions{{item.Input}};
            Graph->Transitions[item.Output] = TEpsilonTransitions{{output}};
        } else {
            auto interim = AddNode();
            auto fromInput = TEpsilonTransitions{{interim}};
            if (factor.QuantityMin == 0) {
                fromInput.To.push_back(output);
            }
            Graph->Transitions[input] = fromInput;
            Graph->Transitions[interim] = TQuantityEnterTransition{item.Input};
            Graph->Transitions[item.Output] = TQuantityExitTransition{
                factor.QuantityMin,
                factor.QuantityMax,
                item.Input,
                output,
            };
        }
        return {input, output};
    }
    TNfaItem BuildVar(ui32 varIndex, bool isUsed) {
        auto input = AddNode();
        auto matchVar = AddNode();
        auto output = AddNode();
        Graph->Transitions[input] = TEpsilonTransitions({matchVar});
        Graph->Transitions[matchVar] = TMatchedVarTransition{
            varIndex,
            isUsed,
            output,
        };
        return {input, output};
    }
public:
    static TNfaTransitionGraph::TPtr Create(const TRowPattern& pattern, const THashMap<TString, size_t>& varNameToIndex) {
        auto result = std::make_shared<TNfaTransitionGraph>();
        TNfaTransitionGraphBuilder builder(result);
        auto item = builder.BuildTerms(pattern, varNameToIndex);
        result->Input = item.Input;
        result->Output = item.Output;
        TNfaTransitionGraphOptimizer optimizer(result);
        optimizer.DoOptimizations();
        return result;
    }
private:
    TNfaTransitionGraph::TPtr Graph;
};

class TNfa {
    using TRange = TSparseList::TRange;
    using TMatchedVars = TMatchedVars<TRange>;

    struct TState {
        size_t BeginMatchIndex;
        size_t EndMatchIndex;
        size_t Index;
        TMatchedVars Vars;
        std::deque<ui64, TMKQLAllocator<ui64>> Quantifiers;

        void Save(TMrOutputSerializer& serializer) const {
            serializer.Write(BeginMatchIndex);
            serializer.Write(EndMatchIndex);
            serializer.Write(Index);
            serializer.Write(Vars.size());
            for (const auto& vector : Vars) {
                serializer.Write(vector.size());
                for (const auto& range : vector) {
                    range.Save(serializer);
                }
            }
            serializer.Write(Quantifiers.size());
            for (ui64 qnt : Quantifiers) {
                serializer.Write(qnt);
            }
        }

        void Load(TMrInputSerializer& serializer) {
            serializer.Read(BeginMatchIndex);
            serializer.Read(EndMatchIndex);
            serializer.Read(Index);
            auto varsSize = serializer.Read<TMatchedVars::size_type>();
            Vars.clear();
            Vars.resize(varsSize);
            for (auto& subvec: Vars) {
                ui64 vectorSize = serializer.Read<ui64>();
                subvec.resize(vectorSize);
                for (auto& item : subvec) {
                    item.Load(serializer);
                }
            }
            Quantifiers.clear();
            auto quantifiersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < quantifiersSize; ++i) {
                ui64 qnt = serializer.Read<ui64>();
                Quantifiers.push_back(qnt);
            }
        }

        friend inline bool operator<(const TState& lhs, const TState& rhs) {
            auto lhsEndMatchIndex = -static_cast<i64>(lhs.EndMatchIndex);
            auto rhsEndMatchIndex = -static_cast<i64>(rhs.EndMatchIndex);
            return std::tie(lhs.BeginMatchIndex, lhsEndMatchIndex, lhs.Index, lhs.Quantifiers, lhs.Vars) < std::tie(rhs.BeginMatchIndex, rhsEndMatchIndex, rhs.Index, rhs.Quantifiers, rhs.Vars);
        }
        friend inline bool operator==(const TState& lhs, const TState& rhs) {
            return std::tie(lhs.BeginMatchIndex, lhs.EndMatchIndex, lhs.Index, lhs.Quantifiers, lhs.Vars) == std::tie(rhs.BeginMatchIndex, rhs.EndMatchIndex, rhs.Index, rhs.Quantifiers, rhs.Vars);
        }
    };
public:

    TNfa(TNfaTransitionGraph::TPtr transitionGraph, IComputationExternalNode* matchedRangesArg, const TComputationNodePtrVector& defines)
        : TransitionGraph(transitionGraph)
        , MatchedRangesArg(matchedRangesArg)
        , Defines(defines) {
    }

    void ProcessRow(TSparseList::TRange&& currentRowLock, TComputationContext& ctx) {
        TState state(currentRowLock.From(), currentRowLock.To(), TransitionGraph->Input, TMatchedVars(Defines.size()), std::deque<ui64, TMKQLAllocator<ui64>>{});
        Insert(std::move(state));
        MakeEpsilonTransitions();
        TStateSet newStates;
        TStateSet deletedStates;
        for (const auto& state : ActiveStates) {
            //Here we handle only transitions of TMatchedVarTransition type,
            //all other transitions are handled in MakeEpsilonTransitions
            if (const auto* matchedVarTransition = std::get_if<TMatchedVarTransition>(&TransitionGraph->Transitions[state.Index])) {
                MatchedRangesArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TRange>>(ctx.HolderFactory, state.Vars));
                const auto varIndex = matchedVarTransition->VarIndex;
                const auto& v = Defines[varIndex]->GetValue(ctx);
                if (v && v.Get<bool>()) {
                    if (matchedVarTransition->SaveState) {
                        auto vars = state.Vars; //TODO get rid of this copy
                        auto& matchedVar = vars[varIndex];
                        Extend(matchedVar, currentRowLock);
                        newStates.emplace(state.BeginMatchIndex, currentRowLock.To(), matchedVarTransition->To, std::move(vars), state.Quantifiers);
                    } else {
                        newStates.emplace(state.BeginMatchIndex, currentRowLock.To(), matchedVarTransition->To, state.Vars, state.Quantifiers);
                    }
                }
                deletedStates.insert(state);
            }
        }
        for (auto& state : deletedStates) {
            Erase(std::move(state));
        }
        for (auto& state : newStates) {
            Insert(std::move(state));
        }
        MakeEpsilonTransitions();
    }

    bool HasMatched() const {
        for (auto& state: ActiveStates) {
            if (auto activeStateIter = ActiveStateCounters.find(state.BeginMatchIndex),
                finishedStateIter = FinishedStateCounters.find(state.BeginMatchIndex);
                ((activeStateIter != ActiveStateCounters.end() &&
                finishedStateIter != FinishedStateCounters.end() &&
                activeStateIter->second == finishedStateIter->second) ||
                EndOfData) &&
                state.Index == TransitionGraph->Output) {
                return true;
            }
        }
        return false;
    }

    std::optional<TMatchedVars> GetMatched() {
        for (auto& state: ActiveStates) {
            if (auto activeStateIter = ActiveStateCounters.find(state.BeginMatchIndex),
                finishedStateIter = FinishedStateCounters.find(state.BeginMatchIndex);
                ((activeStateIter != ActiveStateCounters.end() &&
                finishedStateIter != FinishedStateCounters.end() &&
                activeStateIter->second == finishedStateIter->second) ||
                EndOfData) &&
                state.Index == TransitionGraph->Output) {
                auto result = state.Vars;
                Erase(std::move(state));
                return result;
            }
        }
        return std::nullopt;
    }

    size_t GetActiveStatesCount() const {
        return ActiveStates.size();
    }

    void Save(TMrOutputSerializer& serializer) const {
        // TransitionGraph is not saved/loaded, passed in constructor.
        serializer.Write(ActiveStates.size());
        for (const auto& state : ActiveStates) {
            state.Save(serializer);
        }
        serializer.Write(ActiveStateCounters.size());
        for (const auto& counter : ActiveStateCounters) {
            serializer(counter);
        }
        serializer.Write(FinishedStateCounters.size());
        for (const auto& counter : FinishedStateCounters) {
            serializer(counter);
        }
    }

    void Load(TMrInputSerializer& serializer) {
        {
            ActiveStates.clear();
            auto activeStatesSize = serializer.Read<ui64>();
            for (size_t i = 0; i < activeStatesSize; ++i) {
                TState state;
                state.Load(serializer);
                ActiveStates.emplace(state);
            }
        }
        {
            ActiveStateCounters.clear();
            auto activeStateCountersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < activeStateCountersSize; ++i) {
                using map_type = decltype(ActiveStateCounters);
                auto beginMatchIndex = serializer.Read<map_type::key_type>();
                auto counter = serializer.Read<map_type::mapped_type>();
                ActiveStateCounters.emplace(beginMatchIndex, counter);
            }
        }
        {
            FinishedStateCounters.clear();
            auto finishedStateCountersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < finishedStateCountersSize; ++i) {
                using map_type = decltype(FinishedStateCounters);
                auto beginMatchIndex = serializer.Read<map_type::key_type>();
                auto counter = serializer.Read<map_type::mapped_type>();
                FinishedStateCounters.emplace(beginMatchIndex, counter);
            }
        }
    }

    bool ProcessEndOfData(const TComputationContext& ctx) {
        EndOfData = true;
        return HasMatched();
    }

    void Clear() {
        ActiveStates.clear();
        ActiveStateCounters.clear();
        FinishedStateCounters.clear();
    }

private:
    //TODO (zverevgeny): Consider to change to std::vector for the sake of perf
    using TStateSet = std::set<TState, std::less<TState>, TMKQLAllocator<TState>>;

    bool MakeEpsilonTransitionsImpl() {
        TStateSet newStates;
        TStateSet deletedStates;
        for (const auto& state: ActiveStates) {
            std::visit(TOverloaded {
                [&](const TVoidTransition&) {
                    //Do nothing for void
                },
                [&](const TMatchedVarTransition&) {
                    //Transitions of TMatchedVarTransition type are handled in ProcessRow method
                },
                [&](const TEpsilonTransitions& epsilonTransitions) {
                    deletedStates.insert(state);
                    for (const auto& i : epsilonTransitions.To) {
                        newStates.emplace(state.BeginMatchIndex, state.EndMatchIndex, i, state.Vars, state.Quantifiers);
                    }
                },
                [&](const TQuantityEnterTransition& quantityEnterTransition) {
                    deletedStates.insert(state);
                    auto quantifiers = state.Quantifiers; //TODO get rid of this copy
                    quantifiers.push_back(0);
                    newStates.emplace(state.BeginMatchIndex, state.EndMatchIndex, quantityEnterTransition.To, state.Vars, std::move(quantifiers));
                },
                [&](const TQuantityExitTransition& quantityExitTransition) {
                    deletedStates.insert(state);
                    auto [quantityMin, quantityMax, toFindMore, toMatched] = quantityExitTransition;
                    if (state.Quantifiers.back() + 1 < quantityMax) {
                        auto q = state.Quantifiers;
                        q.back()++;
                        newStates.emplace(state.BeginMatchIndex, state.EndMatchIndex, toFindMore, state.Vars, std::move(q));
                    }
                    if (quantityMin <= state.Quantifiers.back() + 1 && state.Quantifiers.back() + 1 <= quantityMax) {
                        auto q = state.Quantifiers;
                        q.pop_back();
                        newStates.emplace(state.BeginMatchIndex, state.EndMatchIndex, toMatched, state.Vars, std::move(q));
                    }
                },
            }, TransitionGraph->Transitions[state.Index]);
        }
        bool result = newStates != deletedStates;
        for (auto& state : deletedStates) {
            Erase(std::move(state));
        }
        for (auto& state : newStates) {
            Insert(std::move(state));
        }
        return result;
    }

    void MakeEpsilonTransitions() {
        while (MakeEpsilonTransitionsImpl());
    }

    static void Add(THashMap<size_t, i64>& counters, size_t index, i64 value) {
        auto countersIter = counters.try_emplace(index, 0).first;
        MKQL_ENSURE(countersIter != counters.end(), "Internal logic error");
        countersIter->second += value;
        if (countersIter->second == 0) {
            counters.erase(countersIter);
        }
    }

    void Insert(TState state) {
        auto beginMatchIndex = state.BeginMatchIndex;
        const auto& transition = TransitionGraph->Transitions[state.Index];
        auto diff = static_cast<i64>(ActiveStates.insert(std::move(state)).second);
        Add(ActiveStateCounters, beginMatchIndex, diff);
        if (std::holds_alternative<TVoidTransition>(transition)) {
            Add(FinishedStateCounters, beginMatchIndex, diff);
        }
    }

    void Erase(TState state) {
        auto beginMatchIndex = state.BeginMatchIndex;
        const auto& transition = TransitionGraph->Transitions[state.Index];
        auto diff = -static_cast<i64>(ActiveStates.erase(std::move(state)));
        Add(ActiveStateCounters, beginMatchIndex, diff);
        if (std::holds_alternative<TVoidTransition>(transition)) {
            Add(FinishedStateCounters, beginMatchIndex, diff);
        }
    }

    TNfaTransitionGraph::TPtr TransitionGraph;
    IComputationExternalNode* const MatchedRangesArg;
    const TComputationNodePtrVector Defines;
    TStateSet ActiveStates; //NFA state
    THashMap<size_t, i64> ActiveStateCounters;
    THashMap<size_t, i64> FinishedStateCounters;
    bool EndOfData = false;
};

}//namespace NKikimr::NMiniKQL::NMatchRecognize
