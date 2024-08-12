#pragma once

#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_save_load.h"
#include "../computation/mkql_computation_node_holders.h"
#include "../computation/mkql_computation_node_impl.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <util/generic/hash_table.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

using namespace NYql::NMatchRecognize;

struct TVoidTransition {
    friend bool operator==(const TVoidTransition&, const TVoidTransition&) {
        return true;
    }
};
using TEpsilonTransition = size_t; //to
using TEpsilonTransitions = std::vector<TEpsilonTransition, TMKQLAllocator<TEpsilonTransition>>;
using TMatchedVarTransition = std::pair<std::pair<ui32, bool>, size_t>; //{{varIndex, saveState}, to}
using TQuantityEnterTransition = size_t; //to
using TQuantityExitTransition = std::pair<std::pair<ui64, ui64>, std::pair<size_t, size_t>>; //{{min, max}, {foFindMore, toMatched}}

template <typename... Ts>
struct TVariantHelper {
    using TVariant =  std::variant<Ts...>;
    using TTuple =  std::tuple<Ts...>;

    static std::variant<Ts...> getVariantByIndex(size_t i) {
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
    std::function<size_t(size_t)> callback;

    template<typename Callback>
    explicit TNfaTransitionDestinationVisitor(Callback callback)
        : callback(std::move(callback)) {}

    TNfaTransition operator()(TVoidTransition tr) const {
        return tr;
    }

    TNfaTransition operator()(TMatchedVarTransition tr) const {
        tr.second = callback(tr.second);
        return tr;
    }

    TNfaTransition operator()(TEpsilonTransitions tr) const {
        for (size_t& toNode: tr) {
            toNode = callback(toNode);
        }
        return tr;
    }

    TNfaTransition operator()(TQuantityEnterTransition tr) const {
        return callback(tr);
    }

    TNfaTransition operator()(TQuantityExitTransition tr) const {
        tr.second.first = callback(tr.second.first);
        tr.second.second = callback(tr.second.second);
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
            std::visit(serializer, Transitions[i]);
        }
        serializer(Input, Output);
    }

    void Load(TMrInputSerializer& serializer) {
        ui64 transitionSize = serializer.Read<TTransitions::size_type>();
        Transitions.resize(transitionSize);
        for (ui64 i = 0; i < transitionSize; ++i) {
            size_t index = serializer.Read<std::size_t>();
            Transitions[i] = TNfaTransitionHelper::getVariantByIndex(index);
            std::visit(serializer, Transitions[i]);
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
                auto dfsStack = *ts;
                while (!dfsStack.empty()) {
                    auto curNode = dfsStack.back();
                    dfsStack.pop_back();
                    if (auto* curTs = std::get_if<TEpsilonTransitions>(&Graph->Transitions[curNode])) {
                        std::copy(curTs->begin(), curTs->end(), std::back_inserter(dfsStack));
                    } else {
                        optimizedTs.push_back(curNode);
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
                    if (tr->size() == 1) {
                        return (*tr)[0];
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
        std::vector<TEpsilonTransition, TMKQLAllocator<TEpsilonTransition>> fromInput;
        for (const auto& t: terms) {
            auto a = BuildTerm(t, varNameToIndex);
            fromInput.push_back(a.Input);
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
            Graph->Transitions[input] = TEpsilonTransitions{item.Input};
            Graph->Transitions[item.Output] = TEpsilonTransitions{output};
        } else {
            auto interim = AddNode();
            auto fromInput = TEpsilonTransitions{interim};
            if (factor.QuantityMin == 0) {
                fromInput.push_back(output);
            }
            Graph->Transitions[input] = fromInput;
            Graph->Transitions[interim] = TQuantityEnterTransition{item.Input};
            Graph->Transitions[item.Output] = std::pair{
                std::pair{factor.QuantityMin, factor.QuantityMax},
                std::pair{item.Input, output}
            };
        }
        return {input, output};
    }
    TNfaItem BuildVar(ui32 varIndex, bool isUsed) {
        auto input = AddNode();
        auto matchVar = AddNode();
        auto output = AddNode();
        Graph->Transitions[input] = TEpsilonTransitions({matchVar});
        Graph->Transitions[matchVar] = std::pair{std::pair{varIndex, isUsed}, output};
        return {input, output};
    }
public:
    using TPatternConfigurationPtr = TNfaTransitionGraph::TPtr;
    static TPatternConfigurationPtr Create(const TRowPattern& pattern, const THashMap<TString, size_t>& varNameToIndex) {
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
        
        TState() {}

        TState(size_t index, const TMatchedVars& vars, std::stack<ui64, std::deque<ui64, TMKQLAllocator<ui64>>>&& quantifiers)
            : Index(index)
            , Vars(vars)
            , Quantifiers(quantifiers) {}
        size_t Index;
        TMatchedVars Vars;
        
        using TQuantifiersStdStack = std::stack<
            ui64,
            std::deque<ui64, TMKQLAllocator<ui64>>>; //get rid of this

        struct TQuantifiersStack: public TQuantifiersStdStack {
            template<typename...TArgs>
            TQuantifiersStack(TArgs... args) : TQuantifiersStdStack(args...) {}
            
            auto begin() const { return c.begin(); }
            auto end() const { return c.end(); }
            auto clear() { return c.clear(); }
        };

        TQuantifiersStack Quantifiers;

        void Save(TMrOutputSerializer& serializer) const {
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
                Quantifiers.push(qnt);
            }
        }

        friend inline bool operator<(const TState& lhs, const TState& rhs) {
            return std::tie(lhs.Index, lhs.Quantifiers, lhs.Vars) < std::tie(rhs.Index, rhs.Quantifiers, rhs.Vars);
        }
        friend inline bool operator==(const TState& lhs, const TState& rhs) {
            return std::tie(lhs.Index, lhs.Quantifiers, lhs.Vars) == std::tie(rhs.Index, rhs.Quantifiers, rhs.Vars);
        }
    };
public:

    TNfa(TNfaTransitionGraph::TPtr transitionGraph, IComputationExternalNode* matchedRangesArg, const TComputationNodePtrVector& defines)
        : TransitionGraph(transitionGraph)
        , MatchedRangesArg(matchedRangesArg)
        , Defines(defines) {
    }

    void ProcessRow(TSparseList::TRange&& currentRowLock, TComputationContext& ctx) {        
        ActiveStates.emplace(TransitionGraph->Input, TMatchedVars(Defines.size()), std::stack<ui64, std::deque<ui64, TMKQLAllocator<ui64>>>{});
        MakeEpsilonTransitions();
        std::set<TState, std::less<TState>, TMKQLAllocator<TState>> newStates;
        std::set<TState, std::less<TState>, TMKQLAllocator<TState>> deletedStates;
        for (const auto& s: ActiveStates) {
            //Here we handle only transitions of TMatchedVarTransition type,
            //all other transitions are handled in MakeEpsilonTransitions
            if (const auto* matchedVarTransition = std::get_if<TMatchedVarTransition>(&TransitionGraph->Transitions[s.Index])) {
                MatchedRangesArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TRange>>(ctx.HolderFactory, s.Vars));
                const auto varIndex = matchedVarTransition->first.first;
                const auto& v = Defines[varIndex]->GetValue(ctx);
                if (v && v.Get<bool>()) {
                    if (matchedVarTransition->first.second) {
                        auto vars = s.Vars; //TODO get rid of this copy
                        auto& matchedVar = vars[varIndex];
                        Extend(matchedVar, currentRowLock);
                        newStates.emplace(matchedVarTransition->second, std::move(vars), std::stack<ui64, std::deque<ui64, TMKQLAllocator<ui64>>>(s.Quantifiers));
                    } else {
                        newStates.emplace(matchedVarTransition->second, s.Vars, std::stack<ui64, std::deque<ui64, TMKQLAllocator<ui64>>>(s.Quantifiers));
                    }
                }
                deletedStates.insert(s);
            }
        }
        for (auto& s: deletedStates)
            ActiveStates.erase(s);
        ActiveStates.insert(newStates.begin(), newStates.end());
        MakeEpsilonTransitions();
        EpsilonTransitionsLastRow = 0;
    }

    bool HasMatched() const {
        for (auto& s: ActiveStates) {
            if (s.Index == TransitionGraph->Output) {
                return true;
            }
        }
        return false;
    }

    std::optional<TMatchedVars> GetMatched() {
        for (auto& s: ActiveStates) {
            if (s.Index == TransitionGraph->Output) {
                auto result = s.Vars;
                ActiveStates.erase(s);
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
        serializer.Write(EpsilonTransitionsLastRow);
    }

    void Load(TMrInputSerializer& serializer) {
        auto stateSize = serializer.Read<ui64>();
        for (size_t i = 0; i < stateSize; ++i) {
            TState state;
            state.Load(serializer);
            ActiveStates.emplace(state);
        }
        serializer.Read(EpsilonTransitionsLastRow);
    }

private:
    //TODO (zverevgeny): Consider to change to std::vector for the sake of perf
    using TStateSet = std::set<TState, std::less<TState>, TMKQLAllocator<TState>>;
    struct TTransitionVisitor {
        TTransitionVisitor(const TState& state, TStateSet& newStates, TStateSet& deletedStates)
            : State(state)
            , NewStates(newStates)
            , DeletedStates(deletedStates) {}
        void operator()(const TVoidTransition&) {
            //Do nothing for void
        }
        void operator()(const TMatchedVarTransition& var) {
            //Transitions of TMatchedVarTransition type are handled in ProcessRow method
            Y_UNUSED(var);
        }
        void operator()(const TEpsilonTransitions& epsilonTransitions) {
            for (const auto& i: epsilonTransitions) {
                NewStates.emplace(i, TMatchedVars(State.Vars), std::stack<ui64, std::deque<ui64, TMKQLAllocator<ui64>>>(State.Quantifiers));
            }
            DeletedStates.insert(State);
        }
        void operator()(const TQuantityEnterTransition& quantityEnterTransition) {
            DeletedStates.insert(State);
            auto quantifiers = State.Quantifiers; //TODO get rid of this copy
            quantifiers.push(0);
            NewStates.emplace(quantityEnterTransition, TMatchedVars(State.Vars), std::move(quantifiers));
        }
        void operator()(const TQuantityExitTransition& quantityExitTransition) {
            DeletedStates.insert(State);
            auto minQuantity = quantityExitTransition.first.first;
            auto maxQuantity = quantityExitTransition.first.second;
            if (State.Quantifiers.top() + 1 < quantityExitTransition.first.second) {
                auto q = State.Quantifiers;
                q.top()++;
                NewStates.emplace(quantityExitTransition.second.first, TMatchedVars(State.Vars), std::move(q));
            }
            if (State.Quantifiers.top() + 1 >= minQuantity && State.Quantifiers.top() + 1 <= maxQuantity) {
                auto q = State.Quantifiers;
                q.pop();
                NewStates.emplace(quantityExitTransition.second.second, TMatchedVars(State.Vars), std::move(q));
            }

        }
        const TState& State;
        TStateSet& NewStates;
        TStateSet& DeletedStates;
    };

    bool MakeEpsilonTransitionsImpl() {
        TStateSet newStates;
        TStateSet deletedStates;
        for (const auto& s: ActiveStates) {
            ++EpsilonTransitionsLastRow;
            std::visit(TTransitionVisitor(s, newStates, deletedStates), TransitionGraph->Transitions[s.Index]);
        }
        bool result = newStates != deletedStates;
        for (auto& s: deletedStates) {
            ActiveStates.erase(s);
        }
        ActiveStates.insert(newStates.begin(), newStates.end());
        return result;
    }

    void MakeEpsilonTransitions() {
        do {

        } while (MakeEpsilonTransitionsImpl());
    }

    TNfaTransitionGraph::TPtr TransitionGraph;
    IComputationExternalNode* const MatchedRangesArg;
    const TComputationNodePtrVector Defines;
    TStateSet ActiveStates; //NFA state
    size_t EpsilonTransitionsLastRow = 0;
};

}//namespace NKikimr::NMiniKQL::NMatchRecognize
