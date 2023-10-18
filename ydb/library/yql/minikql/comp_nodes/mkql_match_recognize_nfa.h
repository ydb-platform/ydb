#pragma once

#include "mkql_match_recognize_matched_vars.h"
#include "../computation/mkql_computation_node_holders.h"
#include "../computation/mkql_computation_node_impl.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <util/generic/hash_table.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

using namespace NYql::NMatchRecognize;

struct TVoidTransition{};
using TEpsilonTransition = size_t; //to
using TEpsilonTransitions = std::vector<TEpsilonTransition>;
using TMatchedVarTransition = std::pair<ui32, size_t>; //{varIndex, to}
using TQuantityEnterTransition = size_t; //to
using TQuantityExitTransition = std::pair<std::pair<ui64, ui64>, std::pair<size_t, size_t>>; //{{min, max}, {foFindMore, toMatched}}
using TNfaTransition = std::variant<
    TVoidTransition,
    TMatchedVarTransition,
    TEpsilonTransitions,
    TQuantityEnterTransition,
    TQuantityExitTransition
>;

struct TNfaTransitionGraph {
    std::vector<TNfaTransition> Transitions;
    size_t Input;
    size_t Output;

    using TPtr = std::shared_ptr<TNfaTransitionGraph>;
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
        Graph->Transitions.resize(Graph->Transitions.size() + 1);
        return Graph->Transitions.size() - 1;
    }

    TNfaItem BuildTerms(const std::vector<TRowPatternTerm>& terms, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        std::vector<TEpsilonTransition> fromInput;
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
        std::vector<TNfaItem> automata;
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
                    BuildVar(varNameToIndex.at(std::get<0>(factor.Primary))) :
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
    TNfaItem BuildVar(ui32 varIndex) {
        auto input = AddNode();
        auto matchVar = AddNode();
        auto output = AddNode();
        Graph->Transitions[input] = TEpsilonTransitions({matchVar});
        Graph->Transitions[matchVar] = std::pair{varIndex, output};
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
        return result;
    }
private:
    TNfaTransitionGraph::TPtr Graph;
};

class TNfa {
    using TRange = TSparseList::TRange;
    using TMatchedVars = TMatchedVars<TRange>;
    struct TState {
        TState(size_t index, const TMatchedVars& vars, std::stack<ui64>&& quantifiers)
            : Index(index)
            , Vars(vars)
            , Quantifiers(quantifiers) {}
        const size_t Index;
        TMatchedVars Vars;
        std::stack<ui64> Quantifiers; //get rid of this

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
        ActiveStates.emplace(TransitionGraph->Input, TMatchedVars(Defines.size()), std::stack<ui64>{});
        MakeEpsilonTransitions();
        std::set<TState> newStates;
        std::set<TState> deletedStates;
        for (const auto& s: ActiveStates) {
            //Here we handle only transitions of TMatchedVarTransition type,
            //all other transitions are handled in MakeEpsilonTransitions
            if (const auto* matchedVarTransition = std::get_if<TMatchedVarTransition>(&TransitionGraph->Transitions[s.Index])) {
                MatchedRangesArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TRange>>(ctx.HolderFactory, s.Vars));
                const auto varIndex = matchedVarTransition->first;
                const auto& v = Defines[varIndex]->GetValue(ctx);
                if (v && v.Get<bool>()) {
                    auto vars = s.Vars; //TODO get rid of this copy
                    auto& matchedVar = vars[varIndex];
                    Extend(matchedVar, currentRowLock);
                    newStates.emplace(matchedVarTransition->second, std::move(vars), std::stack<ui64>(s.Quantifiers));
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

private:
    //TODO (zverevgeny): Consider to change to std::vector for the sake of perf
    using TStateSet = std::set<TState>;
    struct TTransitionVisitor {
        TTransitionVisitor(const TState& state, TStateSet& newStates, TStateSet& deletedStates)
            : State(state)
            , NewStates(newStates)
            , DeletedStates(deletedStates)
        {}
        void operator()(const TVoidTransition&) {
            //Do nothing for void
        }
        void operator()(const TMatchedVarTransition& var) {
            //Transitions of TMatchedVarTransition type are handled in ProcessRow method
            Y_UNUSED(var);
        }
        void operator()(const TEpsilonTransitions& epsilonTransitions) {
            for (const auto& i: epsilonTransitions) {
                NewStates.emplace(i, TMatchedVars(State.Vars), std::stack<ui64>(State.Quantifiers));
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
