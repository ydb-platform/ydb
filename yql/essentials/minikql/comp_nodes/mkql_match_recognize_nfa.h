#pragma once

#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_save_load.h"
#include "../computation/mkql_computation_node_holders.h"
#include "util/generic/overloaded.h"
#include <yql/essentials/core/sql_types/match_recognize.h>
#include <util/generic/hash_table.h>
#include <util/generic/string.h>

#include <array>
#include <utility>

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
    size_t To;
    ui32 VarIndex;
    bool SaveState;
    bool ExcludeFromOutput;
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
    using TVariant = std::variant<Ts...>;
    using TTuple = std::tuple<Ts...>;

    static std::variant<Ts...> GetVariantByIndex(size_t i) {
        MKQL_ENSURE(i < sizeof...(Ts), "Wrong variant index");
        static std::array<std::variant<Ts...>, sizeof...(Ts)> Table = {Ts{}...};
        return Table[i];
    }
};

using TNfaTransitionHelper = TVariantHelper<
    TVoidTransition,
    TMatchedVarTransition,
    TEpsilonTransitions,
    TQuantityEnterTransition,
    TQuantityExitTransition>;

using TNfaTransition = TNfaTransitionHelper::TVariant;

struct TNfaTransitionDestinationVisitor {
    std::function<size_t(size_t)> Callback;

    template <typename Callback>
    explicit TNfaTransitionDestinationVisitor(Callback callback)
        : Callback(std::move(callback))
    {
    }

    TNfaTransition operator()(TVoidTransition tr) const {
        return tr;
    }

    TNfaTransition operator()(TMatchedVarTransition tr) const {
        tr.To = Callback(tr.To);
        return tr;
    }

    TNfaTransition operator()(TEpsilonTransitions tr) const {
        for (size_t& toNode : tr.To) {
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

    template <class>
    inline constexpr static bool always_false_v = false;

    void Save(TMrOutputSerializer& serializer) const {
        serializer(Transitions.size());
        for (const auto& transition : Transitions) {
            serializer.Write(transition.index());
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
                       }, transition);
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
        return Transitions == other.Transitions && Input == other.Input && Output == other.Output;
    }
};

class TNfaTransitionGraphOptimizer {
public:
    explicit TNfaTransitionGraphOptimizer(TNfaTransitionGraph::TPtr graph)
        : Graph_(std::move(graph))
    {
    }

    void DoOptimizations() {
        EliminateEpsilonChains();
        EliminateSingleEpsilons();
        CollectGarbage();
    }

private:
    void EliminateEpsilonChains() {
        for (size_t node = 0; node != Graph_->Transitions.size(); node++) {
            if (auto* ts = std::get_if<TEpsilonTransitions>(&Graph_->Transitions[node])) {
                // new vector of eps transitions,
                // contains refs to all nodes which are reachable from oldNode via eps transitions
                TEpsilonTransitions optimizedTs;
                auto dfsStack = ts->To;
                while (!dfsStack.empty()) {
                    auto curNode = dfsStack.back();
                    dfsStack.pop_back();
                    if (auto* curTs = std::get_if<TEpsilonTransitions>(&Graph_->Transitions[curNode])) {
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
        for (size_t node = 0; node != Graph_->Transitions.size(); node++) {
            if (std::holds_alternative<TEpsilonTransitions>(Graph_->Transitions[node])) {
                continue;
            }
            Graph_->Transitions[node] = std::visit(TNfaTransitionDestinationVisitor([&](size_t toNode) -> size_t {
                                                       if (auto* tr = std::get_if<TEpsilonTransitions>(&Graph_->Transitions[toNode])) {
                                                           if (tr->To.size() == 1) {
                                                               return tr->To[0];
                                                           }
                                                       }
                                                       return toNode;
                                                   }), Graph_->Transitions[node]);
        }
    }
    void CollectGarbage() {
        auto oldInput = Graph_->Input;
        auto oldOutput = Graph_->Output;
        decltype(Graph_->Transitions) oldTransitions;
        Graph_->Transitions.swap(oldTransitions);
        // Scan for reachable nodes and map old node ids to new node ids
        std::vector<std::optional<size_t>> mapping(oldTransitions.size(), std::nullopt);
        std::vector<size_t> dfsStack = {oldInput};
        mapping[oldInput] = 0;
        Graph_->Transitions.emplace_back();
        while (!dfsStack.empty()) {
            auto oldNode = dfsStack.back();
            dfsStack.pop_back();
            std::visit(TNfaTransitionDestinationVisitor([&](size_t oldToNode) {
                           if (!mapping[oldToNode]) {
                               mapping[oldToNode] = Graph_->Transitions.size();
                               Graph_->Transitions.emplace_back();
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
                Graph_->Input = node;
            }
            if (oldNode == oldOutput) {
                Graph_->Output = node;
            }
            Graph_->Transitions[node] = oldTransitions[oldNode];
            Graph_->Transitions[node] = std::visit(TNfaTransitionDestinationVisitor([&](size_t oldToNode) {
                                                       return mapping[oldToNode].value();
                                                   }), Graph_->Transitions[node]);
        }
    }

    TNfaTransitionGraph::TPtr Graph_;
};

class TNfaTransitionGraphBuilder {
private:
    struct TNfaItem {
        size_t Input;
        size_t Output;
    };

    explicit TNfaTransitionGraphBuilder(TNfaTransitionGraph::TPtr graph)
        : Graph_(std::move(graph))
    {
    }

    size_t AddNode() {
        Graph_->Transitions.emplace_back();
        return Graph_->Transitions.size() - 1;
    }

    TNfaItem BuildTerms(const TVector<TRowPatternTerm>& terms, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        TEpsilonTransitions fromInput;
        for (const auto& t : terms) {
            auto a = BuildTerm(t, varNameToIndex);
            fromInput.To.push_back(a.Input);
            Graph_->Transitions[a.Output] = TEpsilonTransitions({output});
        }
        Graph_->Transitions[input] = std::move(fromInput);
        return {.Input = input, .Output = output};
    }
    TNfaItem BuildTerm(const TRowPatternTerm& term, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        std::vector<TNfaItem, TMKQLAllocator<TNfaItem>> automata;
        for (const auto& f : term) {
            automata.push_back(BuildFactor(f, varNameToIndex));
        }
        for (size_t i = 0; i != automata.size() - 1; ++i) {
            Graph_->Transitions[automata[i].Output] = TEpsilonTransitions({automata[i + 1].Input});
        }
        Graph_->Transitions[input] = TEpsilonTransitions({automata.front().Input});
        Graph_->Transitions[automata.back().Output] = TEpsilonTransitions({output});
        return {.Input = input, .Output = output};
    }
    TNfaItem BuildFactor(const TRowPatternFactor& factor, const THashMap<TString, size_t>& varNameToIndex) {
        auto input = AddNode();
        auto output = AddNode();
        auto item = factor.Primary.index() == 0 ? BuildVar(varNameToIndex.at(std::get<0>(factor.Primary)), !factor.Unused, !factor.Output) : BuildTerms(std::get<1>(factor.Primary), varNameToIndex);
        if (1 == factor.QuantityMin && 1 == factor.QuantityMax) { // simple linear case
            Graph_->Transitions[input] = TEpsilonTransitions{{item.Input}};
            Graph_->Transitions[item.Output] = TEpsilonTransitions{{output}};
        } else {
            auto interim = AddNode();
            auto fromInput = TEpsilonTransitions{{interim}};
            if (factor.QuantityMin == 0) {
                fromInput.To.push_back(output);
            }
            Graph_->Transitions[input] = fromInput;
            Graph_->Transitions[interim] = TQuantityEnterTransition{item.Input};
            Graph_->Transitions[item.Output] = TQuantityExitTransition{
                .QuantityMin = factor.QuantityMin,
                .QuantityMax = factor.QuantityMax,
                .ToFindMore = item.Input,
                .ToMatched = output,
            };
        }
        return {.Input = input, .Output = output};
    }
    TNfaItem BuildVar(ui32 varIndex, bool isUsed, bool excludeFromOutput) {
        auto input = AddNode();
        auto matchVar = AddNode();
        auto output = AddNode();
        Graph_->Transitions[input] = TEpsilonTransitions({matchVar});
        Graph_->Transitions[matchVar] = TMatchedVarTransition{
            .To = output,
            .VarIndex = varIndex,
            .SaveState = isUsed,
            .ExcludeFromOutput = excludeFromOutput,
        };
        return {.Input = input, .Output = output};
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
    TNfaTransitionGraph::TPtr Graph_;
};

class TNfa {
    using TRange = TSparseList::TRange;
    using TMatchedVars = TMatchedVars<TRange>;

public:
    struct TMatch {
        size_t BeginIndex;
        size_t EndIndex;
        TMatchedVars Vars;

        void Save(TMrOutputSerializer& serializer) const {
            serializer(BeginIndex, EndIndex);
            serializer.Write(Vars.size());
            for (const auto& vector : Vars) {
                serializer.Write(vector.size());
                for (const auto& range : vector) {
                    range.Save(serializer);
                }
            }
        }

        void Load(TMrInputSerializer& serializer) {
            serializer(BeginIndex, EndIndex);
            auto varsSize = serializer.Read<size_t>();
            Vars.clear();
            Vars.resize(varsSize);
            for (auto& subvec : Vars) {
                ui64 vectorSize = serializer.Read<ui64>();
                subvec.resize(vectorSize);
                for (auto& item : subvec) {
                    item.Load(serializer);
                }
            }
        }
    };

private:
    struct TState {
        size_t Index;
        TMatch Match;
        std::deque<ui64, TMKQLAllocator<ui64>> Quantifiers;

        void Save(TMrOutputSerializer& serializer) const {
            serializer.Write(Index);
            Match.Save(serializer);
            serializer.Write(Quantifiers.size());
            for (ui64 qnt : Quantifiers) {
                serializer.Write(qnt);
            }
        }

        void Load(TMrInputSerializer& serializer) {
            serializer.Read(Index);
            Match.Load(serializer);
            Quantifiers.clear();
            auto quantifiersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < quantifiersSize; ++i) {
                ui64 qnt = serializer.Read<ui64>();
                Quantifiers.push_back(qnt);
            }
        }

        friend inline bool operator<(const TState& lhs, const TState& rhs) {
            auto lhsMatchEndIndex = -static_cast<i64>(lhs.Match.EndIndex);
            auto rhsMatchEndIndex = -static_cast<i64>(rhs.Match.EndIndex);
            return std::tie(lhs.Match.BeginIndex, lhsMatchEndIndex, lhs.Index, lhs.Match.Vars, lhs.Quantifiers) < std::tie(rhs.Match.BeginIndex, rhsMatchEndIndex, rhs.Index, rhs.Match.Vars, rhs.Quantifiers);
        }
        friend inline bool operator==(const TState& lhs, const TState& rhs) {
            return std::tie(lhs.Match.BeginIndex, lhs.Match.EndIndex, lhs.Index, lhs.Match.Vars, lhs.Quantifiers) == std::tie(rhs.Match.BeginIndex, rhs.Match.EndIndex, rhs.Index, rhs.Match.Vars, rhs.Quantifiers);
        }
    };

public:
    TNfa(
        TNfaTransitionGraph::TPtr transitionGraph,
        IComputationExternalNode* matchedRangesArg,
        TComputationNodePtrVector defines,
        TAfterMatchSkipTo skipTo)
        : TransitionGraph_(std::move(transitionGraph))
        , MatchedRangesArg_(matchedRangesArg)
        , Defines_(std::move(defines))
        , SkipTo_(std::move(skipTo))
    {
    }

    void ProcessRow(TSparseList::TRange&& currentRowLock, TComputationContext& ctx) {
        TState state(TransitionGraph_->Input, TMatch{.BeginIndex = currentRowLock.From(), .EndIndex = currentRowLock.To(), .Vars = TMatchedVars(Defines_.size())}, std::deque<ui64, TMKQLAllocator<ui64>>{});
        Insert(std::move(state));
        MakeEpsilonTransitions();
        TStateSet newStates;
        TStateSet deletedStates;
        for (const auto& state : ActiveStates_) {
            // Here we handle only transitions of TMatchedVarTransition type,
            // all other transitions are handled in MakeEpsilonTransitions
            if (const auto* matchedVarTransition = std::get_if<TMatchedVarTransition>(&TransitionGraph_->Transitions[state.Index])) {
                MatchedRangesArg_->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TRange>>(ctx.HolderFactory, state.Match.Vars));
                const auto varIndex = matchedVarTransition->VarIndex;
                const auto& v = Defines_[varIndex]->GetValue(ctx);
                if (v && v.Get<bool>()) {
                    if (matchedVarTransition->SaveState) {
                        auto vars = state.Match.Vars; // TODO get rid of this copy
                        auto& matchedVar = vars[varIndex];
                        currentRowLock.NfaIndex(state.Index);
                        Extend(matchedVar, currentRowLock);
                        newStates.emplace(matchedVarTransition->To, TMatch{.BeginIndex = state.Match.BeginIndex, .EndIndex = currentRowLock.To(), .Vars = std::move(vars)}, state.Quantifiers);
                    } else {
                        newStates.emplace(matchedVarTransition->To, TMatch{.BeginIndex = state.Match.BeginIndex, .EndIndex = currentRowLock.To(), .Vars = state.Match.Vars}, state.Quantifiers);
                    }
                }
                deletedStates.insert(state);
            }
        }
        for (auto& state : deletedStates) {
            Erase(state);
        }
        for (auto& state : newStates) {
            Insert(state);
        }
        MakeEpsilonTransitions();
    }

    bool HasMatched() const {
        for (auto& state : ActiveStates_) {
            if (auto activeStateIter = ActiveStateCounters_.find(state.Match.BeginIndex),
                finishedStateIter = FinishedStateCounters_.find(state.Match.BeginIndex);
                ((activeStateIter != ActiveStateCounters_.end() &&
                  finishedStateIter != FinishedStateCounters_.end() &&
                  activeStateIter->second == finishedStateIter->second) ||
                 EndOfData_) &&
                state.Index == TransitionGraph_->Output) {
                return true;
            }
        }
        return false;
    }

    std::optional<TMatch> GetMatched() {
        for (auto& state : ActiveStates_) {
            if (auto activeStateIter = ActiveStateCounters_.find(state.Match.BeginIndex),
                finishedStateIter = FinishedStateCounters_.find(state.Match.BeginIndex);
                ((activeStateIter != ActiveStateCounters_.end() &&
                  finishedStateIter != FinishedStateCounters_.end() &&
                  activeStateIter->second == finishedStateIter->second) ||
                 EndOfData_) &&
                state.Index == TransitionGraph_->Output) {
                auto result = state.Match;
                Erase(state);
                return result;
            }
        }
        return std::nullopt;
    }

    size_t GetActiveStatesCount() const {
        return ActiveStates_.size();
    }

    void Save(TMrOutputSerializer& serializer) const {
        // TransitionGraph is not saved/loaded, passed in constructor.
        serializer.Write(ActiveStates_.size());
        for (const auto& state : ActiveStates_) {
            state.Save(serializer);
        }
        serializer.Write(ActiveStateCounters_.size());
        for (const auto& counter : ActiveStateCounters_) {
            serializer(counter);
        }
        serializer.Write(FinishedStateCounters_.size());
        for (const auto& counter : FinishedStateCounters_) {
            serializer(counter);
        }
    }

    void Load(TMrInputSerializer& serializer) {
        {
            ActiveStates_.clear();
            auto activeStatesSize = serializer.Read<ui64>();
            for (size_t i = 0; i < activeStatesSize; ++i) {
                TState state;
                state.Load(serializer);
                ActiveStates_.emplace(state);
            }
        }
        {
            ActiveStateCounters_.clear();
            auto activeStateCountersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < activeStateCountersSize; ++i) {
                using map_type = decltype(ActiveStateCounters_);
                auto matchBeginIndex = serializer.Read<map_type::key_type>();
                auto counter = serializer.Read<map_type::mapped_type>();
                ActiveStateCounters_.emplace(matchBeginIndex, counter);
            }
        }
        {
            FinishedStateCounters_.clear();
            auto finishedStateCountersSize = serializer.Read<ui64>();
            for (size_t i = 0; i < finishedStateCountersSize; ++i) {
                using map_type = decltype(FinishedStateCounters_);
                auto matchBeginIndex = serializer.Read<map_type::key_type>();
                auto counter = serializer.Read<map_type::mapped_type>();
                FinishedStateCounters_.emplace(matchBeginIndex, counter);
            }
        }
    }

    bool ProcessEndOfData(const TComputationContext& /* ctx */) {
        EndOfData_ = true;
        return HasMatched();
    }

    void AfterMatchSkip(const TMatch& match) {
        const auto skipToRowIndex = [&]() {
            switch (SkipTo_.To) {
                case EAfterMatchSkipTo::NextRow:
                    return match.BeginIndex + 1;
                case EAfterMatchSkipTo::PastLastRow:
                    return match.EndIndex + 1;
                case EAfterMatchSkipTo::ToFirst:
                    MKQL_ENSURE(false, "AFTER MATCH SKIP TO FIRST is not implemented yet");
                case EAfterMatchSkipTo::ToLast:
                    [[fallthrough]];
                case EAfterMatchSkipTo::To:
                    MKQL_ENSURE(false, "AFTER MATCH SKIP TO LAST is not implemented yet");
            }
        }();

        TStateSet deletedStates;
        for (const auto& state : ActiveStates_) {
            if (state.Match.BeginIndex < skipToRowIndex) {
                deletedStates.insert(state);
            }
        }
        for (auto& state : deletedStates) {
            Erase(state);
        }
    }

    const TNfaTransitionGraph& GetTransitionGraph() const {
        return *TransitionGraph_;
    }

private:
    // TODO (zverevgeny): Consider to change to std::vector for the sake of perf
    using TStateSet = std::set<TState, std::less<>, TMKQLAllocator<TState>>;

    bool MakeEpsilonTransitionsImpl() {
        TStateSet newStates;
        TStateSet deletedStates;
        for (const auto& state : ActiveStates_) {
            std::visit(TOverloaded{
                           [&](const TVoidTransition&) {
                               // Do nothing for void
                           },
                           [&](const TMatchedVarTransition&) {
                               // Transitions of TMatchedVarTransition type are handled in ProcessRow method
                           },
                           [&](const TEpsilonTransitions& epsilonTransitions) {
                               deletedStates.insert(state);
                               for (const auto& i : epsilonTransitions.To) {
                                   newStates.emplace(i, state.Match, state.Quantifiers);
                               }
                           },
                           [&](const TQuantityEnterTransition& quantityEnterTransition) {
                               deletedStates.insert(state);
                               auto quantifiers = state.Quantifiers; // TODO get rid of this copy
                               quantifiers.push_back(0);
                               newStates.emplace(quantityEnterTransition.To, state.Match, std::move(quantifiers));
                           },
                           [&](const TQuantityExitTransition& quantityExitTransition) {
                               deletedStates.insert(state);
                               auto [quantityMin, quantityMax, toFindMore, toMatched] = quantityExitTransition;
                               if (state.Quantifiers.back() + 1 < quantityMax) {
                                   auto q = state.Quantifiers;
                                   q.back()++;
                                   newStates.emplace(toFindMore, state.Match, std::move(q));
                               }
                               if (quantityMin <= state.Quantifiers.back() + 1 && state.Quantifiers.back() + 1 <= quantityMax) {
                                   auto q = state.Quantifiers;
                                   q.pop_back();
                                   newStates.emplace(toMatched, state.Match, std::move(q));
                               }
                           },
                       }, TransitionGraph_->Transitions[state.Index]);
        }
        bool result = newStates != deletedStates;
        for (auto& state : deletedStates) {
            Erase(state);
        }
        for (auto& state : newStates) {
            Insert(state);
        }
        return result;
    }

    void MakeEpsilonTransitions() {
        while (MakeEpsilonTransitionsImpl()) {
        }
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
        auto matchBeginIndex = state.Match.BeginIndex;
        const auto& transition = TransitionGraph_->Transitions[state.Index];
        auto diff = static_cast<i64>(ActiveStates_.insert(std::move(state)).second);
        Add(ActiveStateCounters_, matchBeginIndex, diff);
        if (std::holds_alternative<TVoidTransition>(transition)) {
            Add(FinishedStateCounters_, matchBeginIndex, diff);
        }
    }

    void Erase(TState state) {
        auto matchBeginIndex = state.Match.BeginIndex;
        const auto& transition = TransitionGraph_->Transitions[state.Index];
        auto diff = -static_cast<i64>(ActiveStates_.erase(state));
        Add(ActiveStateCounters_, matchBeginIndex, diff);
        if (std::holds_alternative<TVoidTransition>(transition)) {
            Add(FinishedStateCounters_, matchBeginIndex, diff);
        }
    }

    TNfaTransitionGraph::TPtr TransitionGraph_;
    IComputationExternalNode* const MatchedRangesArg_;
    const TComputationNodePtrVector Defines_;
    TStateSet ActiveStates_; // NFA state
    THashMap<size_t, i64> ActiveStateCounters_;
    THashMap<size_t, i64> FinishedStateCounters_;
    bool EndOfData_ = false;
    TAfterMatchSkipTo SkipTo_;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
