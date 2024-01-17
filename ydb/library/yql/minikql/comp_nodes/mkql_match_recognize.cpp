#include "mkql_match_recognize_list.h"
#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_measure_arg.h"
#include "mkql_match_recognize_nfa.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <deque>

namespace NKikimr::NMiniKQL {

namespace NMatchRecognize {

enum class EOutputColumnSource {PartitionKey, Measure};
using TOutputColumnOrder = std::vector<std::pair<EOutputColumnSource, size_t>, TMKQLAllocator<std::pair<EOutputColumnSource, size_t>>>;

using namespace NYql::NMatchRecognize;

struct TMatchRecognizeProcessorParameters {
    IComputationExternalNode* InputDataArg;
    TRowPattern               Pattern;
    TUnboxedValueVector       VarNames;
    THashMap<TString, size_t> VarNamesLookup;
    IComputationExternalNode* MatchedVarsArg;
    IComputationExternalNode* CurrentRowIndexArg;
    TComputationNodePtrVector Defines;
    IComputationExternalNode* MeasureInputDataArg;
    TMeasureInputColumnOrder  MeasureInputColumnOrder;
    TComputationNodePtrVector Measures;
    TOutputColumnOrder        OutputColumnOrder;
};

class TBackTrackingMatchRecognize {
    using TPartitionList = TSimpleList;
    using TRange = TPartitionList::TRange;
    using TMatchedVars = TMatchedVars<TRange>;
public:
    //TODO(YQL-16486): create a tree for backtracking(replace var names with indexes)

    struct TPatternConfiguration {};

    struct TPatternConfigurationBuilder {
        using TPatternConfigurationPtr = std::shared_ptr<TPatternConfiguration>;
        static TPatternConfigurationPtr Create(const TRowPattern& pattern, const THashMap<TString, size_t>& varNameToIndex) {
            Y_UNUSED(pattern);
            Y_UNUSED(varNameToIndex);
            return std::make_shared<TPatternConfiguration>();
        }
    };

    TBackTrackingMatchRecognize(
        NUdf::TUnboxedValue&& partitionKey,
        const TMatchRecognizeProcessorParameters& parameters,
        const TPatternConfigurationBuilder::TPatternConfigurationPtr pattern,
        const TContainerCacheOnContext& cache
    )
    : PartitionKey(std::move(partitionKey))
    , Parameters(parameters)
    , Cache(cache)
    , CurMatchedVars(parameters.Defines.size())
    , MatchNumber(0)
    {
        //TODO(YQL-16486)
        Y_UNUSED(pattern);
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        Y_UNUSED(ctx);
        Rows.Append(std::move(row));
        return false;
    }
    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        if (Matches.empty())
            return NUdf::TUnboxedValue{};
        Parameters.MatchedVarsArg->SetValue(ctx, ToValue(ctx.HolderFactory, std::move(Matches.front())));
        Matches.pop_front();
        Parameters.MeasureInputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TMeasureInputDataValue>(
                Parameters.InputDataArg->GetValue(ctx),
                Parameters.MeasureInputColumnOrder,
                Parameters.MatchedVarsArg->GetValue(ctx),
                Parameters.VarNames,
                ++MatchNumber
        ));
        NUdf::TUnboxedValue *itemsPtr = nullptr;
        const auto result = Cache.NewArray(ctx, Parameters.OutputColumnOrder.size(), itemsPtr);
        for (auto const& c: Parameters.OutputColumnOrder) {
            switch(c.first) {
                case EOutputColumnSource::Measure:
                    *itemsPtr++ = Parameters.Measures[c.second]->GetValue(ctx);
                    break;
                case EOutputColumnSource::PartitionKey:
                    *itemsPtr++ = PartitionKey.GetElement(c.second);
                    break;
            }
        }
        return result;
    }
    bool ProcessEndOfData(TComputationContext& ctx) {
        //Assume, that data moved to IComputationExternalNode node, will not be modified or released
        //till the end of the current function
        auto rowsSize = Rows.Size();
        Parameters.InputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TListValue<TPartitionList>>(Rows));
        for (size_t i = 0; i != rowsSize; ++i) {
            Parameters.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(static_cast<ui64>(i)));
            for (size_t v = 0; v != Parameters.Defines.size(); ++v) {
                const auto &d = Parameters.Defines[v]->GetValue(ctx);
                if (d && d.GetOptionalValue().Get<bool>()) {
                    Extend(CurMatchedVars[v], TRange{i});
                }
            }
            //for the sake of dummy usage assume non-overlapped matches at every 5th row of any partition
            if (i % 5 == 0) {
                TMatchedVars temp;
                temp.swap(CurMatchedVars);
                Matches.emplace_back(std::move(temp));
                CurMatchedVars.resize(Parameters.Defines.size());
            }
        }
        return not Matches.empty();
    }
private:
    const NUdf::TUnboxedValue PartitionKey;
    const TMatchRecognizeProcessorParameters& Parameters;
    const TContainerCacheOnContext& Cache;
    TSimpleList Rows;
    TMatchedVars CurMatchedVars;
    std::deque<TMatchedVars, TMKQLAllocator<TMatchedVars>> Matches;
    ui64 MatchNumber;
};

class TStreamingMatchRecognize {
    using TPartitionList = TSparseList;
    using TRange = TPartitionList::TRange;
    using TMatchedVars = TMatchedVars<TRange>;
public:
    using TPatternConfigurationBuilder = TNfaTransitionGraphBuilder;
    TStreamingMatchRecognize(
        NUdf::TUnboxedValue&& partitionKey,
        const TMatchRecognizeProcessorParameters& parameters,
        TNfaTransitionGraph::TPtr nfaTransitions,
        const TContainerCacheOnContext& cache
    )
    : PartitionKey(std::move(partitionKey))
    , Parameters(parameters)
    , Nfa(nfaTransitions, parameters.MatchedVarsArg, parameters.Defines)
    , Cache(cache)
    {
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        Parameters.InputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TListValue<TSparseList>>(Rows));
        Parameters.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(Rows.Size()));
        Nfa.ProcessRow(Rows.Append(std::move(row)), ctx);
        return Nfa.HasMatched();
    }
    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        auto match = Nfa.GetMatched();
        if (!match.has_value())
            return NUdf::TUnboxedValue{};
        Parameters.MatchedVarsArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TRange>>(ctx.HolderFactory, match.value()));
        Parameters.MeasureInputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TMeasureInputDataValue>(
            ctx.HolderFactory.Create<TListValue<TSparseList>>(Rows),
            Parameters.MeasureInputColumnOrder,
            Parameters.MatchedVarsArg->GetValue(ctx),
            Parameters.VarNames,
            MatchNumber
            ));
        NUdf::TUnboxedValue *itemsPtr = nullptr;
        const auto result = Cache.NewArray(ctx, Parameters.OutputColumnOrder.size(), itemsPtr);
        for (auto const& c: Parameters.OutputColumnOrder) {
            switch(c.first) {
                case EOutputColumnSource::Measure:
                    *itemsPtr++ = Parameters.Measures[c.second]->GetValue(ctx);
                    break;
                case EOutputColumnSource::PartitionKey:
                    *itemsPtr++ = PartitionKey.GetElement(c.second);
                    break;
            }
        }
        return result;
    }
    bool ProcessEndOfData(TComputationContext& ctx) {
        Y_UNUSED(ctx);
        return false;
    }
private:
    const NUdf::TUnboxedValue PartitionKey;
    const TMatchRecognizeProcessorParameters& Parameters;
    TSparseList Rows;
    TNfa Nfa;
    const TContainerCacheOnContext& Cache;
    ui64 MatchNumber = 0;
};

template <typename Algo>
class TStateForNonInterleavedPartitions
    : public TComputationValue<TStateForNonInterleavedPartitions<Algo>>
{
    using TRowPatternConfigurationBuilder = typename Algo::TPatternConfigurationBuilder;
public:
    TStateForNonInterleavedPartitions(
            TMemoryUsageInfo* memInfo,
            IComputationExternalNode* inputRowArg,
            IComputationNode* partitionKey,
            TType* partitionKeyType,
            const TMatchRecognizeProcessorParameters& parameters,
            const TContainerCacheOnContext& cache
    )
    : TComputationValue<TStateForNonInterleavedPartitions>(memInfo)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyPacker(true, partitionKeyType)
    , Parameters(parameters)
    , RowPatternConfiguration(TRowPatternConfigurationBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
    , Cache(cache)
    , Terminating(false)
    {}
    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        MKQL_ENSURE(not DelayedRow, "Internal logic error"); //we're finalizing previous partition
        InputRowArg->SetValue(ctx, NUdf::TUnboxedValue(row));
        auto partitionKey = PartitionKey->GetValue(ctx);
        const auto packedKey = PartitionKeyPacker.Pack(partitionKey);
        //TODO switch to tuple compare for comparable types
        if (packedKey == CurPartitionPackedKey) { //continue in the same partition
            MKQL_ENSURE(PartitionHandler, "Internal logic error");
            return PartitionHandler->ProcessInputRow(std::move(row), ctx);
        }
        //either the first or next partition
        DelayedRow = std::move(row);
        if (PartitionHandler) {
            return PartitionHandler->ProcessEndOfData(ctx);
        }
        //be aware that the very first partition is created in the same manner as subsequent
        return false;
    }
    bool ProcessEndOfData(TComputationContext& ctx) {
        if (Terminating)
            return false;
        Terminating = true;
        if (PartitionHandler) {
            return PartitionHandler->ProcessEndOfData(ctx);
        }
        return false;
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        if (PartitionHandler) {
            auto result = PartitionHandler->GetOutputIfReady(ctx);
            if (result) {
                return result;
            }
        }
        if (DelayedRow) {
            //either the first partition or
            //we're finalizing a partition and expect no more output from this partition
            NUdf::TUnboxedValue temp;
            std::swap(temp, DelayedRow);
            InputRowArg->SetValue(ctx, NUdf::TUnboxedValue(temp));
            auto partitionKey = PartitionKey->GetValue(ctx);
            CurPartitionPackedKey = PartitionKeyPacker.Pack(partitionKey);
            PartitionHandler.reset(new Algo(
                    std::move(partitionKey),
                    Parameters,
                    RowPatternConfiguration,
                    Cache
            ));
            PartitionHandler->ProcessInputRow(std::move(temp), ctx);
        }
        if (Terminating) {
            return NUdf::TUnboxedValue::MakeFinish();
        }
        return NUdf::TUnboxedValue{};
    }
private:
    TString CurPartitionPackedKey;
    std::unique_ptr<Algo> PartitionHandler;
    IComputationExternalNode* InputRowArg;
    IComputationNode* PartitionKey;
    TValuePackerGeneric<false> PartitionKeyPacker;
    const TMatchRecognizeProcessorParameters& Parameters;
    const typename TRowPatternConfigurationBuilder::TPatternConfigurationPtr RowPatternConfiguration;
    const TContainerCacheOnContext& Cache;
    NUdf::TUnboxedValue DelayedRow;
    bool Terminating;
};

class TStateForInterleavedPartitions
    : public TComputationValue<TStateForInterleavedPartitions>
{
    using TPartitionMapValue = std::unique_ptr<TStreamingMatchRecognize>;
    using TPartitionMap = std::unordered_map<TString, TPartitionMapValue, std::hash<TString>, std:: equal_to<TString>, TMKQLAllocator<std::pair<const TString, TPartitionMapValue>>>;
public:
    TStateForInterleavedPartitions(
        TMemoryUsageInfo* memInfo,
        IComputationExternalNode* inputRowArg,
        IComputationNode* partitionKey,
        TType* partitionKeyType,
        const TMatchRecognizeProcessorParameters& parameters,
        const TContainerCacheOnContext& cache
    )
    : TComputationValue<TStateForInterleavedPartitions>(memInfo)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyPacker(true, partitionKeyType)
    , Parameters(parameters)
    , NfaTransitionGraph(TNfaTransitionGraphBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
    , Cache(cache)
{
}
   bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        auto partition = GetPartitionHandler(row, ctx);
        if (partition->second->ProcessInputRow(std::move(row), ctx)) {
            HasReadyOutput.push(partition);
        }
        return !HasReadyOutput.empty();
    }

    bool ProcessEndOfData(TComputationContext& ctx) {
        for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
            auto b = it->second->ProcessEndOfData(ctx);
            if (b) {
                HasReadyOutput.push(it);
            }
        }
        Terminating = true;
        return !HasReadyOutput.empty();
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        while (!HasReadyOutput.empty()) {
            auto r = HasReadyOutput.top()->second->GetOutputIfReady(ctx);
            if (not r) {
                //dried up
                HasReadyOutput.pop();
                continue;
            } else {
                return r;
            }
        }
        return Terminating ? NUdf::TUnboxedValue(NUdf::TUnboxedValue::MakeFinish()) : NUdf::TUnboxedValue{};
    }

private:
    TPartitionMap::iterator GetPartitionHandler(const NUdf::TUnboxedValue& row, TComputationContext &ctx) {
        InputRowArg->SetValue(ctx, NUdf::TUnboxedValue(row));
        auto partitionKey = PartitionKey->GetValue(ctx);
        const auto packedKey = PartitionKeyPacker.Pack(partitionKey);
        if (const auto it = Partitions.find(TString(packedKey)); it != Partitions.end()) {
            return it;
        } else {
            return Partitions.emplace_hint(it, TString(packedKey), std::make_unique<TStreamingMatchRecognize>(
                    std::move(partitionKey),
                    Parameters,
                    NfaTransitionGraph,
                    Cache
            ));
        }
    }

private:
    TPartitionMap Partitions;
    std::stack<TPartitionMap::iterator, std::deque<TPartitionMap::iterator, TMKQLAllocator<TPartitionMap::iterator>>> HasReadyOutput;
    bool Terminating = false;

    IComputationExternalNode* InputRowArg;
    IComputationNode* PartitionKey;
    //TODO switch to tuple compare
    TValuePackerGeneric<false> PartitionKeyPacker;
    const TMatchRecognizeProcessorParameters& Parameters;
    const TNfaTransitionGraph::TPtr NfaTransitionGraph;
    const TContainerCacheOnContext& Cache;
};

template<class State>
class TMatchRecognizeWrapper : public TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>> {
    using TBaseComputation = TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>>;
public:
    TMatchRecognizeWrapper(TComputationMutables &mutables, EValueRepresentation kind, IComputationNode *inputFlow,
       IComputationExternalNode *inputRowArg,
       IComputationNode *partitionKey,
       TType* partitionKeyType,
       const TMatchRecognizeProcessorParameters& parameters
    )
    :TBaseComputation(mutables, inputFlow, kind, EValueRepresentation::Embedded)
    , InputFlow(inputFlow)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyType(partitionKeyType)
    , Parameters(parameters)
    , Cache(mutables)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue &stateValue, TComputationContext &ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<State>(
                    InputRowArg,
                    PartitionKey,
                    PartitionKeyType,
                    Parameters,
                    Cache
            );
        }
        auto state = static_cast<State*>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto output = state->GetOutputIfReady(ctx); output) {
                return output;
            }
            auto item = InputFlow->GetValue(ctx);
            if (item.IsFinish()) {
                state->ProcessEndOfData(ctx);
                continue;
            } else if (item.IsSpecial()) {
                return item;
            }
            state->ProcessInputRow(std::move(item), ctx);
        }
    }
private:
    using TBaseComputation::Own;
    using TBaseComputation::DependsOn;
    void RegisterDependencies() const final {
        if (const auto flow = TBaseComputation::FlowDependsOn(InputFlow)) {
            Own(flow, InputRowArg);
            Own(flow, Parameters.InputDataArg);
            Own(flow, Parameters.MatchedVarsArg);
            Own(flow, Parameters.CurrentRowIndexArg);
            Own(flow, Parameters.MeasureInputDataArg);
            DependsOn(flow, PartitionKey);
            for (auto& m: Parameters.Measures) {
                DependsOn(flow, m);
            }
            for (auto& d: Parameters.Defines) {
                DependsOn(flow, d);
            }
        }
    }

    IComputationNode* const InputFlow;
    IComputationExternalNode* const InputRowArg;
    IComputationNode* const PartitionKey;
    TType* const PartitionKeyType;
    const TMatchRecognizeProcessorParameters Parameters;
    TNfaTransitionGraph::TPtr NfaTransitionGraph;
    const TContainerCacheOnContext Cache;
};

TOutputColumnOrder GetOutputColumnOrder(TRuntimeNode partitionKyeColumnsIndexes, TRuntimeNode measureColumnsIndexes) {
    using tempMapValue = std::pair<EOutputColumnSource, size_t>;
    std::unordered_map<size_t, tempMapValue, std::hash<size_t>, std::equal_to<size_t>, TMKQLAllocator<std::pair<const size_t, tempMapValue>, EMemorySubPool::Temporary>> temp;
    {
        auto list = AS_VALUE(TListLiteral, partitionKyeColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = std::make_pair(EOutputColumnSource::PartitionKey, i);
        }
    }
    {
        auto list = AS_VALUE(TListLiteral, measureColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = std::make_pair(EOutputColumnSource::Measure, i);
        }
    }
    if (temp.empty())
        return {};
    auto outputSize = max_element(temp.cbegin(), temp.cend())->first + 1;
    TOutputColumnOrder result(outputSize);
    for (const auto& [i, v]: temp) {
        result[i] = v;
    }
    return result;
}

TRowPattern ConvertPattern(const TRuntimeNode& pattern) {
    TVector<TRowPatternTerm> result;
    const auto& inputPattern = AS_VALUE(TTupleLiteral, pattern);
    for (ui32 i = 0; i != inputPattern->GetValuesCount(); ++i) {
        const auto& inputTerm = AS_VALUE(TTupleLiteral, inputPattern->GetValue(i));
        TVector<TRowPatternFactor> term;
        for (ui32 j = 0; j != inputTerm->GetValuesCount(); ++j) {
            const auto& inputFactor = AS_VALUE(TTupleLiteral,  inputTerm->GetValue(j));
            MKQL_ENSURE(inputFactor->GetValuesCount() == 6, "Internal logic error");
            const auto& primary = inputFactor->GetValue(0);
            term.push_back(TRowPatternFactor{
                    primary.GetRuntimeType()->IsData() ?
                    TRowPatternPrimary(TString(AS_VALUE(TDataLiteral, primary)->AsValue().AsStringRef())) :
                        ConvertPattern(primary),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(1))->AsValue().Get<ui64>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(2))->AsValue().Get<ui64>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(3))->AsValue().Get<bool>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(4))->AsValue().Get<bool>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(5))->AsValue().Get<bool>()
            });
        }
        result.push_back(std::move(term));
    }
    return result;
}

TMeasureInputColumnOrder GetMeasureColumnOrder(const TListLiteral& specialColumnIndexes, ui32 inputRowColumnCount) {
    using NYql::NMatchRecognize::EMeasureInputDataSpecialColumns;
    //Use Last enum value to denote that c colum comes from the input table
    TMeasureInputColumnOrder result(inputRowColumnCount + specialColumnIndexes.GetItemsCount(), std::make_pair(EMeasureInputDataSpecialColumns::Last, 0));
    if (specialColumnIndexes.GetItemsCount() != 0) {
        MKQL_ENSURE(specialColumnIndexes.GetItemsCount() == static_cast<size_t>(EMeasureInputDataSpecialColumns::Last),
                    "Internal logic error");
        for (size_t i = 0; i != specialColumnIndexes.GetItemsCount(); ++i) {
            auto ind = AS_VALUE(TDataLiteral, specialColumnIndexes.GetItems()[i])->AsValue().Get<ui32>();
            result[ind] = std::make_pair(static_cast<EMeasureInputDataSpecialColumns>(i), 0);
        }
    }
    //update indexes for input table columns
    ui32 inputIdx = 0;
    for (auto& [t, i]: result) {
        if (EMeasureInputDataSpecialColumns::Last == t) {
            i = inputIdx++;
        }
    }
    return result;
}

TComputationNodePtrVector ConvertVectorOfCallables(const TRuntimeNode::TList& v, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector result;
    result.reserve(v.size());
    for (auto& c: v) {
        result.push_back(LocateNode(ctx.NodeLocator, *c.GetNode()));
    }
    return result;
}

std::pair<TUnboxedValueVector, THashMap<TString, size_t>> ConvertListOfStrings(const TRuntimeNode& l) {
    TUnboxedValueVector vec;
    THashMap<TString, size_t> lookup;
    const auto& list = AS_VALUE(TListLiteral, l);
    vec.reserve(list->GetItemsCount());
    for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
        const auto& varName = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().AsStringRef();
        vec.push_back(MakeString(varName));
        lookup[TString(varName)] = i;
    }
    return {vec, lookup};
}

} //namespace NMatchRecognize



IComputationNode* WrapMatchRecognizeCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    using namespace NMatchRecognize;
    size_t inputIndex = 0;
    const auto& inputFlow = callable.GetInput(inputIndex++);
    const auto& inputRowArg = callable.GetInput(inputIndex++);
    const auto& partitionKeySelector = callable.GetInput(inputIndex++);
    const auto& partitionColumnIndexes = callable.GetInput(inputIndex++);
    const auto& measureInputDataArg = callable.GetInput(inputIndex++);
    const auto& measureSpecialColumnIndexes = callable.GetInput(inputIndex++);
    const auto& inputRowColumnCount = callable.GetInput(inputIndex++);
    const auto& matchedVarsArg = callable.GetInput(inputIndex++);
    const auto& measureColumnIndexes = callable.GetInput(inputIndex++);
    TRuntimeNode::TList measures;
    for (size_t i = 0; i != AS_VALUE(TListLiteral, measureColumnIndexes)->GetItemsCount(); ++i) {
        measures.push_back(callable.GetInput(inputIndex++));
    }
    const auto& pattern = callable.GetInput(inputIndex++);
    const auto& currentRowIndexArg = callable.GetInput(inputIndex++);
    const auto& inputDataArg = callable.GetInput(inputIndex++);
    const auto& varNames = callable.GetInput(inputIndex++);
    TRuntimeNode::TList defines;
    for (size_t i = 0; i != AS_VALUE(TListLiteral, varNames)->GetItemsCount(); ++i) {
        defines.push_back(callable.GetInput(inputIndex++));
    }
    const auto& streamingMode = callable.GetInput(inputIndex++);
    MKQL_ENSURE(callable.GetInputsCount() == inputIndex, "Wrong input count");

    const auto& [vars, varsLookup] = ConvertListOfStrings(varNames);

    const auto parameters = TMatchRecognizeProcessorParameters {
        static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputDataArg.GetNode()))
        , ConvertPattern(pattern)
        , vars
        , varsLookup
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *matchedVarsArg.GetNode()))
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *currentRowIndexArg.GetNode()))
        , ConvertVectorOfCallables(defines, ctx)
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *measureInputDataArg.GetNode()))
        , GetMeasureColumnOrder(
                *AS_VALUE(TListLiteral, measureSpecialColumnIndexes),
                AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>()
        )
        , ConvertVectorOfCallables(measures, ctx)
        , GetOutputColumnOrder(partitionColumnIndexes, measureColumnIndexes)
    };
    if (AS_VALUE(TDataLiteral, streamingMode)->AsValue().Get<bool>()) {
        return new TMatchRecognizeWrapper<TStateForInterleavedPartitions>(ctx.Mutables
            , GetValueRepresentation(inputFlow.GetStaticType())
            , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
            , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
            , LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode())
            , partitionKeySelector.GetStaticType()
            , std::move(parameters)
        );
    } else {
        const bool useNfaForTables = true; //TODO(YQL-16486) get this flag from an optimizer
        if (useNfaForTables) {
            return new TMatchRecognizeWrapper<TStateForNonInterleavedPartitions<TStreamingMatchRecognize>>(ctx.Mutables
                , GetValueRepresentation(inputFlow.GetStaticType())
                , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
                , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
                , LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode())
                , partitionKeySelector.GetStaticType()
                , std::move(parameters)
            );
        } else {
            return new TMatchRecognizeWrapper<TStateForNonInterleavedPartitions<TBackTrackingMatchRecognize>>(ctx.Mutables
                , GetValueRepresentation(inputFlow.GetStaticType())
                , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
                , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
                , LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode())
                , partitionKeySelector.GetStaticType()
                , std::move(parameters)
            );
        }
    }
}

} //namespace NKikimr::NMiniKQL
