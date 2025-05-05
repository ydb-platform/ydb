#include "mkql_match_recognize_list.h"
#include "mkql_match_recognize_measure_arg.h"
#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_nfa.h"
#include "mkql_match_recognize_rows_formatter.h"
#include "mkql_match_recognize_save_load.h"
#include "mkql_match_recognize_version.h"

#include <yql/essentials/core/sql_types/match_recognize.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <deque>

namespace NKikimr::NMiniKQL {

namespace NMatchRecognize {

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
    TAfterMatchSkipTo         SkipTo;
};

class TStreamingMatchRecognize {
public:
    TStreamingMatchRecognize(
        NUdf::TUnboxedValue&& partitionKey,
        const TMatchRecognizeProcessorParameters& parameters,
        const IRowsFormatter::TState& rowsFormatterState,
        TNfaTransitionGraph::TPtr nfaTransitions)
    : PartitionKey(std::move(partitionKey))
    , Parameters(parameters)
    , RowsFormatter_(IRowsFormatter::Create(rowsFormatterState))
    , Nfa(nfaTransitions, parameters.MatchedVarsArg, parameters.Defines, parameters.SkipTo)
    {}

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        Parameters.InputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TListValue>(Rows));
        Parameters.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(Rows.LastRowIndex()));
        Nfa.ProcessRow(Rows.Append(std::move(row)), ctx);
        return HasMatched();
    }

    bool HasMatched() const {
        return Nfa.HasMatched();
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        if (auto result = RowsFormatter_->GetOtherMatchRow(ctx, Rows, PartitionKey, Nfa.GetTransitionGraph())) {
            return result;
        }
        auto match = Nfa.GetMatched();
        if (!match) {
            return NUdf::TUnboxedValue{};
        }
        Parameters.MatchedVarsArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TSparseList::TRange>>(ctx.HolderFactory, match->Vars));
        Parameters.MeasureInputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TMeasureInputDataValue>(
            ctx.HolderFactory.Create<TListValue>(Rows),
            Parameters.MeasureInputColumnOrder,
            Parameters.MatchedVarsArg->GetValue(ctx),
            Parameters.VarNames,
            MatchNumber
        ));
        auto result = RowsFormatter_->GetFirstMatchRow(ctx, Rows, PartitionKey, Nfa.GetTransitionGraph(), *match);
        Nfa.AfterMatchSkip(*match);
        return result;
    }

    bool ProcessEndOfData(TComputationContext& ctx) {
        return Nfa.ProcessEndOfData(ctx);
    }

    void Save(TMrOutputSerializer& serializer) const {
        // PartitionKey saved in TStateForInterleavedPartitions as key.
        Rows.Save(serializer);
        Nfa.Save(serializer);
        serializer.Write(MatchNumber);
        RowsFormatter_->Save(serializer);
    }

    void Load(TMrInputSerializer& serializer) {
        // PartitionKey passed in contructor.
        Rows.Load(serializer);
        Nfa.Load(serializer);
        MatchNumber = serializer.Read<ui64>();
        if (serializer.GetStateVersion() >= 2U) {
            RowsFormatter_->Load(serializer);
        }
    }

private:
    NUdf::TUnboxedValue PartitionKey;
    const TMatchRecognizeProcessorParameters& Parameters;
    std::unique_ptr<IRowsFormatter> RowsFormatter_;
    TSparseList Rows;
    TNfa Nfa;
    ui64 MatchNumber = 0;
};

class TStateForNonInterleavedPartitions
    : public TComputationValue<TStateForNonInterleavedPartitions>
{
public:
    TStateForNonInterleavedPartitions(
        TMemoryUsageInfo* memInfo,
        IComputationExternalNode* inputRowArg,
        IComputationNode* partitionKey,
        TType* partitionKeyType,
        const TMatchRecognizeProcessorParameters& parameters,
        const IRowsFormatter::TState& rowsFormatterState,
        TComputationContext &ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker
    )
    : TComputationValue<TStateForNonInterleavedPartitions>(memInfo)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyPacker(true, partitionKeyType)
    , Parameters(parameters)
    , RowsFormatterState(rowsFormatterState)
    , RowPatternConfiguration(TNfaTransitionGraphBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
    , Terminating(false)
    , SerializerContext(ctx, rowType, rowPacker)
    , Ctx(ctx)
    {}

    NUdf::TUnboxedValue Save() const override {
        TMrOutputSerializer out(SerializerContext, EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);
        out.Write(CurPartitionPackedKey);
        bool isValid = static_cast<bool>(PartitionHandler);
        out.Write(isValid);
        if (isValid) {
            PartitionHandler->Save(out);
        }
        isValid = static_cast<bool>(DelayedRow);
        out.Write(isValid);
        if (isValid) {
            out.Write(DelayedRow);
        }
        return out.MakeState();
    }

    bool Load2(const NUdf::TUnboxedValue& state) override {
        TMrInputSerializer in(SerializerContext, state);

        in.Read(CurPartitionPackedKey);
        bool validPartitionHandler = in.Read<bool>();
        if (validPartitionHandler) {
            NUdf::TUnboxedValue key = PartitionKeyPacker.Unpack(CurPartitionPackedKey, SerializerContext.Ctx.HolderFactory);
            PartitionHandler.reset(new TStreamingMatchRecognize(
                std::move(key),
                Parameters,
                RowsFormatterState,
                RowPatternConfiguration
            ));
            PartitionHandler->Load(in);
        }
        bool validDelayedRow = in.Read<bool>();
        if (validDelayedRow) {
            in(DelayedRow);
        }
        if (in.GetStateVersion() < 2U) {
            auto restoredRowPatternConfiguration = std::make_shared<TNfaTransitionGraph>();
            restoredRowPatternConfiguration->Load(in);
            MKQL_ENSURE(*restoredRowPatternConfiguration == *RowPatternConfiguration, "Restored and current RowPatternConfiguration is different");
        }
        MKQL_ENSURE(in.Empty(), "State is corrupted");
        return true;
    }

    bool HasListItems() const override {
        return false;
    }

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
            PartitionHandler.reset(new TStreamingMatchRecognize(
                    std::move(partitionKey),
                    Parameters,
                    RowsFormatterState,
                    RowPatternConfiguration
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
    std::unique_ptr<TStreamingMatchRecognize> PartitionHandler;
    IComputationExternalNode* InputRowArg;
    IComputationNode* PartitionKey;
    TValuePackerGeneric<false> PartitionKeyPacker;
    const TMatchRecognizeProcessorParameters& Parameters;
    const IRowsFormatter::TState& RowsFormatterState;
    const TNfaTransitionGraph::TPtr RowPatternConfiguration;
    NUdf::TUnboxedValue DelayedRow;
    bool Terminating;
    TSerializerContext SerializerContext;
    TComputationContext& Ctx;
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
        const IRowsFormatter::TState& rowsFormatterState,
        TComputationContext &ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker
    )
    : TComputationValue<TStateForInterleavedPartitions>(memInfo)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyPacker(true, partitionKeyType)
    , Parameters(parameters)
    , RowsFormatterState(rowsFormatterState)
    , NfaTransitionGraph(TNfaTransitionGraphBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
    , SerializerContext(ctx, rowType, rowPacker)
    , Ctx(ctx)
    {}

    NUdf::TUnboxedValue Save() const override {
        TMrOutputSerializer serializer(SerializerContext, EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);
        serializer.Write(Partitions.size());

        for (const auto& [key, state] : Partitions) {
            serializer.Write(key);
            state->Save(serializer);
        }
        // HasReadyOutput is not packed because when loading we can recalculate HasReadyOutput from Partitions.
        serializer.Write(Terminating);
        return serializer.MakeState();
    }

    bool Load2(const NUdf::TUnboxedValue& state) override {
        TMrInputSerializer in(SerializerContext, state);

        Partitions.clear();
        auto partitionsCount = in.Read<TPartitionMap::size_type>();
        Partitions.reserve(partitionsCount);
        for (size_t i = 0; i < partitionsCount; ++i) {
            auto packedKey = in.Read<TPartitionMap::key_type, std::string_view>();
            NUdf::TUnboxedValue key = PartitionKeyPacker.Unpack(packedKey, SerializerContext.Ctx.HolderFactory);
            auto pair = Partitions.emplace(
                packedKey,
                std::make_unique<TStreamingMatchRecognize>(
                    std::move(key),
                    Parameters,
                    RowsFormatterState,
                    NfaTransitionGraph
                )
            );
            pair.first->second->Load(in);
        }

        for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
            if (it->second->HasMatched()) {
                HasReadyOutput.push(it);
            }
        }
        in.Read(Terminating);
        if (in.GetStateVersion() < 2U) {
            auto restoredTransitionGraph = std::make_shared<TNfaTransitionGraph>();
            restoredTransitionGraph->Load(in);
            MKQL_ENSURE(NfaTransitionGraph, "Empty NfaTransitionGraph");
            MKQL_ENSURE(*restoredTransitionGraph == *NfaTransitionGraph, "Restored and current NfaTransitionGraph is different");
        }
        MKQL_ENSURE(in.Empty(), "State is corrupted");
        return true;
    }

    bool HasListItems() const override {
        return false;
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
                    RowsFormatterState,
                    NfaTransitionGraph
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
    const IRowsFormatter::TState& RowsFormatterState;
    const TNfaTransitionGraph::TPtr NfaTransitionGraph;
    TSerializerContext SerializerContext;
    TComputationContext& Ctx;
};

template<class State>
class TMatchRecognizeWrapper : public TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>, true> {
    using TBaseComputation = TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>, true>;
public:
    TMatchRecognizeWrapper(
        TComputationMutables& mutables,
        EValueRepresentation kind,
        IComputationNode *inputFlow,
        IComputationExternalNode *inputRowArg,
        IComputationNode *partitionKey,
        TType* partitionKeyType,
        TMatchRecognizeProcessorParameters&& parameters,
        IRowsFormatter::TState&& rowsFormatterState,
        TType* rowType)
    : TBaseComputation(mutables, inputFlow, kind, EValueRepresentation::Embedded)
    , InputFlow(inputFlow)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyType(partitionKeyType)
    , Parameters(std::move(parameters))
    , RowsFormatterState(std::move(rowsFormatterState))
    , RowType(rowType)
    , RowPacker(mutables)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue &stateValue, TComputationContext &ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<State>(
                InputRowArg,
                PartitionKey,
                PartitionKeyType,
                Parameters,
                RowsFormatterState,
                ctx,
                RowType,
                RowPacker
            );
        } else if (stateValue.HasValue()) {
            MKQL_ENSURE(stateValue.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = stateValue.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue state = ctx.HolderFactory.Create<State>(
                    InputRowArg,
                    PartitionKey,
                    PartitionKeyType,
                    Parameters,
                    RowsFormatterState,
                    ctx,
                    RowType,
                    RowPacker
                );
                state.Load2(stateValue);
                stateValue = state;
            }
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
            for (auto& m: RowsFormatterState.Measures) {
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
    TMatchRecognizeProcessorParameters Parameters;
    IRowsFormatter::TState RowsFormatterState;
    TType* const RowType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> RowPacker;
};

TOutputColumnOrder GetOutputColumnOrder(TRuntimeNode partitionKyeColumnsIndexes, TRuntimeNode measureColumnsIndexes) {
    std::unordered_map<size_t, TOutputColumnEntry, std::hash<size_t>, std::equal_to<size_t>, TMKQLAllocator<std::pair<const size_t, TOutputColumnEntry>, EMemorySubPool::Temporary>> temp;
    {
        auto list = AS_VALUE(TListLiteral, partitionKyeColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = {i, EOutputColumnSource::PartitionKey};
        }
    }
    {
        auto list = AS_VALUE(TListLiteral, measureColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = {i, EOutputColumnSource::Measure};
        }
    }
    if (temp.empty())
        return {};
    auto outputSize = std::ranges::max_element(temp, {}, &std::pair<const size_t, TOutputColumnEntry>::first)->first + 1;
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
    const auto& defineNames = callable.GetInput(inputIndex++);
    TRuntimeNode::TList defines;
    for (size_t i = 0; i != AS_VALUE(TListLiteral, defineNames)->GetItemsCount(); ++i) {
        defines.push_back(callable.GetInput(inputIndex++));
    }
    const auto& streamingMode = callable.GetInput(inputIndex++);
    NYql::NMatchRecognize::TAfterMatchSkipTo skipTo = {NYql::NMatchRecognize::EAfterMatchSkipTo::NextRow, ""};
    if (inputIndex + 2 <= callable.GetInputsCount()) {
        skipTo.To = static_cast<EAfterMatchSkipTo>(AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().Get<i32>());
        skipTo.Var = AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().AsStringRef();
    }
    NYql::NMatchRecognize::ERowsPerMatch rowsPerMatch = NYql::NMatchRecognize::ERowsPerMatch::OneRow;
    TOutputColumnOrder outputColumnOrder;
    if (inputIndex + 2 <= callable.GetInputsCount()) {
        rowsPerMatch = static_cast<ERowsPerMatch>(AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().Get<i32>());
        outputColumnOrder = IRowsFormatter::GetOutputColumnOrder(callable.GetInput(inputIndex++));
    } else {
        outputColumnOrder = GetOutputColumnOrder(partitionColumnIndexes, measureColumnIndexes);
    }
    MKQL_ENSURE(callable.GetInputsCount() == inputIndex, "Wrong input count");

    const auto& [varNames, varNamesLookup] = ConvertListOfStrings(defineNames);
    auto* rowType = AS_TYPE(TStructType, AS_TYPE(TFlowType, inputFlow.GetStaticType())->GetItemType());

    auto parameters = TMatchRecognizeProcessorParameters {
        static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputDataArg.GetNode())),
        ConvertPattern(pattern),
        varNames,
        varNamesLookup,
        static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *matchedVarsArg.GetNode())),
        static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *currentRowIndexArg.GetNode())),
        ConvertVectorOfCallables(defines, ctx),
        static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *measureInputDataArg.GetNode())),
        GetMeasureColumnOrder(
                *AS_VALUE(TListLiteral, measureSpecialColumnIndexes),
                AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>()
        ),
        skipTo
    };
    IRowsFormatter::TState rowsFormatterState(ctx, outputColumnOrder, ConvertVectorOfCallables(measures, ctx), rowsPerMatch);
    if (AS_VALUE(TDataLiteral, streamingMode)->AsValue().Get<bool>()) {
        return new TMatchRecognizeWrapper<TStateForInterleavedPartitions>(
            ctx.Mutables,
            GetValueRepresentation(inputFlow.GetStaticType()),
            LocateNode(ctx.NodeLocator, *inputFlow.GetNode()),
            static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode())),
            LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode()),
            partitionKeySelector.GetStaticType(),
            std::move(parameters),
            std::move(rowsFormatterState),
            rowType
        );
    } else {
        return new TMatchRecognizeWrapper<TStateForNonInterleavedPartitions>(
            ctx.Mutables,
            GetValueRepresentation(inputFlow.GetStaticType()),
            LocateNode(ctx.NodeLocator, *inputFlow.GetNode()),
            static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode())),
            LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode()),
            partitionKeySelector.GetStaticType(),
            std::move(parameters),
            std::move(rowsFormatterState),
            rowType
        );
    }
}

} //namespace NKikimr::NMiniKQL
