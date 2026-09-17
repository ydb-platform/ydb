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
    TRowPattern Pattern;
    TUnboxedValueVector VarNames;
    THashMap<TString, size_t> VarNamesLookup;
    IComputationExternalNode* MatchedVarsArg;
    IComputationExternalNode* CurrentRowIndexArg;
    TComputationNodePtrVector Defines;
    IComputationExternalNode* MeasureInputDataArg;
    TMeasureInputColumnOrder MeasureInputColumnOrder;
    TAfterMatchSkipTo SkipTo;
};

class TStreamingMatchRecognize {
public:
    TStreamingMatchRecognize(
        NUdf::TUnboxedValue&& partitionKey,
        const TMatchRecognizeProcessorParameters& parameters,
        const IRowsFormatter::TState& rowsFormatterState,
        TNfaTransitionGraph::TPtr nfaTransitions)
        : PartitionKey_(std::move(partitionKey))
        , Parameters_(parameters)
        , RowsFormatter_(IRowsFormatter::Create(rowsFormatterState))
        , Nfa_(nfaTransitions, parameters.MatchedVarsArg, parameters.Defines, parameters.SkipTo)
    {
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        Parameters_.InputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TListValue>(Rows_));
        Parameters_.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(Rows_.LastRowIndex()));
        Nfa_.ProcessRow(Rows_.Append(std::move(row)), ctx);
        return HasMatched();
    }

    bool HasMatched() const {
        return Nfa_.HasMatched();
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        if (auto result = RowsFormatter_->GetOtherMatchRow(ctx, Rows_, PartitionKey_, Nfa_.GetTransitionGraph())) {
            return result;
        }
        auto match = Nfa_.GetMatched();
        if (!match) {
            return NUdf::TUnboxedValue{};
        }
        Parameters_.MatchedVarsArg->SetValue(ctx, ctx.HolderFactory.Create<TMatchedVarsValue<TSparseList::TRange>>(ctx.HolderFactory, match->Vars));
        Parameters_.MeasureInputDataArg->SetValue(ctx, ctx.HolderFactory.Create<TMeasureInputDataValue>(
                                                           ctx.HolderFactory.Create<TListValue>(Rows_),
                                                           Parameters_.MeasureInputColumnOrder,
                                                           Parameters_.MatchedVarsArg->GetValue(ctx),
                                                           Parameters_.VarNames,
                                                           MatchNumber_));
        auto result = RowsFormatter_->GetFirstMatchRow(ctx, Rows_, PartitionKey_, Nfa_.GetTransitionGraph(), *match);
        Nfa_.AfterMatchSkip(*match);
        return result;
    }

    bool ProcessEndOfData(TComputationContext& ctx) {
        return Nfa_.ProcessEndOfData(ctx);
    }

    void Save(TMrOutputSerializer& serializer) const {
        // PartitionKey saved in TStateForInterleavedPartitions as key.
        Rows_.Save(serializer);
        Nfa_.Save(serializer);
        serializer.Write(MatchNumber_);
        RowsFormatter_->Save(serializer);
    }

    void Load(TMrInputSerializer& serializer) {
        // PartitionKey passed in contructor.
        Rows_.Load(serializer);
        Nfa_.Load(serializer);
        MatchNumber_ = serializer.Read<ui64>();
        if (serializer.GetStateVersion() >= 2U) {
            RowsFormatter_->Load(serializer);
        }
    }

private:
    NUdf::TUnboxedValue PartitionKey_;
    const TMatchRecognizeProcessorParameters& Parameters_;
    std::unique_ptr<IRowsFormatter> RowsFormatter_;
    TSparseList Rows_;
    TNfa Nfa_;
    ui64 MatchNumber_ = 0;
};

class TStateForNonInterleavedPartitions
    : public TComputationValue<TStateForNonInterleavedPartitions> {
public:
    TStateForNonInterleavedPartitions(
        TMemoryUsageInfo* memInfo,
        IComputationExternalNode* inputRowArg,
        IComputationNode* partitionKey,
        TType* partitionKeyType,
        const TMatchRecognizeProcessorParameters& parameters,
        const IRowsFormatter::TState& rowsFormatterState,
        TComputationContext& ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker)
        : TComputationValue<TStateForNonInterleavedPartitions>(memInfo)
        , InputRowArg_(inputRowArg)
        , PartitionKey_(partitionKey)
        , PartitionKeyPacker_(/*stable=*/true, partitionKeyType)
        , Parameters_(parameters)
        , RowsFormatterState_(rowsFormatterState)
        , RowPatternConfiguration_(TNfaTransitionGraphBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
        , Terminating_(false)
        , SerializerContext_(ctx, rowType, rowPacker)
        , Ctx_(ctx)
    {
    }

    NUdf::TUnboxedValue Save() const override {
        TMrOutputSerializer out(SerializerContext_, EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx_);
        out.Write(CurPartitionPackedKey_);
        bool isValid = static_cast<bool>(PartitionHandler_);
        out.Write(isValid);
        if (isValid) {
            PartitionHandler_->Save(out);
        }
        isValid = static_cast<bool>(DelayedRow_);
        out.Write(isValid);
        if (isValid) {
            out.Write(DelayedRow_);
        }
        return out.MakeState();
    }

    bool Load2(const NUdf::TUnboxedValue& state) override {
        TMrInputSerializer in(SerializerContext_, state);

        in.Read(CurPartitionPackedKey_);
        bool validPartitionHandler = in.Read<bool>();
        if (validPartitionHandler) {
            NUdf::TUnboxedValue key = PartitionKeyPacker_.Unpack(CurPartitionPackedKey_, SerializerContext_.Ctx.HolderFactory);
            PartitionHandler_ = std::make_unique<TStreamingMatchRecognize>(
                std::move(key),
                Parameters_,
                RowsFormatterState_,
                RowPatternConfiguration_);
            PartitionHandler_->Load(in);
        }
        bool validDelayedRow = in.Read<bool>();
        if (validDelayedRow) {
            in(DelayedRow_);
        }
        if (in.GetStateVersion() < 2U) {
            auto restoredRowPatternConfiguration = std::make_shared<TNfaTransitionGraph>();
            restoredRowPatternConfiguration->Load(in);
            MKQL_ENSURE(*restoredRowPatternConfiguration == *RowPatternConfiguration_, "Restored and current RowPatternConfiguration is different");
        }
        MKQL_ENSURE(in.Empty(), "State is corrupted");
        return true;
    }

    bool HasListItems() const override {
        return false;
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
        MKQL_ENSURE(not DelayedRow_, "Internal logic error"); // we're finalizing previous partition
        InputRowArg_->SetValue(ctx, NUdf::TUnboxedValue(row));
        auto partitionKey = PartitionKey_->GetValue(ctx);
        const auto packedKey = PartitionKeyPacker_.Pack(partitionKey);
        // TODO switch to tuple compare for comparable types
        if (packedKey == CurPartitionPackedKey_) { // continue in the same partition
            MKQL_ENSURE(PartitionHandler_, "Internal logic error");
            return PartitionHandler_->ProcessInputRow(std::move(row), ctx);
        }
        // either the first or next partition
        DelayedRow_ = std::move(row);
        if (PartitionHandler_) {
            return PartitionHandler_->ProcessEndOfData(ctx);
        }
        // be aware that the very first partition is created in the same manner as subsequent
        return false;
    }
    bool ProcessEndOfData(TComputationContext& ctx) {
        if (Terminating_) {
            return false;
        }
        Terminating_ = true;
        if (PartitionHandler_) {
            return PartitionHandler_->ProcessEndOfData(ctx);
        }
        return false;
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        if (PartitionHandler_) {
            auto result = PartitionHandler_->GetOutputIfReady(ctx);
            if (result) {
                return result;
            }
        }
        if (DelayedRow_) {
            // either the first partition or
            // we're finalizing a partition and expect no more output from this partition
            NUdf::TUnboxedValue temp;
            std::swap(temp, DelayedRow_);
            InputRowArg_->SetValue(ctx, NUdf::TUnboxedValue(temp));
            auto partitionKey = PartitionKey_->GetValue(ctx);
            CurPartitionPackedKey_ = PartitionKeyPacker_.Pack(partitionKey);
            PartitionHandler_ = std::make_unique<TStreamingMatchRecognize>(
                std::move(partitionKey),
                Parameters_,
                RowsFormatterState_,
                RowPatternConfiguration_);
            PartitionHandler_->ProcessInputRow(std::move(temp), ctx);
        }
        if (Terminating_) {
            return NUdf::TUnboxedValue::MakeFinish();
        }
        return NUdf::TUnboxedValue{};
    }

private:
    TString CurPartitionPackedKey_;
    std::unique_ptr<TStreamingMatchRecognize> PartitionHandler_;
    IComputationExternalNode* InputRowArg_;
    IComputationNode* PartitionKey_;
    TValuePackerGeneric<false> PartitionKeyPacker_;
    const TMatchRecognizeProcessorParameters& Parameters_;
    const IRowsFormatter::TState& RowsFormatterState_;
    const TNfaTransitionGraph::TPtr RowPatternConfiguration_;
    NUdf::TUnboxedValue DelayedRow_;
    bool Terminating_;
    TSerializerContext SerializerContext_;
    TComputationContext& Ctx_;
};

class TStateForInterleavedPartitions
    : public TComputationValue<TStateForInterleavedPartitions> {
    using TPartitionMapValue = std::unique_ptr<TStreamingMatchRecognize>;
    using TPartitionMap = std::unordered_map<TString, TPartitionMapValue, std::hash<TString>, std::equal_to<>, TMKQLAllocator<std::pair<const TString, TPartitionMapValue>>>;

public:
    TStateForInterleavedPartitions(
        TMemoryUsageInfo* memInfo,
        IComputationExternalNode* inputRowArg,
        IComputationNode* partitionKey,
        TType* partitionKeyType,
        const TMatchRecognizeProcessorParameters& parameters,
        const IRowsFormatter::TState& rowsFormatterState,
        TComputationContext& ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker)
        : TComputationValue<TStateForInterleavedPartitions>(memInfo)
        , InputRowArg_(inputRowArg)
        , PartitionKey_(partitionKey)
        , PartitionKeyPacker_(/*stable=*/true, partitionKeyType)
        , Parameters_(parameters)
        , RowsFormatterState_(rowsFormatterState)
        , NfaTransitionGraph_(TNfaTransitionGraphBuilder::Create(parameters.Pattern, parameters.VarNamesLookup))
        , SerializerContext_(ctx, rowType, rowPacker)
        , Ctx_(ctx)
    {
    }

    NUdf::TUnboxedValue Save() const override {
        TMrOutputSerializer serializer(SerializerContext_, EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx_);
        serializer.Write(Partitions_.size());

        for (const auto& [key, state] : Partitions_) {
            serializer.Write(key);
            state->Save(serializer);
        }
        // HasReadyOutput is not packed because when loading we can recalculate HasReadyOutput from Partitions.
        serializer.Write(Terminating_);
        return serializer.MakeState();
    }

    bool Load2(const NUdf::TUnboxedValue& state) override {
        TMrInputSerializer in(SerializerContext_, state);

        Partitions_.clear();
        auto partitionsCount = in.Read<TPartitionMap::size_type>();
        Partitions_.reserve(partitionsCount);
        for (size_t i = 0; i < partitionsCount; ++i) {
            auto packedKey = in.Read<TPartitionMap::key_type, std::string_view>();
            NUdf::TUnboxedValue key = PartitionKeyPacker_.Unpack(packedKey, SerializerContext_.Ctx.HolderFactory);
            auto pair = Partitions_.emplace(
                packedKey,
                std::make_unique<TStreamingMatchRecognize>(
                    std::move(key),
                    Parameters_,
                    RowsFormatterState_,
                    NfaTransitionGraph_));
            pair.first->second->Load(in);
        }

        for (auto it = Partitions_.begin(); it != Partitions_.end(); ++it) {
            if (it->second->HasMatched()) {
                HasReadyOutput_.push(it);
            }
        }
        in.Read(Terminating_);
        if (in.GetStateVersion() < 2U) {
            auto restoredTransitionGraph = std::make_shared<TNfaTransitionGraph>();
            restoredTransitionGraph->Load(in);
            MKQL_ENSURE(NfaTransitionGraph_, "Empty NfaTransitionGraph");
            MKQL_ENSURE(*restoredTransitionGraph == *NfaTransitionGraph_, "Restored and current NfaTransitionGraph is different");
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
            HasReadyOutput_.push(partition);
        }
        return !HasReadyOutput_.empty();
    }

    bool ProcessEndOfData(TComputationContext& ctx) {
        for (auto it = Partitions_.begin(); it != Partitions_.end(); ++it) {
            auto b = it->second->ProcessEndOfData(ctx);
            if (b) {
                HasReadyOutput_.push(it);
            }
        }
        Terminating_ = true;
        return !HasReadyOutput_.empty();
    }

    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
        while (!HasReadyOutput_.empty()) {
            auto r = HasReadyOutput_.top()->second->GetOutputIfReady(ctx);
            if (not r) {
                // dried up
                HasReadyOutput_.pop();
                continue;
            } else {
                return r;
            }
        }
        return Terminating_ ? NUdf::TUnboxedValue(NUdf::TUnboxedValue::MakeFinish()) : NUdf::TUnboxedValue{};
    }

private:
    TPartitionMap::iterator GetPartitionHandler(const NUdf::TUnboxedValue& row, TComputationContext& ctx) {
        InputRowArg_->SetValue(ctx, NUdf::TUnboxedValue(row));
        auto partitionKey = PartitionKey_->GetValue(ctx);
        const auto packedKey = PartitionKeyPacker_.Pack(partitionKey);
        if (const auto it = Partitions_.find(TString(packedKey)); it != Partitions_.end()) {
            return it;
        } else {
            return Partitions_.emplace_hint(it, TString(packedKey), std::make_unique<TStreamingMatchRecognize>(std::move(partitionKey),
                                                                                                               Parameters_,
                                                                                                               RowsFormatterState_,
                                                                                                               NfaTransitionGraph_));
        }
    }

    TPartitionMap Partitions_;
    std::stack<TPartitionMap::iterator, std::deque<TPartitionMap::iterator, TMKQLAllocator<TPartitionMap::iterator>>> HasReadyOutput_;
    bool Terminating_ = false;

    IComputationExternalNode* InputRowArg_;
    IComputationNode* PartitionKey_;
    // TODO switch to tuple compare
    TValuePackerGeneric<false> PartitionKeyPacker_;
    const TMatchRecognizeProcessorParameters& Parameters_;
    const IRowsFormatter::TState& RowsFormatterState_;
    const TNfaTransitionGraph::TPtr NfaTransitionGraph_;
    TSerializerContext SerializerContext_;
    TComputationContext& Ctx_;
};

template <class State>
class TMatchRecognizeWrapper: public TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>, true> {
    using TBaseComputation = TStatefulFlowComputationNode<TMatchRecognizeWrapper<State>, true>;

public:
    TMatchRecognizeWrapper(
        TComputationMutables& mutables,
        EValueRepresentation kind,
        IComputationNode* inputFlow,
        IComputationExternalNode* inputRowArg,
        IComputationNode* partitionKey,
        TType* partitionKeyType,
        TMatchRecognizeProcessorParameters&& parameters,
        IRowsFormatter::TState&& rowsFormatterState,
        TType* rowType)
        : TBaseComputation(mutables, inputFlow, kind, EValueRepresentation::Embedded)
        , InputFlow_(inputFlow)
        , InputRowArg_(inputRowArg)
        , PartitionKey_(partitionKey)
        , PartitionKeyType_(partitionKeyType)
        , Parameters_(std::move(parameters))
        , RowsFormatterState_(std::move(rowsFormatterState))
        , RowType_(rowType)
        , RowPacker_(mutables)
    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<State>(
                InputRowArg_,
                PartitionKey_,
                PartitionKeyType_,
                Parameters_,
                RowsFormatterState_,
                ctx,
                RowType_,
                RowPacker_);
        } else if (stateValue.HasValue()) {
            MKQL_ENSURE(stateValue.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = stateValue.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue state = ctx.HolderFactory.Create<State>(
                    InputRowArg_,
                    PartitionKey_,
                    PartitionKeyType_,
                    Parameters_,
                    RowsFormatterState_,
                    ctx,
                    RowType_,
                    RowPacker_);
                state.Load2(stateValue);
                stateValue = state;
            }
        }
        auto state = static_cast<State*>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto output = state->GetOutputIfReady(ctx); output) {
                return output;
            }
            auto item = InputFlow_->GetValue(ctx);
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
    using TBaseComputation::DependsOn;
    using TBaseComputation::Own;
    void RegisterDependencies() const final {
        if (const auto flow = TBaseComputation::FlowDependsOn(InputFlow_)) {
            Own(flow, InputRowArg_);
            Own(flow, Parameters_.InputDataArg);
            Own(flow, Parameters_.MatchedVarsArg);
            Own(flow, Parameters_.CurrentRowIndexArg);
            Own(flow, Parameters_.MeasureInputDataArg);
            DependsOn(flow, PartitionKey_);
            for (auto& m : RowsFormatterState_.Measures) {
                DependsOn(flow, m);
            }
            for (auto& d : Parameters_.Defines) {
                DependsOn(flow, d);
            }
        }
    }

    IComputationNode* const InputFlow_;
    IComputationExternalNode* const InputRowArg_;
    IComputationNode* const PartitionKey_;
    TType* const PartitionKeyType_;
    TMatchRecognizeProcessorParameters Parameters_;
    IRowsFormatter::TState RowsFormatterState_;
    TType* const RowType_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> RowPacker_;
};

TOutputColumnOrder GetOutputColumnOrder(TRuntimeNode partitionKyeColumnsIndexes, TRuntimeNode measureColumnsIndexes) {
    std::unordered_map<size_t, TOutputColumnEntry, std::hash<size_t>, std::equal_to<>, TMKQLAllocator<std::pair<const size_t, TOutputColumnEntry>, EMemorySubPool::Temporary>> temp;
    {
        auto list = AS_VALUE(TListLiteral, partitionKyeColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = {.Index = i, .SourceType = EOutputColumnSource::PartitionKey};
        }
    }
    {
        auto list = AS_VALUE(TListLiteral, measureColumnsIndexes);
        for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
            auto index = AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<ui32>();
            temp[index] = {.Index = i, .SourceType = EOutputColumnSource::Measure};
        }
    }
    if (temp.empty()) {
        return {};
    }
    auto outputSize = std::ranges::max_element(temp, {}, &std::pair<const size_t, TOutputColumnEntry>::first)->first + 1;
    TOutputColumnOrder result(outputSize);
    for (const auto& [i, v] : temp) {
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
            const auto& inputFactor = AS_VALUE(TTupleLiteral, inputTerm->GetValue(j));
            MKQL_ENSURE(inputFactor->GetValuesCount() == 6, "Internal logic error");
            const auto& primary = inputFactor->GetValue(0);
            term.push_back(TRowPatternFactor{
                .Primary = primary.GetRuntimeType()->IsData() ? TRowPatternPrimary(TString(AS_VALUE(TDataLiteral, primary)->AsValue().AsStringRef())) : ConvertPattern(primary),
                .QuantityMin = AS_VALUE(TDataLiteral, inputFactor->GetValue(1))->AsValue().Get<ui64>(),
                .QuantityMax = AS_VALUE(TDataLiteral, inputFactor->GetValue(2))->AsValue().Get<ui64>(),
                .Greedy = AS_VALUE(TDataLiteral, inputFactor->GetValue(3))->AsValue().Get<bool>(),
                .Output = AS_VALUE(TDataLiteral, inputFactor->GetValue(4))->AsValue().Get<bool>(),
                .Unused = AS_VALUE(TDataLiteral, inputFactor->GetValue(5))->AsValue().Get<bool>()});
        }
        result.push_back(std::move(term));
    }
    return result;
}

TMeasureInputColumnOrder GetMeasureColumnOrder(const TListLiteral& specialColumnIndexes, ui32 inputRowColumnCount) {
    // Use Last enum value to denote that c colum comes from the input table
    TMeasureInputColumnOrder result(inputRowColumnCount + specialColumnIndexes.GetItemsCount(), std::make_pair(EMeasureInputDataSpecialColumns::Last, 0));
    if (specialColumnIndexes.GetItemsCount() != 0) {
        MKQL_ENSURE(specialColumnIndexes.GetItemsCount() == static_cast<size_t>(EMeasureInputDataSpecialColumns::Last),
                    "Internal logic error");
        for (size_t i = 0; i != specialColumnIndexes.GetItemsCount(); ++i) {
            auto ind = AS_VALUE(TDataLiteral, specialColumnIndexes.GetItems()[i])->AsValue().Get<ui32>();
            result[ind] = std::make_pair(static_cast<EMeasureInputDataSpecialColumns>(i), 0);
        }
    }
    // update indexes for input table columns
    ui32 inputIdx = 0;
    for (auto& [t, i] : result) {
        if (EMeasureInputDataSpecialColumns::Last == t) {
            i = inputIdx++;
        }
    }
    return result;
}

TComputationNodePtrVector ConvertVectorOfCallables(const TRuntimeNode::TList& v, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector result;
    result.reserve(v.size());
    for (auto& c : v) {
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
        vec.emplace_back(MakeString(varName));
        lookup[TString(varName)] = i;
    }
    return {vec, lookup};
}

} // namespace NMatchRecognize

IComputationNode* WrapMatchRecognizeCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    using namespace NMatchRecognize;
    size_t inputIndex = 0;
    const auto& inputFlow = callable.GetInput(inputIndex++);
    const auto& inputRowArg = callable.GetInput(inputIndex++);
    const auto& partitionKeySelector = callable.GetInput(inputIndex++);
    Y_UNUSED(callable.GetInput(inputIndex++));
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
    NYql::NMatchRecognize::TAfterMatchSkipTo skipTo = {.To = NYql::NMatchRecognize::EAfterMatchSkipTo::NextRow, .Var = ""};
    skipTo.To = static_cast<EAfterMatchSkipTo>(AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().Get<i32>());
    skipTo.Var = AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().AsStringRef();
    NYql::NMatchRecognize::ERowsPerMatch rowsPerMatch = static_cast<ERowsPerMatch>(AS_VALUE(TDataLiteral, callable.GetInput(inputIndex++))->AsValue().Get<i32>());
    TOutputColumnOrder outputColumnOrder = IRowsFormatter::GetOutputColumnOrder(callable.GetInput(inputIndex++));
    MKQL_ENSURE(callable.GetInputsCount() == inputIndex, "Wrong input count");

    const auto& [varNames, varNamesLookup] = ConvertListOfStrings(defineNames);
    auto* rowType = AS_TYPE(TStructType, AS_TYPE(TFlowType, inputFlow.GetStaticType())->GetItemType());

    auto parameters = TMatchRecognizeProcessorParameters{
        .InputDataArg = static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputDataArg.GetNode())),
        .Pattern = ConvertPattern(pattern),
        .VarNames = varNames,
        .VarNamesLookup = varNamesLookup,
        .MatchedVarsArg = static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *matchedVarsArg.GetNode())),
        .CurrentRowIndexArg = static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *currentRowIndexArg.GetNode())),
        .Defines = ConvertVectorOfCallables(defines, ctx),
        .MeasureInputDataArg = static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *measureInputDataArg.GetNode())),
        .MeasureInputColumnOrder = GetMeasureColumnOrder(
            *AS_VALUE(TListLiteral, measureSpecialColumnIndexes),
            AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>()),
        .SkipTo = skipTo};
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
            rowType);
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
            rowType);
    }
}

} // namespace NKikimr::NMiniKQL
