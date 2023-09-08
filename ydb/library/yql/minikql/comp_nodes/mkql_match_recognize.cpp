#include "mkql_match_recognize_matched_vars.h"
#include "mkql_match_recognize_measure_arg.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <deque>

namespace NKikimr::NMiniKQL {

namespace NMatchRecognize {

enum class EOutputColumnSource {PartitionKey, Measure};
using TOutputColumnOrder = TVector<std::pair<EOutputColumnSource, size_t>>;

using namespace NYql::NMatchRecognize;

//Process one partition of input data
struct IProcessMatchRecognize {
    ///return true if it has output data ready
    virtual bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) = 0;
    virtual NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) = 0;
    virtual bool ProcessEndOfData(TComputationContext& ctx) = 0;
    virtual ~IProcessMatchRecognize(){}
};

struct TMatchRecognizeProcessorParameters {
    IComputationExternalNode* InputDataArg;
    IComputationExternalNode* MatchedVarsArg;
    IComputationExternalNode* CurrentRowIndexArg;
    TComputationNodePtrVector Defines;
    IComputationExternalNode* MeasureInputDataArg;
    TMeasureInputColumnOrder  MeasureInputColumnOrder;
    TUnboxedValueVector       VarNames;
    TComputationNodePtrVector Measures;
    TOutputColumnOrder        OutputColumnOrder;
};

class TBackTrackingMatchRecognize: public IProcessMatchRecognize {
public:
    TBackTrackingMatchRecognize(
        NUdf::TUnboxedValue&& partitionKey,
        const TMatchRecognizeProcessorParameters& parameters,
        const TContainerCacheOnContext& cache
    )
        : PartitionKey(std::move(partitionKey))
        , Parameters(parameters)
        , Cache(cache)
        , CurMatchedVars(parameters.Defines.size())
        , MatchNumber(0)
    {
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) override {
        Y_UNUSED(ctx);
        Rows.push_back(std::move(row));
        return false;
    }
    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) override {
        if (Matches.empty())
            return NUdf::TUnboxedValue{};
        Parameters.MatchedVarsArg->SetValue(ctx, ToValue(ctx, std::move(Matches.front())));
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
    bool ProcessEndOfData(TComputationContext& ctx) override {
        //Assume, that data moved to IComputationExternalNode node, will not be modified or released
        //till the end of the current function
        auto rowsSize = Rows.size();
        Parameters.InputDataArg->SetValue(ctx, ctx.HolderFactory.VectorAsVectorHolder(std::move(Rows)));
        for (size_t i = 0; i != rowsSize; ++i) {
            Parameters.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(static_cast<ui64>(i)));
            for (size_t v = 0; v != Parameters.Defines.size(); ++v) {
                const auto &d = Parameters.Defines[v]->GetValue(ctx);
                if (d && d.GetOptionalValue().Get<bool>()) {
                    auto &var = CurMatchedVars[v];
                    if (var.empty()) {
                        var.emplace_back(i, i);
                    } else if (var.back().From + 1 == i) {
                        ++var.back().To;
                    } else {
                        var.emplace_back(i, i);
                    }
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
    TUnboxedValueVector Rows;
    TMatchedVars CurMatchedVars;
    std::deque<TMatchedVars> Matches;
    ui64 MatchNumber;
};

class TStreamingMatchRecognize: public IProcessMatchRecognize {
public:
    TStreamingMatchRecognize(
            NUdf::TUnboxedValue&& partitionKey,
            const TMatchRecognizeProcessorParameters& parameters,
            const TContainerCacheOnContext& cache
    )
        : PartitionKey(std::move(partitionKey))
        , Parameters(parameters)
        , Cache(cache)
        , MatchedVars(parameters.Defines.size())
        , HasMatch(false)
        , RowCount(0)
    {
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) override{
        Y_UNUSED(row);
        Parameters.CurrentRowIndexArg->SetValue(ctx, NUdf::TUnboxedValuePod(RowCount));
        for (size_t i = 0; i != Parameters.Defines.size(); ++i) {
            const auto& d = Parameters.Defines[i]->GetValue(ctx);
            if (d && d.GetOptionalValue().Get<bool>()) {
                auto& var = MatchedVars[i];
                if (var.empty()) {
                    var.emplace_back(RowCount, RowCount);
                }
                else if (var.back().To + 1 == RowCount) {
                    ++var.back().To;
                }
                else {
                    var.emplace_back(RowCount, RowCount);
                }
            }
        }
        ++RowCount;
        return HasMatch;
    }
    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) override {
        if (!HasMatch)
            return NUdf::TUnboxedValue{};
        Parameters.MatchedVarsArg->SetValue(ctx, ToValue(ctx, MatchedVars));
        HasMatch = false;
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
    bool ProcessEndOfData(TComputationContext& ctx) override {
        Y_UNUSED(ctx);
        MKQL_ENSURE(false, "Internal logic error. End of partition is not expected for a stream");
        return false;
    }
private:
    const NUdf::TUnboxedValue PartitionKey;
    const TMatchRecognizeProcessorParameters& Parameters;
    const TContainerCacheOnContext& Cache;
    TMatchedVars MatchedVars;
    bool HasMatch;
    size_t RowCount;
};


class TMatchRecognizeWrapper : public TStatefulFlowComputationNode<TMatchRecognizeWrapper> {
    using TBaseComputation = TStatefulFlowComputationNode<TMatchRecognizeWrapper>;
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
            stateValue = ctx.HolderFactory.Create<TState>(
                    InputRowArg,
                    PartitionKey,
                    PartitionKeyType,
                    Parameters,
                    Cache
            );
        }
        auto& state = *static_cast<TState *>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto output = state.GetOutputIfReady(ctx); output) {
                return output;
            }
            auto item = InputFlow->GetValue(ctx);
            if (item.IsFinish()) {
                state.ProcessEndOfData(ctx);
                continue;
            } else if (item.IsSpecial()) {
                return item;
            }
            state.ProcessInputRow(std::move(item), ctx);
        }
    }
private:
    class TState: public TComputationValue<TState> {
        using TPartitionMap = std::unordered_map<TString, std::unique_ptr<IProcessMatchRecognize>>;
    public:
        TState(
            TMemoryUsageInfo* memInfo,
            IComputationExternalNode* inputRowArg,
            IComputationNode* partitionKey,
            TType* partitionKeyType,
            const TMatchRecognizeProcessorParameters& parameters,
            const TContainerCacheOnContext& cache
        )
            : TComputationValue<TState>(memInfo)
            , InputRowArg(inputRowArg)
            , PartitionKey(partitionKey)
            , PartitionKeyPacker(true, partitionKeyType)
            , Parameters(parameters)
            , Cache(cache)
        {
        }

        void ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
            auto partition = GetPartitionHandler(row, ctx);
            if (partition->second->ProcessInputRow(std::move(row), ctx)) {
                HasReadyOutput.push(partition);
            }
        }

        void ProcessEndOfData(TComputationContext& ctx) {
            for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
                auto b = it->second->ProcessEndOfData(ctx);
                if (b) {
                    HasReadyOutput.push(it);
                }
            }
            Terminating = true;
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
                return Partitions.emplace_hint(it, TString(packedKey), std::make_unique<TBackTrackingMatchRecognize>(
                        std::move(partitionKey),
                        Parameters,
                        Cache
                ));
            }
        }

    private:
        TPartitionMap Partitions;
        std::stack<TPartitionMap::iterator> HasReadyOutput;
        bool Terminating = false;

        IComputationExternalNode* InputRowArg;
        IComputationNode* PartitionKey;
        //TODO switch to tuple compare
        TValuePackerGeneric<false> PartitionKeyPacker;
        const TMatchRecognizeProcessorParameters& Parameters;
        const TContainerCacheOnContext& Cache;
    };

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(InputFlow)) {
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
    const TContainerCacheOnContext Cache;
};

TOutputColumnOrder GetOutputColumnOrder(TRuntimeNode partitionKyeColumnsIndexes, TRuntimeNode measureColumnsIndexes) {
    std::unordered_map<size_t, std::pair<EOutputColumnSource, size_t>> temp;
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
            MKQL_ENSURE(inputFactor->GetValuesCount() == 5, "Internal logic error");
            const auto& primary = inputFactor->GetValue(0);
            term.push_back(TRowPatternFactor{
                    primary.IsImmediate() ?
                        TRowPatternPrimary(TString(AS_VALUE(TDataLiteral, primary)->AsValue().AsStringRef())) :
                        ConvertPattern(primary),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(1))->AsValue().Get<ui64>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(2))->AsValue().Get<ui64>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(3))->AsValue().Get<bool>(),
                    AS_VALUE(TDataLiteral, inputFactor->GetValue(4))->AsValue().Get<bool>()
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

TUnboxedValueVector ConvertListOfStrings(const TRuntimeNode& l) {
    TUnboxedValueVector result;
    const auto& list = AS_VALUE(TListLiteral, l);
    result.reserve(list->GetItemsCount());
    for (ui32 i = 0; i != list->GetItemsCount(); ++i) {
        result.push_back(MakeString(AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().AsStringRef()));
    }
    return result;
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
    MKQL_ENSURE(callable.GetInputsCount() == inputIndex, "Wrong input count");

    Y_UNUSED(measureInputDataArg);
    Y_UNUSED(measureSpecialColumnIndexes);
    Y_UNUSED(inputRowColumnCount);
    Y_UNUSED(pattern);

    return new TMatchRecognizeWrapper(ctx.Mutables
        , GetValueRepresentation(inputFlow.GetStaticType())
        , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
        , LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode())
        , partitionKeySelector.GetStaticType()
        , TMatchRecognizeProcessorParameters{
              static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputDataArg.GetNode()))
            , static_cast<IComputationExternalNode *>(LocateNode(ctx.NodeLocator, *matchedVarsArg.GetNode()))
            , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *currentRowIndexArg.GetNode()))
            , ConvertVectorOfCallables(defines, ctx)
            , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *measureInputDataArg.GetNode()))
            , GetMeasureColumnOrder(
                    *AS_VALUE(TListLiteral, measureSpecialColumnIndexes),
                    AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>()
            )
            , ConvertListOfStrings(varNames)
            , ConvertVectorOfCallables(measures, ctx)
            , GetOutputColumnOrder(partitionColumnIndexes, measureColumnIndexes)
            }
    );
}

} //namespace NKikimr::NMiniKQL
