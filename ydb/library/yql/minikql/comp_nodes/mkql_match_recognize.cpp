#include "mkql_match_recognize_matched_vars.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>

namespace NKikimr::NMiniKQL {

namespace {

enum class EMeasureColumnSource {Classifier = 0, MatchNumber = 1, Input};
using TMeasureInputColumnOrder = std::vector<std::pair<EMeasureColumnSource, size_t>>;

enum class EOutputColumnSource {PartitionKey, Measure};
using TOutputColumnOrder = std::vector<std::pair<EOutputColumnSource, size_t>>;

using namespace NMatchRecognize;

//Process one partition of input data
struct IProcessMatchRecognize {
    ///return true if it has output data ready
    virtual bool ProcessInputRow(NUdf::TUnboxedValue&& row) = 0;
    virtual NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) = 0;
    virtual bool ProcessEndOfData() = 0;
    virtual ~IProcessMatchRecognize(){}
};

class TStreamMatchRecognize: public IProcessMatchRecognize {
public:
    TStreamMatchRecognize(
            NUdf::TUnboxedValue&& partitionKey,
            IComputationExternalNode* matchedVarsArg,
            std::vector<IComputationNode*>& measures,
            const TOutputColumnOrder& outputColumnOrder,
            const TContainerCacheOnContext& cache
    )
        : PartitionKey(std::move(partitionKey))
        , MatchedVarsArg(matchedVarsArg)
        , Measures(measures)
        , OutputColumnOrder(outputColumnOrder)
        , Cache(cache)
        , MatchedVars(2) //Assume pattern (A B B)*, where A matches every 3rd row (i%3 == 0) and B matches the rest
        , HasMatch(false)
        , RowCount(0)
    {
    }

    bool ProcessInputRow(NUdf::TUnboxedValue&& row) override{
        Y_UNUSED(row);
        //Assume pattern (A B B)*, where A matches every 3rd row (i%3 == 0) and B matches the rest
        switch (RowCount % 3) {
            case 0:
                MatchedVars[0].push_back({RowCount, RowCount});
                break;
            case 1:
                MatchedVars[1].push_back({RowCount, RowCount});
                break;
            case 2:
                MatchedVars[1].back().second++;
                break;
        }
        ++RowCount;
        return HasMatch;
    }
    NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) override {
        if (!HasMatch)
            return NUdf::TUnboxedValue::Invalid();
        MatchedVarsArg->SetValue(ctx, NUdf::TUnboxedValuePod(new TMatchedVarsValue(&ctx.HolderFactory.GetMemInfo(), MatchedVars)));
        HasMatch = false;
        NUdf::TUnboxedValue *itemsPtr = nullptr;
        const auto result = Cache.NewArray(ctx, OutputColumnOrder.size(), itemsPtr);
        for (auto const& c: OutputColumnOrder) {
            switch(c.first) {
                case EOutputColumnSource::Measure:
                    *itemsPtr++ = Measures[c.second]->GetValue(ctx);
                    break;
                case EOutputColumnSource::PartitionKey:
                    *itemsPtr++ = PartitionKey.GetElement(c.second);
                    break;
                default:
                    MKQL_ENSURE(false, "Internal logic error");
            }
        }
        return result;
    }
    bool ProcessEndOfData() override {
        HasMatch = true;
        return HasMatch;
    }
private:
    const NUdf::TUnboxedValue PartitionKey;
    IComputationExternalNode* const MatchedVarsArg;
    const std::vector<IComputationNode*>& Measures;
    const TOutputColumnOrder& OutputColumnOrder;
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
                           IComputationExternalNode* matchedVarsArg,
                           std::vector<IComputationNode*>&& measures,
                           TOutputColumnOrder&& outputColumnOrder
    )
    :TBaseComputation(mutables, inputFlow, kind, EValueRepresentation::Embedded)
    , InputFlow(inputFlow)
    , InputRowArg(inputRowArg)
    , PartitionKey(partitionKey)
    , PartitionKeyType(partitionKeyType)
    , MatchedVarsArg(matchedVarsArg)
    , Measures(measures)
    , OutputColumnOrder(outputColumnOrder)
    , Cache(mutables)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue &stateValue, TComputationContext &ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(
                    InputRowArg,
                    PartitionKey,
                    PartitionKeyType,
                    MatchedVarsArg,
                    Measures,
                    OutputColumnOrder,
                    Cache
            );
        }
        auto& state = *static_cast<TState *>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto output = state.GetOutputIfReady(ctx); !output.IsInvalid()) {
                return output;
            }
            auto item = InputFlow->GetValue(ctx);
            if (item.IsFinish()) {
                state.ProcessEndOfData();
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
            IComputationExternalNode* matchedVarsArg,
            const std::vector<IComputationNode*>& measures,
            const TOutputColumnOrder& outputColumnOrder,
            const TContainerCacheOnContext& cache
        )
            : TComputationValue<TState>(memInfo)
            , InputRowArg(inputRowArg)
            , PartitionKey(partitionKey)
            , PartitionKeyPacker(true, partitionKeyType)
            , MatchedVarsArg(matchedVarsArg)
            , Measures(measures)
            , OutputColumnOrder(outputColumnOrder)
            , Cache(cache)
        {
        }

        void ProcessInputRow(NUdf::TUnboxedValue&& row, TComputationContext& ctx) {
            auto partition = GetPartitionHandler(row, ctx);
            if (partition->second->ProcessInputRow(std::move(row))) {
                HasReadyOutput.push(partition);
            }
        }

        void ProcessEndOfData() {
            for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
                auto b = it->second->ProcessEndOfData();
                if (b) {
                    HasReadyOutput.push(it);
                }
            }
            Terminating = true;
        }

        NUdf::TUnboxedValue GetOutputIfReady(TComputationContext& ctx) {
            while (!HasReadyOutput.empty()) {
                auto r = HasReadyOutput.top()->second->GetOutputIfReady(ctx);
                if (r.IsInvalid()) {
                    //dried up
                    HasReadyOutput.pop();
                    continue;
                } else {
                    return r;
                }
            }
            return Terminating ? NUdf::TUnboxedValue::MakeFinish() : NUdf::TUnboxedValue::Invalid();
        }

    private:
        TPartitionMap::iterator GetPartitionHandler(const NUdf::TUnboxedValue& row, TComputationContext &ctx) {
            InputRowArg->SetValue(ctx, NUdf::TUnboxedValue(row));
            auto partitionKey = PartitionKey->GetValue(ctx);
            const auto packedKey = PartitionKeyPacker.Pack(partitionKey);
            if (const auto it = Partitions.find(TString(packedKey)); it != Partitions.end()) {
                return it;
            } else {
                return Partitions.emplace_hint(it, TString(packedKey), std::make_unique<TStreamMatchRecognize>(
                        std::move(partitionKey),
                        MatchedVarsArg,
                        Measures,
                        OutputColumnOrder,
                        Cache
                ));
            }
        }

    private:
        //for this class
        TPartitionMap Partitions;
        std::stack<TPartitionMap::iterator> HasReadyOutput;
        bool Terminating = false;

        IComputationExternalNode* InputRowArg;
        IComputationNode* PartitionKey;
        //TODO switch to tuple compare
        TValuePackerGeneric<false> PartitionKeyPacker;

        //to be passed to partitions
        IComputationExternalNode* const MatchedVarsArg;
        std::vector<IComputationNode*> Measures;
        const TOutputColumnOrder& OutputColumnOrder;
        const TContainerCacheOnContext& Cache;
    };

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(InputFlow)) {
            Own(flow, InputRowArg);
            Own(flow, MatchedVarsArg);
            DependsOn(flow, PartitionKey);
            for (auto& m: Measures) {
                DependsOn(flow, m);
            }
        }
    }

    IComputationNode* const InputFlow;
    IComputationExternalNode* const InputRowArg;
    IComputationNode* const PartitionKey;
    TType* const PartitionKeyType;
    IComputationExternalNode* const MatchedVarsArg;
    std::vector<IComputationNode*> Measures;
    TOutputColumnOrder OutputColumnOrder;
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

std::vector<IComputationNode*> ConvertVectorOfCallables(const std::vector<TRuntimeNode>& v, const TComputationNodeFactoryContext& ctx) {
    std::vector<IComputationNode*> result;
    result.reserve(v.size());
    for (auto& c: v) {
        result.push_back(LocateNode(ctx.NodeLocator, *c.GetNode()));
    }
    return result;
}

} //namespace


IComputationNode* WrapMatchRecognizeCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
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
    std::vector<TRuntimeNode> measures;
    for (size_t i = 0; i != AS_VALUE(TListLiteral, measureColumnIndexes)->GetItemsCount(); ++i) {
        measures.push_back(callable.GetInput(inputIndex++));
    }
    MKQL_ENSURE(callable.GetInputsCount() == inputIndex, "Wrong input count");
    Y_UNUSED(measureInputDataArg);
    Y_UNUSED(measureSpecialColumnIndexes);
    Y_UNUSED(inputRowColumnCount);

    return new TMatchRecognizeWrapper(ctx.Mutables, GetValueRepresentation(inputFlow.GetStaticType())
        , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
        , LocateNode(ctx.NodeLocator, *partitionKeySelector.GetNode())
        , partitionKeySelector.GetStaticType()
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *matchedVarsArg.GetNode()))
        , ConvertVectorOfCallables(measures, ctx)
        , GetOutputColumnOrder(partitionColumnIndexes, measureColumnIndexes)
    );
}

} //namespace NKikimr::NMiniKQL
