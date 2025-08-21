#include "dq_combine_vs.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"
#include "subprocess.h"
#include "kqp_setup.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>


namespace NKikimr {
namespace NMiniKQL {

using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

namespace {

using TStreamValues = std::vector<TUnboxedValuePod>;

struct TFieldDescr {
    bool IsString = false;
    bool IsEmbedded = true;
};

void NativeToUnboxed(const ui64 value, NUdf::TUnboxedValuePod& result)
{
    result = NUdf::TUnboxedValuePod(value);
}

void NativeToUnboxed(const std::string& value, bool isEmbedded, NUdf::TUnboxedValuePod& result)
{
    if (isEmbedded) {
        result = NUdf::TUnboxedValuePod::Embedded(value);
    } else {
        result = NUdf::TUnboxedValuePod(NUdf::TStringValue(value));
    }
}

TStreamValues GenerateSample(size_t numKeys, size_t numRows, const std::vector<TFieldDescr>& keys, const std::vector<TFieldDescr>& values)
{
    TStreamValues result;
    result.reserve(numRows * (keys.size() + values.size()));

    auto pushValue = [&](const TFieldDescr& descr, const ui64 key, const bool isKeyColumn) {
        ui64 smolValue = isKeyColumn ? key : (key % 1000);
        result.emplace_back();
        if (descr.IsString) {
            auto value = descr.IsEmbedded ? Sprintf("%08u", smolValue) : Sprintf("%08u.%08u.%08u.", smolValue, smolValue, smolValue);
            NativeToUnboxed(value, descr.IsEmbedded, result.back());
        } else {
            NativeToUnboxed(smolValue, result.back());
        }
    };

    for (size_t i = 0; i < numRows; ++i) {
        const size_t currKey = i % numKeys;
        for (const auto& descr : keys) {
            pushValue(descr, currKey, true);
        }
        for (const auto& descr : values) {
            pushValue(descr, currKey, false);
        }
    }

    return result;
}

size_t CountStreamOutputs(const NUdf::TUnboxedValue& wideStream, size_t width)
{
    std::vector<TUnboxedValue> resultValues;
    resultValues.resize(width);

    Y_ENSURE(wideStream.IsBoxed());

    size_t lineCount = 0;
    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = wideStream.WideFetch(resultValues.data(), width)) != NUdf::EFetchStatus::Finish) {
        if (fetchStatus == NUdf::EFetchStatus::Ok) {
            ++lineCount;
        }
    }

    return lineCount;
}

class TPrebuiltStream : public NUdf::TBoxedValue
{
public:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final override {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }

    TPrebuiltStream(const TStreamValues& values, size_t width, size_t iterations)
        : Values(values)
        , ValuesEnd(Values.end())
        , CurrItem(Values.begin())
        , Width(width)
        , MaxIterations(iterations)
    {
    }

protected:
    bool IsAtTheEnd() const {
        return (CurrItem >= ValuesEnd);
    }

    void TryProceed() {
        if (CurrItem >= ValuesEnd) {
            ++Iteration;
            if (Iteration < MaxIterations) {
                CurrItem = Values.begin();
            }
        }
    }

public:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {
        if (width != Width) {
            ythrow yexception() << "width " << Width << " expected";
        }

        if (IsAtTheEnd()) {
            return NUdf::EFetchStatus::Finish;
        }

        for (size_t i = 0; i < width; ++i) {
            result[i] = *CurrItem;
            ++CurrItem;
        }

        TryProceed();

        return NUdf::EFetchStatus::Ok;
    }

    const TStreamValues& Values;
    const TStreamValues::const_iterator ValuesEnd;
    TStreamValues::const_iterator CurrItem;
    const size_t Width;
    const size_t MaxIterations;
    size_t Iteration = 0;
};



template<bool LLVM>
THolder<IComputationGraph> BuildGraph(TKqpSetup<LLVM>& setup, THolder<NUdf::TBoxedValue> inputStream, bool useDqImpl, std::vector<TType*> keyTypes, std::vector<TType*> valueTypes, size_t memLimit)
{
    TKqpProgramBuilder& pb = setup.GetKqpBuilder();

    std::vector<TType*> mergedTypes;
    mergedTypes.insert(mergedTypes.end(), keyTypes.begin(), keyTypes.end());
    mergedTypes.insert(mergedTypes.end(), valueTypes.begin(), valueTypes.end());

    // const auto streamItemType = pb.NewMultiType({pb.NewDataType(NUdf::TDataType<ui64>::Id), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamItemType = pb.NewMultiType(TArrayRef<TType*>(mergedTypes));
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    auto lambdaKey = [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        for (size_t i = 0; i < keyTypes.size(); ++i) {
            result.push_back(items[i]);
        }
        return result;
    };
    auto lambdaInit = [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        for (size_t i = 0; i < valueTypes.size(); ++i) {
            result.push_back(items[keyTypes.size() + i]);
        }
        return result;
    };
    auto lambdaUpdate = [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        for (size_t i = 0; i < valueTypes.size(); ++i) {
            result.push_back(pb.AggrAdd(items[keyTypes.size() + i], state[i]));
        }
        return result;
    };
    auto lambdaFinalize = [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        result.insert(result.end(), keys.begin(), keys.end());
        result.insert(result.end(), state.begin(), state.end());
        return result;
    };

    TRuntimeNode pgmReturn;

    if (useDqImpl) {
        pgmReturn = pb.DqHashCombine(
            TRuntimeNode(streamCallable, false),
            memLimit,
            lambdaKey,
            lambdaInit,
            lambdaUpdate,
            lambdaFinalize);
    } else {
        pgmReturn = pb.FromFlow(pb.WideCombiner(
            pb.ToFlow(TRuntimeNode(streamCallable, false)),
            memLimit,
            lambdaKey,
            lambdaInit,
            lambdaUpdate,
            lambdaFinalize
        ));
    }

    auto graph = setup.BuildGraph(pgmReturn, {streamCallable});
    auto myStream = NUdf::TUnboxedValuePod(inputStream.Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    return graph;
}

std::vector<TType*> FieldDescrToTypes(const std::vector<TFieldDescr>& fds, TProgramBuilder& pb)
{
    std::vector<TType*> result;
    std::transform(fds.begin(), fds.end(), std::back_inserter(result), [&](const TFieldDescr& fd) {
        if (fd.IsString) {
            return pb.NewDataType(NUdf::TDataType<char*>::Id);
        } else {
            return pb.NewDataType(NUdf::TDataType<ui64>::Id);
        }
    });
    return result;
}

template<bool LLVM>
TRunResult RunTestOverGraph(
    const TRunParams& params,
    const TStreamValues& inputData,
    const std::vector<TFieldDescr>& keyFields,
    const std::vector<TFieldDescr>& valueFields,
    [[maybe_unused]] const bool needsVerification,
    [[maybe_unused]] const bool measureReferenceMemory)
{
    TKqpSetup<LLVM> setup(GetPerfTestFactory());

    NYql::NLog::InitLogger("cerr", false);

    auto makeStream = [&]() -> THolder<NUdf::TBoxedValue> {
        return MakeHolder<TPrebuiltStream>(inputData, keyFields.size() + valueFields.size(), params.NumRuns);
    };

    auto measureGraphTime = [&](auto& computeGraphPtr, bool isDq) {
        Cerr << "Compute " << (isDq ? "DqHashCombine" : "WideCombine") << " graph result" << Endl;
        const auto graphTimeStart = GetThreadCPUTime();
        size_t lineCount = CountStreamOutputs(computeGraphPtr->GetValue(), keyFields.size() + valueFields.size());
        Cerr << lineCount << Endl;

        return GetThreadCPUTimeDelta(graphTimeStart);
    };

    auto keyTypes = FieldDescrToTypes(keyFields, *setup.PgmBuilder);
    auto valueTypes = FieldDescrToTypes(valueFields, *setup.PgmBuilder);

    auto graphRunDq = BuildGraph(setup, makeStream(), true, keyTypes, valueTypes, params.WideCombinerMemLimit);
    auto graphRunWc = BuildGraph(setup, makeStream(), false, keyTypes, valueTypes, params.WideCombinerMemLimit);

    TRunResult result;

    if (measureReferenceMemory) {
        long maxRssBefore = GetMaxRSS();
        result.ReferenceTime = measureGraphTime(graphRunWc, false);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        return result;
    }

    long maxRssBefore = GetMaxRSS();
    result.ResultTime = measureGraphTime(graphRunDq, true);
    result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);

    result.ReferenceTime = measureGraphTime(graphRunWc, false);
    return result;
}

}

template<bool LLVM>
void RunTestDqHashCombineVsWideCombine(const TRunParams& params, TTestResultCollector& printout)
{
    std::vector<TFieldDescr> keyFields;
    for (size_t i = 0; i < 3; ++i) {
        TFieldDescr descr;
        switch (params.SamplerType) {
        case ESamplerType::StringKeysUI64Values:
            descr.IsString = true;
            descr.IsEmbedded = true;
            break;
        case ESamplerType::UI64KeysUI64Values:
            descr.IsString = false;
            descr.IsEmbedded = true;
            break;
        default:
            ythrow yexception() << "Unsupported sampler type for dq-hash-combine test";
        }
        keyFields.emplace_back(descr);
    };

    std::vector<TFieldDescr> valueFields;
    for (size_t i = 0; i < 10; ++i) {
        valueFields.emplace_back(TFieldDescr{
            .IsString = false,
            .IsEmbedded = false,
        });
    };

    std::optional<TRunResult> finalResult;

    const TStreamValues inputData = GenerateSample(params.NumKeys, params.RowsPerRun, keyFields, valueFields);

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", llvm: " << LLVM << ", mem limit: " << params.WideCombinerMemLimit << Endl;

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<LLVM>(params, inputData, keyFields, valueFields, true, false);
    }
    else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ Run " << i << " of " << params.NumAttempts << Endl;

            const bool needsVerification = (i == 1);

            TRunResult runResult = RunForked([&]() {
                return RunTestOverGraph<LLVM>(params, inputData, keyFields, valueFields, needsVerification, false);
            });

            if (finalResult.has_value()) {
                MergeRunResults(runResult, *finalResult);
            } else {
                finalResult.emplace(runResult);
            }
        }
    }

    if (params.MeasureReferenceMemory) {
        Cerr << "------ Reference memory measurement run" << Endl;
        TRunResult runResult = RunForked([&]() {
            return RunTestOverGraph<LLVM>(params, inputData, keyFields, valueFields, false, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }

    printout.SubmitMetrics(params, *finalResult, "DqHashCombine", LLVM);
}

template void RunTestDqHashCombineVsWideCombine<false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestDqHashCombineVsWideCombine<true>(const TRunParams& params, TTestResultCollector& printout);

}
}
