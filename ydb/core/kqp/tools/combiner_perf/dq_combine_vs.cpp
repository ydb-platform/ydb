#include "dq_combine_vs.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"
#include "subprocess.h"
#include "kqp_setup.h"

#include <ydb/library/yql/dq/comp_nodes/ut/utils/preallocated_spiller.h>
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

using TVerificationReferenceData = std::unordered_map<std::string, ui64>;

TStreamValues GenerateSample(size_t numKeys, size_t numRows, const std::vector<TFieldDescr>& keys, const std::vector<TFieldDescr>& values, TVerificationReferenceData* refData)
{
    TStreamValues result;
    result.reserve(numRows * (keys.size() + values.size()));

    std::default_random_engine eng;
    std::vector<ui64> primaryKeys;
    primaryKeys.reserve(numRows);
    for (size_t i = 0; i < numRows; ++i) {
        primaryKeys.emplace_back(i % numKeys);
    }

    std::shuffle(primaryKeys.begin(), primaryKeys.end(), eng);

    auto pushValue = [&](const TFieldDescr& descr, const ui64 key, const bool isKeyColumn, const bool pushToReference) {
        ui64 aggregatable = key % 1000;
        ui64 columnValue = isKeyColumn ? key : aggregatable;
        result.emplace_back();

        std::string refKey;
        if (descr.IsString) {
            refKey = descr.IsEmbedded ? Sprintf("%08u", columnValue) : Sprintf("%08u.%08u.%08u.", columnValue, columnValue, columnValue);
            NativeToUnboxed(refKey, descr.IsEmbedded, result.back());
        } else {
            NativeToUnboxed(columnValue, result.back());
            if (pushToReference) {
                refKey = ToString(columnValue);
            }
        }

        if (pushToReference && refData) {
            auto ii = refData->find(refKey);
            if (ii == refData->end()) {
                refData->emplace(refKey, aggregatable);
            } else {
                ii->second += aggregatable;
            }
        }
    };

    for (ui64 pk : primaryKeys) {
        bool ref = true;

        for (const auto& descr : keys) {
            pushValue(descr, pk, true, ref);
            ref = false;
        }
        for (const auto& descr : values) {
            pushValue(descr, pk, false, false);
        }
    }

    return result;
}

size_t CountStreamOutputs(const NUdf::TUnboxedValue& wideStream, size_t width, bool sleepOnYield)
{
    std::vector<TUnboxedValue> resultValues;
    resultValues.resize(width);

    Y_ENSURE(wideStream.IsBoxed());

    size_t lineCount = 0;
    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = wideStream.WideFetch(resultValues.data(), width)) != NUdf::EFetchStatus::Finish) {
        if (fetchStatus == NUdf::EFetchStatus::Ok) {
            ++lineCount;
        } else if (sleepOnYield) {
            ::Sleep(TDuration::MilliSeconds(1));
        }
    }

    return lineCount;
}

size_t CollectStreamOutputs(const NUdf::TUnboxedValue& wideStream, const std::vector<TFieldDescr>& keyFields, size_t width, TVerificationReferenceData& data, bool sleepOnYield)
{
    std::vector<TUnboxedValue> resultValues;
    Cerr << "collect width: " << width << Endl;
    resultValues.resize(width);

    Y_ENSURE(wideStream.IsBoxed());

    size_t lineCount = 0;

    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = wideStream.WideFetch(resultValues.data(), width)) != NUdf::EFetchStatus::Finish) {
        if (fetchStatus == NUdf::EFetchStatus::Yield) {
            if (sleepOnYield) {
                ::Sleep(TDuration::MilliSeconds(1));
            }
            continue;
        }

        ++lineCount;

        std::string key;
        if (keyFields[0].IsString) {
            key = UnboxedToNative<std::string>(resultValues[0]);
        } else {
            key = ToString<ui64>(UnboxedToNative<ui64>(resultValues[0]));
        }

        ui64 value = UnboxedToNative<ui64>(resultValues.back());
        auto iter = data.find(key);
        if (iter == data.end()) {
            data.emplace(key, value);
        } else {
            iter->second += value;
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

    using TCallback = std::function<void()>;
    TPrebuiltStream(const TStreamValues& values, size_t width, size_t iterations, size_t callbackAfter = 0, TCallback callback = {})
        : Values(values)
        , ValuesEnd(Values.end())
        , CurrItem(Values.begin())
        , Width(width)
        , MaxIterations(iterations)
        , CallbackAfter(callbackAfter)
        , Callback(callback)
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

        ++NumFetches;
        if (CallbackAfter && NumFetches == CallbackAfter) {
            Callback();
        }

        return NUdf::EFetchStatus::Ok;
    }

    const TStreamValues& Values;
    const TStreamValues::const_iterator ValuesEnd;
    TStreamValues::const_iterator CurrItem;
    const size_t Width;
    const size_t MaxIterations;
    size_t CallbackAfter;
    TCallback Callback;
    size_t NumFetches = 0;
    size_t Iteration = 0;
};



template<bool LLVM, bool Spilling>
THolder<IComputationGraph> BuildGraph(
    TKqpSetup<LLVM, Spilling>& setup,
    std::shared_ptr<ISpillerFactory> spillerFactory,
    THolder<NUdf::TBoxedValue> inputStream,
    bool useDqImpl,
    std::vector<TType*> keyTypes,
    std::vector<TType*> valueTypes,
    size_t memLimit)
{
    TKqpProgramBuilder& pb = setup.GetKqpBuilder();

    std::vector<TType*> mergedTypes;
    mergedTypes.insert(mergedTypes.end(), keyTypes.begin(), keyTypes.end());
    mergedTypes.insert(mergedTypes.end(), valueTypes.begin(), valueTypes.end());

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

    constexpr const bool useFlow = true; // TODO: configurable
    if (useDqImpl) {
        if (useFlow) {
            pgmReturn = pb.FromFlow(pb.DqHashAggregate(
                pb.ToFlow(TRuntimeNode(streamCallable, false)),
                Spilling,
                lambdaKey,
                lambdaInit,
                lambdaUpdate,
                lambdaFinalize));
        } else {
            pgmReturn = pb.DqHashAggregate(
                TRuntimeNode(streamCallable, false),
                Spilling,
                lambdaKey,
                lambdaInit,
                lambdaUpdate,
                lambdaFinalize);
        }
    } else if (Spilling) {
        pgmReturn = pb.FromFlow(pb.WideLastCombinerWithSpilling(
            pb.ToFlow(TRuntimeNode(streamCallable, false)),
            lambdaKey,
            lambdaInit,
            lambdaUpdate,
            lambdaFinalize
        ));
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
    if (Spilling) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }

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

using TTestEqualsFunc = std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;
using TTestHashFunc = std::function<NUdf::THashType(const NUdf::TUnboxedValuePod*)>;

struct TTestWideUnboxedEqual {
    TTestWideUnboxedEqual(const TKeyTypes& types, ui64& calls)
        : Types(types)
        , Calls(calls)
    {}

    bool operator()(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        Calls += 1;
        for (ui32 i = 0U; i < Types.size(); ++i)
            if (CompareValues(Types[i].first, true, Types[i].second, left[i], right[i]))
                return false;
        return true;
    }

    const TKeyTypes& Types;
    ui64& Calls;
};

struct TTestWideUnboxedHasher {
    TTestWideUnboxedHasher(const TKeyTypes& types, ui64& calls)
        : Types(types)
        , Calls(calls)
    {}

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        Calls += 1;

        if (Types.size() == 1U)
            if (const auto v = *values)
                return NUdf::GetValueHash(Types.front().first, v);
            else
                return HashOfNull;

        NUdf::THashType hash = 0ULL;
        for (const auto& type : Types) {
            if (const auto v = *values++)
                hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
            else
                hash = CombineHashes(hash, HashOfNull);
        }
        return hash;
    }

    const TKeyTypes& Types;
    ui64& Calls;
};


struct TCachedHasher {
    TCachedHasher(const TKeyTypes& types, const NUdf::THashType& cache, ui64& calls, ui64& rawHashCalls)
        : Types(types)
        , Cache(cache)
        , Calls(calls)
        , RawHashCalls(rawHashCalls)
    {}


    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        /*RawHashCalls += 1;
        if (Cache != 0) {
            return Cache;
        }
        Calls += 1;*/
        if (Types.size() == 1U)
            if (const auto v = *values)
                return NUdf::GetValueHash(Types.front().first, v);
            else
                return HashOfNull;

        NUdf::THashType hash = 0ULL;
        for (const auto& type : Types) {
            if (const auto v = *values++)
                hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
            else
                hash = CombineHashes(hash, HashOfNull);
        }
        return hash;
    }

    const TKeyTypes& Types;
    const NUdf::THashType& Cache;
    ui64& Calls;
    ui64& RawHashCalls;
};

template<bool WithBuckets>
TRunResult RunHashMapTest(
    const TRunParams& params,
    const std::vector<TFieldDescr>& keyFields,
    const std::vector<TFieldDescr>& valueFields
)
{
    Cerr << "cache hash: " << !std::is_arithmetic<TUnboxedValuePod*>::value << Endl;

    TKqpSetup<false, false> setup(GetPerfTestFactory());
    TStreamValues inputData = GenerateSample(params.NumKeys, params.RowsPerRun, keyFields, valueFields, nullptr);
    auto ii = inputData.begin();
    const auto end = inputData.end();
    const ui32 stride = keyFields.size() + valueFields.size();

    using TMap = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TTestWideUnboxedEqual, TTestWideUnboxedHasher, TMKQLAllocator<char, EMemorySubPool::Temporary>>;
    using TMapWithCachedHash = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TTestWideUnboxedEqual, TCachedHasher, TMKQLAllocator<char, EMemorySubPool::Temporary>>;

    TKeyTypes keyTypes;
    for (const auto& key : keyFields) {
        if (key.IsString) {
            keyTypes.emplace_back(NUdf::EDataSlot::String, false);
        } else {
            keyTypes.emplace_back(NUdf::EDataSlot::Uint64, false);
        }
    }

    ui64 hashCalls = 0;
    ui64 rawHashCalls = 0;
    ui64 eqCalls = 0;

    TTestWideUnboxedHasher hasher(keyTypes, hashCalls);
    TTestWideUnboxedEqual eq(keyTypes, eqCalls);

    TDuration timeStart = TDuration::Zero();
    TDuration timeDelta = TDuration::Zero();

    if constexpr (!WithBuckets) {
        std::unique_ptr<TMap> map = std::make_unique<TMap>(hasher, eq, 8_MB);
        Cerr << map->GetCapacity() << Endl;

        timeStart = GetThreadCPUTime();
        while (ii != end) {
            bool isNew = false;
            map->Insert(ii, isNew);
            ii += stride;
        }
        timeDelta = GetThreadCPUTimeDelta(timeStart);
        Cerr << "Map size: " << map->GetSize() << Endl;
    } else {
        std::vector<std::unique_ptr<TMapWithCachedHash>> maps;
        maps.reserve(16);
        ui64 cachedHash = 0;
        TCachedHasher cachedHasher(keyTypes, cachedHash, hashCalls, rawHashCalls);
        for (ui32 i = 0; i < 16; ++i) {
            maps.emplace_back(std::make_unique<TMapWithCachedHash>(cachedHasher, eq, 8_MB / 16));
            Cerr << maps.back()->GetCapacity() << Endl;
        }

        timeStart = GetThreadCPUTime();
        while (ii != end) {
            bool isNew = false;
            cachedHash = cachedHasher(ii);
            ui64 bucket = (cachedHash * 11400714819323198485llu) & (0xFull);
            auto& map = maps[bucket];
            map->Insert(ii, isNew);
            cachedHash = 0;
            ii += stride;
        }
        timeDelta = GetThreadCPUTimeDelta(timeStart);

        for (auto& map : maps) {
            Cerr << "Map size: " << map->GetSize() << Endl;
        }
    }

    Cerr << "Hash calls: " << hashCalls << Endl;
    Cerr << "Raw hash calls: " << rawHashCalls << Endl;
    Cerr << "Eq calls: " << hashCalls << Endl;

    TRunResult result;
    result.ResultTime = timeDelta;
    return result;
}

template<bool LLVM, bool Spilling = true>
TRunResult RunTestOverGraph(
    const TRunParams& params,
    const std::vector<TFieldDescr>& keyFields,
    const std::vector<TFieldDescr>& valueFields,
    const bool doVerification,
    const bool measureReference,
    const bool slowSpilling = false)
{
    TKqpSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    NYql::NLog::InitLogger("cerr", false);

    std::shared_ptr<TPreallocatedSpillerFactory> realSpillerFactory = Spilling ? std::make_shared<TPreallocatedSpillerFactory>() : nullptr;
    std::shared_ptr<ISpillerFactory> spillerFactory = realSpillerFactory;
    if (realSpillerFactory && slowSpilling) {
        spillerFactory = std::make_shared<TSlowSpillerFactory>(spillerFactory);
    }

    TVerificationReferenceData referenceData;
    TVerificationReferenceData collectedData;

    TStreamValues inputData = GenerateSample(params.NumKeys, params.RowsPerRun, keyFields, valueFields,
        doVerification ? &referenceData : nullptr);

    auto makeStream = [&]() -> THolder<NUdf::TBoxedValue> {
        size_t yellowZoneTrigger = Spilling ? 1000000 : std::numeric_limits<size_t>::max();
        return MakeHolder<TPrebuiltStream>(inputData, keyFields.size() + valueFields.size(), params.NumRuns, yellowZoneTrigger, [&](){
            Cerr << "Enabling yellow zone" << Endl;
            setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
        });
    };

    auto measureGraphTime = [&](auto& computeGraphPtr, bool isDq) {
        Cerr << "Compute " << (isDq ? "DqHashCombine" : "WideCombine") << " graph result" << Endl;
        const auto graphTimeStart = GetThreadCPUTime();
        size_t lineCount;
        if (!doVerification) {
            lineCount = CountStreamOutputs(computeGraphPtr->GetValue(), keyFields.size() + valueFields.size(), slowSpilling);
            Cerr << "Output row count: " << lineCount << Endl;
        } else {
            lineCount = CollectStreamOutputs(computeGraphPtr->GetValue(), keyFields, keyFields.size() + valueFields.size(), collectedData, slowSpilling);
            Cerr << "Output row count: " << lineCount << Endl;
            Cerr << "Output distinct value count: " << collectedData.size() << Endl;
        }

        auto duration = GetThreadCPUTimeDelta(graphTimeStart);
        if (realSpillerFactory) {
            for (auto& spiller : realSpillerFactory->GetCreatedSpillers()) {
                auto preallocSpiller = dynamic_cast<TPreallocatedSpiller*>(spiller.get());
                if (!preallocSpiller) {
                    continue;
                }
                Cerr << "Preallocated spiller usage: " << Endl;
                Cerr << "Put operations: " << preallocSpiller->GetPutSizes().size() << Endl;
                auto putAmount = std::accumulate(preallocSpiller->GetPutSizes().begin(), preallocSpiller->GetPutSizes().end(), 0ull);
                Cerr << "\"Written\" MB: " << putAmount / 1_MB << Endl;
            }
            realSpillerFactory->ForgetSpillers();
        }
        return duration;
    };

    auto keyTypes = FieldDescrToTypes(keyFields, *setup.PgmBuilder);
    auto valueTypes = FieldDescrToTypes(valueFields, *setup.PgmBuilder);

    auto graphRunDq = BuildGraph(setup, spillerFactory, makeStream(), true, keyTypes, valueTypes, params.WideCombinerMemLimit);
    auto graphRunWc = BuildGraph(setup, spillerFactory, makeStream(), false, keyTypes, valueTypes, params.WideCombinerMemLimit);

    TRunResult result;
    long maxRssBefore = GetMaxRSS();

    if (measureReference) {
        result.ReferenceTime = measureGraphTime(graphRunWc, false);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
    } else {
        result.ResultTime = measureGraphTime(graphRunDq, true);
        result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
    }

    if (doVerification) {
        if (referenceData.size() != collectedData.size()) {
            ythrow yexception() << "Reference dataset has " << referenceData.size() << " unique keys, combiner result has " << collectedData.size();
        }

        for (auto kv : referenceData) {
            auto iter = collectedData.find(kv.first);
            if (iter == collectedData.end()) {
                ythrow yexception() << "Key " << kv.first << " from the reference dataset not found in result";
            }
            if (iter->second != kv.second) {
                ythrow yexception() << "Key " << kv.first << " has different values. Reference data value: " << kv.second << ", result value: " << iter->second;
            }
        }

        Cerr << "Verification passed" << Endl;
    }

    return result;
}

}

std::vector<std::string> GetColumnConfigurationNames()
{
    return std::vector<std::string> {"1k1v", "3k1v", "1k10v", "3k3v", "3k10v"};
}

void PrepareColumnDescriptions(const TRunParams& params, std::vector<TFieldDescr>& keyFields, std::vector<TFieldDescr>& valueFields)
{
    size_t numKeys = 1;
    size_t numValues = 1;
    if (params.CombineVsTestColumnSet == "1k1v") {
        // default
    } else if (params.CombineVsTestColumnSet == "3k1v") {
        numKeys = 3;
    } else if (params.CombineVsTestColumnSet == "3k3v") {
        numKeys = 3;
        numValues = 3;
    } else if (params.CombineVsTestColumnSet == "1k10v") {
        numValues = 10;
    } else if (params.CombineVsTestColumnSet == "3k10v") {
        numKeys = 3;
        numValues = 10;
    } else {
        ythrow yexception() << "Unknown column configuration: " << params.CombineVsTestColumnSet;
    }

    Cerr << "Key columns: " << numKeys << ", aggregations: " << numValues << Endl;
    Cerr << "Key column types: " << Endl;

    for (size_t i = 0; i < numKeys; ++i) {
        TFieldDescr descr;
        switch (params.SamplerType) {
        case ESamplerType::StringKeysUI64Values:
            Cerr << "string ";
            descr.IsString = true;
            descr.IsEmbedded = !params.LongStringKeys;
            break;
        case ESamplerType::UI64KeysUI64Values:
            Cerr << "ui64 ";
            descr.IsString = false;
            descr.IsEmbedded = !params.LongStringKeys;
            break;
        default:
            ythrow yexception() << "Unsupported sampler type for dq-hash-combine test";
        }
        keyFields.emplace_back(descr);
    };
    Cerr << Endl;

    for (size_t i = 0; i < numValues; ++i) {
        valueFields.emplace_back(TFieldDescr{
            .IsString = false,
            .IsEmbedded = false,
        });
    };
}

template<bool LLVM, bool Spilling>
void RunTestDqHashCombineVsWideCombine(const TRunParams& params, TTestResultCollector& printout)
{
    std::vector<TFieldDescr> keyFields;
    std::vector<TFieldDescr> valueFields;

    PrepareColumnDescriptions(params, keyFields, valueFields);

    std::optional<TRunResult> finalResult;

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", llvm: " << LLVM << ", mem limit: " << params.WideCombinerMemLimit << Endl;

    /*
    finalResult = RunHashMapTest<false>(params, keyFields, valueFields);
    printout.SubmitMetrics(params, *finalResult, "DqHashCombine", LLVM);
    if (true) {
        return;
    }
    */

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<LLVM, Spilling>(params, keyFields, valueFields, false, false);
        if (params.EnableVerification) {
            RunTestOverGraph<LLVM, Spilling>(params, keyFields, valueFields, true, false);
        }
    }
    else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ Run " << i << " of " << params.NumAttempts << Endl;

            TRunResult runResultDq = RunForked([&]() {
                return RunTestOverGraph<LLVM, Spilling>(params, keyFields, valueFields, false, false);
            });

            TRunResult runResultWide = RunForked([&]() {
                return RunTestOverGraph<LLVM, Spilling>(params, keyFields, valueFields, false, true);
            });

            if (finalResult.has_value()) {
                MergeRunResults(runResultDq, *finalResult);
            } else {
                finalResult.emplace(runResultDq);
            }
            MergeRunResults(runResultWide, *finalResult);

            if (params.EnableVerification && i == 1) {
                RunForked([&]() {
                    return RunTestOverGraph<LLVM, Spilling>(params, keyFields, valueFields, true, false);
                });
            }
        }
    }

    /*
    if (params.MeasureReferenceMemory) {
        Cerr << "------ Reference memory measurement run" << Endl;
        TRunResult runResult = RunForked([&]() {
            return RunTestOverGraph<LLVM>(params, inputData, keyFields, valueFields, false, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }
    */

    printout.SubmitMetrics(params, *finalResult, "DqHashCombine", LLVM);
}

template void RunTestDqHashCombineVsWideCombine<false, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestDqHashCombineVsWideCombine<false, true>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestDqHashCombineVsWideCombine<true, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestDqHashCombineVsWideCombine<true, true>(const TRunParams& params, TTestResultCollector& printout);

}
}
