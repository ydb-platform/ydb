#include "simple_grace_join.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"
#include "subprocess.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {


template<typename T>
std::vector<std::pair<T, ui64>> MakeKeyedSamples(const ui64 seed, const size_t numSamples, const unsigned int maxKey, const bool longStrings, size_t keyOffset);

template<>
std::vector<std::pair<ui64, ui64>> MakeKeyedSamples<ui64>(const ui64 seed, const size_t numSamples, const unsigned int maxKey, const bool longStrings, size_t keyOffset)
{
    Y_UNUSED(longStrings);
    return MakeKeyed6464Samples(seed, numSamples, maxKey, keyOffset);
}

template<>
std::vector<std::pair<std::string, ui64>> MakeKeyedSamples<std::string>(const ui64 seed, const size_t numSamples, const unsigned int maxKey, const bool longStrings, size_t keyOffset)
{
    return MakeKeyedString64Samples(seed, numSamples, maxKey, longStrings, keyOffset);
}

struct TPackedValueChain
{
    ui64 Value;
    TPackedValueChain* Next;
};

struct TPackedValueRef
{
    TPackedValueChain* Chains[2];
};

struct IJoinDataSampler
{
    virtual ~IJoinDataSampler()
    {
    }

    virtual THolder<IWideStream> MakeLeftStream(const THolderFactory& holderFactory) const = 0;
    virtual THolder<IWideStream> MakeRightStream(const THolderFactory& holderFactory) const = 0;
    virtual TType* GetKeyType(const TTypeEnvironment& env) const = 0;
    virtual size_t CountReferenceJoinResults(const THolderFactory& holderFactory) const = 0;
    virtual void VerifyComputedValueVsReference(const THolderFactory& holderFactory, const NUdf::TUnboxedValue& wideStream) const = 0;
};

template<typename TMapImpl, typename K, typename V>
struct TJoinDataSampler: public IJoinDataSampler
{
    const TKVStream<K, V, true>::TSamples SamplesLeft;
    const TKVStream<K, V, true>::TSamples SamplesRight;
    const size_t StreamNumIters = 0;
    const bool LongKeys = false;

    TJoinDataSampler(ui64 seed, size_t numSamplesLeft, size_t numSamplesRight, size_t numKeys, bool longKeys, size_t overlap)
        : SamplesLeft(MakeKeyedSamples<K>(seed + 42, numSamplesLeft, numKeys - 1, longKeys, 0))
        , SamplesRight(MakeKeyedSamples<K>(seed, numSamplesRight, numKeys - 1, longKeys, numKeys - overlap))
        , StreamNumIters(1)
        , LongKeys(longKeys)
    {
    }

    TType* GetKeyType(const TTypeEnvironment& env) const override
    {
        return GetVerySimpleDataType<K>(env);
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory, const TKVStream<K, V, true>::TSamples& samples) const {
        if (LongKeys) {
            return THolder(new TKVStream<K, ui64, false, 1>(holderFactory, samples, StreamNumIters));
        } else {
            return THolder(new TKVStream<K, ui64, true, 1>(holderFactory, samples, StreamNumIters));
        }
    }

    THolder<IWideStream> MakeLeftStream(const THolderFactory& holderFactory) const override
    {
        return MakeStream(holderFactory, SamplesLeft);
    }

    THolder<IWideStream> MakeRightStream(const THolderFactory& holderFactory) const override
    {
        return MakeStream(holderFactory, SamplesRight);
    }

    template<typename TResultRowCallback>
    void ReferenceJoin(IWideStream& referenceLeftStream, IWideStream& referenceRightStream, TResultRowCallback&& callback) const
    {
        NUdf::TUnboxedValue inputs[2];
        typename TMapImpl::TMapType expects;
        std::deque<TPackedValueChain> packedValues;

        while (referenceRightStream.WideFetch(inputs, 2) == NUdf::EFetchStatus::Ok) {
            const K key = UnboxedToNative<K>(inputs[0]);
            const V tmp = inputs[1].Get<V>();
            if constexpr (!TMapImpl::CustomOps) {
                auto ii = expects.find(key);
                if (ii == expects.end()) {
                    ii = expects.emplace(key, TPackedValueChain{.Value = tmp, .Next = nullptr}).first;
                    continue;
                }
                TPackedValueChain& packedRef = ii->second;
                packedValues.emplace_back(TPackedValueChain{
                    .Value = tmp,
                    .Next = packedRef.Next
                });
                auto& newValue = packedValues.back();
                packedRef.Next = &newValue;
            } else {
                ythrow yexception() << "Not implemented yet";
            }
        }

        while (referenceLeftStream.WideFetch(inputs, 2) == NUdf::EFetchStatus::Ok) {
            const K key = UnboxedToNative<K>(inputs[0]);
            const V tmp = inputs[1].Get<V>();
            if constexpr (!TMapImpl::CustomOps) {
                const auto ii = expects.find(key);
                if (ii == expects.end()) {
                    continue;
                }
                const TPackedValueChain* chain = &(ii->second);
                while (chain != nullptr) {
                    callback(key, tmp, chain->Value);
                    chain = chain->Next;
                }
            } else {
                ythrow yexception() << "Not implemented yet";
            }
        }
    };

    size_t CountReferenceJoinResults(const THolderFactory& holderFactory) const override
    {
        size_t numRows = 0;
        auto leftStream = MakeLeftStream(holderFactory);
        auto rightStream = MakeRightStream(holderFactory);
        ReferenceJoin(*leftStream, *rightStream, [&](const K&, const V&, const V&) {
            ++numRows;
        });
        return numRows;
    }

    void VerifyComputedValueVsReference(const THolderFactory& holderFactory, const NUdf::TUnboxedValue& wideStream) const override
    {
        NUdf::TUnboxedValue resultValues[3];

        Y_ENSURE(wideStream.IsBoxed());

        std::unordered_map<K, std::pair<ui64, ui64>> refResults;
        std::unordered_map<K, std::pair<ui64, ui64>> graphResults;

        auto leftStream = MakeLeftStream(holderFactory);
        auto rightStream = MakeRightStream(holderFactory);

        ReferenceJoin(*leftStream, *rightStream, [&](const K& key, const V& leftValue, const V& rightValue) {
            auto& vals = refResults[key];
            vals.first += leftValue;
            vals.second += rightValue;
        });

        size_t numFetches = 0;

        NUdf::EFetchStatus status;

        while ((status = wideStream.WideFetch(resultValues, 3)) != NUdf::EFetchStatus::Finish) {
            if (status != NUdf::EFetchStatus::Ok) {
                continue; // operators can yield sometimes
            }
            const K key = UnboxedToNative<K>(resultValues[0]);
            V value[2];
            for (size_t i = 0; i < 2; ++i) {
                value[i] = UnboxedToNative<ui64>(resultValues[1 + i]);
            }
            auto& tuple = graphResults[key];
            tuple.first += value[0];
            tuple.second += value[1];
            ++numFetches;
        }

        Cerr << "VerifyComputedValueVsReference: " << numFetches << " WideFetch calls, " << graphResults.size() << " keys" << Endl;

        if constexpr (!TMapImpl::CustomOps) {
            // UNIT_ASSERT_VALUES_EQUAL(graphResult.size(), Expects.size());
            for (const auto referencePair : graphResults) {
                const auto ii = refResults.find(referencePair.first);
                UNIT_ASSERT(ii != refResults.end());
                UNIT_ASSERT_VALUES_EQUAL(referencePair.second, ii->second);
            }
        } else {
            ythrow yexception() << "Not implemented yet";
        }
    }
};

THolder<IJoinDataSampler> CreateJoinSamplerFromParams(const TRunParams& params)
{
    Y_ENSURE(params.RandomSeed.has_value());
    Y_ENSURE(params.JoinRightRows > 0);
    Y_ENSURE(params.JoinOverlap <= params.RowsPerRun);
    Y_ENSURE(params.JoinOverlap <= params.JoinRightRows);

    Cerr << "Left rows: " << params.RowsPerRun << ", right rows: " << params.JoinRightRows
         << ", distinct keys: " << params.NumKeys << ", keys in the intersection: " << params.JoinOverlap << Endl;

    switch (params.SamplerType) {
    case ESamplerType::StringKeysUI64Values: {
        auto next = [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = TJoinDataSampler<MapImpl, std::string, ui64>;
            return MakeHolder<SamplerType>(*params.RandomSeed, params.RowsPerRun, params.JoinRightRows, params.NumKeys, params.LongStringKeys, params.JoinOverlap);
        };
        return DispatchByMap<std::string, TPackedValueChain, IJoinDataSampler>(params.ReferenceHashType, next);
    }
    case ESamplerType::UI64KeysUI64Values: {
        auto next = [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = TJoinDataSampler<MapImpl, ui64, ui64>;
            return MakeHolder<SamplerType>(*params.RandomSeed, params.RowsPerRun, params.JoinRightRows, params.NumKeys, false, params.JoinOverlap);
        };
        return DispatchByMap<ui64, TPackedValueChain, IJoinDataSampler>(params.ReferenceHashType, next);
    }
    }
}


template<bool LLVM, bool Spilling>
THolder<IComputationGraph> BuildGraph(TSetup<LLVM, Spilling>& setup, std::shared_ptr<ISpillerFactory> spillerFactory, IJoinDataSampler& sampler)
{
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb.GetTypeEnvironment()), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto leftStreamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();
    const auto rightStreamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();
    const auto resultType = pb.NewMultiType({
        sampler.GetKeyType(pb.GetTypeEnvironment()),
        pb.NewDataType(NUdf::TDataType<ui64>::Id),
        pb.NewDataType(NUdf::TDataType<ui64>::Id)
    });

    const auto resultFlowType = pb.NewFlowType(resultType);

    /*
    flow: generate a flow of wide MultiType values ({ key, value } pairs) from the input stream
    extractor: get key from an item
    init: initialize the state with the item value
    update: state += value
    finish: return key + state
    */
    const auto pgmReturn = pb.FromFlow(pb.GraceJoin(
        pb.ToFlow(TRuntimeNode(leftStreamCallable, false)),
        pb.ToFlow(TRuntimeNode(rightStreamCallable, false)),
        EJoinKind::Inner,
        {0u},
        {0u},
        {0u, 0u, 1u, 1u},
        {1u, 2u},
        resultFlowType
    ));

    auto graph = setup.BuildGraph(pgmReturn, {leftStreamCallable, rightStreamCallable});
    if (Spilling) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }

    auto myStream = NUdf::TUnboxedValuePod(sampler.MakeLeftStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    myStream = NUdf::TUnboxedValuePod(sampler.MakeRightStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(1, true)->SetValue(graph->GetContext(), std::move(myStream));

    return graph;
}

TDuration MeasureGeneratorTime(IComputationGraph& graph, const IJoinDataSampler& sampler)
{
    const auto leftStream = sampler.MakeLeftStream(graph.GetHolderFactory());
    const auto rightStream = sampler.MakeRightStream(graph.GetHolderFactory());
    const auto devnullStart = GetThreadCPUTime();
    {
        NUdf::TUnboxedValue columns[3];
        while (leftStream->WideFetch(columns, 2) == NUdf::EFetchStatus::Ok) {
        }
        while (rightStream->WideFetch(columns, 2) == NUdf::EFetchStatus::Ok) {
        }
    }
    return GetThreadCPUTimeDelta(devnullStart);
}

template<bool LLVM, bool Spilling>
TRunResult RunTestOverGraph(const TRunParams& params, const bool needsVerification, const bool measureReferenceMemory)
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    THolder<IJoinDataSampler> sampler = CreateJoinSamplerFromParams(params);


    auto measureGraphTime = [&](auto& computeGraphPtr) {
        // Compute node implementation

        Cerr << "Compute graph result" << Endl;
        const auto graphTimeStart = GetThreadCPUTime();
        size_t lineCount = CountWideStreamOutputs<3>(computeGraphPtr->GetValue());
        Cerr << lineCount << Endl;

        return GetThreadCPUTimeDelta(graphTimeStart);
    };

    auto measureRefTime = [&](auto& computeGraphPtr, IJoinDataSampler& sampler) {
        // Reference implementation (join via a hashmap)

        Cerr << "Compute reference result" << Endl;

        const auto cppTimeStart = GetThreadCPUTime();

        sampler.CountReferenceJoinResults(
            computeGraphPtr->GetHolderFactory()
        );

        return GetThreadCPUTimeDelta(cppTimeStart);
    };


    auto graphRun1 = BuildGraph(setup, nullptr, *sampler);

    TRunResult result;

    if (measureReferenceMemory || params.TestMode == ETestMode::RefOnly) {
        long maxRssBefore = GetMaxRSS();
        result.ReferenceTime = measureRefTime(graphRun1, *sampler);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        return result;
    }

    if (params.TestMode == ETestMode::GraphOnly || params.TestMode == ETestMode::Full) {
        long maxRssBefore = GetMaxRSS();
        Cerr << "Raw MaxRSS before: " << maxRssBefore << Endl;
        result.ResultTime = measureGraphTime(graphRun1);
        result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);

        Cerr << "Raw MaxRSS after: " << GetMaxRSS() << Endl;
        Cerr << "MaxRSS delta, bytes: " << result.MaxRSSDelta << Endl;

        if (params.TestMode == ETestMode::GraphOnly) {
            return result;
        }
    }

    if (params.TestMode == ETestMode::GeneratorOnly || params.TestMode == ETestMode::Full) {
        // Measure the input stream run time

        Cerr << "Generator run" << Endl;
        result.GeneratorTime = MeasureGeneratorTime(*graphRun1, *sampler);

        if (params.TestMode == ETestMode::GeneratorOnly) {
            return result;
        }
    }

    result.ReferenceTime = measureRefTime(graphRun1, *sampler);

    // Verification (re-run the graph again but actually collect the output values into a map)
    if (needsVerification) {
        Cerr << "Compute and verify the graph result" << Endl;
        auto graphRun2 = BuildGraph(setup, nullptr, *sampler);
        sampler->VerifyComputedValueVsReference(graphRun2->GetHolderFactory(), graphRun2->GetValue());
    }

    return result;
}

void RunTestGraceJoinSimple(const TRunParams& params, TTestResultCollector& printout)
{
    std::optional<TRunResult> finalResult;

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", llvm: " << false << Endl;

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<false, false>(params, params.EnableVerification, false);
    } else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ : run " << i << " of " << params.NumAttempts << Endl;

            const bool needsVerification = (i == 1) && params.EnableVerification;

            TRunResult runResult = RunForked([&]() {
                return RunTestOverGraph<false, false>(params, needsVerification, false);
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
            return RunTestOverGraph<false, false>(params, false, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }

    printout.SubmitMetrics(params, *finalResult, "GraceJoinSimple", false, false);
}

}
}