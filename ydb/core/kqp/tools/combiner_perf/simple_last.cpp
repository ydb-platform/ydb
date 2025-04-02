#include "simple_last.h"

#include "factories.h"
#include "converters.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

using T6464Samples = std::vector<std::pair<ui64, ui64>>;
using TString64Samples = std::vector<std::pair<std::string, ui64>>;

T6464Samples MakeKeyed6464Samples(const size_t numSamples, const unsigned int maxKey) {
    std::default_random_engine eng;
    std::uniform_int_distribution<unsigned int> keys(0, maxKey);
    std::uniform_int_distribution<ui64> unif(0, 100000.0);

    T6464Samples samples(numSamples);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            return std::make_pair<ui64, ui64>(keys(eng), unif(eng));
        }
    );

    return samples;
}

TString64Samples MakeKeyedString64Samples(const size_t numSamples, const unsigned int maxKey, const bool longStrings) {
    std::default_random_engine eng;
    std::uniform_int_distribution<unsigned int> keys(0, maxKey);
    std::uniform_int_distribution<ui64> unif(0, 100000.0);

    TString64Samples samples(numSamples);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            auto key = keys(eng);
            std::string strKey;
            if (!longStrings) {
                strKey = std::string(ToString(key));
            } else {
                strKey = Sprintf("%07u.%07u.%07u.", key, key, key);
            }
            return std::make_pair<std::string, ui64>(std::move(strKey), unif(eng));
        }
    );

    return samples;
}

struct IWideStream : public NUdf::TBoxedValue
{
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) = 0;
};

template<typename K, typename V, bool EmbeddedKeys>
struct TStream : public IWideStream {
    using TSamples = std::vector<std::pair<K, V>>;

    TStream(const THolderFactory& holderFactory, const TSamples& samples, size_t iterations)
        : HolderFactory(holderFactory)
        , Samples(samples)
        , End(Samples.end())
        , MaxIterations(iterations)
        , CurrSample(Samples.begin())
    {
        Y_ENSURE(samples.size() > 0);
    }

private:
    const THolderFactory& HolderFactory;
    const TSamples& Samples;
    const TSamples::const_iterator End;
    const size_t MaxIterations;
    TSamples::const_iterator CurrSample = 0;
    size_t Iteration = 0;

public:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
        Y_UNUSED(HolderFactory); // for future use

        if (CurrSample == End) {
            ++Iteration;
            if (Iteration >= MaxIterations) {
                return NUdf::EFetchStatus::Finish;
            }
            CurrSample = Samples.begin();
        }

        if (width != 2) {
            ythrow yexception() << "width 2 expected";
        }

        // TODO: support embedded strings in values?
        NativeToUnboxed<EmbeddedKeys>(CurrSample->first, result[0]);
        NativeToUnboxed<false>(CurrSample->second, result[1]);

        ++CurrSample;

        return NUdf::EFetchStatus::Ok;
    }
};

template<typename K, typename V>
std::unordered_map<K, V> ComputeSumReferenceResult(IWideStream& referenceStream)
{
    std::unordered_map<K, V> expects;
    {
        NUdf::TUnboxedValue inputs[2];

        while (referenceStream.WideFetch(inputs, 2) == NUdf::EFetchStatus::Ok) {
            expects[UnboxedToNative<K>(inputs[0])] += inputs[1].Get<V>();
        }
    }
    return expects;
}

template<typename K, typename V>
void VerifyListVsUnorderedMap(const NUdf::TUnboxedValue& pairList, const std::unordered_map<K, V>& map)
{
    UNIT_ASSERT_VALUES_EQUAL(pairList.GetListLength(), map.size());

    const auto ptr = pairList.GetElements();
    for (size_t i = 0ULL; i < map.size(); ++i) {
        const auto elements = ptr[i].GetElements();
        const auto key = UnboxedToNative<K>(elements[0]);
        const auto value = UnboxedToNative<V>(elements[1]);
        const auto ii = map.find(key);
        UNIT_ASSERT(ii != map.end());
        UNIT_ASSERT_VALUES_EQUAL(value, ii->second);
    }
}

class IDataSampler
{
    virtual THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const = 0;
    virtual TType* GetKeyType(TProgramBuilder& pb) const = 0;
    virtual void ComputeReferenceResult(IWideStream& referenceStream) = 0;
    virtual void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const = 0;
    virtual std::string Describe() const = 0;
};

class T6464DataSampler : public IDataSampler
{
public:
    TStream<ui64, ui64, false>::TSamples Samples;
    std::unordered_map<ui64, ui64> Expects;

    size_t StreamNumIters = 0;

    T6464DataSampler(size_t numSamples, size_t maxKey, size_t numIters)
        : Samples(MakeKeyed6464Samples(numSamples, maxKey))
        , StreamNumIters(numIters)
    {
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const override
    {
        return THolder(new TStream<ui64, ui64, false>(holderFactory, Samples, StreamNumIters));
    }

    TType* GetKeyType(TProgramBuilder& pb) const override
    {
        return pb.NewDataType(NUdf::TDataType<ui64>::Id);
    }

    void ComputeReferenceResult(IWideStream& referenceStream) override
    {
        Y_ENSURE(Expects.empty());

        Expects = ComputeSumReferenceResult<ui64, ui64>(referenceStream);
    }

    void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const override
    {
        VerifyListVsUnorderedMap(value, Expects);
    }

    std::string Describe() const override
    {
        return "ui64 keys, ui64 values";
    }
};

class TString64DataSampler : public IDataSampler
{
public:
    TStream<std::string, ui64, false>::TSamples Samples;
    std::unordered_map<std::string, ui64> Expects;

    size_t StreamNumIters = 0;
    bool LongStrings = false;

    TString64DataSampler(size_t numSamples, size_t maxKey, size_t numIters, bool longStrings)
        : Samples(MakeKeyedString64Samples(numSamples, maxKey, longStrings))
        , StreamNumIters(numIters)
        , LongStrings(longStrings)
    {
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const override
    {
        if (LongStrings) {
            return THolder(new TStream<std::string, ui64, false>(holderFactory, Samples, StreamNumIters));
        } else {
            return THolder(new TStream<std::string, ui64, true>(holderFactory, Samples, StreamNumIters));
        }
    }

    TType* GetKeyType(TProgramBuilder& pb) const override
    {
        return pb.NewDataType(NUdf::TDataType<char*>::Id);
    }

    void ComputeReferenceResult(IWideStream& referenceStream) override
    {
        Y_ENSURE(Expects.empty());

        Expects = ComputeSumReferenceResult<std::string, ui64>(referenceStream);
    }

    void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const override
    {
        VerifyListVsUnorderedMap(value, Expects);
    }

    std::string Describe() const override
    {
        return Sprintf("%s string keys, ui64 values", LongStrings ? "Long (24 byte)" : "Embedded");
    }
};

template<bool LLVM, bool Spilling>
void RunTestCombineLastSimple(const TRunParams& params)
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    const size_t numSamples = params.RowsPerRun;
    const size_t numIters = params.NumRuns; // Will process numSamples * numIters items
    const size_t maxKey = params.MaxKey; // maxKey + 1 distinct keys, each key multiplied by 64 for some reason

    Cerr << "Data rows total: " << numSamples << " x " << numIters << Endl;
    Cerr << (maxKey + 1) << " distinct numeric keys" << Endl;

    // TString64DataSampler sampler(numSamples, maxKey, numIters, params.LongStringKeys);
    T6464DataSampler sampler(numSamples, maxKey, numIters);
    Cerr << "Sampler type: " << sampler.Describe() << Endl;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    /*
    flow: generate a flow of wide MultiType values (uint64 { key, value } pairs) from the input stream
    extractor: get key from an item
    init: initialize the state with the item value
    update: state += value (why AggrAdd though?)
    finish: return key + state
    */
    const auto pgmReturn = pb.Collect(pb.NarrowMap(
        WideLastCombiner<Spilling>(
            pb,
            pb.ToFlow(TRuntimeNode(streamCallable, false)),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; }, // extractor
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; }, // init
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            }, // update
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; } // finish
        ), // NarrowMap flow
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));

    const auto graph = setup.BuildGraph(pgmReturn, {streamCallable});
    if (Spilling) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

    // Reference implementation (sum via an std::unordered_map)
    auto referenceStream = sampler.MakeStream(graph->GetHolderFactory());
    const auto t = TInstant::Now();
    sampler.ComputeReferenceResult(*referenceStream);
    const auto cppTime = TInstant::Now() - t;

    // Compute graph implementation
    auto myStream = NUdf::TUnboxedValuePod(sampler.MakeStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    const auto t1 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t2 = TInstant::Now();

    // Verification
    sampler.VerifyComputedValueVsReference(value);

    Cerr << "WideLastCombiner graph runtime is: " << t2 - t1 << " vs. reference C++ implementation: " << cppTime << "" << Endl << Endl;
}

template void RunTestCombineLastSimple<false, false>(const TRunParams& params);
template void RunTestCombineLastSimple<false, true>(const TRunParams& params);
template void RunTestCombineLastSimple<true, false>(const TRunParams& params);
template void RunTestCombineLastSimple<true, true>(const TRunParams& params);

}
}
