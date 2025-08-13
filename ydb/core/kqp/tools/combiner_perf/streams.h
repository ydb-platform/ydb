// Sample data generators, streams, and reference aggregation implementations

#pragma once

#include "converters.h"
#include "run_params.h"
#include "value_wrapper.h"
#include "hashmaps.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

using T6464Samples = std::vector<std::pair<ui64, ui64>>;
T6464Samples MakeKeyed6464Samples(const ui64 seed, const size_t numSamples, const unsigned int maxKey);

using TString64Samples = std::vector<std::pair<std::string, ui64>>;
TString64Samples MakeKeyedString64Samples(const ui64 seed, const size_t numSamples, const unsigned int maxKey, const bool longStrings);


struct IWideStream : public NUdf::TBoxedValue
{
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final override {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override = 0;
};

template<typename K, typename V, bool EmbeddedKeys, size_t NumAggregates = 1>
struct TKVStream : public IWideStream {
    using TSamples = std::vector<std::pair<K, V>>;

    TKVStream(const THolderFactory& holderFactory, const TSamples& samples, size_t iterations)
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

        if (width != 1 + NumAggregates) {
            ythrow yexception() << "width " << (1 + NumAggregates) << " expected";
        }

        // TODO: support embedded strings in values?
        NativeToUnboxed<EmbeddedKeys>(CurrSample->first, result[0]);
        NUdf::TUnboxedValuePod val;
        NativeToUnboxed<false>(CurrSample->second, val);
        for (size_t i = 0; i < NumAggregates; ++i) {
            result[1 + i] = val;
        }

        ++CurrSample;

        return NUdf::EFetchStatus::Ok;
    }
};


template<typename K, typename V, size_t NumAggregates>
std::unordered_map<K, V> ComputeStreamSumResult(const NUdf::TUnboxedValue& wideStream)
{
    std::unordered_map<K, V> expects;
    NUdf::TUnboxedValue resultValues[2];

    Y_ENSURE(wideStream.IsBoxed());

    size_t numFetches = 0;
    NUdf::EFetchStatus status;

    while ((status = wideStream.WideFetch(resultValues, 2)) != NUdf::EFetchStatus::Finish) {
        if (status != NUdf::EFetchStatus::Ok) {
            continue; // spilling combiners do yield sometimes
        }
        const K key = UnboxedToNative<K>(resultValues[0]);
        V value;
        for (size_t i = 0; i < NumAggregates; ++i) {
            value[i] = UnboxedToNative<typename V::TElement>(resultValues[1 + i]);
        }
        expects[key] += value;
        ++numFetches;
    }

    Cerr << "ComputeStreamSumResult: " << numFetches << " WideFetch calls, " << expects.size() << " keys" << Endl;
    return expects;
}

// Direct version without the BoxedValueAccessor for use in reference timing measurements
template<typename K, typename V, size_t NumAggregates, typename TMapImpl>
void ComputeReferenceStreamSumResult(IWideStream& referenceStream, typename TMapImpl::TMapType& refResult)
{
    NUdf::TUnboxedValue inputs[2];

    while (referenceStream.WideFetch(inputs, 1 + NumAggregates) == NUdf::EFetchStatus::Ok) {
        V tmp;
        for (size_t i = 0; i < V::ArrayWidth; ++i) {
            tmp[i] = inputs[1 + i].Get<typename V::TElement>();
        }
        if constexpr (!TMapImpl::CustomOps) {
            refResult[UnboxedToNative<K>(inputs[0])] += tmp;
        } else {
            TMapImpl::AggregateByKey(refResult, UnboxedToNative<K>(inputs[0]), tmp);
        }
    }
}

template<typename K, typename V, typename TMapImpl>
void VerifyMapsAreEqual(const std::unordered_map<K, V> computedMap, const typename TMapImpl::TMapType& refMap)
{
    if constexpr (!TMapImpl::CustomOps) {
        UNIT_ASSERT_VALUES_EQUAL(computedMap.size(), refMap.size());
        for (const auto referencePair : refMap) {
            const auto ii = computedMap.find(referencePair.first);
            UNIT_ASSERT(ii != computedMap.end());
            UNIT_ASSERT_VALUES_EQUAL(referencePair.second, ii->second);
        }
    } else {
        UNIT_ASSERT_VALUES_EQUAL(computedMap.size(), TMapImpl::Size(refMap));
        TMapImpl::IteratePairs(refMap, [computedMap](const K& k, const V& v){
            const auto ii = computedMap.find(k);
            UNIT_ASSERT(ii != computedMap.end());
            UNIT_ASSERT_VALUES_EQUAL(v, ii->second);
        });
    }
}

template<typename TMapImpl, typename K, typename V, size_t NumAggregates>
void VerifyStreamVsMap(const NUdf::TUnboxedValue& wideStream, const typename TMapImpl::TMapType& refMap)
{
    auto resultMap = ComputeStreamSumResult<K, V, NumAggregates>(wideStream);
    VerifyMapsAreEqual<K, V, TMapImpl>(resultMap, refMap);
}

template<typename TMapImpl, typename K, typename V, size_t NumAggregates>
void VerifyListVsMap(const NUdf::TUnboxedValue& pairList, const typename TMapImpl::TMapType& refMap)
{
    std::unordered_map<K, V> resultMap;

    const auto ptr = pairList.GetElements();
    for (size_t i = 0; i < pairList.GetListLength(); ++i) {
        const auto elements = ptr[i].GetElements();
        const auto key = UnboxedToNative<K>(elements[0]);
        V value;
        for (size_t i = 0; i < NumAggregates; ++i) {
            value[i] = UnboxedToNative<typename V::TElement>(elements[1 + i]);
        }
        resultMap[key] += value;
    }

    VerifyMapsAreEqual<K, V, TMapImpl>(resultMap, refMap);
}

template<size_t Width>
size_t CountWideStreamOutputs(const NUdf::TUnboxedValue& wideStream)
{
    NUdf::TUnboxedValue resultValues[Width];

    Y_ENSURE(wideStream.IsBoxed());

    size_t lineCount = 0;
    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = wideStream.WideFetch(resultValues, Width)) != NUdf::EFetchStatus::Finish) {
        if (fetchStatus == NUdf::EFetchStatus::Ok) {
            ++lineCount;
        }
    }

    return lineCount;
}

class IDataSampler
{
public:
    virtual ~IDataSampler() {};

    virtual THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const = 0;
    virtual TType* GetKeyType(TProgramBuilder& pb) const = 0;
    virtual void ComputeReferenceResult(IWideStream& referenceStream) = 0;

    virtual void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const = 0;
    virtual void VerifyStreamVsReference(const NUdf::TUnboxedValue& wideStream) const = 0;
    virtual std::string Describe() const = 0;
};

template<typename TMapImpl, size_t NumAggregates>
class T6464DataSampler : public IDataSampler
{
public:
    TKVStream<ui64, ui64, false>::TSamples Samples;
    TMapImpl::TMapType Expects;

    size_t StreamNumIters = 0;

    T6464DataSampler(ui64 seed, size_t numSamples, size_t maxKey, size_t numIters)
        : Samples(MakeKeyed6464Samples(seed, numSamples, maxKey))
        , StreamNumIters(numIters)
    {
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const override
    {
        return THolder(new TKVStream<ui64, ui64, false, NumAggregates>(holderFactory, Samples, StreamNumIters));
    }

    TType* GetKeyType(TProgramBuilder& pb) const override
    {
        return pb.NewDataType(NUdf::TDataType<ui64>::Id);
    }

    void ComputeReferenceResult(IWideStream& referenceStream) override
    {
        Y_ENSURE(MapEmpty<TMapImpl>(Expects));

        ComputeReferenceStreamSumResult<ui64, TValueWrapper<ui64, NumAggregates>, NumAggregates, TMapImpl>(referenceStream, Expects);
    }

    void VerifyStreamVsReference(const NUdf::TUnboxedValue& wideStream) const override
    {
        VerifyStreamVsMap<TMapImpl, ui64, TValueWrapper<ui64, NumAggregates>, NumAggregates>(wideStream, Expects);
    }

    void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const override
    {
        VerifyListVsMap<TMapImpl, ui64, TValueWrapper<ui64, NumAggregates>, NumAggregates>(value, Expects);
    }

    std::string Describe() const override
    {
        return std::string("keys: ui64, values: ") + TValueWrapper<ui64, NumAggregates>::Describe();
    }
};

template<typename TMapImpl, size_t NumAggregates>
class TString64DataSampler : public IDataSampler
{
public:
    TKVStream<std::string, ui64, false, NumAggregates>::TSamples Samples;
    TMapImpl::TMapType Expects;

    size_t StreamNumIters = 0;
    bool LongStrings = false;

    TString64DataSampler(ui64 seed, size_t numSamples, size_t maxKey, size_t numIters, bool longStrings)
        : Samples(MakeKeyedString64Samples(seed, numSamples, maxKey, longStrings))
        , StreamNumIters(numIters)
        , LongStrings(longStrings)
    {
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const override
    {
        if (LongStrings) {
            return THolder(new TKVStream<std::string, ui64, false, NumAggregates>(holderFactory, Samples, StreamNumIters));
        } else {
            return THolder(new TKVStream<std::string, ui64, true, NumAggregates>(holderFactory, Samples, StreamNumIters));
        }
    }

    TType* GetKeyType(TProgramBuilder& pb) const override
    {
        return pb.NewDataType(NUdf::TDataType<char*>::Id);
    }

    void ComputeReferenceResult(IWideStream& referenceStream) override
    {
        Y_ENSURE(MapEmpty<TMapImpl>(Expects));

        ComputeReferenceStreamSumResult<std::string, TValueWrapper<ui64, NumAggregates>, NumAggregates, TMapImpl>(referenceStream, Expects);
    }

    void VerifyStreamVsReference(const NUdf::TUnboxedValue& wideStream) const override
    {
        VerifyStreamVsMap<TMapImpl, std::string, TValueWrapper<ui64, NumAggregates>, NumAggregates>(wideStream, Expects);
    }

    void VerifyComputedValueVsReference(const NUdf::TUnboxedValue& value) const override
    {
        VerifyListVsMap<TMapImpl, std::string, TValueWrapper<ui64, NumAggregates>, NumAggregates>(value, Expects);
    }

    std::string Describe() const override
    {
        std::string valueDescr = TValueWrapper<ui64, NumAggregates>::Describe();
        return Sprintf("keys: %s string, values: %s", LongStrings ? "Long (24 byte)" : "Embedded", valueDescr.c_str());
    }
};


template<typename K, typename V>
class TBlockKVStream : public IWideStream {
public:
    using TSamples = std::vector<std::pair<K, V>>;

    TBlockKVStream(const TComputationContext& ctx, const TSamples& samples, size_t iterations, size_t blockSize, const std::vector<TType*> types)
        : Context(ctx)
        , Samples(samples)
        , End(Samples.end())
        , MaxIterations(iterations)
        , CurrSample(Samples.begin())
        , BlockSize(blockSize)
        , Types(types)
    {
        Y_ENSURE(samples.size() > 0);
    }

private:
    const TComputationContext& Context;
    const TSamples& Samples;
    const TSamples::const_iterator End;
    const size_t MaxIterations;
    TSamples::const_iterator CurrSample;
    size_t Iteration = 0;
    size_t BlockSize = 1;
    const std::vector<TType*> Types;

    const TSamples::value_type* NextSample() {
        if (CurrSample == End) {
            ++Iteration;
            if (Iteration >= MaxIterations) {
                return nullptr;
            }
            CurrSample = Samples.begin();
        }
        return CurrSample++;
    }

    bool IsAtTheEnd() const {
        return (CurrSample == End) && (Iteration >= MaxIterations);
    }

public:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {
        if (width != 3) {
            ythrow yexception() << "width 3 expected";
        }

        TVector<std::unique_ptr<IArrayBuilder>> builders;
        std::transform(Types.cbegin(), Types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, Context.ArrowMemoryPool, BlockSize, &Context.Builder->GetPgBuilder());
        });

        size_t count = 0;
        for (; count < BlockSize; ++count) {
            const auto nextTuple = NextSample();
            if (!nextTuple) {
                break;
            }

            NUdf::TUnboxedValuePod key;
            NativeToUnboxed<false>(nextTuple->first, key);
            builders[0]->Add(key);
            key.DeleteUnreferenced();

            NUdf::TUnboxedValuePod value;
            NativeToUnboxed<false>(nextTuple->second, value);
            builders[1]->Add(value);
            value.DeleteUnreferenced();
        }

        if (count > 0) {
            const bool finish = IsAtTheEnd();
            result[0] = Context.HolderFactory.CreateArrowBlock(builders[0]->Build(finish));
            result[1] = Context.HolderFactory.CreateArrowBlock(builders[1]->Build(finish));
            result[2] = Context.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(count)));

            return NUdf::EFetchStatus::Ok;
        } else {
            return NUdf::EFetchStatus::Finish;
        }
    }
};

class IBlockSampler
{
public:
    virtual ~IBlockSampler() {};

    virtual THolder<IWideStream> MakeStream(const TComputationContext& ctx) const = 0;
    virtual TType* BuildKeyType(const TTypeEnvironment& env) const = 0;
    virtual TType* BuildValueType(const TTypeEnvironment& env) const = 0;

    virtual void ComputeRawResult() = 0;
    virtual void ComputeReferenceResult(TComputationContext& ctx) = 0;

    virtual void VerifyReferenceResultAgainstRaw() = 0;
    virtual void VerifyGraphResultAgainstReference(const NUdf::TUnboxedValue& blockList) = 0;
};

THolder<IDataSampler> CreateWideSamplerFromParams(const TRunParams& params);
THolder<IBlockSampler> CreateBlockSamplerFromParams(const TRunParams& params);

}
}
