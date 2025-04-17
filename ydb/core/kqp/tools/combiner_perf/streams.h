// Sample data generators and streams

#pragma once

#include "converters.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

using T6464Samples = std::vector<std::pair<ui64, ui64>>;
T6464Samples MakeKeyed6464Samples(const size_t numSamples, const unsigned int maxKey);

using TString64Samples = std::vector<std::pair<std::string, ui64>>;
TString64Samples MakeKeyedString64Samples(const size_t numSamples, const unsigned int maxKey, const bool longStrings);

struct IWideStream : public NUdf::TBoxedValue
{
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final override {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override = 0;
};

template<typename K, typename V, bool EmbeddedKeys>
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
    TKVStream<ui64, ui64, false>::TSamples Samples;
    std::unordered_map<ui64, ui64> Expects;

    size_t StreamNumIters = 0;

    T6464DataSampler(size_t numSamples, size_t maxKey, size_t numIters)
        : Samples(MakeKeyed6464Samples(numSamples, maxKey))
        , StreamNumIters(numIters)
    {
    }

    THolder<IWideStream> MakeStream(const THolderFactory& holderFactory) const override
    {
        return THolder(new TKVStream<ui64, ui64, false>(holderFactory, Samples, StreamNumIters));
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
    TKVStream<std::string, ui64, false>::TSamples Samples;
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
            return THolder(new TKVStream<std::string, ui64, false>(holderFactory, Samples, StreamNumIters));
        } else {
            return THolder(new TKVStream<std::string, ui64, true>(holderFactory, Samples, StreamNumIters));
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


template<typename K, typename V>
struct TBlockKVStream : public IWideStream {
    using TSamples = std::vector<std::pair<K, V>>;

    TBlockKVStream(TComputationContext& ctx, const TSamples& samples, size_t iterations, size_t blockSize, const std::vector<TType*> types)
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
    TComputationContext& Context;
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

            NUdf::TUnboxedValue key;
            NativeToUnboxed<false>(nextTuple->first, key);
            builders[0]->Add(key);

            NUdf::TUnboxedValue value;
            NativeToUnboxed<false>(nextTuple->second, value);
            builders[1]->Add(value);
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

}
}
