#include "streams.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

namespace NKikimr {
namespace NMiniKQL {

T6464Samples MakeKeyed6464Samples(const ui64 seed, const size_t numSamples, const unsigned int maxKey) {
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, 100000.0);

    size_t currKey = 0;

    T6464Samples samples(numSamples);

    eng.seed(seed);
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            if (currKey > maxKey) {
                currKey = 0;
            }
            return std::make_pair<ui64, ui64>(currKey++, unif(eng));
        }
    );

    std::shuffle(samples.begin(), samples.end(), eng);

    return samples;
}

TString64Samples MakeKeyedString64Samples(const ui64 seed, const size_t numSamples, const unsigned int maxKey, const bool longStrings) {
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, 100000.0);

    size_t currKey = 0;

    TString64Samples samples(numSamples);

    eng.seed(seed);
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            auto key = (currKey++);
            if (currKey > maxKey) {
                currKey = 0;
            }
            std::string strKey;
            if (!longStrings) {
                strKey = std::string(ToString(key));
            } else {
                strKey = Sprintf("%08u.%08u.%08u.", key, key, key);
            }
            return std::make_pair<std::string, ui64>(std::move(strKey), unif(eng));
        }
    );

    std::shuffle(samples.begin(), samples.end(), eng);

    return samples;
}

template<typename K, typename V, typename R, typename Next>
THolder<R> DispatchByMap(EHashMapImpl implType, Next&& next)
{
    if (implType == EHashMapImpl::UnorderedMap) {
        return next(TUnorderedMapImpl<K, V>());
    } else {
        return next(TAbslMapImpl<K, V>());
    }
}

THolder<IDataSampler> CreateWideSamplerFromParams(const TRunParams& params)
{
    Y_ENSURE(params.RandomSeed.has_value());

    switch (params.SamplerType) {
    case ESamplerType::StringKeysUI64Values:
        return DispatchByMap<std::string, ui64, IDataSampler>(params.ReferenceHashType, [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = TString64DataSampler<MapImpl>;
            return MakeHolder<SamplerType>(*params.RandomSeed, params.RowsPerRun, params.NumKeys - 1, params.NumRuns, params.LongStringKeys);
        });
    case ESamplerType::UI64KeysUI64Values:
        return DispatchByMap<ui64, ui64, IDataSampler>(params.ReferenceHashType, [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = T6464DataSampler<MapImpl>;
            return MakeHolder<SamplerType>(*params.RandomSeed, params.RowsPerRun, params.NumKeys - 1, params.NumRuns);
        });
    }
}

template<typename TMapType, typename K>
struct TUpdateMapFromBlocks
{
    static void Update(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, TMapType& result);
};

template<typename TMapType>
struct TUpdateMapFromBlocks<TMapType, ui64>
{
    static void Update(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, TMapType& result)
    {

        auto datumKey = TArrowBlock::From(key).GetDatum();
        auto datumValue = TArrowBlock::From(value).GetDatum();
        UNIT_ASSERT(datumKey.is_array());
        UNIT_ASSERT(datumValue.is_array());

        const auto ui64keys = datumKey.template array_as<arrow::UInt64Array>();
        const auto ui64values = datumValue.template array_as<arrow::UInt64Array>();
        UNIT_ASSERT(!!ui64keys);
        UNIT_ASSERT(!!ui64values);
        UNIT_ASSERT_VALUES_EQUAL(ui64keys->length(), ui64values->length());

        const size_t length = ui64keys->length();
        for (size_t i = 0; i < length; ++i) {
            result[ui64keys->Value(i)] += ui64values->Value(i);
        }
    }
};

template<typename TMapType>
struct TUpdateMapFromBlocks<TMapType, std::string>
{
    static void Update(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, TMapType& result)
    {
        auto datumKey = TArrowBlock::From(key).GetDatum();
        auto datumValue = TArrowBlock::From(value).GetDatum();
        UNIT_ASSERT(datumKey.is_arraylike());
        UNIT_ASSERT(datumValue.is_array());

        const auto ui64values = datumValue.template array_as<arrow::UInt64Array>();
        UNIT_ASSERT(!!ui64values);

        int64_t valueOffset = 0;

        for (const auto& chunk : datumKey.chunks()) {
            auto* barray = dynamic_cast<arrow::BinaryArray*>(chunk.get());
            UNIT_ASSERT(barray != nullptr);
            for (int64_t i = 0; i < barray->length(); ++i) {
                auto key = barray->GetString(i);
                auto val = ui64values->Value(valueOffset);
                result[key] += val;
                ++valueOffset;
            }
        }
    }
};

template<typename T>
TType* GetVerySimpleDataType(const TTypeEnvironment& env)
{
    return TDataType::Create(NUdf::TDataType<T>::Id, env);
}

template<>
TType* GetVerySimpleDataType<std::string>(const TTypeEnvironment& env)
{
    return TDataType::Create(NUdf::TDataType<char*>::Id, env);
}

template<typename K, typename V, typename TMapImpl>
class TBlockSampler : public IBlockSampler
{
    using TSamples = TBlockKVStream<K, V>::TSamples;

public:
    // TODO: Also store the TTypeEnvironment& and K/V TTypes? It's shared between multiple stream runs.

    TBlockSampler(const TRunParams& params, TSamples&& samples)
        : Samples(std::move(samples))
        , NumIters(params.NumRuns)
        , BlockSize(params.BlockSize)
    {
    }

    THolder<IWideStream> MakeStream(const TComputationContext& ctx) const override
    {
        std::vector<TType*> types {BuildKeyType(ctx.TypeEnv), BuildValueType(ctx.TypeEnv)};
        return MakeHolder<TBlockKVStream<K, V>>(
            ctx,
            Samples,
            NumIters,
            BlockSize,
            std::move(types)
        );
    }

    TType* BuildKeyType(const TTypeEnvironment& env) const override
    {
        return GetVerySimpleDataType<K>(env);
    }

    TType* BuildValueType(const TTypeEnvironment& env) const override
    {
        return GetVerySimpleDataType<V>(env);
    }

    void ComputeRawResult() override
    {
        Y_ENSURE(RawResult.empty());

        for (const auto& tuple : Samples) {
            RawResult[tuple.first] += (tuple.second * NumIters);
        }
    }

    void ComputeReferenceResult(TComputationContext& ctx) override
    {
        Y_ENSURE(RefResult.empty());

        const THolder<IWideStream> refStreamPtr = MakeStream(ctx);
        IWideStream& refStream = *refStreamPtr;

        NUdf::TUnboxedValue columns[3];

        while (refStream.WideFetch(columns, 3) == NUdf::EFetchStatus::Ok) {
            TUpdateMapFromBlocks<typename TMapImpl::TMapType, K>::Update(columns[0], columns[1], RefResult);
        }
    }

    void VerifyReferenceResultAgainstRaw() override
    {
        Y_ENSURE(!RefResult.empty());
        Y_ENSURE(!RawResult.empty());

        // TODO: Replace UNIT_ASSERTS with something else, or actually set up the unit test thread context
        UNIT_ASSERT_VALUES_EQUAL(RefResult.size(), RawResult.size());
        for (const auto& tuple : RawResult) {
            auto otherIt = RefResult.find(tuple.first);
            UNIT_ASSERT(otherIt != RefResult.end());
            UNIT_ASSERT_VALUES_EQUAL(tuple.second, otherIt->second);
        }
    }

    void VerifyGraphResultAgainstReference(const NUdf::TUnboxedValue& blockList) override
    {
        Y_ENSURE(!RefResult.empty());

        size_t numResultItems = blockList.GetListLength();
        Cerr << "Result block count: " << numResultItems << Endl;

        typename TMapImpl::TMapType graphResult;

        const auto ptr = blockList.GetElements();
        for (size_t i = 0ULL; i < numResultItems; ++i) {
            // TODO: Replace UNIT_ASSERTS with something else, or actually set up the unit test thread context
            UNIT_ASSERT(ptr[i].GetListLength() >= 2);

            const auto elements = ptr[i].GetElements();

            TUpdateMapFromBlocks<typename TMapImpl::TMapType, K>::Update(elements[0], elements[1], graphResult);
        }

        UNIT_ASSERT_VALUES_EQUAL(RefResult.size(), graphResult.size());
        for (const auto& tuple : RefResult) {
            auto graphIt = graphResult.find(tuple.first);
            UNIT_ASSERT(graphIt != graphResult.end());
            UNIT_ASSERT_VALUES_EQUAL(tuple.second, graphIt->second);
        }
    }

private:
    TSamples Samples;
    size_t NumIters;
    size_t BlockSize;

    TMapImpl::TMapType RawResult;
    TMapImpl::TMapType RefResult;
};

THolder<IBlockSampler> CreateBlockSamplerFromParams(const TRunParams& params)
{
    Y_ENSURE(params.RandomSeed.has_value());

    switch(params.SamplerType) {
    case ESamplerType::StringKeysUI64Values:
        return DispatchByMap<std::string, ui64, IBlockSampler>(params.ReferenceHashType, [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = TBlockSampler<std::string, ui64, MapImpl>;
            return MakeHolder<SamplerType>(
                params,
                MakeKeyedString64Samples(*params.RandomSeed, params.RowsPerRun, params.NumKeys - 1, params.LongStringKeys));
        });
    case ESamplerType::UI64KeysUI64Values:
        return DispatchByMap<ui64, ui64, IBlockSampler>(params.ReferenceHashType, [&](auto&& impl) {
            using MapImpl = std::decay_t<decltype(impl)>;
            using SamplerType = TBlockSampler<ui64, ui64, MapImpl>;
            return MakeHolder<SamplerType>(
                params,
                MakeKeyed6464Samples(*params.RandomSeed, params.RowsPerRun, params.NumKeys - 1));
        });
    }
}

}
}