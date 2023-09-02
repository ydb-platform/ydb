#include "bernoulli_sampler.h"

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBernoulliSampler::TBernoulliSampler(
    std::optional<double> samplingRate,
    std::optional<ui64> seed)
{
    if (samplingRate) {
        SamplingRate_ = samplingRate;
        Seed_ = seed;
        Distribution_ = std::bernoulli_distribution(*SamplingRate_);
        if (seed) {
            Generator_ = std::mt19937(*seed);
        }
    }
}

bool TBernoulliSampler::Sample()
{
    if (!SamplingRate_) {
        return true;
    }

    return Distribution_(Generator_);
}

bool TBernoulliSampler::Sample(ui64 salt)
{
    if (!SamplingRate_) {
        return true;
    }

    std::minstd_rand0 generator(FarmFingerprint(salt ^ Seed_.value_or(0)));
    return Distribution_(generator);
}

void TBernoulliSampler::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SamplingRate_);
    Persist(context, Seed_);
    // TODO(max42): Understand which type properties should make this possible
    // and fix TPodSerializer instead of doing this utter garbage.
    #define SERIALIZE_AS_POD(field) do { \
        std::vector<char> bytes; \
        if (context.IsLoad()) { \
            Persist(context, bytes); \
            YT_VERIFY(sizeof(field) == bytes.size()); \
            memcpy(&field, bytes.data(), sizeof(field)); \
        } else { \
            bytes.resize(sizeof(field)); \
            memcpy(bytes.data(), &field, sizeof(field)); \
            Persist(context, bytes); \
        } \
    } while (0)
    SERIALIZE_AS_POD(Generator_);
    SERIALIZE_AS_POD(Distribution_);
    #undef SERIALIZE_AS_POD
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
