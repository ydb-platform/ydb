#include "streams.h"

namespace NKikimr {
namespace NMiniKQL {

T6464Samples MakeKeyed6464Samples(const size_t numSamples, const unsigned int maxKey) {
    std::default_random_engine eng;
    std::uniform_int_distribution<unsigned int> keys(0, maxKey);
    std::uniform_int_distribution<uint64_t> unif(0, 100000.0);

    T6464Samples samples(numSamples);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            return std::make_pair<uint64_t, uint64_t>(keys(eng), unif(eng));
        }
    );

    return samples;
}

TString64Samples MakeKeyedString64Samples(const size_t numSamples, const unsigned int maxKey, const bool longStrings) {
    std::default_random_engine eng;
    std::uniform_int_distribution<unsigned int> keys(0, maxKey);
    std::uniform_int_distribution<uint64_t> unif(0, 100000.0);

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
            return std::make_pair<std::string, uint64_t>(std::move(strKey), unif(eng));
        }
    );

    return samples;
}

}
}