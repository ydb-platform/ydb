#include "random.h"

#include <util/random/shuffle.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

ui64 TRandomGenerator::GenerateInteger()
{
    // Parameters are taken from http://en.wikipedia.org/wiki/Linear_congruential_generator
    // Taking modulo 2^64 is implemented via overflow in ui64
    Current_ = 6364136223846793005ll * Current_ + 1442695040888963407ll;
    return Current_;
}

double TRandomGenerator::GenerateDouble()
{
    // This formula is taken from util/random/mersenne64.h
    return (GenerateInteger() >> 11) * (1.0 / 9007199254740992.0);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<size_t> GetRandomIndexes(int size, int count)
{
    count = std::min(count, size);

    std::vector<size_t> result;
    if (count == 0) {
        return result;
    }

    result.reserve(count);

    if (count == 1) {
        result.push_back(RandomNumber<size_t>(size));
        return result;
    }

    // If count is close to max, shuffle the array
    // not to cause hashset allocations and excessive random generation iterations.
    if (size * 3 <= count * 4) {
        result.resize(size);
        std::iota(result.begin(), result.end(), 0);
        PartialShuffle(result.begin(), result.end(), count);
        result.resize(count);
        return result;
    }

    THashSet<int> uniqueValues;
    uniqueValues.reserve(count);
    while (std::ssize(uniqueValues) < count) {
        if (int value = RandomNumber<size_t>(size); uniqueValues.insert(value).second) {
            // Store to vector to preserve original random order.
            result.push_back(value);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
