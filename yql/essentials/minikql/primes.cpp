#include "primes.h"

#include <util/generic/array_size.h>
#include <util/generic/algorithm.h>

#include <array>

namespace NKikimr {

static const auto PRIMES = std::to_array<unsigned long>({
#include "primes.gen"
});

unsigned long FindNearestPrime(unsigned long num) {
    if (num <= *PRIMES.data()) {
        return *PRIMES.data();
    }

    return *LowerBound(PRIMES.data(), PRIMES.data() + PRIMES.size() - 1, num);
}

} // namespace NKikimr
