#include "primes.h"

#include <util/generic/array_size.h>
#include <util/generic/algorithm.h>

namespace NKikimr {

static const unsigned long PRIMES[] = {
#include "primes.gen"
};

unsigned long FindNearestPrime(unsigned long num) {
    if (num <= *PRIMES) {
        return *PRIMES;
    }

    return *LowerBound(PRIMES, PRIMES + Y_ARRAY_SIZE(PRIMES) - 1, num);
}

}
