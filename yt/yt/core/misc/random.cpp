#include "random.h"

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

} // namespace NYT
