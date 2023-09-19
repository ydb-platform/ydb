#pragma once

#include <yt/yt/core/misc/common.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRandomCharGenerator
{
    std::default_random_engine Engine;
    std::uniform_int_distribution<int> Uniform;

    explicit TRandomCharGenerator(size_t seed)
        : Engine(seed)
        , Uniform(0, 'z' - 'a' + '9' - '0' + 1)
    { }

    char operator() ()
    {
        char symbol = Uniform(Engine);
        auto result = symbol >= 10 ? symbol - 10 + 'a' : symbol + '0';
        YT_VERIFY((result <= 'z' &&  result >= 'a') || (result <= '9' &&  result >= '0'));
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
