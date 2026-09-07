#pragma once

#include <util/generic/string.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>
#include <util/thread/singleton.h>

#include <random>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

template <class T, T Min, T Max>
struct TRandomGenerator
{
    std::random_device Device;
    std::mt19937 Generator;
    std::uniform_int_distribution<T> Distribution;

    inline TRandomGenerator()
        : Generator(Device())
        , Distribution(Min, Max)
    {}

    inline T Get()
    {
        return Distribution(Generator);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, T Min = Min<T>(), T Max = Max<T>(), size_t Prio = 2>
T RandInt()
{
    return FastTlsSingletonWithPriority<TRandomGenerator<T, Min, Max>, Prio>()
        ->Get();
}

TString SafeCreateGuidAsString();

}   // namespace NYdb::NBS
