#pragma once

#include <string>
#include "Types.h"

namespace DB_CHDB
{

struct RegionEntry
{
    RegionID id;
    RegionID parent_id;
    RegionType type;
    RegionDepth depth;
    RegionPopulation population;
};

struct RegionNameEntry
{
    RegionID id;
    std::string name;
};

}
