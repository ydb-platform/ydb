#pragma once

#include <memory>
#include "RegionsHierarchies.h"
#include "RegionsNames.h"

#include <CHDBPoco/Util/AbstractConfiguration.h>

namespace DB_CHDB
{

// Default implementation of geo dictionaries loader used by native server application
class GeoDictionariesLoader
{
public:
    static std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(const CHDBPoco::Util::AbstractConfiguration & config);
    static std::unique_ptr<RegionsNames> reloadRegionsNames(const CHDBPoco::Util::AbstractConfiguration & config);
};

}
