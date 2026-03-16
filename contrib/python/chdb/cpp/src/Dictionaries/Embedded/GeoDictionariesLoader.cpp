#include "GeoDictionariesLoader.h"

#include <CHDBPoco/Util/AbstractConfiguration.h>
#include "GeodataProviders/HierarchiesProvider.h"
#include "GeodataProviders/NamesProvider.h"

namespace DB_CHDB
{

std::unique_ptr<RegionsHierarchies> GeoDictionariesLoader::reloadRegionsHierarchies(const CHDBPoco::Util::AbstractConfiguration & config)
{
    static constexpr auto config_key = "path_to_regions_hierarchy_file";

    if (!config.has(config_key))
        return {};

    const auto default_hierarchy_file = config.getString(config_key);
    auto data_provider = std::make_unique<RegionsHierarchiesDataProvider>(default_hierarchy_file);
    return std::make_unique<RegionsHierarchies>(std::move(data_provider));
}

std::unique_ptr<RegionsNames> GeoDictionariesLoader::reloadRegionsNames(const CHDBPoco::Util::AbstractConfiguration & config)
{
    static constexpr auto config_key = "path_to_regions_names_files";

    if (!config.has(config_key))
        return {};

    const auto directory = config.getString(config_key);
    auto data_provider = std::make_unique<RegionsNamesDataProvider>(directory);
    return std::make_unique<RegionsNames>(std::move(data_provider));
}

}
