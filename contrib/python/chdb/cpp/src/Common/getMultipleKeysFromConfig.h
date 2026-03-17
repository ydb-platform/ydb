#pragma once
#include <string>
#include <vector>

namespace CHDBPoco
{
namespace Util
{
    class AbstractConfiguration;
}
}
namespace DB_CHDB
{
/// get all internal key names for given key
std::vector<std::string> getMultipleKeysFromConfig(const CHDBPoco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
/// Get all values for given key
std::vector<std::string> getMultipleValuesFromConfig(const CHDBPoco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
}
