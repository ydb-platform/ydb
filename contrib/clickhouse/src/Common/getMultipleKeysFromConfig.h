#pragma once
#include <string>
#include <vector>

namespace DBPoco
{
namespace Util
{
    class AbstractConfiguration;
}
}
namespace DB
{
/// get all internal key names for given key
std::vector<std::string> getMultipleKeysFromConfig(const DBPoco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
/// Get all values for given key
std::vector<std::string> getMultipleValuesFromConfig(const DBPoco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
}
