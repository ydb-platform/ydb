#include <Common/getMultipleKeysFromConfig.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace NDB
{
std::vector<std::string> getMultipleKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name)
{
    std::vector<std::string> values;
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(root, config_keys);
    for (const auto & key : config_keys)
    {
        if (key != name && !(key.starts_with(name + "[") && key.ends_with("]")))
            continue;
        values.emplace_back(key);
    }
    return values;
}


std::vector<std::string> getMultipleValuesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name)
{
    std::vector<std::string> values;
    for (const auto & key : NDB::getMultipleKeysFromConfig(config, root, name))
        values.emplace_back(config.getString(root.empty() ? key : root + "." + key));
    return values;
}

}
