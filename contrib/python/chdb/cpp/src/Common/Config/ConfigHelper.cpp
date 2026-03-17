#include <Common/Config/ConfigHelper.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <CHDBPoco/Util/XMLConfiguration.h>


namespace DB_CHDB
{

namespace ConfigHelper
{

namespace
{
    void cloneImpl(CHDBPoco::Util::AbstractConfiguration & dest, const CHDBPoco::Util::AbstractConfiguration & src, const std::string & prefix = "")
    {
        std::vector<std::string> keys;
        src.keys(prefix, keys);
        if (!keys.empty())
        {
            std::string prefix_with_dot = prefix + ".";
            for (const auto & key : keys)
                cloneImpl(dest, src, prefix_with_dot + key);
        }
        else if (!prefix.empty())
        {
            dest.setString(prefix, src.getRawString(prefix));
        }
    }
}


CHDBPoco::AutoPtr<CHDBPoco::Util::AbstractConfiguration> clone(const CHDBPoco::Util::AbstractConfiguration & src)
{
    CHDBPoco::AutoPtr<CHDBPoco::Util::AbstractConfiguration> res(new CHDBPoco::Util::XMLConfiguration());
    cloneImpl(*res, src);
    return res;
}

bool getBool(const CHDBPoco::Util::AbstractConfiguration & config, const std::string & key, bool default_, bool empty_as)
{
    if (!config.has(key))
        return default_;
    CHDBPoco::Util::AbstractConfiguration::Keys sub_keys;
    config.keys(key, sub_keys);
    if (sub_keys.empty() && config.getString(key).empty())
        return empty_as;
    return config.getBool(key, default_);
}

}

}
