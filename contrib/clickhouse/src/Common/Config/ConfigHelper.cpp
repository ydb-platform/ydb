#include <Common/Config/ConfigHelper.h>
#include <DBPoco/Util/AbstractConfiguration.h>
#include <DBPoco/Util/XMLConfiguration.h>


namespace DB
{

namespace ConfigHelper
{

namespace
{
    void cloneImpl(DBPoco::Util::AbstractConfiguration & dest, const DBPoco::Util::AbstractConfiguration & src, const std::string & prefix = "")
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


DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> clone(const DBPoco::Util::AbstractConfiguration & src)
{
    DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> res(new DBPoco::Util::XMLConfiguration());
    cloneImpl(*res, src);
    return res;
}

DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> createEmpty()
{
    return new DBPoco::Util::XMLConfiguration();
}

bool getBool(const DBPoco::Util::AbstractConfiguration & config, const std::string & key, bool default_, bool empty_as)
{
    if (!config.has(key))
        return default_;
    DBPoco::Util::AbstractConfiguration::Keys sub_keys;
    config.keys(key, sub_keys);
    if (sub_keys.empty() && config.getString(key).empty())
        return empty_as;
    return config.getBool(key, default_);
}

}

}
