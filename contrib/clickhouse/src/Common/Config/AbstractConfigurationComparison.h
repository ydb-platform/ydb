#pragma once

#include <base/types.h>
#include <unordered_set>

namespace DBPoco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
    /// Returns true if two configurations contains the same keys and values.
    /// NOTE: These functions assume no configuration has items having both children and a value
    /// (i.e. items like "<test>value<child1/></test>").
    bool isSameConfiguration(const DBPoco::Util::AbstractConfiguration & left,
                             const DBPoco::Util::AbstractConfiguration & right);

    /// Config may have multiple keys with one name. For example:
    /// <root>
    ///     <some_key>...</some_key>
    ///     <some_key>...</some_key>
    /// </root>
    /// Returns true if the specified subview of the two configurations contains
    /// the same keys and values for each key with the given name.
    bool isSameConfigurationWithMultipleKeys(const DBPoco::Util::AbstractConfiguration & left,
                                             const DBPoco::Util::AbstractConfiguration & right,
                                             const String & root, const String & name);

    /// Returns true if the specified subview of the two configurations contains the same keys and values.
    bool isSameConfiguration(const DBPoco::Util::AbstractConfiguration & left,
                             const DBPoco::Util::AbstractConfiguration & right,
                             const String & key);

    /// Returns true if specified subviews of the two configurations contains the same keys and values.
    /// If `ignore_keys` is specified then the function skips those keys while comparing
    /// (even if their values differ, they're considered to be the same.)
    bool isSameConfiguration(const DBPoco::Util::AbstractConfiguration & left, const String & left_key,
                             const DBPoco::Util::AbstractConfiguration & right, const String & right_key,
                             const std::unordered_set<std::string_view> & ignore_keys = {});

    inline bool operator==(const DBPoco::Util::AbstractConfiguration & left, const DBPoco::Util::AbstractConfiguration & right)
    {
        return isSameConfiguration(left, right);
    }

    inline bool operator!=(const DBPoco::Util::AbstractConfiguration & left, const DBPoco::Util::AbstractConfiguration & right)
    {
        return !isSameConfiguration(left, right);
    }
}
