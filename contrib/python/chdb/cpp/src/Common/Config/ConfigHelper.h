#pragma once

#include <CHDBPoco/AutoPtr.h>
#include <string>


namespace CHDBPoco::Util
{
    class AbstractConfiguration;
}


namespace DB_CHDB::ConfigHelper
{

/// Clones a configuration.
/// NOTE: This function assumes the source configuration doesn't have items having both children and a value
/// (i.e. items like "<test>value<child1/></test>").
CHDBPoco::AutoPtr<CHDBPoco::Util::AbstractConfiguration> clone(const CHDBPoco::Util::AbstractConfiguration & src);

/// The behavior is like `config.getBool(key, default_)`,
/// except when the tag is empty (aka. self-closing), `empty_as` will be used instead of throwing CHDBPoco::Exception.
bool getBool(const CHDBPoco::Util::AbstractConfiguration & config, const std::string & key, bool default_ = false, bool empty_as = true);

}
