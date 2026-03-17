#pragma once

#include <DBPoco/AutoPtr.h>
#include <string>


namespace DBPoco::Util
{
    class AbstractConfiguration;
}


namespace DB::ConfigHelper
{

/// Clones a configuration.
/// NOTE: This function assumes the source configuration doesn't have items having both children and a value
/// (i.e. items like "<test>value<child1/></test>").
DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> clone(const DBPoco::Util::AbstractConfiguration & src);

DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> createEmpty();

/// The behavior is like `config.getBool(key, default_)`,
/// except when the tag is empty (aka. self-closing), `empty_as` will be used instead of throwing DBPoco::Exception.
bool getBool(const DBPoco::Util::AbstractConfiguration & config, const std::string & key, bool default_ = false, bool empty_as = true);

}
