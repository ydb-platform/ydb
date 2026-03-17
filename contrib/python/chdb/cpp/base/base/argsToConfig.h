#pragma once

#include <CHDBPoco/Util/Application.h>
#include <string>
#include <unordered_set>

namespace CHDBPoco::Util
{
class LayeredConfiguration; // NOLINT(cppcoreguidelines-virtual-class-destructor)
}

/// Import extra command line arguments to configuration. These are command line arguments after --.
void argsToConfig(const CHDBPoco::Util::Application::ArgVec & argv,
                  CHDBPoco::Util::LayeredConfiguration & config,
                  int priority,
                  const std::unordered_set<std::string>* registered_alias_names = nullptr);
