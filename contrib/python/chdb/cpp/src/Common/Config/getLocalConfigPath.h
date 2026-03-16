#pragma once

#include <string>
#include <optional>

namespace DB_CHDB
{

/// Return path to existing configuration file.
std::optional<std::string> getLocalConfigPath(const std::string & home_path);

}
