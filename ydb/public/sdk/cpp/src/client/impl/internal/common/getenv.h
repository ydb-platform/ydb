#pragma once

#include <string>

namespace NYdb::inline Dev {

std::string GetStrFromEnv(const char* envVarName, const std::string& defaultValue = "");

} // namespace NYdb

