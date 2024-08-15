#pragma once

#include <string>

namespace NYdb {

std::string GetStrFromEnv(const char* envVarName, const std::string& defaultValue = "");

} // namespace NYdb

