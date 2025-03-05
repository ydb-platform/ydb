#pragma once

#include <string>

namespace NYdb::inline V3 {

std::string GetStrFromEnv(const char* envVarName, const std::string& defaultValue = "");

} // namespace NYdb

