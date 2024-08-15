#include "getenv.h"

#include <stdlib.h>

namespace NYdb {

std::string GetStrFromEnv(const char* envVarName, const std::string& defaultValue) {
    auto envVarPointer = getenv(envVarName);
    return envVarPointer ? std::string(envVarPointer) : defaultValue;
}

} // namespace NYdb