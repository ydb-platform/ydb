#include "getenv.h"

#include <stdlib.h>

namespace NYdb {

TStringType GetStrFromEnv(const char* envVarName, const TStringType& defaultValue) {
    auto envVarPointer = getenv(envVarName);
    return envVarPointer ? TStringType(envVarPointer) : defaultValue;
}

} // namespace NYdb