#pragma once

#include "interface.h"

namespace NYql::NPureCalc {
void InitLogging(const TLoggingOptions& options);
void EnsureLoggingInitialized();
} // namespace NYql::NPureCalc
