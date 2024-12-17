#pragma once

#include <util/generic/string.h>

namespace NYql {

// Use NYT::ILogger::ELevel for level
void SetYtLoggerGlobalBackend(int level, size_t debugLogBufferSize = 0, const TString& debugLogFile = TString(), bool debugLogAlwaysWrite = false);
void FlushYtDebugLog();
void DropYtDebugLog();

} // NYql
