#pragma once

#include "interface.h"

namespace NYql {
    namespace NPureCalc {
        void InitLogging(const TLoggingOptions& options);
        void EnsureLoggingInitialized();
    }
}
