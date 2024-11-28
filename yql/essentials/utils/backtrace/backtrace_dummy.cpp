#include "backtrace_lib.h"

#include <util/system/backtrace.h>

namespace NYql {
    namespace NBacktrace {
        size_t CollectBacktrace(void** addresses, size_t limit, void*) {
            return BackTrace(addresses, limit);
        }
    }
}