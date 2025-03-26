#ifdef __unix__
#include <sys/resource.h>
#endif
#include <util/generic/yexception.h>

#include "mem_limit.h"

namespace NYql {

void SetAddressSpaceLimit(ui64 memLimit) {
    if (memLimit) {
        #ifdef __unix__
            auto memLimitBytes = memLimit * 1024 * 1024;

            struct rlimit rl;
            if (getrlimit(RLIMIT_AS, &rl)) {
                throw TSystemError() << "Cannot getrlimit(RLIMIT_AS)";
            }

            rl.rlim_cur = memLimitBytes;
            if (setrlimit(RLIMIT_AS, &rl)) {
                throw TSystemError() << "Cannot setrlimit(RLIMIT_AS) to " << memLimitBytes << " bytes";
            }
        #else
            throw yexception() << "Memory limit can not be set on this platfrom";
        #endif
    }
}

} // namespace NYql
