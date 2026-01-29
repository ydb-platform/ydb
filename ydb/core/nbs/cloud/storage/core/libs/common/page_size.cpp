#include "page_size.h"

#include <unistd.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

size_t GetPlatformPageSize()
{
    const static size_t size = []() -> size_t
    {
        const long result = sysconf(_SC_PAGESIZE);
        if (result <= 0) {
            return 4096;
        }
        return result;
    }();

    return size;
}

}   // namespace NCloud
