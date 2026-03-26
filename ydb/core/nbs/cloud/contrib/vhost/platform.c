#include <errno.h>
#include <unistd.h>

#include "platform.h"

int init_platform_page_size(void)
{
    if (!platform_page_size) {
        long result = sysconf(_SC_PAGESIZE);
        if (result < 0) {
            return errno;
        }
        platform_page_size = result;
    }

    return 0;
}
