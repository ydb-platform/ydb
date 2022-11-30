#include "mlock.h"

#include <stdio.h>
#include <sys/mman.h>
#include <stdint.h>
#include <inttypes.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void PopulateFile(void* ptr, size_t size)
{
    constexpr size_t PageSize = 4096;

    auto* begin = static_cast<volatile char*>(ptr);
    for (auto* current = begin; current < begin + size; current += PageSize) {
        *current;
    }
}

bool MlockFileMappings(bool populate)
{
    auto* file = ::fopen("/proc/self/maps", "r");
    if (!file) {
        return false;
    }

    // Each line of /proc/<pid>/smaps has the following format:
    // address           perms offset  dev   inode   path
    // E.g.
    // 08048000-08056000 r-xp 00000000 03:0c 64593   /usr/sbin/gpm

    bool failed = false;
    while (true) {
        char line[1024];
        if (!fgets(line, sizeof(line), file)) {
            break;
        }

        char addressStr[64];
        char permsStr[64];
        char offsetStr[64];
        char devStr[64];
        int inode;
        if (sscanf(line, "%s %s %s %s %d",
            addressStr,
            permsStr,
            offsetStr,
            devStr,
            &inode) != 5)
        {
            continue;
        }

        if (inode == 0) {
            continue;
        }

        if (permsStr[0] != 'r') {
            continue;
        }

        uintptr_t startAddress;
        uintptr_t endAddress;
        if (sscanf(addressStr, "%" PRIx64 "-%" PRIx64,
                &startAddress,
                &endAddress) != 2)
        {
            continue;
        }

        if (::mlock(reinterpret_cast<const void*>(startAddress), endAddress - startAddress) != 0) {
            failed = true;
            continue;
        }

        if (populate) {
            PopulateFile(reinterpret_cast<void*>(startAddress), endAddress - startAddress);
        }
    }

    ::fclose(file);
    return !failed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
