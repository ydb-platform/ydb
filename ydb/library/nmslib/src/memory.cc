/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include "memory.h"
#include "logging.h"


#ifdef __linux

#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>   

#endif

#ifdef _MSC_VER

#include <windows.h>
#include <Psapi.h>

#endif

namespace similarity {

MemUsage::MemUsage() {
#ifdef __linux
            int pid = getpid();
            snprintf(status_file_, sizeof(status_file_),
                "/proc/%d/status", pid);
#endif
}

double 
MemUsage::get_vmsize() {
#ifdef __linux
    FILE* f = fopen(status_file_, "rt");
    if (!f) {
        return -1.0;
    }
    char buf[100];
    int vmsize = -1024;
    while (fgets(buf, sizeof(buf), f)) {
        if (strncmp(buf, "VmSize:", 7) == 0) {
            sscanf(buf + 7, "%d", &vmsize);
            break;
        }
    }
    fclose(f);
    return vmsize / 1024.0;
#endif
#ifdef _MSC_VER
    PROCESS_MEMORY_COUNTERS memCounter;
    bool result = GetProcessMemoryInfo(GetCurrentProcess(),
        &memCounter,
        sizeof(memCounter));

    /*
     * This seems to be a decent estimates of currently used 
     * RESIDENT memory size
     * see, e.g., http://social.msdn.microsoft.com/Forums/vstudio/en-US/bda6a289-42e7-41f7-a42b-6a89b623c89c/process-ram-usage-in-visual-c?forum=vcgeneral
     */
    return memCounter.WorkingSetSize / 1024.0 / 1024.0;
#endif
    return -1.0;
}



}
