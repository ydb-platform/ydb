#include "proc_alive.h"

#include <util/system/platform.h>
#include <util/system/compat.h>
#include <util/system/error.h>
#include <util/system/winint.h>

#include <errno.h>


namespace NYql {

bool IsProcessAlive(TProcessId pid) {
#ifdef _unix_
    // If sending a null signal fails with the error ESRCH, then we know
    // the process doesn’t exist. If the call fails with the error
    // EPERM - the process exists, but we don’t have permission to send
    // a signal to it.
    kill(pid, 0);
    return errno != ESRCH;
#elif defined(_win_)
    HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, pid);
    if (process == NULL) {
        return false;
    }

    DWORD ret = WaitForSingleObject(process, 0);
    CloseHandle(process);
    return ret == WAIT_TIMEOUT;
#else
    Y_UNUSED(pid);
    return false;
#endif
}

} // NYql

