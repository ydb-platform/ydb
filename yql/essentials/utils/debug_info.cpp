#include "debug_info.h"

#include <util/system/thread.h>
#include <util/system/tls.h>
#include <util/stream/file.h>
#include <util/generic/string.h>

#include <string.h>


namespace NYql {

static const size_t OPERATION_ID_MAX_LENGTH = 24;
static const size_t THREAD_NAME_MAX_LENGTH = 16;


struct TDebugInfo {
    char OperationId[OPERATION_ID_MAX_LENGTH + 1];
};

Y_POD_THREAD(TDebugInfo) TlsDebugInfo;


void SetCurrentOperationId(const char* operationId) {
    size_t len = strlcpy(
            (&TlsDebugInfo)->OperationId,
            operationId,
            OPERATION_ID_MAX_LENGTH);

    const char* threadName = nullptr;
    if (len > THREAD_NAME_MAX_LENGTH) {
        threadName = operationId + (len - THREAD_NAME_MAX_LENGTH + 1);
    } else {
        threadName = operationId;
    }
    TThread::SetCurrentThreadName(threadName);
}

long GetRunnigThreadsCount() {
    TString procStat = TFileInput("/proc/self/stat").ReadAll();
    long num_threads = -2;         // Number of threads in this process (since Linux 2.6)

    int n = sscanf(procStat.data(),
        "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %*u %*u %*d %*d %*d %*d %ld",
        &num_threads);

    return n == 1 ? num_threads : -2;
}

} // namespace NYql
