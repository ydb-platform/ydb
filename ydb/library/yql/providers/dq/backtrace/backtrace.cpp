#include "backtrace.h"

#include <library/cpp/malloc/api/malloc.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <util/system/mlock.h>
#include <util/system/backtrace.h>
#include <util/stream/output.h>
#include <util/string/printf.h>

#ifdef _linux_
#include <dlfcn.h>
#include <link.h>
#endif

namespace NYql {

namespace NBacktrace {

namespace {

TAtomic BacktraceStarted = 0;
void* Stack[300];
THashMap<TString, TString> Mapping;

struct TDllInfo {
    TString Path;
    ui64 BaseAddress;
};

#ifdef _linux_
int DlIterCallback(struct dl_phdr_info *info, size_t size, void *data)
{
    Y_UNUSED(size);
    Y_UNUSED(data);
    if (*info->dlpi_name) {
        TDllInfo dllInfo{ info->dlpi_name, (ui64)info->dlpi_addr };
        ((THashMap<TString, TDllInfo>*)data)->emplace(dllInfo.Path, dllInfo);
    }

    return 0;
}
#endif

} /* namespace */

void PrintBacktraceToStderr(int signum)
{
    if (!NMalloc::IsAllocatorCorrupted) {
        /* we want memory allocation for backtrace printing */

        if (!AtomicTryLock(&BacktraceStarted)) {
            return;
        }

        // Unlock memory to avoid extra RSS consumption while touching debug info
        UnlockAllMemory();

        const size_t s = BackTrace(Stack, Y_ARRAY_SIZE(Stack));

        TString binaryPath = "EXE";
        THashMap<TString, TDllInfo> dlls;
#ifdef _linux_
        dl_iterate_phdr(DlIterCallback, &dlls);
#endif

        Cerr << "StackFrames: " << s << Endl;

        for (size_t i = 0; i < s; ++i) {
            ui64 address = (ui64)Stack[i] - 1; // last byte of the call instruction
            ui64 offset = 0;
            TString modulePath = binaryPath;
#ifdef _linux_
            Dl_info dlInfo;
            memset(&dlInfo, 0, sizeof(dlInfo));
            auto ret = dladdr((void*)address, &dlInfo);
            if (ret) {
                auto path = dlInfo.dli_fname;
                auto it = dlls.find(path);
                if (it != dlls.end()) {
                    modulePath = path;
                    offset = it->second.BaseAddress;
                }
            }
#endif

            auto it = Mapping.find(modulePath);
            if (it != Mapping.end()) {
                modulePath = it->second;
            }

            Cerr << "StackFrame: " << modulePath << " " << address << " " << offset << Endl;
        }
    }
    /* Now reraise the signal. We reactivate the signal’s default handling,
       which is to terminate the process. We could just call exit or abort,
       but reraising the signal sets the return status from the process
       correctly. */
    raise(signum);
}

void SetModulesMapping(const THashMap<TString, TString>& mapping) {
    Mapping = mapping;
}

} /* namespace NBacktrace */

} /* namespace NYql */
