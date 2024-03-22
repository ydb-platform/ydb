#include "backtrace_lib.h"

#include <util/generic/hash.h>
#include <util/system/execpath.h>

#if defined(_linux_) && defined(_x86_64_)
#include <dlfcn.h>
#include <link.h>
#endif

namespace {
    const size_t Limit = 400;
    void* Stack[Limit];

    struct TDllInfo {
        const char* Path;
        ui64 BaseAddress;
    };

    const size_t MaxDLLCnt = 100;
    std::pair<const char*, TDllInfo> DLLs[MaxDLLCnt];
    size_t DLLCount = 0;

#if defined(_linux_) && defined(_x86_64_)
    int DlIterCallback(struct dl_phdr_info *info, size_t, void *data) {
        if (*info->dlpi_name) {
            TDllInfo dllInfo{ info->dlpi_name, (ui64)info->dlpi_addr };
            *reinterpret_cast<decltype(DLLs)*>(data)[DLLCount++] = {dllInfo.Path, dllInfo};
        }
        return 0;
    }
#endif

}

namespace NYql {
    namespace NBacktrace {
        TCollectedFrame::TCollectedFrame(uintptr_t addr) {
            File = GetPersistentExecPath().c_str();
            Address = addr;
#if defined(_linux_) && defined(_x86_64_)
            Dl_info dlInfo;
            memset(&dlInfo, 0, sizeof(dlInfo));
            auto ret = dladdr(reinterpret_cast<void*>(addr), &dlInfo);
            if (ret) {
                for (size_t i = 0; i < DLLCount; ++i) {
                    if (strcmp(dlInfo.dli_fname, DLLs[i].first)) {
                        continue;
                    }
                    File = dlInfo.dli_fname;
                    Address -= DLLs[i].second.BaseAddress;
                    break;
                }
            }
#endif
        }

        size_t CollectFrames(TCollectedFrame* frames, void* data) {
#if defined(_linux_) && defined(_x86_64_)
            DLLCount = 0;
            dl_iterate_phdr(DlIterCallback, &DLLs);
#endif
            size_t cnt = CollectBacktrace(Stack, Limit, data);
            return CollectFrames(frames, Stack, cnt);
        }

        size_t CollectFrames(TCollectedFrame* frames, void** stack, size_t cnt) {
            for (size_t i = 0; i < cnt; ++i) {
                new (frames + i)TCollectedFrame(reinterpret_cast<uintptr_t>(stack[i]));
            }
            return cnt;
        }
    }
}