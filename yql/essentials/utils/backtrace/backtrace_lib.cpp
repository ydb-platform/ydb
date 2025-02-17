#include "backtrace_lib.h"

#include <util/generic/hash.h>
#include <util/system/execpath.h>

#include <algorithm>

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
    TDllInfo DLLs[MaxDLLCnt];
    size_t DLLCount = 0;

#if defined(_linux_) && defined(_x86_64_)
    int DlIterCallback(struct dl_phdr_info *info, size_t, void *data) {
        if (*info->dlpi_name) {
            if (DLLCount + 1 < MaxDLLCnt) {
                reinterpret_cast<std::remove_reference_t<decltype(DLLs[0])>*>(data)[DLLCount++] = { info->dlpi_name, (ui64)info->dlpi_addr };
            }
        }
        return 0;
    }
#endif
    bool comp(const TDllInfo& a, const TDllInfo& b) {
        return strcmp(a.Path, b.Path) < 0;
    }

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
                auto it = std::lower_bound(DLLs, DLLs + DLLCount, std::remove_reference_t<decltype(DLLs[0])> {dlInfo.dli_fname, {}}, comp);
                if (it != DLLs + DLLCount && !strcmp(it->Path, dlInfo.dli_fname)) {
                    File = it->Path;
                    Address -= it->BaseAddress;
                }
            }
#endif
        }

        size_t CollectFrames(TCollectedFrame* frames, void* data) {
#if defined(_linux_) && defined(_x86_64_)
            DLLCount = 0;
            dl_iterate_phdr(DlIterCallback, &DLLs);
#endif
            std::stable_sort(DLLs, DLLs + DLLCount, comp);
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