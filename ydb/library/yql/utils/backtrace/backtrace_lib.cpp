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
        TString Path;
        ui64 BaseAddress;
    };

    THashMap<TString, TDllInfo> DLLs;

#if defined(_linux_) && defined(_x86_64_)
    int DlIterCallback(struct dl_phdr_info *info, size_t, void *data) {
        if (*info->dlpi_name) {
            TDllInfo dllInfo{ info->dlpi_name, (ui64)info->dlpi_addr };
            reinterpret_cast<THashMap<TString, TDllInfo>*>(data)->emplace(dllInfo.Path, dllInfo);
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
                auto path = dlInfo.dli_fname;
                auto it = DLLs.find(path);
                if (it != DLLs.end()) {
                    File = path;
                    Address -= it->second.BaseAddress;
                }
            }
#endif
        }

        size_t CollectFrames(TCollectedFrame* frames, void* data) {
#if defined(_linux_) && defined(_x86_64_)
            DLLs.clear();
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