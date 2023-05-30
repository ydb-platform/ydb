#include "backtrace.h"
#include "symbolizer.h"

#include <util/generic/hash.h>
#ifdef _linux_
#include <dlfcn.h>
#include <link.h>
#include <signal.h>

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

namespace NYql {

namespace NBacktrace {
extern THashMap<TString, TString> Mapping;
}
}

class TBacktraceSymbolizer : public IBacktraceSymbolizer {
public:
    TBacktraceSymbolizer() : IBacktraceSymbolizer() {
    #ifdef _linux_
        dl_iterate_phdr(DlIterCallback, &DLLs_);
    #endif
    }
    TString SymbolizeFrame(void* ptr) override {
        
        ui64 address = (ui64)ptr - 1; // last byte of the call instruction
        ui64 offset = 0;
        TString modulePath = BinaryPath_;
#ifdef _linux_
        Dl_info dlInfo;
        memset(&dlInfo, 0, sizeof(dlInfo));
        auto ret = dladdr((void*)address, &dlInfo);
        if (ret) {
            auto path = dlInfo.dli_fname;
            auto it = DLLs_.find(path);
            if (it != DLLs_.end()) {
                modulePath = path;
                offset = it->second.BaseAddress;
            }
        }
#endif

        auto it = NYql::NBacktrace::Mapping.find(modulePath);
        if (it != NYql::NBacktrace::Mapping.end()) {
            modulePath = it->second;
        }

        return "StackFrame: " + modulePath + " " + address + " " + offset + "\n";
    }

    ~TBacktraceSymbolizer() override {
        
    }
private:
    TString BinaryPath_ = "EXE";
    THashMap<TString, TDllInfo> DLLs_;
};

std::unique_ptr<IBacktraceSymbolizer> BuildSymbolizer(bool) {
    return std::unique_ptr<IBacktraceSymbolizer>(new TBacktraceSymbolizer());
}
