#include "symbolizer.h"
#include "fake_llvm_symbolizer/fake_llvm_symbolizer.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/malloc/api/malloc.h>

#include <util/generic/hash.h>
#include <util/system/execpath.h>

#include <dlfcn.h>
#include <link.h>
#include <signal.h>

#include <iostream>
#include <sstream>

namespace NYql {

namespace NBacktrace {
    
extern THashMap<TString, TString> Mapping;

}
}

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

class TBacktraceSymbolizer : public IBacktraceSymbolizer {
public:
    TBacktraceSymbolizer(bool kikimrFormat) : IBacktraceSymbolizer(), KikimrFormat_(kikimrFormat) {
        dl_iterate_phdr(DlIterCallback, &DLLs_);
        
    }

    TString SymbolizeFrame(void* ptr) override {
        ui64 address = (ui64)ptr - 1;
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
        if (!KikimrFormat_) {
            return "StackFrame: " + modulePath + " " + address + " " + offset + "\n";
        }

        llvm::object::SectionedAddress secAddr;
        secAddr.Address = address - offset;
        return NYql::NBacktrace::SymbolizeAndDumpToString(modulePath, secAddr, offset);
    }

    ~TBacktraceSymbolizer() override {

    }
    
private:
    THashMap<TString, TDllInfo> DLLs_;
    TString BinaryPath_ = GetPersistentExecPath();
    bool KikimrFormat_;
};

std::unique_ptr<IBacktraceSymbolizer> BuildSymbolizer(bool format) {
    return std::unique_ptr<IBacktraceSymbolizer>(new TBacktraceSymbolizer(format));
}
