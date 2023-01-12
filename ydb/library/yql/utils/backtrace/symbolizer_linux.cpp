#include "symbolizer.h"

#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/DebugInfo/Symbolize/DIPrinter.h>
#include <llvm/Support/raw_ostream.h>

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

class TRawOStreamProxy: public llvm::raw_ostream {
public:
    TRawOStreamProxy(std::ostream& out)
        : llvm::raw_ostream(true) // unbuffered
        , Slave_(out)
    {
    }
    void write_impl(const char* ptr, size_t size) override {
        Slave_.write(ptr, size);
    }
    uint64_t current_pos() const override {
        return 0;
    }
    size_t preferred_buffer_size() const override {
        return 0;
    }
private:
    std::ostream& Slave_;
};

class TBacktraceSymbolizer : public IBacktraceSymbolizer {
public:
    TBacktraceSymbolizer(bool kikimrFormat) : IBacktraceSymbolizer(), Symbolyzer(Opts), KikimrFormat(kikimrFormat) {
        dl_iterate_phdr(DlIterCallback, &DLLs);
        
    }

    TString SymbolizeFrame(void* ptr) override {
        ui64 address = (ui64)ptr - 1;
        ui64 offset = 0;
        TString modulePath = BinaryPath;
#ifdef _linux_
        Dl_info dlInfo;
        memset(&dlInfo, 0, sizeof(dlInfo));
        auto ret = dladdr((void*)address, &dlInfo);
        if (ret) {
            auto path = dlInfo.dli_fname;
            auto it = DLLs.find(path);
            if (it != DLLs.end()) {
                modulePath = path;
                offset = it->second.BaseAddress;
            }
        }
#endif
        if (!KikimrFormat) {
            return "StackFrame: " + modulePath + " " + address + " " + offset + "\n";
        }

        llvm::symbolize::SectionedAddress secAddr;
        secAddr.Address = address - offset;
        auto resOrErr = Symbolyzer.symbolizeCode(modulePath, secAddr);
        if (resOrErr) {
            auto value = resOrErr.get();
            if (value.FileName == "<invalid>" && offset > 0) {
                value.FileName = modulePath;
            }

            std::stringstream ss;
            TRawOStreamProxy stream_proxy(ss);
            llvm::symbolize::DIPrinter printer(stream_proxy, true, true, false);
            printer << value;
            ss.flush();

            return ss.str();
        } else {
            return "LLVMSymbolizer: error reading file...";
        }
    }

    ~TBacktraceSymbolizer() override {

    }
    
private:
    THashMap<TString, TDllInfo> DLLs;
    llvm::symbolize::LLVMSymbolizer::Options Opts;
    llvm::symbolize::LLVMSymbolizer Symbolyzer;
    TString BinaryPath = GetPersistentExecPath();
    bool KikimrFormat;
};

std::unique_ptr<IBacktraceSymbolizer> BuildSymbolizer(bool format) {
    return std::unique_ptr<IBacktraceSymbolizer>(new TBacktraceSymbolizer(format));
}
