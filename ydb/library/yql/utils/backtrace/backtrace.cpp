#include "backtrace.h"

#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/DebugInfo/Symbolize/DIPrinter.h>
#include <llvm/Support/raw_ostream.h>

#include <library/cpp/malloc/api/malloc.h>
 
#include <util/generic/hash.h>
#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/system/backtrace.h>
#include <util/system/type_name.h>
#include <util/system/execpath.h>
#include <util/system/platform.h>
#include <util/system/mlock.h>

#ifdef _linux_
#include <dlfcn.h>
#include <link.h>
#endif

#ifndef _win_

namespace {

bool SetSignalHandler(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_RESETHAND;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    return sigaction(signo, &sa, nullptr) != -1;
}

} // namespace

#endif // _win_

void KikimrBackTrace() {
    KikimrBacktraceFormatImpl(&Cerr);
}

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

TAtomic BacktraceStarted = 0;

class TRawOStreamProxy: public llvm::raw_ostream {
public:
    TRawOStreamProxy(IOutputStream& out)
        : llvm::raw_ostream(true) // unbuffered
        , Slave_(out)
    {
    }
    void write_impl(const char* ptr, size_t size) override {
        Slave_.Write(ptr, size);
    }
    uint64_t current_pos() const override {
        return 0;
    }
    size_t preferred_buffer_size() const override {
        return 0;
    }
private:
    IOutputStream& Slave_;
};


void KikimrBacktraceFormatImpl(IOutputStream* out) {
    if (out == &Cerr && !AtomicTryLock(&BacktraceStarted)) {
        return;
    }

    // Unlock memory to avoid extra RSS consumption while touching debug info
    UnlockAllMemory();

    void* array[300];
    const size_t s = BackTrace(array, Y_ARRAY_SIZE(array));
    KikimrBacktraceFormatImpl(out, array, s);
}

void KikimrBacktraceFormatImpl(IOutputStream* out, void* const* stack, size_t stackSize) {
    using namespace llvm;
    using namespace symbolize;

    TRawOStreamProxy outStream(*out);
    THashMap<TString, TDllInfo> dlls;
#ifdef _linux_
    dl_iterate_phdr(DlIterCallback, &dlls);
#endif
    auto binaryPath = GetPersistentExecPath();
    DIPrinter printer(outStream, true, true, false);
    LLVMSymbolizer::Options opts;
    LLVMSymbolizer symbolyzer(opts);
    for (const auto i : xrange(stackSize)) {
        ui64 address = (ui64)stack[i] - 1; // last byte of the call instruction
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
        SectionedAddress secAddr;
        secAddr.Address = address - offset;
        auto resOrErr = symbolyzer.symbolizeCode(modulePath, secAddr);
        if (resOrErr) {
            auto value = resOrErr.get();
            if (value.FileName == "<invalid>" && offset > 0) {
                value.FileName = modulePath;
            }

            printer << value;
        } else {
            logAllUnhandledErrors(resOrErr.takeError(), outStream,
                "LLVMSymbolizer: error reading file: ");
        }
    }
}

void PrintBacktraceToStderr(int signum)
{
    if (!NMalloc::IsAllocatorCorrupted) { 
        /* we want memory allocation for backtrace printing */ 
        KikimrBacktraceFormatImpl(&Cerr); 
    } 
    /* Now reraise the signal. We reactivate the signal’s default handling,
       which is to terminate the process. We could just call exit or abort,
       but reraising the signal sets the return status from the process
       correctly. */
    raise(signum);
}

void EnableKikimrBacktraceFormat()
{
    SetFormatBackTraceFn(KikimrBacktraceFormatImpl);
}

void SetFatalSignalHandler(void (*handler)(int))
{
    Y_UNUSED(handler);
#ifndef _win_
    for (int signo: {SIGSEGV, SIGILL, SIGABRT, SIGFPE}) {
        if (!SetSignalHandler(signo, handler)) {
            ythrow TSystemError() << "Cannot set handler for signal " << strsignal(signo);
        }
    }
#endif
}
