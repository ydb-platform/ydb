#include "backtrace.h"

#include <util/system/backtrace.h>

#if defined(__linux__) || defined(__APPLE__)
#include <library/cpp/dwarf_backtrace/backtrace.h>
#include <util/stream/format.h>
#endif

namespace NKikimr {

#if defined(__linux__) || defined(__APPLE__)

namespace {

void FormatBacktraceDwarf(IOutputStream* out, void* const* backtrace, size_t backtraceSize) {
    size_t frameIndex = 0;
    bool allFramesUnknown = true;

    auto error = NDwarf::ResolveBacktrace({backtrace, backtraceSize}, [out, &frameIndex, &allFramesUnknown](const NDwarf::TLineInfo& info) {
        const TString & fileName = info.FileName == "???" ? "??" : info.FileName;
        if (fileName != "??") {
            allFramesUnknown = false;
        }

        const TString & functionName = info.FunctionName == "???" ? "??" : info.FunctionName;
        *out << frameIndex << ". " << fileName << ":" << info.Line << ": " << functionName << " @ " << Hex(info.Address, HF_ADDX) << Endl;
        ++frameIndex;

        return NDwarf::EResolving::Continue;
    });

    /// Fallback to default backtrace format
    if (error || allFramesUnknown) {
        FormatBackTrace(out, backtrace, backtraceSize);
    }
}

}

void EnableYDBBacktraceFormat() {
    SetFormatBackTraceFn(FormatBacktraceDwarf);
}

#else

void EnableYDBBacktraceFormat() {
    SetFormatBackTraceFn(FormatBackTrace);
}

#endif

}
