#include "symbolizer.h"

#include <contrib/libs/backtrace/backtrace.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/type_name.h>
#include <util/system/execpath.h>
#include <util/string/builder.h>

#include <mutex>
#include <numeric>
#include <algorithm>

#include <util/stream/mem.h>

#ifdef __GNUC__
    #include <cxxabi.h>
#endif

namespace {
const size_t MaxStrLen = 512;
const size_t MaxDemangleLen = 1024 * 1024;
char Buff[MaxDemangleLen];

class TNoThrowingMemoryOutput : public TMemoryOutput {
public:
    TNoThrowingMemoryOutput(void* c, size_t l) : TMemoryOutput(c, l) {}
    void Truncate() {
        *(Buf_ - 1) = '.';
        *(Buf_ - 2) = '.';
        *(Buf_ - 3) = '.';
    }

    void DoWrite(const void* buf, size_t len) override {
        bool truncated = Buf_ + len > End_;
        if (truncated) {
            len = std::min(len, (size_t)(End_ - Buf_));
        }
        memcpy(Buf_, buf, len);
        Buf_ += len;
        if (truncated) {
            Truncate();
        }
    }

    void DoWriteC(char c) override {
        if (Buf_ == End_) {
            Truncate();
        } else {
            *Buf_++ = c;
        }
    }
};

void HandleLibBacktraceError(void* data, const char* msg, int) {
    if (!data) {
        Cerr << msg;
        return;
    }
    TNoThrowingMemoryOutput out(data, MaxStrLen - 1);
    out << msg;
}

const char* Demangle(const char* name) {
#ifndef __GNUC__
    return name;
#else
    int status;
    size_t len = MaxDemangleLen - 1;
    const char* res = __cxxabiv1::__cxa_demangle(name, Buff, &len, &status);

    if (!res) {
        return name;
    }
    return res;
#endif
}

int HandleLibBacktraceFrame(void* data, uintptr_t, const char* filename, int lineno, const char* function) {
    TNoThrowingMemoryOutput out(data, MaxStrLen - 1);
    const char* fileName = filename ? filename : "???";
    const char* functionName = function ? Demangle(function) : "???";
    out << functionName << " at " << fileName << ":" << lineno << ":0";
    return 0;
}
}

namespace NYql {
    namespace NBacktrace {
        namespace {
            std::mutex Mutex;
            char* Result[Limit];
            size_t Order[Limit];
            char TmpBuffer[MaxStrLen * Limit]{};
            auto CreateState(const char* filename) {
                return backtrace_create_state(
                    filename,
                    0,
                    HandleLibBacktraceError,
                    nullptr
                );
            }
        }

        void Symbolize(const TStackFrame* frames, size_t count, IOutputStream* out) {
            if (!count) {
                return;
            }
            memset(TmpBuffer, 0, sizeof(TmpBuffer));
            Result[0] = TmpBuffer;
            for (size_t i = 1; i < Limit; ++i) {
                Result[i] = Result[i - 1] + MaxStrLen;
            }
            const std::lock_guard lock{Mutex};

            std::iota(Order, Order + count, 0u);
            std::sort(Order, Order + count, [&frames](auto a, auto b) { return strcmp(frames[a].File, frames[b].File) < 0; });

            struct backtrace_state* state = nullptr;
            for (size_t i = 0; i < count; ++i) {
                if (!i || frames[Order[i - 1]].File != frames[Order[i]].File) {
                    state = CreateState(frames[Order[i]].File);
                }

                if (!state) {
                    Result[Order[i]] = nullptr; // File not found
                    continue;
                }

                int status = backtrace_pcinfo(
                    state,
                    reinterpret_cast<uintptr_t>(frames[Order[i]].Address) - 1, // last byte of the call instruction
                    HandleLibBacktraceFrame,
                    HandleLibBacktraceError,
                    reinterpret_cast<void*>(Result[Order[i]]));
                if (0 != status) {
                    break;
                }
            }
            for (size_t i = 0; i < count; ++i) {
                if (Result[i]) {
                    *out << Result[i] << "\n";
                } else {
                    *out << "File `" << frames[i].File << "` not found\n"; 
                }
            }
        }
    }
}