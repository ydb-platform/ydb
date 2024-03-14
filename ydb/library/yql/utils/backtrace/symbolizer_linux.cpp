#include "symbolizer.h"

#include <contrib/libs/backtrace/backtrace.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/type_name.h>
#include <util/system/execpath.h>
#include <util/string/builder.h>

#include <functional>
#include <mutex>
#include <vector>
#include <numeric>
#include <algorithm>
#include <filesystem>

namespace {
void HandleLibBacktraceError(void* data, const char* msg, int) {
    if (!data) {
        Cerr << msg;
        return;
    }
    *(reinterpret_cast<TString*>(data)) = msg;
}

int HandleLibBacktraceFrame(void* data, uintptr_t, const char* filename, int lineno, const char* function) {
    TString fileName = filename ? filename : "???";
    TString functionName = function ? CppDemangle(function) : "???";
    *reinterpret_cast<TString*>(data) = TStringBuilder() << functionName << " at " << fileName << ":" << lineno << ":0";
    return 0;
}
}

namespace NYql {
    namespace NBacktrace {
        namespace {
            auto CreateState(TStringBuf filename) {
                return backtrace_create_state(
                    filename.data(),
                    0,
                    HandleLibBacktraceError,
                    nullptr
                );
            }
        }

        TVector<TString> Symbolize(const TVector<TStackFrame>& frames) {
            if (frames.empty()) {
                return {};
            }
            static std::mutex mutex;
            const std::lock_guard lock{mutex};

            TVector<TString> result(frames.size());
            std::vector<size_t> order(frames.size());
            std::iota(order.begin(), order.end(), 0u);
            std::sort(order.begin(), order.end(), [&frames](auto a, auto b) { return frames[a].File < frames[b].File; });

            struct backtrace_state* state = nullptr;

            for (size_t i = 0; i < order.size(); ++i) {
                if (!i || frames[order[i - 1]].File != frames[order[i]].File) {
                    if (!std::filesystem::exists(frames[order[i]].File.c_str())) {
                        state = nullptr;
                    } else {
                        state = CreateState(frames[order[i]].File);
                    }
                }

                if (!state) {
                    result[order[i]] = TStringBuilder() << "File not found: " << frames[order[i]].File;
                    continue;
                }

                int status = backtrace_pcinfo(
                    state,
                    reinterpret_cast<uintptr_t>(frames[order[i]].Address) - 1, // last byte of the call instruction
                    HandleLibBacktraceFrame,
                    HandleLibBacktraceError,
                    &result[order[i]]);
                if (0 != status) {
                    break;
                }
            }
            return result;
        }
    }
}