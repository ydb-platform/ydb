#include "backtrace.h"

#include <contrib/libs/backtrace/backtrace.h>

#include <util/generic/yexception.h>
#include <util/system/type_name.h>
#include <util/system/execpath.h>

namespace NDwarf {
    namespace {
        struct TContext {
            TCallback& Callback;
            int Counter = 0;
            TMaybe<TError> Error;
        };

        void HandleLibBacktraceError(void* data, const char* msg, int errnum) {
            auto* context = reinterpret_cast<TContext*>(data);
            context->Error = TError{.Code = errnum, .Message=msg};
        }

        int HandleLibBacktraceFrame(void* data, uintptr_t pc, const char* filename, int lineno, const char* function) {
            auto* context = reinterpret_cast<TContext*>(data);
            TLineInfo lineInfo{
                .FileName = filename != nullptr ? filename : "???",
                .Line = lineno,
                .Col = 0, // libbacktrace doesn't provide column numbers, so fill this field with a dummy value.
                .FunctionName = function != nullptr ? CppDemangle(function) : "???",
                .Address = pc,
                .Index = context->Counter++,
            };
            return static_cast<int>(context->Callback(lineInfo));
        }
    }

    TMaybe<TError> ResolveBacktrace(TArrayRef<const void* const> backtrace, TCallback callback) {
        TContext context{.Callback = callback};
        // Intentionally never freed (see https://a.yandex-team.ru/arc/trunk/arcadia/contrib/libs/backtrace/backtrace.h?rev=6789902#L80).
        static auto* state = backtrace_create_state(
            GetPersistentExecPath().c_str(),
            1 /* threaded */,
            HandleLibBacktraceError,
            &context /* data for the error callback */
        );
        if (nullptr == state) {
            static const auto initError = context.Error;
            return initError;
        }
        for (const void* address : backtrace) {
            int status = backtrace_pcinfo(
                state,
                reinterpret_cast<uintptr_t>(address) - 1, // last byte of the call instruction
                HandleLibBacktraceFrame,
                HandleLibBacktraceError,
                &context /* data for both callbacks */);
            if (0 != status) {
                break;
            }
        }
        return context.Error;
    }
}
