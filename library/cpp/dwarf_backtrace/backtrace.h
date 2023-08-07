#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NDwarf {
    struct TLineInfo {
        TString FileName;
        int Line;
        int Col;
        TString FunctionName;
        uintptr_t Address;
        int Index;
    };

    struct TError {
        int Code;
        TString Message;
    };

    enum class EResolving {
        Continue = 0,
        Break = 1,
    };

    using TCallback = std::function<EResolving(const TLineInfo&)>;

    // Resolves backtrace addresses and calls the callback for all line infos of inlined functions there.
    // Stops execution if the callback returns `EResolving::Break`.
    [[nodiscard]] TMaybe<TError> ResolveBacktrace(TArrayRef<const void* const> backtrace, TCallback callback);

}
