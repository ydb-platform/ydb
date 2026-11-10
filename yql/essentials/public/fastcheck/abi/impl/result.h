#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NYql::NFastCheck::NAbi {

struct TResult {
    TString Output;
    TString Error;
    bool Exception = true;
};

template <class THandle>
THandle* AllocationFailure() noexcept {
    static THandle Result;
    return &Result;
}

template <class THandle, class TFunction>
THandle* Execute(TFunction&& function) noexcept {
    try {
        auto result = MakeHolder<THandle>();
        try {
            result->Output = function(*result);
            result->Exception = false;
        } catch (...) {
            result->Error = CurrentExceptionMessage();
        }
        return result.Release();
    } catch (...) {
        // Exception reporting can exhaust memory too; this result is immutable.
        return AllocationFailure<THandle>();
    }
}

template <class THandle>
void Free(THandle* handle) noexcept {
    if (handle != AllocationFailure<THandle>()) {
        delete handle;
    }
}

inline const char* GetOutput(const TResult& result, size_t* size) noexcept {
    *size = result.Output.size();
    return result.Output.data();
}

inline const char* GetError(const TResult& result, size_t* size) noexcept {
    const TStringBuf error = result.Exception && result.Error.empty()
                                 ? TStringBuf("Failed to produce fastcheck ABI result")
                                 : TStringBuf(result.Error);
    *size = error.size();
    return error.data();
}

} // namespace NYql::NFastCheck::NAbi
