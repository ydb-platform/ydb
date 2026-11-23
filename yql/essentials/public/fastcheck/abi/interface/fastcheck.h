#pragma once

#include <stddef.h>

#ifdef __cplusplus
    #define YQL_FASTCHECK_NOEXCEPT noexcept
extern "C" {
#else
    #define YQL_FASTCHECK_NOEXCEPT
#endif

    typedef struct TYqlFastCheckHandle TYqlFastCheckHandle; // NOLINT(modernize-use-using): C ABI.

    // Requests and results contain serialized fastcheck ABI protobuf messages.
    // Request buffers are borrowed for the duration of the call.
    // Always returns a non-null handle, including on failure; free it in this library.
    TYqlFastCheckHandle* YqlFastCheckV1(const char* request, size_t size) YQL_FASTCHECK_NOEXCEPT;
    void YqlFastCheckFreeV1(TYqlFastCheckHandle* handle) YQL_FASTCHECK_NOEXCEPT;

    // Accessors require a live handle and a non-null size pointer. Buffers belong
    // to the handle until FreeV1. A nonempty error denotes an exception.
    const char* YqlFastCheckResultV1(const TYqlFastCheckHandle* handle, size_t* size) YQL_FASTCHECK_NOEXCEPT;
    const char* YqlFastCheckErrorV1(const TYqlFastCheckHandle* handle, size_t* size) YQL_FASTCHECK_NOEXCEPT;

#ifdef __cplusplus
}
#endif
#undef YQL_FASTCHECK_NOEXCEPT
