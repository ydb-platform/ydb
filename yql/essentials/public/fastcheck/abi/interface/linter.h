#pragma once

#include <yql/essentials/public/udf_meta/abi/udf_meta.h>

#include <stddef.h>

#ifdef __cplusplus
    #define YQL_LINTER_NOEXCEPT noexcept
extern "C" {
#else
    #define YQL_LINTER_NOEXCEPT
#endif

    typedef struct TYqlLinterHandle TYqlLinterHandle;   // NOLINT(modernize-use-using): C ABI.
    typedef struct TYqlUdfMetaHandle TYqlUdfMetaHandle; // NOLINT(modernize-use-using): C ABI.

    // request contains a serialized TLinterRequest. Metadata is borrowed only for
    // this call; null selects the built-in metadata. No C++ objects cross the ABI.
    // Returns a non-null result handle, including on failure.
    TYqlLinterHandle* YqlLinterV1(const char* request, size_t size, const TYqlUdfMeta* metadata) YQL_LINTER_NOEXCEPT;

    // Returns a dedicated owning handle, including on failure. Check its error
    // before obtaining the lookup cookie. Linter requests only borrow the cookie.
    TYqlUdfMetaHandle* YqlLinterLoadUdfMetaV1(const char* json, size_t size) YQL_LINTER_NOEXCEPT;
    void YqlUdfMetaFreeV1(TYqlUdfMetaHandle* handle) YQL_LINTER_NOEXCEPT;
    const char* YqlUdfMetaErrorV1(const TYqlUdfMetaHandle* handle, size_t* size) YQL_LINTER_NOEXCEPT;
    // Requires a successfully loaded handle. The cookie and its string buffers
    // remain valid until YqlUdfMetaFreeV1; cookie.Free calls that same destructor.
    TYqlUdfMeta YqlUdfMetaGetAbiV1(TYqlUdfMetaHandle* handle) YQL_LINTER_NOEXCEPT;
    TYqlLinterHandle* YqlLinterPreloadIssuesV1(void) YQL_LINTER_NOEXCEPT;
    void YqlLinterFreeV1(TYqlLinterHandle* handle) YQL_LINTER_NOEXCEPT;

    // Accessors require a live handle and a non-null size pointer. Buffers belong
    // to the handle until FreeV1. A nonempty error denotes an exception.
    // Successful preload calls have an empty result payload.
    const char* YqlLinterResultV1(const TYqlLinterHandle* handle, size_t* size) YQL_LINTER_NOEXCEPT;
    const char* YqlLinterErrorV1(const TYqlLinterHandle* handle, size_t* size) YQL_LINTER_NOEXCEPT;

#ifdef __cplusplus
}
#endif
#undef YQL_LINTER_NOEXCEPT
