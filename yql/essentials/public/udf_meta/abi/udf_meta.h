#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
    #define YQL_UDF_META_NOEXCEPT noexcept
extern "C" {
#else
    #define YQL_UDF_META_NOEXCEPT
#endif

    typedef struct TYqlUdfMetaString { // NOLINT(modernize-use-using): C-compatible ABI.
        const char* Data;
        size_t Size;
    } TYqlUdfMetaString;

    // String buffers are immutable and remain valid until the owning cookie is freed.
    typedef struct TYqlUdfMetaFunction { // NOLINT(modernize-use-using): C-compatible ABI.
        int IsTypeAwareness;
        TYqlUdfMetaString CallableType;
        TYqlUdfMetaString RunConfigType;
        uint32_t ArgCount;
        uint32_t OptionalArgCount;
        int IsStrict;
        int SupportsBlocks;
        uint32_t MinLangVer;
        uint32_t MaxLangVer;
        TYqlUdfMetaString PolyArgs;
        TYqlUdfMetaString ResolvedCallableTypes;
    } TYqlUdfMetaFunction;

    // The context is opaque. Accessors execute in the library owning the context;
    // they return 1 for presence, 0 for absence, or -1 for an exception.
    // GetMetadata writes a caller-owned descriptor and never exposes a C++ object.
    // Copies borrow the cookie; Free must be called exactly once by its owner.
    typedef struct TYqlUdfMeta { // NOLINT(modernize-use-using): C-compatible ABI.
        void* Context;
        void (*Free)(void* context) YQL_UDF_META_NOEXCEPT;
        int (*HasModule)(const void* context, TYqlUdfMetaString module) YQL_UDF_META_NOEXCEPT;
        int (*HasFunction)(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function) YQL_UDF_META_NOEXCEPT;
        int (*GetMetadata)(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function, TYqlUdfMetaFunction* result) YQL_UDF_META_NOEXCEPT;
    } TYqlUdfMeta;

#ifdef __cplusplus
}
#endif
#undef YQL_UDF_META_NOEXCEPT
