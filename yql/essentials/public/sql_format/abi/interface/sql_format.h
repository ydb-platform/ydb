#pragma once

#include <stddef.h>

#ifdef __cplusplus
    #define YQL_SQL_FORMAT_NOEXCEPT noexcept
extern "C" {
#else
    #define YQL_SQL_FORMAT_NOEXCEPT
#endif

    typedef struct TYqlSqlFormatHandle TYqlSqlFormatHandle; // NOLINT(modernize-use-using): this header supports C.

    enum EYqlSqlFormatStatus {
        YQL_SQL_FORMAT_OK = 0,
        YQL_SQL_FORMAT_ERROR = 1,
        YQL_SQL_FORMAT_EXCEPTION = 2,
    };

    // Returns a non-null handle, including on failure. query may be null only for
    // size zero. Release the handle using YqlSqlFormatFreeV1 in the same library.
    TYqlSqlFormatHandle* YqlSqlFormatV1(const char* query, size_t size) YQL_SQL_FORMAT_NOEXCEPT;
    void YqlSqlFormatFreeV1(TYqlSqlFormatHandle* handle) YQL_SQL_FORMAT_NOEXCEPT;

    // Accessors require a live handle; size must be non-null. Returned buffers
    // belong to the handle and remain valid until it is released.
    int YqlSqlFormatStatusV1(const TYqlSqlFormatHandle* handle) YQL_SQL_FORMAT_NOEXCEPT;
    const char* YqlSqlFormatOutputV1(const TYqlSqlFormatHandle* handle, size_t* size) YQL_SQL_FORMAT_NOEXCEPT;
    const char* YqlSqlFormatErrorV1(const TYqlSqlFormatHandle* handle, size_t* size) YQL_SQL_FORMAT_NOEXCEPT;

#ifdef __cplusplus
}
#endif
#undef YQL_SQL_FORMAT_NOEXCEPT
