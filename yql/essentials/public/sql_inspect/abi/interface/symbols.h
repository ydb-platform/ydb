#pragma once

#include <stddef.h>

#ifdef __cplusplus
    #define YQL_SQL_INSPECT_NOEXCEPT noexcept
extern "C" {
#else
    #define YQL_SQL_INSPECT_NOEXCEPT
#endif

    typedef struct TYqlSqlInspectHandle TYqlSqlInspectHandle; // NOLINT(modernize-use-using): C ABI.

    // Requests and responses are serialized sql_inspect ABI protobuf messages.
    // The request is borrowed during the call. The returned handle is always non-null.
    TYqlSqlInspectHandle* YqlSqlInspectV1(const char* request, size_t size) YQL_SQL_INSPECT_NOEXCEPT;
    void YqlSqlInspectFreeV1(TYqlSqlInspectHandle* handle) YQL_SQL_INSPECT_NOEXCEPT;

    // Require a live handle and non-null size. Buffers live until FreeV1.
    // A nonempty error denotes an exception, not a SQL diagnostic.
    const char* YqlSqlInspectResultV1(const TYqlSqlInspectHandle* handle, size_t* size) YQL_SQL_INSPECT_NOEXCEPT;
    const char* YqlSqlInspectErrorV1(const TYqlSqlInspectHandle* handle, size_t* size) YQL_SQL_INSPECT_NOEXCEPT;

#ifdef __cplusplus
}
#endif
#undef YQL_SQL_INSPECT_NOEXCEPT
