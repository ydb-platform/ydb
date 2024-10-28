#pragma once

#include <util/system/types.h>

namespace NYql {

enum ENativeTypeCompatFlags: ui64 {
    NTCF_COMPLEX    = 1ull << 0 /* "complex" */,
    NTCF_DATE       = 1ull << 1 /* "date" */,
    NTCF_NULL       = 1ull << 2 /* "null" */,
    NTCF_VOID       = 1ull << 3 /* "void" */,
    NTCF_FLOAT      = 1ull << 4 /* "float" */,
    NTCF_JSON       = 1ull << 5 /* "json" */,
    NTCF_DECIMAL    = 1ull << 6 /* "decimal" */,
    NTCF_BIGDATE    = 1ull << 7 /* "bigdate" */,
    NTCF_UUID       = 1ull << 8 /* "uuid" */,

    NTCF_NO_YT_SUPPORT  = 1ull << 40 /* "_no_yt_support" */,

    NTCF_NONE = 0ull /* "none" */,
    NTCF_LEGACY = NTCF_COMPLEX | NTCF_DATE | NTCF_NULL | NTCF_VOID /* "legacy" */,

    // All supported types by all YT production clusters
    NTCF_PRODUCTION = NTCF_COMPLEX | NTCF_DATE | NTCF_NULL | NTCF_VOID | NTCF_FLOAT | NTCF_JSON | NTCF_DECIMAL | NTCF_UUID /* "production" */,

    // add all new types here, supported by YT
    NTCF_ALL = NTCF_PRODUCTION | NTCF_BIGDATE /* "all" */,
};

} // namespace NYql
