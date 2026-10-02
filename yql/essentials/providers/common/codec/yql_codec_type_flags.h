#pragma once

#include <util/system/types.h>

namespace NYql {

enum ENativeTypeCompatFlags: ui64 {
    NTCF_COMPLEX = 1ULL << 0 /* "complex" */,
    NTCF_DATE = 1ULL << 1 /* "date" */,
    NTCF_NULL = 1ULL << 2 /* "null" */,
    NTCF_VOID = 1ULL << 3 /* "void" */,
    NTCF_FLOAT = 1ULL << 4 /* "float" */,
    NTCF_JSON = 1ULL << 5 /* "json" */,
    NTCF_DECIMAL = 1ULL << 6 /* "decimal" */,
    NTCF_BIGDATE = 1ULL << 7 /* "bigdate" */,
    NTCF_UUID = 1ULL << 8 /* "uuid" */,
    NTCF_TZDATE = 1ULL << 9 /* "tzdate" */,
    NTCF_BIGTZDATE = 1ULL << 10 /* "bigtzdate" */,

    NTCF_NO_YT_SUPPORT = 1ULL << 40 /* "_no_yt_support" */,

    NTCF_NONE = 0ULL /* "none" */,
    NTCF_LEGACY = NTCF_COMPLEX | NTCF_DATE | NTCF_NULL | NTCF_VOID /* "legacy" */,

    // All supported types by all YT production clusters
    NTCF_PRODUCTION = NTCF_COMPLEX | NTCF_DATE | NTCF_NULL | NTCF_VOID | NTCF_FLOAT | NTCF_JSON | NTCF_DECIMAL | NTCF_UUID /* "production" */,

    // add all new types here, supported by YT
    NTCF_ALL = NTCF_PRODUCTION | NTCF_BIGDATE | NTCF_TZDATE | NTCF_BIGTZDATE /* "all" */,
};

} // namespace NYql
