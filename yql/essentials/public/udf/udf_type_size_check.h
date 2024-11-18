#pragma once

#define UDF_ASSERT_TYPE_SIZE(type, expectedSize)         \
    static_assert(sizeof(type) <= (expectedSize),        \
        "Size of " #type " mismatches expected size. "      \
        "Expected size is " #expectedSize)

