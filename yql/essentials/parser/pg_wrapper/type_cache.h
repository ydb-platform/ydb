#pragma once

namespace NYql {

// Private flag bits in the TypeCacheEntry.flags field
constexpr ui32 TCFLAGS_HAVE_PG_TYPE_DATA            = 0x000001;
constexpr ui32 TCFLAGS_CHECKED_BTREE_OPCLASS        = 0x000002;
constexpr ui32 TCFLAGS_CHECKED_HASH_OPCLASS         = 0x000004;
constexpr ui32 TCFLAGS_CHECKED_EQ_OPR               = 0x000008;
constexpr ui32 TCFLAGS_CHECKED_LT_OPR               = 0x000010;
constexpr ui32 TCFLAGS_CHECKED_GT_OPR               = 0x000020;
constexpr ui32 TCFLAGS_CHECKED_CMP_PROC             = 0x000040;
constexpr ui32 TCFLAGS_CHECKED_HASH_PROC            = 0x000080;
constexpr ui32 TCFLAGS_CHECKED_HASH_EXTENDED_PROC   = 0x000100;
constexpr ui32 TCFLAGS_CHECKED_ELEM_PROPERTIES      = 0x000200;
constexpr ui32 TCFLAGS_HAVE_ELEM_EQUALITY           = 0x000400;
constexpr ui32 TCFLAGS_HAVE_ELEM_COMPARE            = 0x000800;
constexpr ui32 TCFLAGS_HAVE_ELEM_HASHING            = 0x001000;
constexpr ui32 TCFLAGS_HAVE_ELEM_EXTENDED_HASHING   = 0x002000;

// Copied from nbtree.h
constexpr ui32 BTORDER_PROC = 1;

}
