#pragma once

// Central configuration for SIMD availability in hash join utilities.
// 
// This header provides a single point of control for SIMD feature detection
// and inclusion. On x86-64 Linux, SIMD optimizations (AVX2/SSE4.2) are enabled.
// On other platforms, fallback implementations are used instead.
//
// Usage: Include this header instead of directly checking platform macros.
// Use YDB_HASH_JOIN_SIMD_ENABLED to conditionally compile SIMD-specific code.

#if defined(__x86_64__) && defined(__linux__)
    #define YDB_HASH_JOIN_SIMD_ENABLED 1
#else
    #define YDB_HASH_JOIN_SIMD_ENABLED 0
#endif

#if YDB_HASH_JOIN_SIMD_ENABLED
    #include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/simd.h>
#endif

