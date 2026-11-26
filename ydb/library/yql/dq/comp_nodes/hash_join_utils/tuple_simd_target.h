#pragma once

// ISA-specific layout libraries are built without -m flags: inline code from
// shared headers (std::, util) must stay callable on any CPU. Put includes
// first, then wrap only the layout code in TUPLE_LAYOUT_TARGET_PUSH/POP.
#if defined(TUPLE_LAYOUT_AVX2)
#ifdef __AVX2__
#error "Build the AVX2 tuple layout without -mavx2"
#endif
#define TUPLE_LAYOUT_TARGET_PUSH _Pragma("clang attribute push(__attribute__((target(\"avx2\"))), apply_to = function)")
#define TUPLE_LAYOUT_TARGET_POP _Pragma("clang attribute pop")
#elif defined(TUPLE_LAYOUT_SSE42)
#define TUPLE_LAYOUT_TARGET_PUSH _Pragma("clang attribute push(__attribute__((target(\"sse4.2\"))), apply_to = function)")
#define TUPLE_LAYOUT_TARGET_POP _Pragma("clang attribute pop")
#else
#define TUPLE_LAYOUT_TARGET_PUSH
#define TUPLE_LAYOUT_TARGET_POP
#endif
