#pragma once

#include <util/system/compiler.h>

#if defined(__GNUC__)
#define ATTRIBUTE(args...) __attribute__((args))
#else
#define ATTRIBUTE(...)
#endif

#if defined(__GNUC__) && !defined(__clang__)
#define COLD_BLOCK_BYVALUE [=]() ATTRIBUTE(noinline, cold) {
#define COLD_BLOCK_BYREF [&]() ATTRIBUTE(noinline, cold) {
#define COLD_BLOCK_END \
    }                  \
    ();
#else
// Clang does not support gnu-style attributes on lambda functions yet
#define COLD_BLOCK_BYVALUE [=]() {
#define COLD_BLOCK_BYREF [&]() {
#define COLD_BLOCK_END \
    }                  \
    ();
#endif
