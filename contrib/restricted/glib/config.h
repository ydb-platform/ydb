#pragma once

#if defined(_MSC_VER) && defined(_WIN64)
#include "config-win-x86_64.h"
#elif defined(__APPLE__) && defined(__x86_64__)
#include "config-osx-x86_64.h"
#elif defined(__APPLE__) && (defined(__aarch64__) || defined(_M_ARM64))
#include "config-osx-arm64.h"
#elif defined(__ANDROID__) && defined(__i686__)
#include "config-android-i686.h"
#elif defined(__ANDROID__) && defined(__x86_64__)
#include "config-android-x86_64.h"
#elif defined(__ANDROID__) && (defined(__arm__) || defined(__ARM__))
#include "config-android-armv7a.h"
#elif defined(__ANDROID__) && defined(__aarch64__)
#include "config-android-armv8a.h"
#elif defined(__linux__) && !defined(__ANDROID__) && defined(__x86_64__)
#include "config-linux-x86_64.h"
#elif defined(__linux__) && !defined(__ANDROID__) && (defined(__arm__) || defined(__ARM__))
#include "config-linux-armv7a.h"
#elif defined(__linux__) && !defined(__ANDROID__) && defined(__aarch64__)
#include "config-linux-armv8a.h"
#else
#error "Not configured for this platform!"
#endif

#ifdef _musl_
#undef HAVE_RES_NQUERY
#undef HAVE_STRTOLL_L
#undef HAVE_STRTOULL_L
#endif
