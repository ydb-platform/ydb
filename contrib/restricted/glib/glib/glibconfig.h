#pragma once

#define GLIB_DISABLE_DEPRECATION_WARNINGS

#if defined(_MSC_VER) && defined(_WIN64)
#include "glibconfig-win-x86_64.h"
#elif defined(__APPLE__) && defined(__x86_64__)
#include "glibconfig-osx-x86_64.h"
#elif defined(__APPLE__) && (defined(__aarch64__) || defined(_M_ARM64))
#include "glibconfig-osx-arm.h"
#elif defined(__ANDROID__) && defined(__i686__)
#include "glibconfig-android-i686.h"
#elif defined(__ANDROID__) && defined(__x86_64__)
#include "glibconfig-android-x86_64.h"
#elif defined(__ANDROID__) && (defined(__arm__) || defined(__ARM__))
#include "glibconfig-android-armv7a.h"
#elif defined(__ANDROID__) && defined(__aarch64__)
#include "glibconfig-android-armv8a.h"
#elif defined(__linux__) && !defined(__ANDROID__) && defined(__x86_64__)
#include "glibconfig-linux-x86_64.h"
#elif defined(__linux__) && !defined(__ANDROID__) && (defined(__arm__) || defined(__ARM__))
#include "glibconfig-linux-armv7a.h"
#elif defined(__linux__) && !defined(__ANDROID__) && defined(__aarch64__)
#include "glibconfig-linux-armv8a.h"
#else
#error "Not configured for this platform!"
#endif
