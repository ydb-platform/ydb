#pragma once 
 
#include <util/system/platform.h>

#if defined(__ANDROID__) && defined(MAPSMOBI_BUILD)
#   include "curl_config-android-maps-mobile.h"
#elif defined(__ANDROID__)
#   include "curl_config-android.h"
#elif defined(__IOS__) && defined(MAPSMOBI_BUILD)
#   include "curl_config-ios-maps-mobile.h"
#elif defined(__IOS__)
#   include "curl_config-ios.h"
#elif defined(__APPLE__)
#   include "curl_config-osx.h"
#elif defined(_MSC_VER)
#   include "curl_config-win.h"
#else
#   include "curl_config-linux.h"
#endif

#if defined(_musl_)
#   include "curl_config-musl.h"
#endif

// Do not misrepresent host on Android and iOS.
#undef OS
#define OS "arcadia"

// c-ares resolver is known to be buggy.
//
// There is no way to configure it properly without a JVM on Android,
// because Android lacks traditional resolv.conf.
//
// For standalone Android programs, it is impossible
// to contact ConnectionManager outside the JVM; this breaks c-ares DNS resolution.
// As we can not distinguish builds of Android apps from standalone Android programs.
//
// During mapkit experiments, c-ares was adding about 10ms to each query timespan.
//
//
// On Linux it caches /etc/resolv.conf contents and does not invalidate it properly

#if defined(ARCADIA_CURL_DNS_RESOLVER_ARES)
    #define USE_ARES
#elif defined(ARCADIA_CURL_DNS_RESOLVER_MULTITHREADED)
    #if defined(USE_ARES)
        #undef USE_ARES
    #endif
    #if defined(__linux__) && !defined(USE_THREADS_POSIX)
        #define USE_THREADS_POSIX 1
    #elif defined(_MSC_VER) && !defined(USE_THREADS_WIN32)
        #define USE_THREADS_WIN32 1
    #endif
#elif defined(ARCADIA_CURL_DNS_RESOLVER_SYNCHRONOUS)
    // force using synchronous resolver by disabling thread support
    #if defined(USE_ARES)
        #undef USE_ARES
    #endif
    #if defined(USE_THREADS_POSIX)
        #undef USE_THREADS_POSIX
    #endif
    #if defined(USE_THREADS_WIN32)
        #undef USE_THREADS_WIN32
    #endif
#else
    #error "No dns resolver is specified or resolver specification is wrong"
#endif
