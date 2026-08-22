#pragma once
#include <util/system/backtrace.h>
#include <yql/essentials/minikql/defs.h>

#ifdef MKQL_ENSURE
#undef MKQL_ENSURE
#endif

// i dont want to print backtrace in production, so i make this guard
#ifdef NDEBUG
#define __DebugBuildBackTrace ((void)0)
#else
#define __DebugBuildBackTrace PrintBackTrace()
#endif

#define MKQL_ENSURE(condition, message)                                                                         \
    do {                                                                                                        \
        if (Y_UNLIKELY(!(condition))) {                                                                         \
            __DebugBuildBackTrace;                                                                            \
            (THROW yexception() << __FUNCTION__ << "(): requirement " << #condition << " failed. " << message); \
        }                                                                                                       \
    } while (0)
