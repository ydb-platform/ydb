#pragma once
#include <util/system/backtrace.h>
#include <yql/essentials/minikql/defs.h>

#ifdef MKQL_ENSURE
#undef MKQL_ENSURE
#endif
#define MKQL_ENSURE(condition, message)                                                                         \
    do {                                                                                                        \
        if (Y_UNLIKELY(!(condition))) {                                                                         \
            PrintBackTrace();                                                                                   \
            (THROW yexception() << __FUNCTION__ << "(): requirement " << #condition << " failed. " << message); \
        }                                                                                                       \
    } while (0)
