#pragma once

// unique tag to fix pragma once gcc glueing: ./ydb/library/yql/minikql/defs.h

#include <util/system/compiler.h>
#include <util/generic/array_ref.h>
#include <util/generic/yexception.h>

#define THROW ::NKikimr::TThrowable() , __LOCATION__ +

#define MKQL_ENSURE(condition, message)                               \
    do {                                                              \
        if (Y_UNLIKELY(!(condition))) {                               \
            (THROW yexception() << __FUNCTION__ << "(): requirement " \
                << #condition << " failed. " <<  message);            \
        }                                                             \
    } while (0)

#define MKQL_ENSURE_WITH_LOC(location, condition, message)      \
    do {                                                        \
        if (Y_UNLIKELY(!(condition))) {                         \
            ThrowException(location + yexception() << message); \
        }                                                       \
    } while (0)

#define MKQL_ENSURE_S(condition, ...)                                 \
    do {                                                              \
        if (Y_UNLIKELY(!(condition))) {                               \
            (THROW yexception() << __FUNCTION__ << "(): requirement " \
                << #condition << " failed. " << "" __VA_ARGS__);      \
        }                                                             \
    } while (0)

namespace NKikimr {

template <typename T>
[[noreturn]] void ThrowException(T&& e)
{
    throw e;
}

struct TThrowable
{
    template <typename T>
    [[noreturn]] void operator,(T&& e) {
        ThrowException(e);
    }
};

typedef
#ifdef _win_
struct { ui64 Data, Meta; }
#else
unsigned __int128
#endif
TRawUV;

}
