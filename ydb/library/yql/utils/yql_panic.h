#pragma once
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/src_root.h>

namespace NYql {

class TYqlPanic : public yexception
{};

namespace NDetail {
    [[noreturn]] void YqlPanic(const ::NPrivate::TStaticBuf& file, int line, const char* function, const TStringBuf& condition, const TStringBuf& message);
}

#define YQL_ENSURE(CONDITION, ...)     \
    do {                                   \
        if (Y_UNLIKELY(!(CONDITION))) {    \
            ::NYql::NDetail::YqlPanic(__SOURCE_FILE_IMPL__, __LINE__, __FUNCTION__, #CONDITION, TStringBuilder() << "" __VA_ARGS__); \
        }                                  \
    } while (0)

} // namespace NYql
