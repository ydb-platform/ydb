#include "yql_panic.h"

namespace NYql::NDetail {

void YqlPanic(const ::NPrivate::TStaticBuf& file, int line, const char* function,
              const TStringBuf& condition, const TStringBuf& message) {
    throw TYqlPanic()
        << file.As<TStringBuf>() << ":" << line << "  "
        << function << "(): requirement " << condition << " failed"
        << (message.empty() ? "" : ", message: ")
        << (message.empty() ? "" : message);
}

} // namespace NYql::NDetail
