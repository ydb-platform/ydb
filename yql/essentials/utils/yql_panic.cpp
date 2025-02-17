#include "yql_panic.h"

namespace NYql {
namespace NDetail {

void YqlPanic(const ::NPrivate::TStaticBuf& file, int line, const char* function,
    const TStringBuf& condition, const TStringBuf& message) {
    auto err = TYqlPanic() << file.As<TStringBuf>() << ":" << line << "  "
        << function << "(): requirement " << condition << " failed";
    if (!message.empty()) {
        err << ", message: " << message;
    }

    throw err;
}

} // namespace NDetail
} // namespace NYql
