#pragma once

#include "structured_message.h"

#include <initializer_list>

namespace NActors::NStructuredLog {

class TCreateMessageArg;

class TCreateMessageGuard {
    friend class TCreateMessageArg;

public:
    TCreateMessageGuard();
    ~TCreateMessageGuard();
    TStructuredMessage Pop();

protected:
    static TStructuredMessage& PushBuildMessage();
    static TStructuredMessage& GetBuildMessage();
    static TStructuredMessage PopBuildMessage();

    bool Popped{false};
};

// YDB_LOG_CREATE_MESSAGE and YDB_LOG_UPDATE_MESSAGE use lamdba because of following reasons:
// 1. YDB_LOG_CREATE_MESSAGE must return created structured message.
// 2. Lambda is rounded in ( ) - it is necc for working inside another macros. As example,
//    s3_scan.cpp outputs log messages inside STRICT_STFUNC_BODY macro parameters.
// 3. YDB_LOG_UPDATE_MESSAGE doesn't return message, but it should be usable inside another macro parameters too.

#define YDB_LOG_CREATE_MESSAGE(...)                                                                \
    ([&]() -> TStructuredMessage {                                                                 \
        NActors::NStructuredLog::TCreateMessageGuard ydblogGuard;                                  \
        std::initializer_list<NActors::NStructuredLog::TCreateMessageArg> ydblogArgs{__VA_ARGS__}; \
        Y_UNUSED(ydblogArgs);                                                                      \
        return ydblogGuard.Pop();                                                                  \
    }())

#define YDB_LOG_UPDATE_MESSAGE(M,...)                                                              \
    do {([&]() {                                                                                   \
        NActors::NStructuredLog::TCreateMessageGuard ydblogGuard;                                  \
        std::initializer_list<NActors::NStructuredLog::TCreateMessageArg> ydblogArgs{__VA_ARGS__}; \
        Y_UNUSED(ydblogArgs);                                                                      \
        M.AppendMessage(ydblogGuard.Pop());                                                        \
    }());                                                                                          \
    } while (false)

}  // namespace NActors::NStructuredLog
