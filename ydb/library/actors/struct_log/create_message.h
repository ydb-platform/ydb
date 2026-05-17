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
