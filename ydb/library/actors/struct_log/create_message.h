#pragma once

#include "structured_message.h"
#include <initializer_list>

namespace NKikimr::NStructuredLog {

class TCreateMessageArg;

class TCreateMessageGuard {

    friend class TCreateMessageArg;

public:
    TCreateMessageGuard() { PushBuildMessage(); }

    ~TCreateMessageGuard() {
        if (!Poped) {
            PopBuildMessage();
        }
    }

    TStructuredMessage Pop() {
        Poped = true;
        return PopBuildMessage();
    }

protected:
    static TStructuredMessage& PushBuildMessage();
    static TStructuredMessage& GetBuildMessage();
    static TStructuredMessage PopBuildMessage();

    bool Poped{false};
};

#define YDBLOG_CREATE_MESSAGE(...)                                                           \
    [&]() -> TStructuredMessage {                                                            \
        NKikimr::NStructuredLog::TCreateMessageGuard guard;                                  \
        std::initializer_list<NKikimr::NStructuredLog::TCreateMessageArg> args{__VA_ARGS__}; \
        Y_UNUSED(args);                                                                      \
        return guard.Pop();                                                                  \
    }()

#define YDBLOG_UPDATE_MESSAGE(M, ...)                                                        \
    {                                                                                        \
        NKikimr::NStructuredLog::TCreateMessageGuard guard;                                  \
        std::initializer_list<NKikimr::NStructuredLog::TCreateMessageArg> args{__VA_ARGS__}; \
        Y_UNUSED(args);                                                                      \
        M.AppendMessage(guard.Pop());                                                        \
    }                                                                                        \
    Y_SEMICOLON_GUARD

}  // namespace NKikimr::NStructuredLog
