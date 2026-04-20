#pragma once

#include "key_name.h"
#include "structured_message.h"

#include <util/generic/maybe.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NStructLog {

class TCreateMessageArg {
public:

    TCreateMessageArg(){ }

    // Native types support
    template <typename T, typename K = TKeyName, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    TCreateMessageArg(K&& name, const T& value) {
        GetBuildMessage().AppendValue({std::forward<TKeyName>(name)}, value);
    }

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    TCreateMessageArg(TKeyName&& name, const TMaybe<T>& value) {
        if (value.Defined()) {
            GetBuildMessage().AppendValue({std::forward<TKeyName>(name)}, value.GetRef());
        }
    }

    template<unsigned N>
    TCreateMessageArg(TKeyName&& name, const char(&value)[N])
    {
        GetBuildMessage().AppendFixedValue({std::forward<TKeyName>(name)}, value);
    }

    // Structured messages support
    TCreateMessageArg(TKeyName&& name, const TStructuredMessage& subMessage) {
        GetBuildMessage().AppendSubMessage({std::forward<TKeyName>(name)}, subMessage);
    }

    TCreateMessageArg(TKeyName&& name, const TMaybe<TStructuredMessage>& subMessage) {
        if (subMessage.Defined()) {
            GetBuildMessage().AppendSubMessage({std::forward<TKeyName>(name)}, subMessage.GetRef());
        }
    }

    TCreateMessageArg(const TStructuredMessage& message) {
        GetBuildMessage().AppendMessage(message);
    }

    TCreateMessageArg(const TMaybe<TStructuredMessage>& message) {
        if (message.Defined()) {
            GetBuildMessage().AppendMessage(message.GetRef());
        }
    }

    TCreateMessageArg(const TCreateMessageArg&) = delete;
    TCreateMessageArg(TCreateMessageArg&&) = delete;
    TCreateMessageArg& operator=(const TCreateMessageArg&) = delete;
    TCreateMessageArg& operator=(TCreateMessageArg&&) = delete;

    void* operator new(std::size_t sz) = delete;
    void* operator new[](std::size_t sz) = delete;

    class TGuard {
    public:
        TGuard() {
            PushBuildMessage();
        }

        ~TGuard() {
            if (!Poped) {
                PopBuildMessage();
            }
        }

        TStructuredMessage Pop() {
            Poped = true;
            return PopBuildMessage();
        }

    protected:
        bool Poped{false};
    };

protected:

    static TStructuredMessage& PushBuildMessage();
    static TStructuredMessage& GetBuildMessage();
    static TStructuredMessage PopBuildMessage();

};

#define YDBLOG_CREATE_MESSAGE(...) [&]() -> TStructuredMessage { \
    TCreateMessageArg::TGuard guard; \
    std::initializer_list<TCreateMessageArg> args{__VA_ARGS__}; \
    Y_UNUSED(args); \
    return guard.Pop(); }()

#define YDBLOG_UPDATE_MESSAGE(M, ...) { \
    TCreateMessageArg::TGuard guard; \
    std::initializer_list<TCreateMessageArg> args{__VA_ARGS__}; \
    Y_UNUSED(args); \
    M.AppendMessage(guard.Pop()); }

}
