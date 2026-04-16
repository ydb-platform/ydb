#pragma once

#include "key_name.h"
#include "structured_message.h"

#include <initializer_list>
#include <optional>
#include <vector>

namespace NKikimr::NStructLog {

class TCreateMessageArg {
public:

    TCreateMessageArg(){ }

    // Native types support
    template <typename T, typename K = TKeyName, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    TCreateMessageArg(K&& name, const T& value) {
        GetBuildMessage().AppendValue({std::move(name)}, value);
    }

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    TCreateMessageArg(TKeyName&& name, const std::optional<T>& value) {
        if (value.has_value()) {
            GetBuildMessage().AppendValue({std::move(name)}, value.value());
        }
    }

    template<unsigned N>
    TCreateMessageArg(TKeyName&& name, const char(&value)[N])
    {
        GetBuildMessage().AppendFixedValue({std::move(name)}, value);
    }

    // Structured messages support
    TCreateMessageArg(TKeyName&& name, const TStructuredMessage& subMessage) {
        GetBuildMessage().AppendSubMessage({std::move(name)}, subMessage);
    }

    TCreateMessageArg(TKeyName&& name, const std::optional<TStructuredMessage>& subMessage) {
        if (subMessage.has_value()) {
            GetBuildMessage().AppendSubMessage({std::move(name)}, subMessage.value());
        }
    }

    TCreateMessageArg(const TStructuredMessage& message) {
        GetBuildMessage().AppendMessage(message);
    }

    TCreateMessageArg(const std::optional<TStructuredMessage>& message) {
        if (message.has_value()) {
            GetBuildMessage().AppendMessage(message.value());
        }
    }

    TCreateMessageArg(const TCreateMessageArg&) = delete;
    TCreateMessageArg(TCreateMessageArg&&) = delete;
    TCreateMessageArg& operator=(const TCreateMessageArg&) = delete;
    TCreateMessageArg& operator=(TCreateMessageArg&&) = delete;

    void* operator new(std::size_t sz) = delete;
    void* operator new[](std::size_t sz) = delete;

    static TStructuredMessage& PushBuildMessage();
    static TStructuredMessage& GetBuildMessage();
    static TStructuredMessage PopBuildMessage();

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