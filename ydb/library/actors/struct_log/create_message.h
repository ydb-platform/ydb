#pragma once

#include "key_name.h"
#include "structured_message.h"

#include <ydb/library/actors/core/event.h>

#include <util/generic/maybe.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NStructuredLog {

template<typename>
static constexpr bool IsMaybeImpl = false;
template<typename T>
static constexpr bool IsMaybeImpl<TMaybe<T>> = true;
template<typename T>
static constexpr bool IsMaybe = IsMaybeImpl<std::decay_t<T>>;

class TCreateMessageArg {
public:

    TCreateMessageArg() = default;

    // Native types support
    template <typename T, typename K = TKeyName>
    TCreateMessageArg(K&& name, const T& value) {
        if constexpr (std::is_same<T, TStructuredMessage>::value) {
            GetBuildMessage().AppendSubMessage({std::move(name)}, value);
        } else if constexpr (std::is_same<T, TMaybe<TStructuredMessage>>::value) {
            if (value.Defined()) {
                GetBuildMessage().AppendSubMessage({std::move(name)}, value.GetRef());
            }
        } else if constexpr (TNativeTypeSupport<T>::value) {
            GetBuildMessage().AppendValue({std::move(name)}, value);
        } else if constexpr (IsMaybe<T>) {
            if (value.Defined()) {
                TStringBuilder stream;
                stream << value.GetRef();
                GetBuildMessage().AppendValue({std::move(name)}, TString(stream));
            }
        } else {
            TStringBuilder stream;
            stream << value;
            GetBuildMessage().AppendValue({std::move(name)}, TString(stream));
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
    NKikimr::NStructuredLog::TCreateMessageArg::TGuard guard; \
    std::initializer_list<NKikimr::NStructuredLog::TCreateMessageArg> args{__VA_ARGS__}; \
    Y_UNUSED(args); \
    return guard.Pop(); }()

#define YDBLOG_UPDATE_MESSAGE(M, ...) { \
    NKikimr::NStructuredLog::TCreateMessageArg::TGuard guard; \
    std::initializer_list<NKikimr::NStructuredLog::TCreateMessageArg> args{__VA_ARGS__}; \
    Y_UNUSED(args); \
    M.AppendMessage(guard.Pop()); } Y_SEMICOLON_GUARD

}
