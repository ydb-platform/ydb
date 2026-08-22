#pragma once

#include <yql/essentials/minikql/defs.h>

#include <coroutine>
#include <exception>

namespace NKikimr {
namespace NMiniKQL {

struct TCoroTask
{
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    struct promise_type
    {
        TCoroTask get_return_object() {
            return TCoroTask(handle_type::from_promise(*this));
        }

        std::suspend_never initial_suspend() {
            return {};
        }

        std::suspend_always final_suspend() noexcept {
            return {};
        }

        void return_void() {
        }

        void unhandled_exception() {
            throw;
        }

        std::suspend_always yield_value(std::suspend_always) {
            // suspend_always is a marker; so we can co_yield {} just to yield with no particular output value
            return {};
        }
    };

    handle_type Handle;

    TCoroTask()
        : Handle(nullptr)
    {
    }

    TCoroTask(handle_type handle)
        : Handle(handle)
    {
    }

    TCoroTask(const TCoroTask&) = delete;
    TCoroTask& operator=(const TCoroTask&) = delete;

    TCoroTask(TCoroTask&& other) noexcept
        : Handle(other.Handle)
    {
        other.Handle = nullptr;
    }

    TCoroTask& operator=(TCoroTask&& other) {
        if (this != &other) {
            MKQL_ENSURE(!Handle, "The previous async task is still active");
            Handle = other.Handle;
            other.Handle = nullptr;
        }
        return *this;
    }

    ~TCoroTask() {
        if (Handle) {
            Handle.destroy();
        }
    }

    bool CheckPending() {
        if (!Handle) [[likely]] {
            return false;
        }
        if (Handle.done()) {
            Handle.destroy();
            Handle = nullptr;
            return false;
        }
        return true;
    }

    bool operator()()
    {
        if (!CheckPending()) {
            return false;
        }
        Handle();
        return CheckPending();
    }
};

}
}
