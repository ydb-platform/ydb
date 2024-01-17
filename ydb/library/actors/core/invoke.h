#pragma once

#include "actor_bootstrapped.h"
#include "events.h"
#include "event_local.h"

#include <any>
#include <type_traits>
#include <utility>
#include <variant>

#include <util/system/type_name.h>

namespace NActors {

    struct TEvents::TEvInvokeResult
        : TEventLocal<TEvInvokeResult, TSystem::InvokeResult>
    {
        using TProcessCallback = std::function<void(TEvInvokeResult&, const TActorContext&)>;
        TProcessCallback ProcessCallback;
        std::variant<std::any /* the value */, std::exception_ptr> Result;

        // This constructor creates TEvInvokeResult with the result of calling callback(args...) or exception_ptr,
        // if exception occurs during evaluation.
        template<typename TCallback, typename... TArgs>
        TEvInvokeResult(TProcessCallback&& process, TCallback&& callback, TArgs&&... args)
            : ProcessCallback(std::move(process))
        {
            try {
                if constexpr (std::is_void_v<std::invoke_result_t<TCallback, TArgs...>>) {
                    // just invoke callback without saving any value
                    std::invoke(std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
                } else {
                    Result.emplace<std::any>(std::invoke(std::forward<TCallback>(callback), std::forward<TArgs>(args)...));
                }
            } catch (...) {
                Result.emplace<std::exception_ptr>(std::current_exception());
            }
        }

        void Process(const TActorContext& ctx) {
            ProcessCallback(*this, ctx);
        }

        template<typename TCallback>
        std::invoke_result_t<TCallback, const TActorContext&> GetResult() {
            using T = std::invoke_result_t<TCallback, const TActorContext&>;
            return std::visit([](auto& arg) -> T {
                using TArg = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<TArg, std::exception_ptr>) {
                    std::rethrow_exception(arg);
                } else if constexpr (std::is_void_v<T>) {
                    Y_ABORT_UNLESS(!arg.has_value());
                } else if (auto *value = std::any_cast<T>(&arg)) {
                    return std::move(*value);
                } else {
                    Y_ABORT("unspported return type for TEvInvokeResult: actual# %s != expected# %s",
                        TypeName(arg.type()).data(), TypeName<T>().data());
                }
            }, Result);
        }
    };

    // Invoke Actor is used to make different procedure calls in specific threads pools.
    //
    // Actor is created by CreateInvokeActor(callback, complete) where `callback` is the function that will be invoked
    // upon actor registration, which will issue then TEvInvokeResult to the parent actor with the result of called
    // function. If the called function throws exception, then the exception will arrive in the result. Receiver of
    // this message can either handle it by its own means calling ev.GetResult() (which will rethrow exception if it
    // has occured in called function or return its return value; notice that when there is no return value, then
    // GetResult() should also be called to prevent losing exception), or invoke ev.Process(), which will invoke
    // callback provided as `complete` parameter to the CreateInvokeActor function. Complete handler is invoked with
    // the result-getter lambda as the first argument and the actor system context as the second one. Result-getter
    // should be called to obtain resulting value or exception like the GetResult() method of the TEvInvokeResult event.
    //
    // Notice that `callback` execution usually occurs in separate actor on separate mailbox and should not use parent
    // actor's class. But `complete` handler is invoked in parent context and can use its contents. Do not forget to
    // handle TEvInvokeResult event by calling Process/GetResult method, whichever is necessary.

    template<typename TCallback, typename TCompletion, class TEnum>
    class TInvokeActor : public TActorBootstrapped<TInvokeActor<TCallback, TCompletion, TEnum>> {
    private:
        using TBase = TActorBootstrapped<TInvokeActor<TCallback, TCompletion, TEnum>>;
        TCallback Callback;
        TCompletion Complete;
        const TEnum Activity;
        static_assert(std::is_enum<TEnum>::value);
    public:
        TInvokeActor(TCallback&& callback, TCompletion&& complete, const TEnum activity)
            : TBase(activity)
            , Callback(std::move(callback))
            , Complete(std::move(complete))
            , Activity(activity)
        {}

        void Bootstrap(const TActorId& parentId, const TActorContext& ctx) {
            auto process = [complete = std::move(Complete)](TEvents::TEvInvokeResult& res, const TActorContext& ctx) {
                complete([&] { return res.GetResult<TCallback>(); }, ctx);
            };
            ctx.Send(parentId, new TEvents::TEvInvokeResult(std::move(process), std::move(Callback), ctx));
            TActorBootstrapped<TInvokeActor>::Die(ctx);
        }
    };

    template<typename TEnum, typename TCallback, typename TCompletion>
    std::unique_ptr<IActor> CreateInvokeActor(TCallback&& callback, TCompletion&& complete, const TEnum activity) {
        return std::make_unique<TInvokeActor<std::decay_t<TCallback>, std::decay_t<TCompletion>, TEnum>>(
            std::forward<TCallback>(callback), std::forward<TCompletion>(complete), activity);
    }

    template <class TInvokeExecutor>
    class TScheduledInvokeActivity: public TActor<TScheduledInvokeActivity<TInvokeExecutor>> {
    private:
        using TBase = TActor<TScheduledInvokeActivity<TInvokeExecutor>>;
        const TMonotonic Timestamp;
        TInvokeExecutor Executor;
    public:
        TScheduledInvokeActivity(TInvokeExecutor&& executor, const TMonotonic timestamp)
            : TBase(&TBase::TThis::StateFunc)
            , Timestamp(timestamp)
            , Executor(std::move(executor)) {
        }

        void StateFunc(STFUNC_SIG) {
            Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvents::TSystem::Wakeup);
            auto g = TBase::PassAwayGuard();
            Executor();
        }

        void Registered(TActorSystem* sys, const TActorId& owner) override {
            sys->Schedule(Timestamp, new IEventHandle(TEvents::TSystem::Wakeup, 0, TBase::SelfId(), owner, nullptr, 0));
        }
    };

    template<class TInvokeExecutor>
    void ScheduleInvokeActivity(TInvokeExecutor&& executor, const TDuration d) {
        TActivationContext::Register(new TScheduledInvokeActivity<TInvokeExecutor>(std::move(executor), TMonotonic::Now() + d));
    }

    template<class TInvokeExecutor>
    void ScheduleInvokeActivity(TInvokeExecutor&& executor, const TMonotonic timestamp) {
        TActivationContext::Register(new TScheduledInvokeActivity<TInvokeExecutor>(std::move(executor), timestamp));
    }

} // NActors
