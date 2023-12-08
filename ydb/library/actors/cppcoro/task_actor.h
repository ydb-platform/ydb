#include <ydb/library/actors/core/actor.h>
#include "task.h"

namespace NActors {

    struct TTaskActorNextEvent {
        static constexpr bool await_ready() noexcept { return false; }

        static void await_suspend(std::coroutine_handle<> h) noexcept;

        static std::unique_ptr<IEventHandle> await_resume();
    };

    class TAfterAwaiter {
    public:
        TAfterAwaiter(TDuration duration)
            : Duration(duration)
        {}

        static constexpr bool await_ready() noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) noexcept;

        void await_resume() {
            Result.Value();
        }

    private:
        TDuration Duration;
        TTaskResult<void> Result;
    };

    class TBindAwaiter {
    public:
        TBindAwaiter(TActorSystem* sys, const TActorId& actorId)
            : Sys(sys)
            , ActorId(actorId)
        {}

        bool await_ready() noexcept;

        void await_suspend(std::coroutine_handle<> h) noexcept;

        void await_resume() {
            Result.Value();
        }

    private:
        TActorSystem* Sys;
        TActorId ActorId;
        TTaskResult<void> Result;
    };

    class TTaskActor {
    public:
        /**
         * Creates a new actor that will run the specified task.
         */
        static IActor* Create(TTask<void>&& task);

        /**
         * Returns the next actor event when awaited
         */
        static constexpr TTaskActorNextEvent NextEvent{};

        /**
         * Returns the identity of current task actor.
         */
        static TActorIdentity SelfId() noexcept;

        /**
         * Returns an actor id of the actor that registered current task actor.
         */
        static TActorId ParentId() noexcept;

        /**
         * Returns awaiter that completes after the specified timeout.
         */
        static TAfterAwaiter After(TDuration duration) noexcept {
            return TAfterAwaiter{ duration };
        }

        /**
         * Returns awaiter that completes on actor thread when awaited.
         */
        static TBindAwaiter Bind() noexcept {
            TActorId actorId = SelfId();
            TActorSystem* sys = TActivationContext::ActorSystem();
            return TBindAwaiter{ sys, actorId };
        }

        /**
         * Returns a task that runs the specified task, but binds the result
         * back to the actor thread. Useful when the specified task may be
         * working with non-actor coroutines.
         */
        template<class T>
        static TTask<T> Bind(TTask<T>&& task) {
            return [](TTask<T> task, TBindAwaiter bindTask) -> TTask<T> {
                auto result = co_await std::move(task).WhenDone();
                co_await bindTask;
                co_return std::move(result).Value();
            }(std::move(task), Bind());
        }
    };

} // namespace NActors
