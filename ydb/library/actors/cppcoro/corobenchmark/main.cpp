#include <ydb/library/actors/cppcoro/task.h>
#include <ydb/library/actors/cppcoro/await_callback.h>
#include <library/cpp/testing/benchmark/bench.h>

using namespace NActors;

namespace {

    int LastValue = 0;

    Y_NO_INLINE int NextFuncValue() {
        return ++LastValue;
    }

    Y_NO_INLINE void IterateFuncValues(size_t iterations) {
        for (size_t i = 0; i < iterations; ++i) {
            int value = NextFuncValue();
            Y_DO_NOT_OPTIMIZE_AWAY(value);
        }
    }

    Y_NO_INLINE TTask<int> NextTaskValue() {
        co_return ++LastValue;
    }

    Y_NO_INLINE TTask<void> IterateTaskValues(size_t iterations) {
        for (size_t i = 0; i < iterations; ++i) {
            int value = co_await NextTaskValue();
            Y_DO_NOT_OPTIMIZE_AWAY(value);
        }
    }

    std::coroutine_handle<> Paused;

    struct {
        static bool await_ready() noexcept {
            return false;
        }
        static void await_suspend(std::coroutine_handle<> h) noexcept {
            Paused = h;
        }
        static int await_resume() noexcept {
            return ++LastValue;
        }
    } Pause;

    Y_NO_INLINE TTask<void> IteratePauseValues(size_t iterations) {
        for (size_t i = 0; i < iterations; ++i) {
            int value = co_await Pause;
            Y_DO_NOT_OPTIMIZE_AWAY(value);
        }
    }

} // namespace

Y_CPU_BENCHMARK(FuncCalls, iface) {
    IterateFuncValues(iface.Iterations());
}

Y_CPU_BENCHMARK(TaskCalls, iface) {
    bool finished = false;
    AwaitThenCallback(IterateTaskValues(iface.Iterations()), [&]{
        finished = true;
    });
    Y_ABORT_UNLESS(finished);
}

Y_CPU_BENCHMARK(CoroAwaits, iface) {
    bool finished = false;
    AwaitThenCallback(IteratePauseValues(iface.Iterations()), [&]{
        finished = true;
    });
    while (!finished) {
        std::exchange(Paused, {}).resume();
    }
}
