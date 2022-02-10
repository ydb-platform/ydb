#pragma once

#include <util/generic/ptr.h>
#include <util/system/hp_timer.h>
#include <util/system/thread.h>

#include <functional>

namespace NKikimr {

    /**
     * Simplified thread spawning for testing
     */
    class TWorkerThread : public ISimpleThread {
    private:
        std::function<void()> Func;
        double Time = 0.0;

    public:
        TWorkerThread(std::function<void()> func)
            : Func(std::move(func))
        { }

        double GetTime() const {
            return Time;
        }

        static THolder<TWorkerThread> Spawn(std::function<void()> func) {
            THolder<TWorkerThread> thread = MakeHolder<TWorkerThread>(std::move(func));
            thread->Start();
            return thread;
        }

    private:
        void* ThreadProc() noexcept override {
            THPTimer timer;
            Func();
            Time = timer.Passed();
            return nullptr;
        }
    };

}
