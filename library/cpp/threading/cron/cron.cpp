#include "cron.h"

#include <util/system/thread.h>
#include <util/system/event.h>

#include <atomic>
#include <utility>

using namespace NCron;

namespace {
    struct TPeriodicHandle: IHandle {
        TPeriodicHandle(TJob job, const TDuration interval, const TString& threadName)
            : Job(std::move(job))
            , Interval(interval)
            , Done(false)
        {
            TThread::TParams params(DoRun, this);
            if (!threadName.empty()) {
                params.SetName(threadName);
            }
            Thread = MakeHolder<TThread>(params);
            Thread->Start();
        }

        static void* DoRun(void* data) noexcept {
            static_cast<TPeriodicHandle*>(data)->Run();

            return nullptr;
        }

        void Run() noexcept {
            while (true) {
                Job();

                Event.WaitT(Interval);

                if (Done.load()) {
                    return;
                }
            }
        }

        ~TPeriodicHandle() override {
            Done.store(true);
            Event.Signal();
            Thread->Join();
        }

        TJob Job;
        TDuration Interval;
        TManualEvent Event;
        std::atomic<bool> Done;
        THolder<TThread> Thread;
    };
}

IHandlePtr NCron::StartPeriodicJob(TJob job) {
    return NCron::StartPeriodicJob(std::move(job), TDuration::Seconds(0), TString());
}

IHandlePtr NCron::StartPeriodicJob(TJob job, const TDuration interval) {
    return NCron::StartPeriodicJob(std::move(job), interval, TString());
}

IHandlePtr NCron::StartPeriodicJob(TJob job, TDuration interval, const TString& threadName) {
    return new TPeriodicHandle(std::move(job), interval, threadName);
}

IHandle::~IHandle() = default;
