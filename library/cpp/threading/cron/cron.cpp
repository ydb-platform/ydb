#include "cron.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/thread.h>
#include <util/system/event.h>

using namespace NCron;

namespace {
    struct TPeriodicHandle: public IHandle {
        inline TPeriodicHandle(TJob job, TDuration interval, const TString& threadName)
            : Job(job)
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

        static inline void* DoRun(void* data) noexcept {
            ((TPeriodicHandle*)data)->Run();

            return nullptr;
        }

        inline void Run() noexcept {
            while (true) {
                Job();

                Event.WaitT(Interval);

                if (AtomicGet(Done)) {
                    return;
                }
            }
        }

        ~TPeriodicHandle() override {
            AtomicSet(Done, true);
            Event.Signal();
            Thread->Join();
        }

        TJob Job;
        TDuration Interval;
        TManualEvent Event;
        TAtomic Done;
        THolder<TThread> Thread;
    };
}

IHandlePtr NCron::StartPeriodicJob(TJob job) {
    return NCron::StartPeriodicJob(job, TDuration::Seconds(0), "");
}

IHandlePtr NCron::StartPeriodicJob(TJob job, TDuration interval) {
    return NCron::StartPeriodicJob(job, interval, "");
}

IHandlePtr NCron::StartPeriodicJob(TJob job, TDuration interval, const TString& threadName) {
    return new TPeriodicHandle(job, interval, threadName);
}

IHandle::~IHandle() = default;
