#include "persqueue_p.h"
#include "scheduler.h"

#include <util/system/thread.h>

namespace NPersQueue {
void TScheduler::TCallbackHandler::Execute() {
    auto guard = Guard(Lock);
    if (Callback) {
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(std::move(Callback)));
        Callback = nullptr;
    }
}

void TScheduler::TCallbackHandler::TryCancel() {
    auto guard = Guard(Lock);
    if (Callback) {
        Callback = nullptr;
    }
}

bool TScheduler::TCallbackHandlersCompare::operator()(const TIntrusivePtr<TScheduler::TCallbackHandler>& h1, const TIntrusivePtr<TScheduler::TCallbackHandler>& h2) const {
    return h1->Time > h2->Time;
}

TScheduler::TScheduler(TPQLibPrivate* pqLib)
    : Shutdown(false)
    , PQLib(pqLib)
{
    Thread = SystemThreadFactory()->Run([this] {
        this->SchedulerThread();
    });
}

TScheduler::~TScheduler() {
    ShutdownAndWait();
}

void TScheduler::AddToSchedule(TIntrusivePtr<TCallbackHandler> handler) {
    {
        auto guard = Guard(Lock);
        Callbacks.push(std::move(handler));
    }
    Event.Signal();
}

void TScheduler::ShutdownAndWait() {
    AtomicSet(Shutdown, true);
    Event.Signal();
    Thread->Join();
}

void TScheduler::SchedulerThread() {
    TThread::SetCurrentThreadName("pqlib_scheduler");
    while (!AtomicGet(Shutdown)) {
        TInstant deadline = TInstant::Max();
        std::vector<TIntrusivePtr<TCallbackHandler>> callbacks;
        {
            // define next deadline and get expired callbacks
            auto guard = Guard(Lock);
            const TInstant now = TInstant::Now();
            while (!Callbacks.empty()) {
                const auto& top = Callbacks.top();
                if (top->Time <= now) {
                    callbacks.push_back(top);
                    Callbacks.pop();
                } else {
                    deadline = top->Time;
                    break;
                }
            }
        }

        // execute callbacks
        bool shutdown = false;
        for (auto& callback : callbacks) {
            if (shutdown) {
                callback->TryCancel();
            } else {
                callback->Execute();
            }
            shutdown = shutdown || AtomicGet(Shutdown);
        }

        if (!shutdown) {
            Event.WaitD(deadline);
        }
    }

    // cancel all callbacks and clear data
    while (!Callbacks.empty()) {
        Callbacks.top()->TryCancel();
        Callbacks.pop();
    }
}
} // namespace NPersQueue
