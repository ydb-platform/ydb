#include "grpc_io.h"

#include <contrib/libs/grpc/src/core/lib/iomgr/exec_ctx.h>
#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>
#include <contrib/libs/grpc/src/core/lib/surface/completion_queue.h>
#include <contrib/libs/grpc/include/grpc/impl/codegen/log.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/system/env.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NUnifiedAgent {
    namespace {
        std::once_flag GrpcConfigured{};
    }

    TGrpcNotification::TGrpcNotification(grpc::CompletionQueue& completionQueue, THolder<IIOCallback>&& ioCallback)
        : CompletionQueue(completionQueue)
        , IOCallback(std::move(ioCallback))
        , Completion(MakeHolder<grpc_cq_completion>())
        , InQueue(false)
    {
    }

    TGrpcNotification::~TGrpcNotification() = default;

    void TGrpcNotification::Trigger() {
        {
            bool inQueue = false;
            if (!InQueue.compare_exchange_strong(inQueue, true)) {
                return;
            }
        }
        grpc_core::ApplicationCallbackExecCtx callbackExecCtx;
        grpc_core::ExecCtx execCtx;
        IOCallback->Ref();
        Y_ABORT_UNLESS(grpc_cq_begin_op(CompletionQueue.cq(), this));
        grpc_cq_end_op(CompletionQueue.cq(), this, y_absl::OkStatus(),
                       [](void* self, grpc_cq_completion*) {
                           Y_ABORT_UNLESS(static_cast<TGrpcNotification*>(self)->InQueue.exchange(false));
                       },
                       this, Completion.Get());
    }

    bool TGrpcNotification::FinalizeResult(void** tag, bool*) {
        *tag = IOCallback.Get();
        return true;
    }

    TGrpcTimer::TGrpcTimer(grpc::CompletionQueue& completionQueue, THolder<IIOCallback>&& ioCallback)
        : CompletionQueue(completionQueue)
        , IOCallback(std::move(ioCallback))
        , Alarm()
        , AlarmIsSet(false)
        , NextTriggerTime(Nothing())
    {
    }

    void TGrpcTimer::Set(TInstant triggerTime) {
        if (AlarmIsSet) {
            NextTriggerTime = triggerTime;
            Alarm.Cancel();
        } else {
            AlarmIsSet = true;
            Alarm.Set(&CompletionQueue, InstantToTimespec(triggerTime), Ref());
        }
    }

    void TGrpcTimer::Cancel() {
        NextTriggerTime.Clear();
        if (AlarmIsSet) {
            Alarm.Cancel();
        }
    }

    IIOCallback* TGrpcTimer::Ref() {
        IOCallback->Ref();
        return this;
    }

    void TGrpcTimer::OnIOCompleted(EIOStatus status) {
        Y_ABORT_UNLESS(AlarmIsSet);
        if (NextTriggerTime) {
            Alarm.Set(&CompletionQueue, InstantToTimespec(*NextTriggerTime), this);
            NextTriggerTime.Clear();
        } else {
            AlarmIsSet = false;
            IOCallback->OnIOCompleted(status);
        }
    }

    TGrpcCompletionQueuePoller::TGrpcCompletionQueuePoller(grpc::CompletionQueue& queue)
        : Queue(queue)
        , Thread()
    {
    }

    void TGrpcCompletionQueuePoller::Start() {
        Thread = std::thread([this]() {
            TThread::SetCurrentThreadName("ua_grpc_cq");
            void* tag;
            bool ok;
            while (Queue.Next(&tag, &ok)) {
                try {
                    static_cast<IIOCallback*>(tag)->OnIOCompleted(ok ? EIOStatus::Ok : EIOStatus::Error);
                } catch (...) {
                    Y_ABORT("unexpected exception [%s]", CurrentExceptionMessage().c_str());
                }
            }
        });
    }

    void TGrpcCompletionQueuePoller::Join() {
        Thread.join();
    }

    TGrpcCompletionQueueHost::TGrpcCompletionQueueHost()
        : CompletionQueue()
        , Poller(CompletionQueue)
    {
    }

    void TGrpcCompletionQueueHost::Start() {
        Poller.Start();
    }

    void TGrpcCompletionQueueHost::Stop() {
        CompletionQueue.Shutdown();
        Poller.Join();
    }

    gpr_timespec InstantToTimespec(TInstant instant) {
        gpr_timespec result;
        result.clock_type = GPR_CLOCK_REALTIME;
        result.tv_sec = static_cast<int64_t>(instant.Seconds());
        result.tv_nsec = instant.NanoSecondsOfSecond();
        return result;
    }

    void EnsureGrpcConfigured() {
        std::call_once(GrpcConfigured, []() {
            const auto limitStr = GetEnv("UA_GRPC_EXECUTOR_THREADS_LIMIT");
            ui64 limit;
            if (limitStr.Empty() || !TryFromString(limitStr, limit)) {
                limit = 2;
            }
            grpc_core::Executor::SetThreadsLimit(limit);
        });
    }

    void StartGrpcTracing() {
        grpc_tracer_set_enabled("all", true);
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
    }

    void FinishGrpcTracing() {
        grpc_tracer_set_enabled("all", false);
        gpr_set_log_verbosity(GPR_LOG_SEVERITY_ERROR);
    }
}
