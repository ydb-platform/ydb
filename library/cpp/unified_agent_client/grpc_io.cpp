#include "grpc_io.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/src/core/lib/config/core_configuration.h>
#include <contrib/libs/grpc/src/core/lib/event_engine/thread_pool/thread_pool.h>
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

        thread_local grpc::CompletionQueue* TlsCurrentPolledCQ = nullptr;

        struct TNoOpRefStub {
            void Ref() {
            }
            void UnRef() {
            }
        };
        TNoOpRefStub NoOpRefStub;

        /// Guards `AsyncJoiner` during queue posting. Calls `UnRef()` when destroyed,
        /// ensuring the ref is released even if `PostIIOCallbackToCompletionQueue` throws.
        struct TAsyncJoinerGuard {
            TAsyncJoiner* Joiner = nullptr;
            explicit TAsyncJoinerGuard(TAsyncJoiner* j) : Joiner(j) {}
            TAsyncJoinerGuard(const TAsyncJoinerGuard&) = delete;
            TAsyncJoinerGuard& operator=(const TAsyncJoinerGuard&) = delete;
            TAsyncJoinerGuard(TAsyncJoinerGuard&& other) noexcept : Joiner(other.Joiner) {
                other.Joiner = nullptr;
            }
            TAsyncJoinerGuard& operator=(TAsyncJoinerGuard&& other) noexcept {
                if (Joiner) {
                    Joiner->UnRef();
                }
                Joiner = other.Joiner;
                other.Joiner = nullptr;
                return *this;
            }
            ~TAsyncJoinerGuard() {
                if (Joiner) {
                    Joiner->UnRef();
                }
            }
        };

        /// Injected CQ op: FinalizeResult delivers `PollerTag` to the poller (same pattern as TGrpcNotification).
        struct TPostedCompletion final : grpc::internal::CompletionQueueTag {
            grpc::CompletionQueue& CQ;
            THolder<grpc_cq_completion> Completion;
            IIOCallback* PollerTag = nullptr;

            TPostedCompletion(grpc::CompletionQueue& cq, THolder<grpc_cq_completion>&& completion)
                : CQ(cq)
                , Completion(std::move(completion))
            {
            }

            bool FinalizeResult(void** tag, bool*) override {
                Y_ABORT_UNLESS(PollerTag);
                *tag = PollerTag;
                return true;
            }
        };

        /// Bridge: Ref forwards to payload; OnIOCompleted runs payload then deletes itself (heap-allocated).
        struct TPostedBridge final : IIOCallback {
            THolder<TPostedCompletion> Posted;
            THolder<IIOCallback> Payload;

            TPostedBridge(THolder<TPostedCompletion>&& posted, THolder<IIOCallback>&& payload)
                : Posted(std::move(posted))
                , Payload(std::move(payload))
            {
                Posted->PollerTag = this;
            }

            IIOCallback* Ref() override {
                Payload->Ref();
                return this;
            }

            void OnIOCompleted(EIOStatus status) override {
                Payload->OnIOCompleted(status);
                delete this;
            }
        };

        void PostIIOCallbackToCompletionQueue(grpc::CompletionQueue& cq, THolder<IIOCallback>&& payload) {
            auto posted = MakeHolder<TPostedCompletion>(cq, MakeHolder<grpc_cq_completion>());
            TPostedCompletion* postedRaw = posted.Get();
            auto* bridge = new TPostedBridge(std::move(posted), std::move(payload));
            bridge->Ref();
            grpc_core::ApplicationCallbackExecCtx callbackExecCtx;
            grpc_core::ExecCtx execCtx;
            Y_ABORT_UNLESS(grpc_cq_begin_op(cq.cq(), postedRaw));
            grpc_cq_end_op(
                cq.cq(), postedRaw, y_absl::OkStatus(),
                [](void*, grpc_cq_completion*) {
                },
                nullptr, postedRaw->Completion.Get());
        }
    } // namespace

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

    TGrpcTimer::TGrpcTimer(grpc::CompletionQueue& completionQueue, THolder<IIOCallback>&& ioCallback,
                           TAsyncJoiner& asyncJoiner)
        : CompletionQueue(completionQueue)
        , IOCallback(std::move(ioCallback))
        , AsyncJoiner(asyncJoiner)
        , Alarm()
        , AlarmIsSet(false)
        , NextTriggerTime(Nothing())
    {
    }

    void TGrpcTimer::ApplySet(TInstant triggerTime) {
        if (AlarmIsSet) {
            NextTriggerTime = triggerTime;
            Alarm.Cancel();
        } else {
            AlarmIsSet = true;
            Alarm.Set(&CompletionQueue, InstantToTimespec(triggerTime), Ref());
        }
    }

    void TGrpcTimer::ApplyCancel() {
        NextTriggerTime.Clear();
        if (AlarmIsSet) {
            Alarm.Cancel();
        }
    }

    void TGrpcTimer::Set(TInstant triggerTime) {
        if (TlsCurrentPolledCQ == &CompletionQueue) {
            ApplySet(triggerTime);
            return;
        }
        if (!AsyncJoiner.TryRef()) {
            return;
        }
        PostIIOCallbackToCompletionQueue(
            CompletionQueue,
            MakeIOCallback(
                [this, triggerTime, guard = TAsyncJoinerGuard(&AsyncJoiner)](EIOStatus) {
                    ApplySet(triggerTime);
                },
                &NoOpRefStub));
    }

    void TGrpcTimer::Cancel() {
        if (TlsCurrentPolledCQ == &CompletionQueue) {
            ApplyCancel();
            return;
        }
        if (!AsyncJoiner.TryRef()) {
            return;
        }
        PostIIOCallbackToCompletionQueue(
            CompletionQueue,
            MakeIOCallback(
                [this, guard = TAsyncJoinerGuard(&AsyncJoiner)](EIOStatus) {
                    ApplyCancel();
                },
                &NoOpRefStub));
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
                    TlsCurrentPolledCQ = &Queue;
                    static_cast<IIOCallback*>(tag)->OnIOCompleted(ok ? EIOStatus::Ok : EIOStatus::Error);
                    TlsCurrentPolledCQ = nullptr;
                } catch (...) {
                    TlsCurrentPolledCQ = nullptr;
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
            // Set thread limits before any gRPC activity (these are static parameters)
            const auto limitStr = GetEnv("UA_GRPC_EXECUTOR_THREADS_LIMIT");
            ui64 limit;
            if (limitStr.empty() || !TryFromString(limitStr, limit)) {
                limit = 2;
            }
            grpc_core::Executor::SetThreadsLimit(limit);
            grpc_event_engine::experimental::ThreadPool::SetThreadsLimit(limit);

            // Force CoreConfiguration initialization to register all service config parsers
            // This fixes UNIFIEDAGENT-1341 without forcing full grpc_init() which would
            // interfere with other library initializations (e.g., Arrow in YDB)
            // Let grpc_init() happen naturally when first channel is created
            grpc_core::CoreConfiguration::Get();
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
} // namespace NUnifiedAgent
