#pragma once

#include <library/cpp/unified_agent_client/async_joiner.h>
#include <library/cpp/unified_agent_client/f_maybe.h>

#include <contrib/libs/grpc/include/grpcpp/alarm.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>

#include <thread>

struct grpc_cq_completion;

namespace NUnifiedAgent {
    enum class EIOStatus {
        Ok,
        Error
    };

    class IIOCallback {
    public:
        virtual ~IIOCallback() = default;

        virtual IIOCallback* Ref() = 0;

        virtual void OnIOCompleted(EIOStatus status) = 0;
    };

    template<typename TCallback, typename TCounter>
    class TIOCallback: public IIOCallback {
    public:
        explicit TIOCallback(TCallback&& callback, TCounter* counter)
            : Callback(std::move(callback))
            , Counter(counter)
        {
        }

        IIOCallback* Ref() override {
            Counter->Ref();
            return this;
        }

        void OnIOCompleted(EIOStatus status) override {
            Callback(status);
            Counter->UnRef();
        }

    private:
        TCallback Callback;
        TCounter* Counter;
    };

    template<typename TCallback, typename TCounter>
    THolder<IIOCallback> MakeIOCallback(TCallback&& callback, TCounter* counter) {
        return MakeHolder<TIOCallback<TCallback, TCounter>>(std::move(callback), counter);
    }

    template<typename TTarget, typename TCounter = TTarget>
    THolder<IIOCallback> MakeIOCallback(TTarget* target, void (TTarget::*method)(EIOStatus),
                                        TCounter* counter = nullptr)
    {
        return MakeIOCallback([target, method](EIOStatus status) { ((*target).*method)(status); },
                              counter ? counter : target);
    }

    class TGrpcNotification: private ::grpc::internal::CompletionQueueTag {
    public:
        TGrpcNotification(grpc::CompletionQueue& completionQueue, THolder<IIOCallback>&& ioCallback);

        ~TGrpcNotification();

        void Trigger();

    private:
        bool FinalizeResult(void** tag, bool* status) override;

    private:
        grpc::CompletionQueue& CompletionQueue;
        THolder<IIOCallback> IOCallback;
        THolder<grpc_cq_completion> Completion;
        std::atomic<bool> InQueue;
    };

    class TGrpcTimer: private IIOCallback {
    public:
        TGrpcTimer(grpc::CompletionQueue& completionQueue, THolder<IIOCallback>&& ioCallback);

        void Set(TInstant triggerTime);

        void Cancel();

    private:
        IIOCallback* Ref() override;

        void OnIOCompleted(EIOStatus status) override;

    private:
        grpc::CompletionQueue& CompletionQueue;
        THolder<IIOCallback> IOCallback;
        grpc::Alarm Alarm;
        bool AlarmIsSet;
        TFMaybe<TInstant> NextTriggerTime;
    };

    class TGrpcCompletionQueuePoller {
    public:
        explicit TGrpcCompletionQueuePoller(grpc::CompletionQueue& queue);

        void Start();

        void Join();

    private:
        grpc::CompletionQueue& Queue;
        std::thread Thread;
    };

    class TGrpcCompletionQueueHost {
    public:
        TGrpcCompletionQueueHost();

        void Start();

        void Stop();

        inline grpc::CompletionQueue& GetCompletionQueue() noexcept {
            return CompletionQueue;
        }

    private:
        grpc::CompletionQueue CompletionQueue;
        TGrpcCompletionQueuePoller Poller;
    };

    gpr_timespec InstantToTimespec(TInstant instant);

    void EnsureGrpcConfigured();

    void StartGrpcTracing();

    void FinishGrpcTracing();
}
