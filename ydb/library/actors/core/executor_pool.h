#pragma once

#include "event.h"
#include "executor_pool_jail.h"
#include "scheduler_queue.h"

namespace NActors {
    class TActorSystem;
    struct TMailboxHeader;
    struct TWorkerContext;
    struct TExecutorPoolStats;
    struct TExecutorThreadStats;
    class TExecutorPoolJail;
    class ISchedulerCookie;

    struct TCpuConsumption {
        double ConsumedUs = 0;
        double BookedUs = 0;
        ui64 NotEnoughCpuExecutions = 0;

        void Add(const TCpuConsumption& other) {
            ConsumedUs += other.ConsumedUs;
            BookedUs += other.BookedUs;
            NotEnoughCpuExecutions += other.NotEnoughCpuExecutions;
        }
    };

    struct IActorThreadPool : TNonCopyable {

        virtual ~IActorThreadPool() {
        }

        // lifecycle stuff
        virtual void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) = 0;
        virtual void Start() = 0;
        virtual void PrepareStop() = 0;
        virtual void Shutdown() = 0;
        virtual bool Cleanup() = 0;
    };

    class IExecutorPool : public IActorThreadPool {
    public:
        const ui32 PoolId;

        TAtomic ActorRegistrations;
        TAtomic DestroyedActors;

        IExecutorPool(ui32 poolId)
            : PoolId(poolId)
            , ActorRegistrations(0)
            , DestroyedActors(0)
        {
        }

        virtual ~IExecutorPool() {
        }

        // for workers
        virtual void Initialize(TWorkerContext& wctx) {
            Y_UNUSED(wctx);
        }
        virtual ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) = 0;
        virtual void ReclaimMailbox(TMailboxType::EType mailboxType, ui32 hint, TWorkerId workerId, ui64 revolvingCounter) = 0;
        virtual TMailboxHeader *ResolveMailbox(ui32 hint) = 0;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         * @param workerId   index of thread which will perform event dispatching
         */
        virtual void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) = 0;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         * @param workerId   index of thread which will perform event dispatching
         */
        virtual void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) = 0;

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         * @param workerId   index of thread which will perform event dispatching
         */
        virtual void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) = 0;

        // for actorsystem
        virtual bool Send(TAutoPtr<IEventHandle>& ev) = 0;
        virtual bool SpecificSend(TAutoPtr<IEventHandle>& ev) = 0;
        virtual void ScheduleActivation(ui32 activation) = 0;
        virtual void SpecificScheduleActivation(ui32 activation) = 0;
        virtual void ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) = 0;
        virtual TActorId Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingCounter, const TActorId& parentId) = 0;
        virtual TActorId Register(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) = 0;

        virtual void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
            // TODO: make pure virtual and override everywhere
            Y_UNUSED(poolStats);
            Y_UNUSED(statsCopy);
        }

        virtual TString GetName() const {
            return TString();
        }

        virtual ui32 GetThreads() const {
            return 1;
        }

        virtual i16 GetPriority() const {
            return 0;
        }

        // generic
        virtual TAffinity* Affinity() const = 0;

        virtual void SetRealTimeMode() const {}

        virtual float GetThreadCount() const {
            return 1;
        }

        virtual i16 GetFullThreadCount() const {
            return 1;
        }

        virtual void SetFullThreadCount(i16 threads) {
            Y_UNUSED(threads);
        }

        virtual void SetSpinThresholdCycles(ui32 cycles) {
            Y_UNUSED(cycles);
        }

        virtual i16 GetBlockingThreadCount() const {
            return 0;
        }

        virtual float GetDefaultThreadCount() const {
            return 1;
        }

        virtual i16 GetDefaultFullThreadCount() const {
            return 1;
        }

        virtual float GetMinThreadCount() const {
            return 1;
        }


        virtual i16 GetMinFullThreadCount() const {
            return 1;
        }

        virtual float GetMaxThreadCount() const {
            return 1;
        }

        virtual i16 GetMaxFullThreadCount() const {
            return 1;
        }

        virtual TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) {
            Y_UNUSED(threadIdx);
            return TCpuConsumption{0.0, 0.0};
        }
    };

}
