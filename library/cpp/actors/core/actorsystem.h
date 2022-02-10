#pragma once

#include "defs.h"

#include "actor.h"
#include "balancer.h"
#include "config.h"
#include "event.h"
#include "log_settings.h"
#include "scheduler_cookie.h"
#include "mon_stats.h"

#include <library/cpp/threading/future/future.h>
#include <library/cpp/actors/util/ticket_lock.h>

#include <util/generic/vector.h>
#include <util/datetime/base.h>
#include <util/system/mutex.h>

namespace NActors {
    class TActorSystem;
    class TCpuManager;
    class IExecutorPool;
    struct TWorkerContext;

    inline TActorId MakeInterconnectProxyId(ui32 destNodeId) {
        char data[12];
        memcpy(data, "ICProxy@", 8);
        memcpy(data + 8, &destNodeId, sizeof(ui32));
        return TActorId(0, TStringBuf(data, 12));
    }

    inline bool IsInterconnectProxyId(const TActorId& actorId) {
        return actorId.IsService() && !memcmp(actorId.ServiceId().data(), "ICProxy@", 8);
    }

    inline ui32 GetInterconnectProxyNode(const TActorId& actorId) {
        ui32 nodeId;
        memcpy(&nodeId, actorId.ServiceId().data() + 8, sizeof(ui32));
        return nodeId;
    }

    namespace NSchedulerQueue {
        class TReader;
        struct TQueueType;
    }

    class IExecutorPool : TNonCopyable {
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
        virtual ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) = 0;
        virtual void ReclaimMailbox(TMailboxType::EType mailboxType, ui32 hint, TWorkerId workerId, ui64 revolvingCounter) = 0;

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
        virtual void ScheduleActivation(ui32 activation) = 0;
        virtual void ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) = 0;
        virtual TActorId Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingCounter, const TActorId& parentId) = 0;
        virtual TActorId Register(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) = 0;

        // lifecycle stuff
        virtual void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) = 0;
        virtual void Start() = 0;
        virtual void PrepareStop() = 0;
        virtual void Shutdown() = 0;
        virtual bool Cleanup() = 0;

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
 
        // generic
        virtual TAffinity* Affinity() const = 0;

        virtual void SetRealTimeMode() const {}
    };

    // could be proxy to in-pool schedulers (for NUMA-aware executors)
    class ISchedulerThread : TNonCopyable {
    public:
        virtual ~ISchedulerThread() {
        }

        virtual void Prepare(TActorSystem* actorSystem, volatile ui64* currentTimestamp, volatile ui64* currentMonotonic) = 0;
        virtual void PrepareSchedules(NSchedulerQueue::TReader** readers, ui32 scheduleReadersCount) = 0;
        virtual void PrepareStart() { /* empty */ }
        virtual void Start() = 0;
        virtual void PrepareStop() = 0;
        virtual void Stop() = 0;
    };

    struct TActorSetupCmd {
        TMailboxType::EType MailboxType;
        ui32 PoolId;
        IActor* Actor;

        TActorSetupCmd()
            : MailboxType(TMailboxType::HTSwap)
            , PoolId(0)
            , Actor(nullptr)
        {
        }

        TActorSetupCmd(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId)
            : MailboxType(mailboxType)
            , PoolId(poolId)
            , Actor(actor)
        {
        }

        void Set(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) {
            MailboxType = mailboxType;
            PoolId = poolId;
            Actor = actor;
        }
    };

    using TProxyWrapperFactory = std::function<TActorId(TActorSystem*, ui32)>;

    struct TInterconnectSetup {
        TVector<TActorSetupCmd> ProxyActors;
        TProxyWrapperFactory ProxyWrapperFactory;
    };

    struct TActorSystemSetup {
        ui32 NodeId = 0;

        // Either Executors or CpuManager must be initialized
        ui32 ExecutorsCount = 0;
        TArrayHolder<TAutoPtr<IExecutorPool>> Executors;

        TAutoPtr<IBalancer> Balancer; // main implementation will be implicitly created if not set

        TCpuManagerConfig CpuManager;

        TAutoPtr<ISchedulerThread> Scheduler;
        ui32 MaxActivityType = 5; // for default entries

        TInterconnectSetup Interconnect;

        using TLocalServices = TVector<std::pair<TActorId, TActorSetupCmd>>;
        TLocalServices LocalServices;

        ui32 GetExecutorsCount() const {
            return Executors ? ExecutorsCount : CpuManager.GetExecutorsCount();
        }

        TString GetPoolName(ui32 poolId) const {
            return Executors ? Executors[poolId]->GetName() : CpuManager.GetPoolName(poolId);
        }

        ui32 GetThreads(ui32 poolId) const {
            return Executors ? Executors[poolId]->GetThreads() : CpuManager.GetThreads(poolId);
        }
    };

    class TActorSystem : TNonCopyable {
        struct TServiceMap;

    public:
        const ui32 NodeId;

    private:
        THolder<TCpuManager> CpuManager;
        const ui32 ExecutorPoolCount;

        TAutoPtr<ISchedulerThread> Scheduler;
        THolder<TServiceMap> ServiceMap;

        const ui32 InterconnectCount;
        TArrayHolder<TActorId> Interconnect;

        volatile ui64 CurrentTimestamp;
        volatile ui64 CurrentMonotonic;
        volatile ui64 CurrentIDCounter;

        THolder<NSchedulerQueue::TQueueType> ScheduleQueue;
        mutable TTicketLock ScheduleLock;

        friend class TExecutorThread;

        THolder<TActorSystemSetup> SystemSetup;
        TActorId DefSelfID;
        void* AppData0;
        TIntrusivePtr<NLog::TSettings> LoggerSettings0;
        TProxyWrapperFactory ProxyWrapperFactory;
        TMutex ProxyCreationLock;

        bool StartExecuted;
        bool StopExecuted;
        bool CleanupExecuted;

        std::deque<std::function<void()>> DeferredPreStop;
    public:
        TActorSystem(THolder<TActorSystemSetup>& setup, void* appData = nullptr,
                     TIntrusivePtr<NLog::TSettings> loggerSettings = TIntrusivePtr<NLog::TSettings>(nullptr));
        ~TActorSystem();

        void Start();
        void Stop();
        void Cleanup();

        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 executorPool = 0,
                          ui64 revolvingCounter = 0, const TActorId& parentId = TActorId());

        bool Send(TAutoPtr<IEventHandle> ev) const;
        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0) const;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;

        /**
         * A way to interact with actors from non-actor context.
         *
         * This method will send the `event` to the `recipient` and then will wait for a response. When response arrives,
         * it will be passed to the future. If response is not of type `T`, the future will resolve into an exception.
         *
         * @tparam T            expected response type. Must be derived from `TEventBase`,
         *                      or use `IEventBase` to catch any response.
         * @param actorSystem   actor system that will be used to register an actor that'll wait for response.
         * @param recipient     who will get a request.
         * @param event         a request message.
         * @return              future that will be resolved when a message from `recipient` arrives.
         */
        template <typename T>
        [[nodiscard]]
        NThreading::TFuture<THolder<T>> Ask(TActorId recipient, THolder<IEventBase> event, TDuration timeout = TDuration::Max()) {
            if constexpr (std::is_same_v<T, IEventBase>) {
                return AskGeneric(Nothing(), recipient, std::move(event), timeout);
            } else {
                return AskGeneric(T::EventType, recipient, std::move(event), timeout)
                    .Apply([](const NThreading::TFuture<THolder<IEventBase>>& ev) {
                        return THolder<T>(static_cast<T*>(const_cast<THolder<IEventBase>&>(ev.GetValueSync()).Release()));  // =(
                    });
            }
        }

        [[nodiscard]]
        NThreading::TFuture<THolder<IEventBase>> AskGeneric(
            TMaybe<ui32> expectedEventType,
            TActorId recipient,
            THolder<IEventBase> event,
            TDuration timeout);

        ui64 AllocateIDSpace(ui64 count);

        TActorId InterconnectProxy(ui32 destinationNode) const;
        ui32 BroadcastToProxies(const std::function<IEventHandle*(const TActorId&)>&);

        void UpdateLinkStatus(ui8 status, ui32 destinationNode);
        ui8 LinkStatus(ui32 destinationNode);

        TActorId LookupLocalService(const TActorId& x) const;
        TActorId RegisterLocalService(const TActorId& serviceId, const TActorId& actorId);

        ui32 GetMaxActivityType() const {
            return SystemSetup ? SystemSetup->MaxActivityType : 1;
        }

        TInstant Timestamp() const {
            return TInstant::MicroSeconds(RelaxedLoad(&CurrentTimestamp));
        }

        TMonotonic Monotonic() const {
            return TMonotonic::MicroSeconds(RelaxedLoad(&CurrentMonotonic));
        }

        template <typename T>
        T* AppData() const {
            return (T*)AppData0;
        }

        NLog::TSettings* LoggerSettings() const {
            return LoggerSettings0.Get();
        }

        void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const;

        void DeferPreStop(std::function<void()> fn) {
            DeferredPreStop.push_back(std::move(fn));
        }

        /* This is the base for memory profiling tags.
       System sets memory profiling tag for debug version of lfalloc.
       The tag is set as "base_tag + actor_activity_type". */
        static ui32 MemProfActivityBase;
    };
}
