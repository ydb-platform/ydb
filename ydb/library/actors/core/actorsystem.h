#pragma once

#include "defs.h"

#include "config.h"
#include "event.h"
#include "executor_pool.h"
#include "log_settings.h"
#include "scheduler_cookie.h"
#include "cpu_manager.h"
#include "executor_thread.h"

#include <library/cpp/threading/future/future.h>
#include <ydb/library/actors/util/ticket_lock.h>

#include <util/generic/vector.h>
#include <util/datetime/base.h>
#include <util/system/mutex.h>

namespace NActors {
    class IActor;
    class TActorSystem;
    class TCpuManager;
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
        std::unique_ptr<IActor> Actor;

        TActorSetupCmd();
        TActorSetupCmd(const TActorSetupCmd&) = delete;
        TActorSetupCmd(TActorSetupCmd&&);
        TActorSetupCmd& operator=(const TActorSetupCmd&) = delete;
        TActorSetupCmd& operator=(TActorSetupCmd&&);
        TActorSetupCmd(std::unique_ptr<IActor> actor, TMailboxType::EType mailboxType, ui32 poolId);
        ~TActorSetupCmd();

        // For legacy code, please do not use
        TActorSetupCmd(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId);

        void Set(std::unique_ptr<IActor> actor, TMailboxType::EType mailboxType, ui32 poolId);
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

        TCpuManagerConfig CpuManager;

        TAutoPtr<ISchedulerThread> Scheduler;

        TInterconnectSetup Interconnect;

        bool MonitorStuckActors = false;

        using TLocalServices = TVector<std::pair<TActorId, TActorSetupCmd>>;
        TLocalServices LocalServices;

        ui32 GetExecutorsCount() const {
            return Executors ? ExecutorsCount : CpuManager.GetExecutorsCount();
        }

        TString GetPoolName(ui32 poolId) const {
            return Executors ? Executors[poolId]->GetName() : CpuManager.GetPoolName(poolId);
        }

        ui32 GetThreads(ui32 poolId) const {
            auto result = GetThreadsOptional(poolId);
            Y_ABORT_UNLESS(result, "undefined pool id: %" PRIu32, (ui32)poolId);
            return *result;
        }

        std::optional<ui32> GetThreadsOptional(const ui32 poolId) const {
            if (Y_LIKELY(Executors)) {
                if (Y_LIKELY(poolId < ExecutorsCount)) {
                    return Executors[poolId]->GetDefaultThreadCount();
                } else {
                    return {};
                }
            } else {
                return CpuManager.GetThreadsOptional(poolId);
            }
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
        mutable std::vector<TActorId> DynamicProxies;

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

        template <ESendingType SendingType = ESendingType::Common>
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 executorPool = 0,
                          ui64 revolvingCounter = 0, const TActorId& parentId = TActorId());

        bool MonitorStuckActors() const { return SystemSetup->MonitorStuckActors; }

    private:
        typedef bool (IExecutorPool::*TEPSendFunction)(TAutoPtr<IEventHandle>& ev);

        template <TEPSendFunction EPSpecificSend>
        bool GenericSend(TAutoPtr<IEventHandle> ev) const;

    public:
        template <ESendingType SendingType = ESendingType::Common>
        bool Send(TAutoPtr<IEventHandle> ev) const;

        bool SpecificSend(TAutoPtr<IEventHandle> ev, ESendingType sendingType) const;
        bool SpecificSend(TAutoPtr<IEventHandle> ev) const;

        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) const;

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

        THarmonizerStats GetHarmonizerStats() const;

        std::optional<ui32> GetPoolThreadsCount(const ui32 poolId) const {
            if (!SystemSetup) {
                return {};
            }
            return SystemSetup->GetThreadsOptional(poolId);
        }

        void DeferPreStop(std::function<void()> fn) {
            DeferredPreStop.push_back(std::move(fn));
        }

        TVector<IExecutorPool*> GetBasicExecutorPools() const {
            return CpuManager->GetBasicExecutorPools();
        }

    };
}
