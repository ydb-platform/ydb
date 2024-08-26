#include "defs.h"
#include "activity_guard.h"
#include "actorsystem.h"
#include "callstack.h"
#include "cpu_manager.h"
#include "mailbox.h"
#include "events.h"
#include "interconnect.h"
#include "servicemap.h"
#include "scheduler_queue.h"
#include "scheduler_actor.h"
#include "log.h"
#include "probes.h"
#include "ask.h"
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>
#include <util/generic/hash.h>
#include <util/system/rwlock.h>
#include <util/random/random.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TActorSetupCmd::TActorSetupCmd()
        : MailboxType(TMailboxType::HTSwap)
        , PoolId(0)
        , Actor(nullptr)
    {
    }

    TActorSetupCmd::TActorSetupCmd(TActorSetupCmd&&) = default;
    TActorSetupCmd& TActorSetupCmd::operator=(TActorSetupCmd&&) = default;
    TActorSetupCmd::~TActorSetupCmd() = default;

    TActorSetupCmd::TActorSetupCmd(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId)
        : MailboxType(mailboxType)
        , PoolId(poolId)
        , Actor(actor)
    {
    }

    TActorSetupCmd::TActorSetupCmd(std::unique_ptr<IActor> actor, TMailboxType::EType mailboxType, ui32 poolId)
        : MailboxType(mailboxType)
        , PoolId(poolId)
        , Actor(std::move(actor))
    {
    }

    void TActorSetupCmd::Set(std::unique_ptr<IActor> actor, TMailboxType::EType mailboxType, ui32 poolId) {
        MailboxType = mailboxType;
        PoolId = poolId;
        Actor = std::move(actor);
    }

    struct TActorSystem::TServiceMap : TNonCopyable {
        NActors::TServiceMap<TActorId, TActorId, TActorId::THash> LocalMap;
        TTicketLock Lock;

        TActorId RegisterLocalService(const TActorId& serviceId, const TActorId& actorId) {
            TTicketLock::TGuard guard(&Lock);
            const TActorId old = LocalMap.Update(serviceId, actorId);
            return old;
        }

        TActorId LookupLocal(const TActorId& x) {
            return LocalMap.Find(x);
        }
    };

    TActorSystem::TActorSystem(THolder<TActorSystemSetup>& setup, void* appData,
                               TIntrusivePtr<NLog::TSettings> loggerSettings)
        : NodeId(setup->NodeId)
        , CpuManager(new TCpuManager(setup))
        , ExecutorPoolCount(CpuManager->GetExecutorsCount())
        , Scheduler(setup->Scheduler)
        , InterconnectCount((ui32)setup->Interconnect.ProxyActors.size())
        , CurrentTimestamp(0)
        , CurrentMonotonic(0)
        , CurrentIDCounter(RandomNumber<ui64>())
        , SystemSetup(setup.Release())
        , DefSelfID(NodeId, "actorsystem")
        , AppData0(appData)
        , LoggerSettings0(loggerSettings)
        , StartExecuted(false)
        , StopExecuted(false)
        , CleanupExecuted(false)
    {
        ServiceMap.Reset(new TServiceMap());
    }

    TActorSystem::~TActorSystem() {
        Cleanup();
    }

    template <TActorSystem::TEPSendFunction EPSpecificSend>
    bool TActorSystem::GenericSend(TAutoPtr<IEventHandle> ev) const {
        if (Y_UNLIKELY(!ev))
            return false;

        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_SEND, false> activityGuard;
#ifdef USE_ACTOR_CALLSTACK
        ev->Callstack.TraceIfEmpty();
#endif

        TActorId recipient = ev->GetRecipientRewrite();
        const ui32 recpNodeId = recipient.NodeId();

        if (recpNodeId != NodeId && recpNodeId != 0) {
            // if recipient is not local one - rewrite with forward instruction
            Y_DEBUG_ABORT_UNLESS(!ev->HasEvent() || ev->GetBase()->IsSerializable());
            Y_ABORT_UNLESS(ev->Recipient == recipient,
                "Event rewrite from %s to %s would be lost via interconnect",
                ev->Recipient.ToString().c_str(),
                recipient.ToString().c_str());
            recipient = InterconnectProxy(recpNodeId);
            ev->Rewrite(TEvInterconnect::EvForward, recipient);
        }
        if (recipient.IsService()) {
            TActorId target = ServiceMap->LookupLocal(recipient);
            if (!target && IsInterconnectProxyId(recipient) && ProxyWrapperFactory) {
                const TActorId actorId = ProxyWrapperFactory(const_cast<TActorSystem*>(this),
                    GetInterconnectProxyNode(recipient));
                with_lock(ProxyCreationLock) {
                    target = ServiceMap->LookupLocal(recipient);
                    if (!target) {
                        target = actorId;
                        ServiceMap->RegisterLocalService(recipient, target);
                        DynamicProxies.push_back(target);
                    }
                }
                if (target != actorId) {
                    // a race has occured, terminate newly created actor
                    Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
                }
            }
            recipient = target;
            ev->Rewrite(ev->GetTypeRewrite(), recipient);
        }

        Y_DEBUG_ABORT_UNLESS(recipient == ev->GetRecipientRewrite());
        const ui32 recpPool = recipient.PoolID();
        if (recipient && recpPool < ExecutorPoolCount) {
            if ((CpuManager->GetExecutorPool(recpPool)->*EPSpecificSend)(ev)) {
                return true;
            }
        }
        if (ev) {
            Send(IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown));
        }
        return false;
    }

    template
    bool TActorSystem::GenericSend<&IExecutorPool::Send>(TAutoPtr<IEventHandle> ev) const;
    template
    bool TActorSystem::GenericSend<&IExecutorPool::SpecificSend>(TAutoPtr<IEventHandle> ev) const;

    bool TActorSystem::Send(const TActorId& recipient, IEventBase* ev, ui32 flags, ui64 cookie) const {
        return this->Send(new IEventHandle(recipient, DefSelfID, ev, flags, cookie));
    }

    bool TActorSystem::SpecificSend(TAutoPtr<IEventHandle> ev) const {
        return this->GenericSend<&IExecutorPool::SpecificSend>(ev);
    }

    bool TActorSystem::SpecificSend(TAutoPtr<IEventHandle> ev, ESendingType sendingType) const {
        if (!TlsThreadContext) {
            return this->GenericSend<&IExecutorPool::Send>(ev);
        } else {
            ESendingType previousType = std::exchange(TlsThreadContext->SendingType, sendingType);
            bool isSent = this->GenericSend<&IExecutorPool::SpecificSend>(ev);
            TlsThreadContext->SendingType = previousType;
            return isSent;
        }
    }

    void TActorSystem::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        Schedule(deadline - Timestamp(), ev, cookie);
    }

    void TActorSystem::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        const auto current = Monotonic();
        if (deadline < current)
            deadline = current;

        TTicketLock::TGuard guard(&ScheduleLock);
        ScheduleQueue->Writer.Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TActorSystem::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        const auto deadline = Monotonic() + delta;

        TTicketLock::TGuard guard(&ScheduleLock);
        ScheduleQueue->Writer.Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    NThreading::TFuture<THolder<IEventBase>> TActorSystem::AskGeneric(TMaybe<ui32> expectedEventType,
                                                                      TActorId recipient, THolder<IEventBase> event,
                                                                      TDuration timeout) {
        auto promise = NThreading::NewPromise<THolder<IEventBase>>();
        Register(MakeAskActor(expectedEventType, recipient, std::move(event), timeout, promise).Release());
        return promise.GetFuture();
    }

    ui64 TActorSystem::AllocateIDSpace(ui64 count) {
        Y_DEBUG_ABORT_UNLESS(count < Max<ui32>() / 65536);

        static_assert(sizeof(TAtomic) == sizeof(ui64), "expect sizeof(TAtomic) == sizeof(ui64)");

        // get high 32 bits as seconds from epoch
        // it could wrap every century, but we don't expect any actor-reference to live this long so such wrap will do no harm
        const ui64 timeFromEpoch = TInstant::MicroSeconds(RelaxedLoad(&CurrentTimestamp)).Seconds();

        // get low 32 bits as counter value
        ui32 lowPartEnd = (ui32)(AtomicAdd(CurrentIDCounter, count));
        while (lowPartEnd < count) // if our request crosses 32bit boundary - retry
            lowPartEnd = (ui32)(AtomicAdd(CurrentIDCounter, count));

        const ui64 lowPart = lowPartEnd - count;
        const ui64 ret = (timeFromEpoch << 32) | lowPart;

        return ret;
    }

    TActorId TActorSystem::InterconnectProxy(ui32 destinationNode) const {
        if (destinationNode < InterconnectCount)
            return Interconnect[destinationNode];
        else if (destinationNode != NodeId)
            return MakeInterconnectProxyId(destinationNode);
        else
            return TActorId();
    }

    ui32 TActorSystem::BroadcastToProxies(const std::function<IEventHandle*(const TActorId&)>& eventFabric) {
        // TODO: get rid of this method
        ui32 res = 0;
        for (ui32 i = 0; i < InterconnectCount; ++i) {
            Send(eventFabric(Interconnect[i]));
            ++res;
        }

        auto guard = Guard(ProxyCreationLock);
        for (size_t i = 0; i < DynamicProxies.size(); ++i) {
            const TActorId actorId = DynamicProxies[i];
            auto unguard = Unguard(guard);
            Send(eventFabric(actorId));
            ++res;
        }

        return res;
    }

    TActorId TActorSystem::LookupLocalService(const TActorId& x) const {
        return ServiceMap->LookupLocal(x);
    }

    TActorId TActorSystem::RegisterLocalService(const TActorId& serviceId, const TActorId& actorId) {
        // TODO: notify old actor about demotion
        return ServiceMap->RegisterLocalService(serviceId, actorId);
    }

    void TActorSystem::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        CpuManager->GetPoolStats(poolId, poolStats, statsCopy);
    }

    void TActorSystem::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy, TVector<TExecutorThreadStats>& sharedStatsCopy) const {
        CpuManager->GetPoolStats(poolId, poolStats, statsCopy, sharedStatsCopy);
    }

    THarmonizerStats TActorSystem::GetHarmonizerStats() const {
        return CpuManager->GetHarmonizerStats();

    }

    void TActorSystem::Start() {
        Y_ABORT_UNLESS(StartExecuted == false);
        StartExecuted = true;

        ScheduleQueue.Reset(new NSchedulerQueue::TQueueType());
        TVector<NSchedulerQueue::TReader*> scheduleReaders;
        scheduleReaders.push_back(&ScheduleQueue->Reader);
        CpuManager->PrepareStart(scheduleReaders, this);
        Scheduler->Prepare(this, &CurrentTimestamp, &CurrentMonotonic);
        Scheduler->PrepareSchedules(&scheduleReaders.front(), (ui32)scheduleReaders.size());

        // setup interconnect proxies
        {
            TInterconnectSetup& setup = SystemSetup->Interconnect;
            Interconnect.Reset(new TActorId[InterconnectCount + 1]);
            for (ui32 i = 0, e = InterconnectCount; i != e; ++i) {
                TActorSetupCmd& x = setup.ProxyActors[i];
                if (x.Actor) {
                    Interconnect[i] = Register(x.Actor.release(), x.MailboxType, x.PoolId, i);
                    Y_ABORT_UNLESS(!!Interconnect[i]);
                }
            }
            ProxyWrapperFactory = std::move(SystemSetup->Interconnect.ProxyWrapperFactory);
        }

        // setup local services
        {
            for (ui32 i = 0, e = (ui32)SystemSetup->LocalServices.size(); i != e; ++i) {
                std::pair<TActorId, TActorSetupCmd>& x = SystemSetup->LocalServices[i];
                const TActorId xid = Register(x.second.Actor.release(), x.second.MailboxType, x.second.PoolId, i);
                Y_ABORT_UNLESS(!!xid);
                if (!!x.first)
                    RegisterLocalService(x.first, xid);
            }
        }

        Scheduler->PrepareStart();
        CpuManager->Start();
        Send(MakeSchedulerActorId(), new TEvSchedulerInitialize(scheduleReaders, &CurrentTimestamp, &CurrentMonotonic));
        Scheduler->Start();
    }

    void TActorSystem::Stop() {
        if (StopExecuted || !StartExecuted)
            return;

        StopExecuted = true;

        for (auto&& fn : std::exchange(DeferredPreStop, {})) {
            fn();
        }

        Scheduler->PrepareStop();
        CpuManager->PrepareStop();
        Scheduler->Stop();
        CpuManager->Shutdown();
    }

    void TActorSystem::Cleanup() {
        Stop();
        if (CleanupExecuted || !StartExecuted)
            return;
        CleanupExecuted = true;
        CpuManager->Cleanup();
        Scheduler.Destroy();
    }
}
