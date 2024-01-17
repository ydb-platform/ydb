#include "flow_controlled_queue.h"

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/util/datetime.h>

#include <util/generic/deque.h>
#include <util/datetime/cputimer.h>
#include <util/generic/algorithm.h>

namespace NActors {

class TFlowControlledRequestQueue;

class TFlowControlledRequestActor : public IActorCallback {
    TFlowControlledRequestQueue * const QueueActor;

    void HandleReply(TAutoPtr<IEventHandle> &ev);
    void HandleUndelivered(TEvents::TEvUndelivered::TPtr &ev);
public:
    const TActorId Source;
    const ui64 Cookie;
    const ui32 Flags;
    const ui64 StartCounter;

    TFlowControlledRequestActor(ui32 activity, TFlowControlledRequestQueue *queue, TActorId source, ui64 cookie, ui32 flags)
        : IActorCallback(static_cast<TReceiveFunc>(&TFlowControlledRequestActor::StateWait), activity)
        , QueueActor(queue)
        , Source(source)
        , Cookie(cookie)
        , Flags(flags)
        , StartCounter(GetCycleCountFast())
    {}

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
        default:
            HandleReply(ev);
        }
    }

    TDuration AccumulatedLatency() const {
        const ui64 cc = GetCycleCountFast() - StartCounter;
        return CyclesToDuration(cc);
    }

    using IActor::PassAway;
};

class TFlowControlledRequestQueue : public IActorCallback {
    const TActorId Target;
    const TFlowControlledQueueConfig Config;

    TDeque<THolder<IEventHandle>> UnhandledRequests;
    TDeque<TFlowControlledRequestActor *> RegisteredRequests;

    bool Subscribed = false;

    TDuration MinimalSeenLatency;

    bool CanRegister() {
        const ui64 inFly = RegisteredRequests.size();
        if (inFly <= Config.MinAllowedInFly) // <= for handling minAllowed == 0
            return true;

        if (inFly >= Config.MaxAllowedInFly)
            return false;

        if (Config.TargetDynamicRate) {
            if (const ui64 dynMax = MinimalSeenLatency.MicroSeconds() * Config.TargetDynamicRate / 1000000) {
                if (inFly >= dynMax)
                    return false;
            }
        }

        const TDuration currentLatency = RegisteredRequests.front()->AccumulatedLatency();
        if (currentLatency <= Config.MinTrackedLatency)
            return true;

        if (currentLatency <= MinimalSeenLatency * Config.LatencyFactor)
            return true;

        return false;
    }

    void HandleForwardedEvent(TAutoPtr<IEventHandle> &ev) {
        if (CanRegister()) {
            RegisterReqActor(ev);
        } else {
            UnhandledRequests.emplace_back(ev.Release());
        }
    }

    void RegisterReqActor(THolder<IEventHandle> ev) {
        TFlowControlledRequestActor *reqActor = new TFlowControlledRequestActor(ActivityType, this, ev->Sender, ev->Cookie, ev->Flags);
        const TActorId reqActorId = RegisterWithSameMailbox(reqActor);
        RegisteredRequests.emplace_back(reqActor);

        if (!Subscribed && (Target.NodeId() != SelfId().NodeId())) {
            Send(TActivationContext::InterconnectProxy(Target.NodeId()), new TEvents::TEvSubscribe(), IEventHandle::FlagTrackDelivery);
            Subscribed = true;
        }

        TActivationContext::Send(new IEventHandle(Target, reqActorId, ev.Get()->ReleaseBase().Release(), IEventHandle::FlagTrackDelivery, ev->Cookie));
    }

    void PumpQueue() {
        while (RegisteredRequests && RegisteredRequests.front() == nullptr)
            RegisteredRequests.pop_front();

        while (UnhandledRequests && CanRegister()) {
            RegisterReqActor(std::move(UnhandledRequests.front()));
            UnhandledRequests.pop_front();
        }
    }

    void HandleDisconnected() {
        Subscribed = false;

        const ui32 nodeid = Target.NodeId();
        for (TFlowControlledRequestActor *reqActor : RegisteredRequests) {
            if (reqActor) {
                if (reqActor->Flags & IEventHandle::FlagSubscribeOnSession) {
                    TActivationContext::Send(
                        new IEventHandle(reqActor->Source, TActorId(), new TEvInterconnect::TEvNodeDisconnected(nodeid), 0, reqActor->Cookie)
                    );
                }
                reqActor->PassAway();
            }
        }

        RegisteredRequests.clear();

        for (auto &ev : UnhandledRequests) {
            const auto reason = TEvents::TEvUndelivered::Disconnected;
            if (ev->Flags & IEventHandle::FlagTrackDelivery) {
                TActivationContext::Send(
                    new IEventHandle(ev->Sender, ev->Recipient, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), reason), 0, ev->Cookie)
                );
            }
        }

        UnhandledRequests.clear();
    }

    void HandlePoison() {
        HandleDisconnected();

        if (SelfId().NodeId() != Target.NodeId())
            Send(TActivationContext::InterconnectProxy(Target.NodeId()), new TEvents::TEvUnsubscribe());

        PassAway();
    }
public:
    template <class TEnum>
    TFlowControlledRequestQueue(TActorId target, const TEnum activity, const TFlowControlledQueueConfig &config)
        : IActorCallback(static_cast<TReceiveFunc>(&TFlowControlledRequestQueue::StateWork), activity)
        , Target(target)
        , Config(config)
        , MinimalSeenLatency(TDuration::Seconds(1))
    {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleDisconnected);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            cFunc(TEvents::TEvUndelivered::EventType, HandleDisconnected);
            cFunc(TEvents::TEvPoison::EventType, HandlePoison);
        default:
            HandleForwardedEvent(ev);
        }
    }

    void HandleRequestReply(TAutoPtr<IEventHandle> &ev, TFlowControlledRequestActor *reqActor) {
        auto it = Find(RegisteredRequests, reqActor);
        if (it == RegisteredRequests.end())
            return;
        TActivationContext::Send(ev->Forward(reqActor->Source).Release());
        const TDuration reqLatency = reqActor->AccumulatedLatency();
        if (reqLatency < MinimalSeenLatency)
            MinimalSeenLatency = reqLatency;

        *it = nullptr;
        PumpQueue();
    }

    void HandleRequestUndelivered(TEvents::TEvUndelivered::TPtr &ev, TFlowControlledRequestActor *reqActor) {
        auto it = Find(RegisteredRequests, reqActor);
        if (it == RegisteredRequests.end())
            return;

        TActivationContext::Send(ev->Forward(reqActor->Source).Release());

        *it = nullptr;
        PumpQueue();
    }
};

void TFlowControlledRequestActor::HandleReply(TAutoPtr<IEventHandle> &ev) {
    QueueActor->HandleRequestReply(ev, this);
    PassAway();
}

void TFlowControlledRequestActor::HandleUndelivered(TEvents::TEvUndelivered::TPtr &ev) {
    QueueActor->HandleRequestUndelivered(ev, this);
    PassAway();
}

template <class TEnum>
IActor* CreateFlowControlledRequestQueue(TActorId targetId, const TEnum activity, const TFlowControlledQueueConfig &config) {
    return new TFlowControlledRequestQueue(targetId, activity, config);
}

}
